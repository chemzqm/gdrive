-- GDrive SQLite State Store Schema
-- Conforms to v1.md and AGENTS.md requirements:
-- - Bidirectional sync baseline with SQLite as single source of truth
-- - Only SHA-256 for content verification and sync decision
-- - Scanner FileIdentity (device + inode) and mtime/size cache for fast local change detection
-- - Parent-child tree structure (parent_id + name) instead of full path blobs
-- - B/L/R (Baseline, Local, Remote) three-party observations with dirty generation tracking
-- - Operation intent durability before remote requests

PRAGMA foreign_keys = ON;

-- -----------------------------------------------------------------------------
-- Metadata / Schema Versioning
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS store_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

-- -----------------------------------------------------------------------------
-- Roots: Synchronized Directory Pairs
-- Represents binding between a local directory and a Google Drive folder.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS roots (
    root_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    local_root_path TEXT NOT NULL UNIQUE,
    local_root_device INTEGER NOT NULL,
    local_root_inode INTEGER NOT NULL,
    remote_root_id TEXT NOT NULL,
    initial_sync_direction TEXT NOT NULL CHECK (initial_sync_direction IN ('localToRemoteEmpty', 'remoteToLocalEmpty')),
    binding_generation INTEGER NOT NULL DEFAULT 1 CHECK (binding_generation >= 1),
    filter_version INTEGER NOT NULL DEFAULT 1 CHECK (filter_version >= 1),
    bootstrap_state TEXT NOT NULL CHECK (bootstrap_state IN ('freshCreated', 'existingKnown', 'unknownOrLost')),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (account_id, remote_root_id)
);

-- -----------------------------------------------------------------------------
-- Items: File & Directory State Baseline and Observations
-- Uses parent_id + name hierarchy; paths are reconstructed on-demand from parent edges.
-- Stores Baseline (B), Local Observation (L), and Remote Observation (R).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS items (
    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    root_id INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,
    parent_id INTEGER REFERENCES items(item_id) ON DELETE RESTRICT,
    name TEXT NOT NULL CHECK (length(name) > 0 AND instr(name, '/') = 0),
    entry_kind TEXT NOT NULL CHECK (entry_kind IN ('file', 'directory')),

    -- Remote identity in Google Drive (folder ID or file ID)
    remote_file_id TEXT,

    -- Local filesystem identity from DirectoryScanner (dev_t + ino_t)
    local_device INTEGER,
    local_inode INTEGER,

    -- Local metadata cache for fast change detection (§6.2)
    -- local_mtime: nanoseconds since epoch (sec * 1_000_000_000 + nsec)
    local_mtime INTEGER,
    local_size INTEGER CHECK (local_size IS NULL OR local_size >= 0),

    -- Baseline (B): Agreed synchronized state
    base_sha256 TEXT CHECK (base_sha256 IS NULL OR length(base_sha256) = 64),
    base_size INTEGER CHECK (base_size IS NULL OR base_size >= 0),
    base_version INTEGER CHECK (base_version IS NULL OR base_version >= 0),
    base_parent_id INTEGER REFERENCES items(item_id),
    base_name TEXT,

    -- Local Observation (L)
    local_sha256 TEXT CHECK (local_sha256 IS NULL OR length(local_sha256) = 64),
    local_generation INTEGER NOT NULL DEFAULT 0 CHECK (local_generation >= 0),
    local_status TEXT NOT NULL DEFAULT 'unknown' CHECK (local_status IN ('present', 'absent', 'unstable', 'unknown')),

    -- Remote Observation (R)
    remote_sha256 TEXT CHECK (remote_sha256 IS NULL OR length(remote_sha256) = 64),
    remote_size INTEGER CHECK (remote_size IS NULL OR remote_size >= 0),
    remote_version INTEGER CHECK (remote_version IS NULL OR remote_version >= 0),
    remote_parent_file_id TEXT,
    remote_name TEXT,
    remote_generation INTEGER NOT NULL DEFAULT 0 CHECK (remote_generation >= 0),
    remote_status TEXT NOT NULL DEFAULT 'unknown' CHECK (remote_status IN ('present', 'trashed', 'absent', 'unknown')),

    -- Lifecycle & Scheduling Phase (§10.1)
    phase TEXT NOT NULL DEFAULT 'discovered' CHECK (phase IN (
        'discovered', 'waitingEvidence', 'waitingParent', 'waitingInput',
        'ready', 'inFlight', 'verify', 'unknownOutcome',
        'committed', 'conflict', 'blocked'
    )),
    dirty_generation INTEGER NOT NULL DEFAULT 0 CHECK (dirty_generation >= 0),
    is_tombstone INTEGER NOT NULL DEFAULT 0 CHECK (is_tombstone IN (0, 1)),
    tombstone_generation INTEGER CHECK (tombstone_generation IS NULL OR tombstone_generation >= 0),

    -- Conflict resolution tracking (§11.3)
    conflict_id TEXT,
    conflict_winner TEXT CHECK (conflict_winner IS NULL OR conflict_winner IN ('local', 'remote')),

    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,

    CHECK (parent_id IS NOT NULL OR entry_kind = 'directory')
);

-- Unique non-tombstone name under the same parent
CREATE UNIQUE INDEX IF NOT EXISTS idx_items_parent_name
    ON items(root_id, parent_id, name)
    WHERE is_tombstone = 0 AND parent_id IS NOT NULL;

-- Single root entry per synchronized pair
CREATE UNIQUE INDEX IF NOT EXISTS idx_items_root_entry
    ON items(root_id)
    WHERE parent_id IS NULL AND is_tombstone = 0;

-- Fast lookup by remote Drive fileId
CREATE INDEX IF NOT EXISTS idx_items_remote_file_id
    ON items(root_id, remote_file_id)
    WHERE remote_file_id IS NOT NULL AND is_tombstone = 0;

-- Fast lookup by local inode identity during scanner walk (§6.2)
CREATE INDEX IF NOT EXISTS idx_items_local_identity
    ON items(root_id, local_device, local_inode)
    WHERE local_inode IS NOT NULL AND is_tombstone = 0;

-- Dirty items query for scheduler/reconciler
CREATE INDEX IF NOT EXISTS idx_items_dirty
    ON items(root_id, dirty_generation)
    WHERE dirty_generation > 0;

-- Scheduling phase query
CREATE INDEX IF NOT EXISTS idx_items_phase
    ON items(root_id, phase);

-- Child dependency query when parent directory becomes confirmed/ready (§4.3)
CREATE INDEX IF NOT EXISTS idx_items_parent_id
    ON items(root_id, parent_id);

-- -----------------------------------------------------------------------------
-- Operations: In-Flight and Durable Intent Records
-- Intent must be committed before network requests are dispatched (§4.1, §5).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS operations (
    operation_id TEXT PRIMARY KEY NOT NULL,
    root_id INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,
    item_id INTEGER NOT NULL REFERENCES items(item_id) ON DELETE CASCADE,
    operation_type TEXT NOT NULL CHECK (operation_type IN (
        'createDirectory', 'uploadMultipart', 'uploadResumable',
        'download', 'move', 'rename', 'trashRemote', 'deleteLocal'
    )),
    state TEXT NOT NULL CHECK (state IN (
        'ready', 'inFlight', 'verify', 'unknownOutcome',
        'completed', 'failed', 'cancelled'
    )),
    expected_local_generation INTEGER NOT NULL DEFAULT 0 CHECK (expected_local_generation >= 0),
    expected_remote_version INTEGER CHECK (expected_remote_version IS NULL OR expected_remote_version >= 0),
    expected_sha256 TEXT CHECK (expected_sha256 IS NULL OR length(expected_sha256) = 64),
    target_remote_id TEXT,
    target_parent_remote_id TEXT,
    session_uri TEXT,
    confirmed_offset INTEGER NOT NULL DEFAULT 0 CHECK (confirmed_offset >= 0),
    total_bytes INTEGER CHECK (total_bytes IS NULL OR total_bytes >= 0),
    staging_path TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error_code TEXT,
    last_error_message TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operations_item
    ON operations(item_id);

CREATE INDEX IF NOT EXISTS idx_operations_root_state
    ON operations(root_id, state);

CREATE INDEX IF NOT EXISTS idx_operations_target_remote
    ON operations(target_remote_id)
    WHERE target_remote_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Directory Observations: Remote/Local Directory Scan Evidence
-- Tracks pagination tokens, completeness, and proof of emptiness/enumeration (§9.3, §10.3).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS directory_observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    root_id INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,
    item_id INTEGER REFERENCES items(item_id) ON DELETE CASCADE,
    remote_folder_id TEXT NOT NULL,
    local_scan_generation INTEGER NOT NULL DEFAULT 0 CHECK (local_scan_generation >= 0),
    remote_scan_generation INTEGER NOT NULL DEFAULT 0 CHECK (remote_scan_generation >= 0),
    next_page_token TEXT,
    listing_status TEXT NOT NULL CHECK (listing_status IN (
        'unscanned', 'inProgress', 'complete', 'unknownOrIncomplete'
    )),
    child_count INTEGER NOT NULL DEFAULT 0 CHECK (child_count >= 0),
    evidence_status TEXT NOT NULL DEFAULT 'none' CHECK (evidence_status IN (
        'none', 'emptyConfirmed', 'childrenEnumerated', 'pageIncomplete', 'accessDenied', 'notFound'
    )),
    evidence_summary TEXT,
    updated_at REAL NOT NULL,
    UNIQUE (root_id, remote_folder_id)
);

CREATE INDEX IF NOT EXISTS idx_dir_obs_item
    ON directory_observations(item_id);

CREATE INDEX IF NOT EXISTS idx_dir_obs_status
    ON directory_observations(root_id, listing_status);

-- -----------------------------------------------------------------------------
-- Cursors: Change Feed & Event Cursors
-- Tracks Google Drive Changes token (C0...) and local FSEvents stream IDs (§3.3, §9.4, §10.3).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cursors (
    cursor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    root_id INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    cursor_kind TEXT NOT NULL CHECK (cursor_kind IN ('drive_changes', 'local_fsevents')),
    token_value TEXT NOT NULL,
    is_valid INTEGER NOT NULL DEFAULT 1 CHECK (is_valid IN (0, 1)),
    last_event_at REAL,
    updated_at REAL NOT NULL,
    UNIQUE (root_id, cursor_kind)
);

-- -----------------------------------------------------------------------------
-- Cleanup Queue: Staging Files and Transient Publication Cleanup
-- Recycles staging files and canceled upload sessions after recovery check (§9.6, §10.5).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cleanup_queue (
    cleanup_id TEXT PRIMARY KEY NOT NULL,
    root_id INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('stagingFile', 'publicationFile', 'resumableSession')),
    resource_locator TEXT NOT NULL CHECK (length(resource_locator) > 0),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'cleaning', 'completed', 'failed')),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cleanup_due
    ON cleanup_queue(state, next_attempt_at)
    WHERE state = 'pending';
