-- GDrive SQLite State Store Schema
-- Conforms to v1.md and AGENTS.md requirements:
-- - Bidirectional sync baseline with SQLite as single source of truth
-- - Only SHA-256 for content verification and sync decision
-- - Scanner FileIdentity (device + inode) and mtime/ctime/size cache for fast local change detection
-- - Parent-child tree structure (parent_id + name) instead of full path blobs
-- - B/L/R (Baseline, Local, Remote) three-party observations with dirty generation tracking
-- - Operation intent durability before remote requests

PRAGMA foreign_keys = ON;

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
    -- local_mtime/local_ctime: nanoseconds since epoch (sec * 1_000_000_000 + nsec)
    local_mtime INTEGER,
    local_ctime INTEGER,
    local_size INTEGER CHECK (local_size IS NULL OR local_size >= 0),

    -- Baseline (B): Agreed synchronized state
    base_sha256 TEXT CHECK (base_sha256 IS NULL OR length(base_sha256) = 64),
    base_size INTEGER CHECK (base_size IS NULL OR base_size >= 0),
    base_version INTEGER CHECK (base_version IS NULL OR base_version >= 0),

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
    remote_scope_excluded INTEGER NOT NULL DEFAULT 0 CHECK (remote_scope_excluded IN (0, 1)),
    remote_status TEXT NOT NULL DEFAULT 'unknown' CHECK (remote_status IN ('present', 'trashed', 'absent', 'unknown')),

    -- Lifecycle & Scheduling Phase (§10.1)
    phase TEXT NOT NULL DEFAULT 'discovered' CHECK (phase IN (
        'discovered', 'waitingEvidence',
        'ready', 'inFlight', 'verify', 'unknownOutcome',
        'committed', 'blocked'
    )),
    dirty_generation INTEGER NOT NULL DEFAULT 0 CHECK (dirty_generation >= 0),
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,

    CHECK (parent_id IS NOT NULL OR entry_kind = 'directory')
);

-- Unique name under the same parent
CREATE UNIQUE INDEX IF NOT EXISTS idx_items_parent_name
    ON items(root_id, parent_id, name)
    WHERE parent_id IS NOT NULL;

-- Single root entry per synchronized pair
CREATE UNIQUE INDEX IF NOT EXISTS idx_items_root_entry
    ON items(root_id)
    WHERE parent_id IS NULL;

-- Fast lookup by remote Drive fileId
CREATE INDEX IF NOT EXISTS idx_items_remote_file_id
    ON items(root_id, remote_file_id)
    WHERE remote_file_id IS NOT NULL;

-- Fast lookup by local inode identity during scanner walk (§6.2)
CREATE INDEX IF NOT EXISTS idx_items_local_identity
    ON items(root_id, local_device, local_inode)
    WHERE local_inode IS NOT NULL;

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
        'trashRemote', 'deleteLocal'
    )),
    state TEXT NOT NULL CHECK (state IN (
        'ready', 'inFlight', 'verify', 'unknownOutcome',
        'completed', 'failed'
    )),
    expected_local_generation INTEGER NOT NULL DEFAULT 0 CHECK (expected_local_generation >= 0),
    expected_sha256 TEXT CHECK (expected_sha256 IS NULL OR length(expected_sha256) = 64),
    target_remote_id TEXT,
    target_parent_remote_id TEXT,
    session_uri TEXT,
    confirmed_offset INTEGER NOT NULL DEFAULT 0 CHECK (confirmed_offset >= 0),
    total_bytes INTEGER CHECK (total_bytes IS NULL OR total_bytes >= 0),
    payload TEXT,
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_operations_cleanup_pending_item
    ON operations(item_id)
    WHERE operation_type IN ('trashRemote', 'deleteLocal')
        AND state IN ('ready', 'inFlight', 'verify', 'unknownOutcome');

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

-- A13: a cursor acknowledges durable observations, never discarded events.
CREATE TABLE IF NOT EXISTS remote_change_inbox (
    root_id INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,
    remote_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    scan_id TEXT,
    attempted_at REAL NOT NULL DEFAULT 0,
    PRIMARY KEY(root_id, remote_id)
);
CREATE INDEX IF NOT EXISTS idx_remote_inbox_retry ON remote_change_inbox(root_id, attempted_at);
CREATE TABLE IF NOT EXISTS remote_directory_scans (
    root_id INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,
    remote_id TEXT NOT NULL,
    scan_id TEXT NOT NULL,
    page_token TEXT,
    state TEXT NOT NULL CHECK(state IN ('pending', 'complete')),
    PRIMARY KEY(root_id, remote_id)
);
CREATE INDEX IF NOT EXISTS idx_remote_scans_pending ON remote_directory_scans(root_id) WHERE state = 'pending';

-- Remote files that could not be published because the destination was
-- occupied by different local content during bootstrap or incremental sync.
CREATE TABLE IF NOT EXISTS sync_conflicts (
    conflict_id TEXT PRIMARY KEY NOT NULL,
    root_id INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,
    item_id INTEGER NOT NULL UNIQUE REFERENCES items(item_id) ON DELETE CASCADE,
    remote_file_id TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    local_path TEXT NOT NULL,
    conflict_path TEXT,
    remote_sha256 TEXT NOT NULL CHECK(length(remote_sha256) = 64),
    remote_size INTEGER NOT NULL CHECK(remote_size >= 0),
    remote_version INTEGER,
    remote_status TEXT NOT NULL CHECK(remote_status IN ('present', 'trashed', 'removed')),
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(root_id, remote_file_id)
);
CREATE INDEX IF NOT EXISTS idx_sync_conflicts_root ON sync_conflicts(root_id);

-- Actionable item-level failures retained until the whole sync root converges.
CREATE TABLE IF NOT EXISTS sync_issues (
    issue_id TEXT PRIMARY KEY NOT NULL,
    root_id INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,
    subject_key TEXT NOT NULL,
    item_id INTEGER,
    remote_file_id TEXT,
    relative_path TEXT NOT NULL,
    stage TEXT NOT NULL CHECK(stage IN (
        'localScan', 'createDirectory', 'upload', 'download',
        'pathUpdate', 'delete', 'conflictRefresh'
    )),
    category TEXT NOT NULL CHECK(category IN (
        'network', 'rateLimited', 'permissionDenied', 'remoteMissing',
        'remoteConflict', 'integrityMismatch', 'invalidResponse',
        'safetyBlocked', 'localChanged', 'localIO', 'unsupportedFilesystem',
        'staleState', 'unknown'
    )),
    suggested_action TEXT NOT NULL CHECK(suggested_action IN (
        'retry', 'retryLater', 'checkPermissions', 'inspectLocalFile',
        'renameRemote', 'resolveManually'
    )),
    message TEXT NOT NULL,
    retry_at REAL,
    first_seen_at REAL NOT NULL,
    last_seen_at REAL NOT NULL,
    occurrence_count INTEGER NOT NULL DEFAULT 1 CHECK(occurrence_count >= 1),
    UNIQUE(root_id, subject_key, stage)
);
CREATE INDEX IF NOT EXISTS idx_sync_issues_root_seen
    ON sync_issues(root_id, last_seen_at DESC, issue_id);

-- Local files whose content differed from the SQLite baseline when an enclosing
-- directory was trashed because the remote directory had been deleted.
-- These rows intentionally do not reference roots/items: cleanup removes those rows.
CREATE TABLE IF NOT EXISTS trashed_local_changes (
    change_id TEXT PRIMARY KEY NOT NULL,
    batch_id TEXT NOT NULL,
    local_root_path TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    original_path TEXT NOT NULL,
    trash_path TEXT,
    baseline_sha256 TEXT NOT NULL CHECK(length(baseline_sha256) = 64),
    observed_sha256 TEXT NOT NULL CHECK(length(observed_sha256) = 64),
    state TEXT NOT NULL CHECK(state IN ('pending', 'committed')),
    trashed_at REAL NOT NULL,
    UNIQUE(batch_id, relative_path)
);
CREATE INDEX IF NOT EXISTS idx_trashed_local_changes_root
    ON trashed_local_changes(local_root_path, trashed_at);


-- Nonunique to retain distinct item identities when local names are equivalent.
CREATE INDEX IF NOT EXISTS idx_items_local_name_key
ON items(root_id, parent_id, gdrive_name_key(name));

-- Scope protection belongs to the item; exclude roots and their descendants at query time.
CREATE INDEX IF NOT EXISTS idx_items_scope_excluded
    ON items(root_id) WHERE remote_scope_excluded = 1;
