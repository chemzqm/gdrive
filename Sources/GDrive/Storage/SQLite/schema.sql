-- GDrive SQLite state schema: one current row per path; bidirectional sync baseline.
PRAGMA foreign_keys = ON;

CREATE TABLE store_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    storage_schema INTEGER NOT NULL,
    commit_sequence INTEGER NOT NULL CHECK (typeof(commit_sequence) = 'integer' AND commit_sequence >= 0),
    workspace_id TEXT NOT NULL,
    session_key_reference BLOB CHECK (session_key_reference IS NULL OR typeof(session_key_reference) = 'blob')
);

CREATE TABLE file_state (
    file_id INTEGER PRIMARY KEY, -- Internal lookup only; path uniqueness is below.
    job_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    path_key BLOB NOT NULL CHECK (typeof(path_key) = 'blob' AND length(path_key) > 1 AND substr(path_key, 1, 1) <> x'00' AND
        instr(path_key, x'0000') = 0 AND substr(path_key, -1) = x'00'),
    -- NUL-terminated canonical UTF-8 components, never a hash; root parent = x''.
    parent_path_key BLOB NOT NULL CHECK (typeof(parent_path_key) = 'blob'),
    relative_components BLOB NOT NULL,
    entry_kind TEXT NOT NULL CHECK (entry_kind IN ('file', 'directory')),
    phase TEXT NOT NULL CHECK (phase IN (
        'idle', 'preparing', 'transferring', 'verifying', 'conflict',
        'needsReconciliation', 'paused', 'canceled', 'failed'
    )),
    run_id TEXT,
    plan_id TEXT,
    operation_id TEXT,
    transfer_id TEXT,
    direction TEXT CHECK (direction IN ('upload', 'download')),
    attempt_epoch INTEGER NOT NULL DEFAULT 0 CHECK (attempt_epoch >= 0),
    confirmed_offset INTEGER NOT NULL DEFAULT 0 CHECK (typeof(confirmed_offset) = 'integer' AND confirmed_offset >= 0),
    transfer_size INTEGER CHECK (transfer_size IS NULL OR (typeof(transfer_size) = 'integer' AND transfer_size >= 0)),
    -- Source/target identities, checksums, source version and current intent.
    evidence BLOB NOT NULL CHECK (typeof(evidence) = 'blob' AND length(evidence) <= 2048),
    remote_object_id TEXT CHECK (remote_object_id IS NULL OR (typeof(remote_object_id) = 'text' AND length(remote_object_id) > 0)),
    -- Sealed snapshot/download partial metadata and authenticated session ciphertext, never plaintext URI.
    transfer_context BLOB CHECK (transfer_context IS NULL OR
        (typeof(transfer_context) = 'blob' AND length(transfer_context) <= 16384)),
    -- Explicit conflict choice for this attempt; never a persistent force mode.
    resolution TEXT CHECK (resolution IN ('localToRemote', 'remoteToLocal', 'merge')),
    resolution_token TEXT,
    merge_target TEXT CHECK (merge_target IN ('local', 'remote', 'both')),
    queue_session_id TEXT,
    queue_state TEXT NOT NULL DEFAULT 'none' CHECK (queue_state IN ('none', 'checking', 'queued', 'running', 'stopping', 'reconciling', 'blocked')),
    desired_revision INTEGER NOT NULL DEFAULT 0 CHECK (desired_revision >= 0),
    desired_action TEXT NOT NULL DEFAULT 'none' CHECK (desired_action IN ('none', 'recheck', 'upsert', 'absent')),
    active_revision INTEGER NOT NULL DEFAULT 0 CHECK (active_revision >= 0),
    payload_schema INTEGER NOT NULL CHECK (typeof(payload_schema) = 'integer' AND payload_schema > 0),
    row_version INTEGER NOT NULL CHECK (typeof(row_version) = 'integer' AND row_version >= 1),
    updated_at REAL NOT NULL,
    UNIQUE (job_id, binding_id, path_key),
    CHECK (length(parent_path_key) = 0 OR substr(parent_path_key, -1) = x'00'),
    CHECK (length(path_key) > length(parent_path_key) + 1 AND
           substr(path_key, 1, length(parent_path_key)) = parent_path_key AND
           instr(substr(path_key, length(parent_path_key) + 1), x'00') = length(path_key) - length(parent_path_key)),
    CHECK ((transfer_size IS NULL AND confirmed_offset = 0) OR
           (transfer_size IS NOT NULL AND confirmed_offset <= transfer_size)),
    CHECK ((resolution IS NULL) = (resolution_token IS NULL)),
    CHECK ((resolution IS 'merge') = (merge_target IS NOT NULL)),
    CHECK (resolution IS NOT 'localToRemote' OR direction IS 'upload'),
    CHECK (resolution IS NOT 'remoteToLocal' OR direction IS 'download'),
    CHECK (transfer_id IS NULL OR
           (entry_kind = 'file' AND direction IS NOT NULL AND attempt_epoch > 0)),
    CHECK (phase <> 'transferring' OR
           (transfer_id IS NOT NULL AND transfer_context IS NOT NULL AND transfer_size IS NOT NULL)),
    -- Idle rows retain no active operation, selected conflict or old transfer evidence.
    CHECK (phase <> 'idle' OR
           (run_id IS NULL AND plan_id IS NULL AND operation_id IS NULL AND
            transfer_id IS NULL AND direction IS NULL AND confirmed_offset = 0 AND
            transfer_size IS NULL AND transfer_context IS NULL AND resolution IS NULL)),
    CHECK (entry_kind <> 'directory' OR
           (transfer_id IS NULL AND transfer_context IS NULL AND confirmed_offset = 0 AND
            transfer_size IS NULL AND resolution IS NOT 'merge')),
    CHECK (desired_revision >= active_revision)
);

CREATE UNIQUE INDEX file_active_transfer ON file_state(transfer_id) WHERE transfer_id IS NOT NULL;
CREATE UNIQUE INDEX file_remote_object ON file_state(job_id, binding_id, remote_object_id) WHERE remote_object_id IS NOT NULL;
CREATE INDEX file_job_phase ON file_state(job_id, binding_id, phase);
CREATE INDEX file_children ON file_state(job_id, binding_id, parent_path_key);
CREATE INDEX file_run ON file_state(run_id, operation_id) WHERE run_id IS NOT NULL;
CREATE INDEX file_plan ON file_state(plan_id, operation_id) WHERE plan_id IS NOT NULL;
CREATE INDEX file_queue_session ON file_state(queue_session_id, queue_state) WHERE queue_session_id IS NOT NULL;

-- Task records for workspace/job/run-level objects.
CREATE TABLE task_records (
    record_key TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN (
        'registry', 'plan', 'permit', 'run', 'request', 'policy',
        'initialization', 'directory-transaction', 'change-hint',
        'dynamic-upload-session', 'mirror-index'
    )),
    job_id TEXT,
    plan_id TEXT,
    is_pending INTEGER NOT NULL CHECK (is_pending IN (0, 1)),
    recorded_at REAL,
    row_version INTEGER NOT NULL CHECK (typeof(row_version) = 'integer' AND row_version >= 1),
    payload_schema INTEGER NOT NULL CHECK (typeof(payload_schema) = 'integer' AND payload_schema > 0),
    payload BLOB NOT NULL,
    CHECK (kind <> 'mirror-index' OR (typeof(payload) = 'blob' AND length(payload) <= 4096 AND is_pending = 0 AND plan_id IS NULL AND job_id IS NOT NULL))
);
CREATE INDEX task_job ON task_records(job_id, kind, record_key);
CREATE INDEX task_pending ON task_records(job_id, kind, record_key) WHERE is_pending = 1;
CREATE INDEX task_plan ON task_records(plan_id, kind, record_key) WHERE plan_id IS NOT NULL;

-- Per-operation run progress
CREATE TABLE plan_operation_state (
    plan_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('completed')),
    row_version INTEGER NOT NULL CHECK (typeof(row_version) = 'integer' AND row_version >= 1),
    updated_at REAL NOT NULL,
    PRIMARY KEY (plan_id, operation_id)
);
CREATE INDEX plan_operation_job ON plan_operation_state(job_id, plan_id);

-- Cleanup queue for transient/publication files
CREATE TABLE cleanup_queue (
    cleanup_id TEXT PRIMARY KEY NOT NULL,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('temporaryFile', 'publicationFile')),
    resource_locator TEXT NOT NULL CHECK (length(resource_locator) > 0),
    retired_commit_sequence INTEGER NOT NULL CHECK (typeof(retired_commit_sequence) = 'integer' AND retired_commit_sequence >= 0),
    owner_job_id TEXT NOT NULL,
    owner_binding_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'blocked')),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (typeof(retry_count) = 'integer' AND retry_count >= 0),
    last_attempt_at REAL,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_error_code TEXT CHECK (last_error_code IS NULL OR (
        typeof(last_error_code) = 'text' AND length(last_error_code) <= 64 AND
        last_error_code IN ('io', 'permission', 'credentialLocked', 'identityMismatch',
                           'invalidPath', 'diskFull', 'unsupported', 'unknown')
    )),
    UNIQUE (resource_kind, resource_locator)
);
CREATE INDEX cleanup_owner ON cleanup_queue(owner_job_id, owner_binding_id);
CREATE INDEX cleanup_due ON cleanup_queue(next_attempt_at, cleanup_id) WHERE state = 'pending';
