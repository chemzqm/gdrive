import Darwin
import Foundation
import CommonCrypto
import Logging
import DirectoryScanner
import os

/// Synchronization progress statistics indicators
public struct SyncStats: Sendable {
    public var filesScanned: Int = 0
    public var filesSkipped: Int = 0
    public var directoriesCreated: Int = 0
    public var filesUploaded: Int = 0
    public var bytesUploaded: Int64 = 0
    public var filesDownloaded: Int = 0
    public var bytesDownloaded: Int64 = 0
    public var filesDeleted: Int = 0
    public var conflictsResolved: Int = 0
    /// Durable remote observations/pages still awaiting a later reconcile round.
    public var remoteWorkPending: Int = 0
    /// Remote identities blocked by equivalent local names; rename remotely to resolve.
    public var remoteNameConflicts: Int = 0
    public var filesFailed: Int = 0
    public var elapsedSeconds: Double = 0
}

/// SyncEngine Exception type definition
public enum SyncEngineError: Error, LocalizedError, CustomStringConvertible, Sendable, Equatable {
    case localRootNotFound(path: String)
    case rootBusy(path: String)
    case remoteRootLost(remoteId: String, reason: String)
    case rootNotConfigured(remoteId: String)
    case invalidDirectory(path: String)
    case general(String)

    public var errorDescription: String? {
        description
    }

    public var description: String {
        switch self {
        case .localRootNotFound(let path):
            return "The local sync root no longer exists or is not a valid directory: \(path). Sync stopped to prevent remote deletion propagation."
        case .rootBusy(let path):
            return "The local sync root is already being synchronized in this process: \(path)"
        case .remoteRootLost(let remoteId, let reason):
            return "The remote sync root was removed or trashed (\(reason)): \(remoteId). Sync stopped to prevent local deletion propagation."
        case .rootNotConfigured(let remoteId):
            return "No matching sync root was found. Run initial sync first: \(remoteId)"
        case .invalidDirectory(let path):
            return "Path is not a valid directory: \(path)"
        case .general(let msg):
            return msg
        }
    }
}

/// GDrive core sync engine
/// follow v1.md with AGENTS.md Specifications:
/// - Only supports local directory to remote empty directory synchronization (localToRemoteEmpty) Synchronize the remote directory to the local empty directory (remoteToLocalEmpty)
/// - to SQLite is the only synchronization baseline to file SHA-256 as the only basis for judgment
/// - Extremely fast streaming pipeline that scans, creates directories, and uploads while uploading. The first file is sent out with zero delay.
/// - greater than 8MB Files are broken into chunks and resumed at breakpoints.≤ 8MB File Multipart Upload in one step
/// - pre-allocated ID memory pool,0 Network delay ID
public final class SyncEngine: Sendable {
    public let auth: Auth
    public let store: StateStore
    public let client: DriveClient
    public let idPool: IDPool
    /// Downloads stage under this directory's remote-root-ID subdirectory.
    /// Keep it outside all sync roots and on the destination filesystem.
    public var downloadTemporaryDirectory: URL { downloadTemporaryDirectoryStorage.withLock { $0 } }
    private let downloadTemporaryDirectoryStorage: OSAllocatedUnfairLock<URL>

    /// Applies to subsequent sync runs. Existing runs keep their selected directory.
    public func setDownloadTemporaryDirectory(_ directory: URL) throws {
        guard directory.isFileURL else { throw SyncEngineError.general("The temporary download directory must be a local file path") }
        downloadTemporaryDirectoryStorage.withLock { $0 = directory }
    }
    private let logger = Logger(label: "gdrive.engine")

    typealias IncrementalScan = @Sendable (ScanRequest, @escaping @Sendable (ScanBatch) async throws -> Void) async throws -> Void
    private let incrementalScan: IncrementalScan

    public convenience init(
        auth: Auth,
        store: StateStore? = nil,
        client: DriveClient? = nil,
        idPool: IDPool? = nil,
        downloadTemporaryDirectory: URL = DriveClient.defaultDownloadTemporaryDirectory
    ) async throws {
        try await self.init(auth: auth, store: store, client: client, idPool: idPool,
            downloadTemporaryDirectory: downloadTemporaryDirectory, incrementalScan: { request, consume in
            _ = try await DirectoryScanner().scan(request, consume: consume)
        })
    }

    init(auth: Auth, store: StateStore? = nil, client: DriveClient? = nil, idPool: IDPool? = nil,
         downloadTemporaryDirectory: URL = DriveClient.defaultDownloadTemporaryDirectory,
         incrementalScan: @escaping IncrementalScan) async throws {
        guard downloadTemporaryDirectory.isFileURL else {
            throw SyncEngineError.general("The temporary download directory must be a local file path")
        }
        self.downloadTemporaryDirectoryStorage = OSAllocatedUnfairLock(initialState: downloadTemporaryDirectory)
        self.incrementalScan = incrementalScan
        self.auth = auth
        let effectiveClient = client ?? DriveClient(auth: auth)
        self.client = effectiveClient
        if let store {
            self.store = store
        } else {
            self.store = try await StateStore()
        }
        self.idPool = idPool ?? IDPool(api: effectiveClient)
    }

    /// Real-time transfer and rate monitor
    public let monitor = TransferMonitor()

    /// Get a memory snapshot of what's currently being transferred, what's waiting in the queue, and the real-time sliding speed in bytes per second (per 500ms automatic refresh)
    public var transferStatus: TransferSnapshot {
        monitor.getSnapshot()
    }

    /// External direct call to obtain current transmission status snapshot
    public func getTransferStatus() -> TransferSnapshot {
        monitor.getSnapshot()
    }

    private func withRootSyncLock<T: Sendable>(
        localPath: String,
        operation: @Sendable () async throws -> T
    ) async throws -> T {
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        try await RootSyncCoordinator.shared.acquire(localRootPath: resolvedLocalPath)
        do {
            let result = try await operation()
            await RootSyncCoordinator.shared.release(localRootPath: resolvedLocalPath)
            return result
        } catch {
            await RootSyncCoordinator.shared.release(localRootPath: resolvedLocalPath)
            throw error
        }
    }

    // MARK: - Unified synchronization portal (Automatic status detection and direction diversion)

    /// Unified two-way synchronization entrance:
    /// - Automatically detect that the directory pair is in the local SQLite Whether there is already a synchronization baseline in
    /// - If there is a baseline: automatically perform extremely fast bidirectional incremental synchronization (syncIncremental)
    /// - If synchronizing for the first time: automatically initiate status detection to the cloud and local:
    ///   - There is content locally and the cloud is empty: execute automatically localToRemoteEmpty Full streaming upload
    ///   - There is content in the cloud but empty locally: execute automatically remoteToLocalEmpty Full streaming download
    ///   - Both ends are empty: Register an initial empty baseline
    ///   - Both ends are not empty: clear exception protection is thrown to prevent data overwriting caused by blind merge without baseline
    @discardableResult
    public func sync(
        localPath: String,
        remoteFolderId: String,
        concurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        try await withRootSyncLock(localPath: localPath) {
            try await self.syncUnlocked(
                localPath: localPath,
                remoteFolderId: remoteFolderId,
                concurrency: concurrency,
                onProgress: onProgress
            )
        }
    }

    private func syncUnlocked(
        localPath: String,
        remoteFolderId: String,
        concurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?
    ) async throws -> SyncStats {
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath

        // 1. Check SQLite Whether there is already an active synchronization root
        let existingRoot: (rootId: Int64, bootstrapState: String, initialDir: String)? = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT root_id, bootstrap_state, initial_sync_direction FROM roots
            WHERE local_root_path = ? AND remote_root_id = ? AND is_active = 1;
            """)
            stmt.bindText(resolvedLocalPath, at: 1)
            stmt.bindText(remoteFolderId, at: 2)
            defer { stmt.reset() }
            if try stmt.step() {
                return (
                    stmt.columnInt64(at: 0) ?? 0,
                    stmt.columnText(at: 1) ?? "freshCreated",
                    stmt.columnText(at: 2) ?? "localToRemoteEmpty"
                )
            }
            return nil
        }

        if let existing = existingRoot {
            if existing.bootstrapState == "existingKnown" {
                logger.info("[Sync] Found a shared baseline; starting incremental bidirectional sync: \(resolvedLocalPath) <-> \(remoteFolderId)")
                return try await syncIncrementalUnlocked(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxConcurrency: concurrency, onProgress: onProgress)
            } else {
                logger.info("[Sync] Found an unfinished initialization baseline (bootstrapState: \(existing.bootstrapState)); resuming initialization...")
                if existing.initialDir == "remoteToLocalEmpty" {
                    return try await initializeRemoteToLocalEmpty(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxDownloadConcurrency: concurrency, onProgress: onProgress, initialCursor: nil)
                } else {
                    return try await syncLocalToRemoteEmptyUnlocked(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxUploadConcurrency: concurrency, onProgress: onProgress)
                }
            }
        }

        // 2. First synchronization: automatically detect the true status of local and remote directories
        logger.info("[Sync] No baseline exists; checking local and remote directory state...")

        // Detect remote directories: verify existence, whether it is a directory, and whether it contains non-recycle bin subkeys
        let remoteFile = try await client.getFile(remoteId: remoteFolderId)
        guard remoteFile.trashed != true else {
            throw SyncEngineError.remoteRootLost(remoteId: remoteFolderId, reason: "trashed")
        }
        guard remoteFile.isDirectory else {
            throw NSError(domain: "SyncEngine", code: 101, userInfo: [NSLocalizedDescriptionKey: "The remote target is not a valid directory: \(remoteFolderId)"])
        }
        // Probe only the top level; hidden entries are included, only .git directories are pruned.
        let isLocalEmpty = try Self.isLocalRootEmpty(resolvedLocalPath)
        // Capture before listing so a concurrent remote creation cannot fall before the cursor.
        let emptyRootCursor = isLocalEmpty ? try await client.getStartPageToken() : nil
        let remoteChildren = try await client.listChildren(parentId: remoteFolderId)
        let isRemoteEmpty = remoteChildren.isEmpty

        // 3. Safe diversion based on detection results
        if !isLocalEmpty && isRemoteEmpty {
            logger.info("[Sync] Local content and an empty remote directory detected; starting localToRemoteEmpty initialization")
            return try await syncLocalToRemoteEmptyUnlocked(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxUploadConcurrency: concurrency, onProgress: onProgress)
        } else if isLocalEmpty && !isRemoteEmpty {
            logger.info("[Sync] Remote content and an empty local directory detected; starting remoteToLocalEmpty initialization")
            return try await initializeRemoteToLocalEmpty(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxDownloadConcurrency: concurrency, onProgress: onProgress, initialCursor: emptyRootCursor)
        } else if isLocalEmpty && isRemoteEmpty {
            logger.info("[Sync] Both directories are empty; creating an empty baseline")
            try FileManager.default.createDirectory(atPath: resolvedLocalPath, withIntermediateDirectories: true)
            var metadata = stat()
            guard stat(resolvedLocalPath, &metadata) == 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
            let device = Int64(metadata.st_dev)
            let inode = Int64(metadata.st_ino)
            let rootName = URL(fileURLWithPath: resolvedLocalPath).lastPathComponent
            let now = Date().timeIntervalSince1970
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                INSERT INTO roots (
                    account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
                ) VALUES ('default', ?, ?, ?, ?, 'localToRemoteEmpty', 'existingKnown', ?, ?);
                """)
                stmt.bindText(resolvedLocalPath, at: 1)
                stmt.bindInt64(device, at: 2)
                stmt.bindInt64(inode, at: 3)
                stmt.bindText(remoteFolderId, at: 4)
                stmt.bindDouble(now, at: 5)
                stmt.bindDouble(now, at: 6)
                _ = try stmt.step()
                stmt.reset()
                let rootID = conn.lastInsertRowId
                let item = try conn.cachedStatement("""
                INSERT INTO items(root_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status, phase, created_at, updated_at)
                VALUES (?, ?, 'directory', ?, ?, ?, 'present', 'present', 'committed', ?, ?);
                """)
                item.bindInt64(rootID, at: 1)
                item.bindText(rootName, at: 2)
                item.bindText(remoteFolderId, at: 3)
                item.bindInt64(device, at: 4)
                item.bindInt64(inode, at: 5)
                item.bindDouble(now, at: 6)
                item.bindDouble(now, at: 7)
                _ = try item.step()
                item.reset()
                let cursor = try conn.cachedStatement("""
                INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at)
                VALUES (?, 'default', 'drive_changes', ?, ?);
                """)
                cursor.bindInt64(rootID, at: 1)
                cursor.bindText(emptyRootCursor!, at: 2)
                cursor.bindDouble(now, at: 3)
                _ = try cursor.step()
                cursor.reset()
            }
            return SyncStats()
        } else {
            // Both ends are not empty
            throw NSError(domain: "SyncEngine", code: 103, userInfo: [
                NSLocalizedDescriptionKey: "Initial bidirectional sync requires one side to be empty. Both the local path (\(resolvedLocalPath)) and remote folder (\(remoteFolderId)) contain files. Use an empty directory for initialization to avoid overwrites or widespread conflicts."
            ])
        }
    }

    /// O(1) memory, no recursion or content reads; matches includeHidden + excludeDirectory(".git").
    static func isLocalRootEmpty(_ path: String) throws -> Bool {
        guard let directory = opendir(path) else {
            if errno == ENOENT { return true }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        defer { closedir(directory) }
        while true {
            errno = 0
            guard let entry = readdir(directory) else {
                if errno != 0 { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
                return true
            }
            let name = withUnsafePointer(to: &entry.pointee.d_name) {
                $0.withMemoryRebound(to: CChar.self, capacity: Int(entry.pointee.d_namlen) + 1) { String(cString: $0) }
            }
            if name == "." || name == ".." { continue }
            if name == ".git" {
                var type = entry.pointee.d_type
                if type == UInt8(DT_UNKNOWN) {
                    var metadata = stat()
                    guard fstatat(dirfd(directory), name, &metadata, AT_SYMLINK_NOFOLLOW) == 0 else {
                        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
                    }
                    if metadata.st_mode & S_IFMT == S_IFDIR { type = UInt8(DT_DIR) }
                }
                if type == UInt8(DT_DIR) { continue }
            }
            return false
        }
    }

    // MARK: - mode 1:local directory -> Extremely fast upload of remote empty directories (localToRemoteEmpty)

    /// Full streaming synchronization of local non-empty directories to remote empty directories
    @discardableResult
    public func syncLocalToRemoteEmpty(
        localPath: String,
        remoteRootId: String,
        maxUploadConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        try await withRootSyncLock(localPath: localPath) {
            try await self.syncLocalToRemoteEmptyUnlocked(
                localPath: localPath,
                remoteRootId: remoteRootId,
                maxUploadConcurrency: maxUploadConcurrency,
                onProgress: onProgress
            )
        }
    }

    private func syncLocalToRemoteEmptyUnlocked(
        localPath: String,
        remoteRootId: String,
        maxUploadConcurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)

        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)

        // Verify local root directory
        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir), isDir.boolValue else {
            throw NSError(domain: "SyncEngine", code: 1, userInfo: [NSLocalizedDescriptionKey: "The local path does not exist or is not a directory: \(resolvedLocalPath)"])
        }

        // Verify that the remote root directory exists
        let remoteRoot = try await client.getFile(remoteId: remoteRootId)
        guard remoteRoot.isDirectory else {
            throw NSError(domain: "SyncEngine", code: 2, userInfo: [NSLocalizedDescriptionKey: "The remote target is not a directory: \(remoteRootId)"])
        }

        // Only verify whether the remote end is an empty directory when the root baseline is not established for the first time.
        let rootExists: Bool = try await store.read { conn in
            let stmt = try conn.cachedStatement("SELECT 1 FROM roots WHERE remote_root_id = ? AND is_active = 1;")
            stmt.bindText(remoteRootId, at: 1)
            defer { stmt.reset() }
            return try stmt.step()
        }
        if !rootExists {
            let existingChildren = try await client.listChildren(parentId: remoteRootId)
            guard existingChildren.isEmpty else {
                throw NSError(domain: "SyncEngine", code: 20, userInfo: [NSLocalizedDescriptionKey: "The remote target is not empty; localToRemoteEmpty cannot run: \(remoteRootId)"])
            }
        }

        let now = Date().timeIntervalSince1970

        // 1. Register or get Root Records and root entries
        let (rootId, rootItemId): (Int64, Int64) = try await store.write { conn in
            let rootStmt = try conn.cachedStatement("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'freshCreated', ?, ?)
            ON CONFLICT (account_id, remote_root_id) DO UPDATE SET updated_at = excluded.updated_at;
            """)
            rootStmt.bindText(resolvedLocalPath, at: 1)
            rootStmt.bindText(remoteRootId, at: 2)
            rootStmt.bindDouble(now, at: 3)
            rootStmt.bindDouble(now, at: 4)
            _ = try rootStmt.step()
            rootStmt.reset()

            let rootQuery = try conn.cachedStatement("SELECT root_id FROM roots WHERE account_id = 'default' AND remote_root_id = ?;")
            rootQuery.bindText(remoteRootId, at: 1)
            guard try rootQuery.step(), let rId = rootQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 3, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root_id"])
            }
            rootQuery.reset()

            let itemStmt = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (?, NULL, ?, 'directory', ?, ?, ?)
            ON CONFLICT (root_id) WHERE parent_id IS NULL AND is_tombstone = 0 DO UPDATE SET updated_at = excluded.updated_at;
            """)
            itemStmt.bindInt64(rId, at: 1)
            itemStmt.bindText(rootURL.lastPathComponent, at: 2)
            itemStmt.bindText(remoteRootId, at: 3)
            itemStmt.bindDouble(now, at: 4)
            itemStmt.bindDouble(now, at: 5)
            _ = try itemStmt.step()
            itemStmt.reset()

            let itemQuery = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL AND is_tombstone = 0;")
            itemQuery.bindInt64(rId, at: 1)
            guard try itemQuery.step(), let rItemId = itemQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 4, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root item_id"])
            }
            itemQuery.reset()

            return (rId, rItemId)
        }

        try await RemoteChanges.saveInitialCursor(store: store, client: client, rootID: rootId, requireExisting: rootExists)

        // 2. Initialize the directory asynchronous wake-up scheduler (the root directory is defaulted to ready)
        let directoryTracker = DirectoryTracker(remoteRootId: remoteRootId)

        // Record the relative path of the local directory and its database itemId
        final class LocalDirMap: @unchecked Sendable {
            private var map: [String: Int64] = [:]
            private var lock = os_unfair_lock()

            init(rootItemId: Int64) {
                map[""] = rootItemId
            }

            func set(_ path: String, id: Int64) {
                os_unfair_lock_lock(&lock)
                map[path] = id
                os_unfair_lock_unlock(&lock)
            }

            func get(_ path: String) -> Int64? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return map[path]
            }
        }
        let localDirMap = LocalDirMap(rootItemId: rootItemId)

        // in advance from SQLite Load all known subdirectory mappings, restore bootstrap Or reuse it resolutely when re-running to avoid blindly re-creating
        let existingDirs: [(id: Int64, parentId: Int64, name: String, remoteId: String)] = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT item_id, parent_id, name, remote_file_id
            FROM items
            WHERE root_id = ? AND entry_kind = 'directory' AND is_tombstone = 0 AND parent_id IS NOT NULL;
            """)
            stmt.bindInt64(rootId, at: 1)
            defer { stmt.reset() }
            var list: [(id: Int64, parentId: Int64, name: String, remoteId: String)] = []
            while try stmt.step() {
                if let id = stmt.columnInt64(at: 0),
                   let pId = stmt.columnInt64(at: 1),
                   let name = stmt.columnText(at: 2),
                   let rId = stmt.columnText(at: 3) {
                    list.append((id, pId, name, rId))
                }
            }
            return list
        }

        var dirPathsById: [Int64: String] = [rootItemId: ""]
        var registered = Set<Int64>([rootItemId])
        var remainingDirs = existingDirs
        var topoProgress = true
        while !remainingDirs.isEmpty && topoProgress {
            topoProgress = false
            remainingDirs.removeAll { dir in
                if registered.contains(dir.parentId), let parentPath = dirPathsById[dir.parentId] {
                    let relPath = parentPath.isEmpty ? dir.name : "\(parentPath)/\(dir.name)"
                    dirPathsById[dir.id] = relPath
                    localDirMap.set(relPath, id: dir.id)
                    registered.insert(dir.id)
                    topoProgress = true
                    return true
                }
                return false
            }
        }

        for (id, relPath) in dirPathsById where id != rootItemId {
            if let dir = existingDirs.first(where: { $0.id == id }) {
                await directoryTracker.markDirectoryReady(relPath: relPath, remoteId: dir.remoteId)
            }
        }

        // 3. Set up a bounded concurrent upload pipeline (strict upper limit 64 Concurrency)
        let effectiveConcurrency = max(1, min(64, maxUploadConcurrency))
        let uploadSemaphore = AsyncSemaphore(count: effectiveConcurrency)
        // Bound all bootstrap work admitted beyond the scanner. This window is
        // deliberately larger than the transfer pool so scanning can stay ahead
        // of slow requests without retaining one Task per tree entry.
        let bootstrapTaskWindow = AsyncSemaphore(count: 512)
        // Directory metadata requests do not consume file transfer slots, but
        // they still need their own network concurrency bound.
        let directorySemaphore = AsyncSemaphore(count: max(1, min(8, effectiveConcurrency)))
        let uploadGroup = DispatchGroup()

        // Load fast change comparison baseline cache (§6.2)
        let baselineCache = try await LocalBaselineCache.load(store: store, rootId: rootId)

        final class ProgressTracker: @unchecked Sendable {
            private var _filesUploaded = 0
            private var _filesSkipped = 0
            private var _filesFailed = 0
            private var _bytesUploaded: Int64 = 0
            private var _dirsCreated = 0
            private var lock = os_unfair_lock()

            var filesUploaded: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _filesUploaded
            }

            var filesSkipped: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _filesSkipped
            }

            var filesFailed: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _filesFailed
            }

            var bytesUploaded: Int64 {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _bytesUploaded
            }

            var dirsCreated: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _dirsCreated
            }

            func recordSuccess(bytes: Int64) {
                os_unfair_lock_lock(&lock)
                _filesUploaded += 1
                _bytesUploaded += bytes
                os_unfair_lock_unlock(&lock)
            }

            func recordFailure() {
                os_unfair_lock_lock(&lock)
                _filesFailed += 1
                os_unfair_lock_unlock(&lock)
            }

            func recordSkipped() {
                os_unfair_lock_lock(&lock)
                _filesSkipped += 1
                os_unfair_lock_unlock(&lock)
            }

            func recordDirCreated() {
                os_unfair_lock_lock(&lock)
                _dirsCreated += 1
                os_unfair_lock_unlock(&lock)
            }
        }
        let progress = ProgressTracker()

        // 4. start DirectoryScanner concurrent scan
        let staticPrefix = resolvedLocalPath.hasSuffix("/") ? resolvedLocalPath : resolvedLocalPath + "/"
        let prefixBytes = Array(staticPrefix.utf8)

        let scanOptions = ScanOptions(
            mode: .basic,
            emission: .all,
            includeHidden: true,
            workers: min(6, ProcessInfo.processInfo.activeProcessorCount),
            batchCapacity: 512,
            delimiter: 0x00,
            pathPrefix: prefixBytes
        )
        let scanFilters: [FilterRule] = [.excludeDirectory(".git")]
        let request = ScanRequest(root: resolvedLocalPath, filters: scanFilters, options: scanOptions)
        let scanner = DirectoryScanner()

        _ = try await scanner.scan(request) { batch in
            struct BootstrapScanRecord: Sendable {
                let type: EntryType
                let metadata: FileMetadata?
                let fullPath: String
            }
            var copiedRecords: [BootstrapScanRecord] = []
            copiedRecords.reserveCapacity(batch.count)
            batch.withRawData { rawBuf in
                guard let basePtr = rawBuf.baseAddress else { return }

                for idx in 0..<batch.count {
                    let record = batch.records[idx]
                    let rawPtr = UnsafeRawPointer(basePtr + Int(record.offset))
                    let cPath = rawPtr.assumingMemoryBound(to: CChar.self)
                    copiedRecords.append(BootstrapScanRecord(
                        type: record.type,
                        metadata: record.metadata,
                        fullPath: String(cString: cPath)
                    ))
                }
            }

            for record in copiedRecords {
                    let fullPath = record.fullPath

                    let relPath: String
                    if fullPath.hasPrefix(staticPrefix) {
                        relPath = String(fullPath.dropFirst(staticPrefix.count))
                    } else {
                        relPath = (fullPath as NSString).lastPathComponent
                    }

                    if relPath.isEmpty || relPath == ".git" || relPath.hasPrefix(".git/") { continue }

                    let parentRel = (relPath as NSString).deletingLastPathComponent
                    let name = (relPath as NSString).lastPathComponent

                    if record.type == .directory {
                        // Directory processing: If the directory already exists and the remote end has been registered ID,Skip remote creation directly to prevent repeated creation
                        if localDirMap.get(relPath) != nil {
                            continue
                        }
                        await bootstrapTaskWindow.wait()
                        uploadGroup.enter()
                        Task {
                            defer {
                                bootstrapTaskWindow.signal()
                                uploadGroup.leave()
                            }
                            var createIntent: DurableCreateIntent?
                            do {
                                let remoteParentId = try await directoryTracker.awaitParentReady(parentRelPath: parentRel)
                                await directorySemaphore.wait()
                                defer { directorySemaphore.signal() }
                                let candidateRemoteId = try await self.idPool.nextId()
                                let grandParentItemId = localDirMap.get(parentRel) ?? rootItemId
                                let intent = try await DurableCreateIntentStore.prepareDirectory(
                                    store: self.store,
                                    rootID: rootId,
                                    parentItemID: grandParentItemId,
                                    name: name,
                                    targetParentRemoteID: remoteParentId,
                                    candidateRemoteID: candidateRemoteId,
                                    device: Int64(record.metadata?.identity.device ?? 1),
                                    inode: Int64(record.metadata?.identity.inode ?? 0)
                                )
                                createIntent = intent

                                // Intent of group-commit Only after confirmation can the remote creation request be issued.
                                _ = try await self.client.createDirectory(
                                    name: name,
                                    parentId: intent.targetParentRemoteID,
                                    remoteId: intent.targetRemoteID
                                )

                                try await self.store.batchWrite { conn in
                                    let ts = Date().timeIntervalSince1970
                                    let stmt = try conn.cachedStatement("""
                                    UPDATE items SET
                                        remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                                    WHERE item_id = ?;
                                    """)
                                    stmt.bindDouble(ts, at: 1)
                                    stmt.bindInt64(intent.itemID, at: 2)
                                    _ = try stmt.step()
                                    stmt.reset()
                                    try DurableCreateIntentStore.completeOperation(conn: conn, operationID: intent.operationID, now: ts)
                                }
                                localDirMap.set(relPath, id: intent.itemID)

                                // Broadcast wake-up waiting for all children of the directory
                                await directoryTracker.markDirectoryReady(relPath: relPath, remoteId: intent.targetRemoteID)
                                progress.recordDirCreated()
                            } catch {
                                await directoryTracker.markDirectoryFailed(relPath: relPath, error: error)
                                if let createIntent {
                                    await DurableCreateIntentStore.markUnknownOutcome(
                                        store: self.store,
                                        operationID: createIntent.operationID,
                                        error: error
                                    )
                                }
                                self.logger.error("Failed to create remote directory [\(relPath)]: \(error)")
                            }
                        }
                    } else if record.type == .file {
                        let dev = Int64(record.metadata?.identity.device ?? 1)
                        let ino = Int64(record.metadata?.identity.inode ?? 0)
                        let mtime = (record.metadata?.modificationTime.seconds ?? 0) * 1_000_000_000 + Int64(record.metadata?.modificationTime.nanoseconds ?? 0)
                        let fileSize = record.metadata?.fileSize ?? 0

                        // Rapid change detection (§6.2):dev + inode + mtime + size Matching skips content reading and hash calculations
                        if let _ = baselineCache.lookupUnchanged(device: dev, inode: ino, mtime: mtime, size: fileSize) {
                            progress.recordSkipped()
                            continue
                        }

                        await bootstrapTaskWindow.wait()
                        // File handling: wait for its immediate parent directory to be ready and upload immediately
                        notifier.addDiscovered(files: 1, bytes: Int64(fileSize))
                        self.monitor.enqueueUpload(id: fullPath, name: name, totalBytes: Int64(record.metadata?.fileSize ?? 0))
                        uploadGroup.enter()
                        Task {
                            defer {
                                bootstrapTaskWindow.signal()
                                uploadGroup.leave()
                            }
                            var createIntent: DurableCreateIntent?
                            var didAcquireSemaphore = false

                            defer {
                                if didAcquireSemaphore {
                                    uploadSemaphore.signal()
                                }
                                self.monitor.finishUpload(id: fullPath)
                                notifier.addCompleted(files: 1, bytes: Int64(fileSize))
                            }

                            do {
                                // First wait for the direct parent directory to be in Google Drive The remote end is ready to avoid occupying concurrent upload slots.
                                let remoteParentId = try await directoryTracker.awaitParentReady(parentRelPath: parentRel)

                                await uploadSemaphore.wait()
                                didAcquireSemaphore = true

                                let fileURL = URL(fileURLWithPath: fullPath)
                                let limit8MB: Int64 = 8 * 1024 * 1024

                                let input = try StableUploadInput.capture(at: fileURL)
                                let sha256Hex = input.sha256
                                let fileSize = input.size
                                let smallContent = input.data

                                self.monitor.startUpload(id: fullPath, name: name, totalBytes: fileSize)

                                let dev = input.version.device
                                let ino = input.version.inode
                                let mtime = input.version.mtime

                                let parentDirItemId = localDirMap.get(parentRel) ?? rootItemId

                                // Check whether the file is already in SQLite recorded (whether it has been committed Still unfinished inFlight)
                                struct ExistingFileTarget {
                                    let itemID: Int64
                                    let remoteID: String?
                                    let phase: String
                                    let remoteStatus: String
                                    let hasBaseline: Bool
                                    let localGeneration: Int64
                                    let remoteGeneration: Int64
                                    let dirtyGeneration: Int64
                                }

                                let existingTarget: ExistingFileTarget? = try await self.store.read { conn in
                                    let stmt = try conn.cachedStatement("""
                                    SELECT item_id, remote_file_id, phase, remote_status, base_sha256,
                                           local_generation, remote_generation, dirty_generation
                                    FROM items
                                    WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;
                                    """)
                                    stmt.bindInt64(rootId, at: 1)
                                    stmt.bindInt64(parentDirItemId, at: 2)
                                    stmt.bindText(name, at: 3)
                                    defer { stmt.reset() }
                                    if try stmt.step(), let iId = stmt.columnInt64(at: 0) {
                                        return ExistingFileTarget(
                                            itemID: iId,
                                            remoteID: stmt.columnText(at: 1),
                                            phase: stmt.columnText(at: 2) ?? "discovered",
                                            remoteStatus: stmt.columnText(at: 3) ?? "unknown",
                                            hasBaseline: stmt.columnText(at: 4) != nil,
                                            localGeneration: stmt.columnInt64(at: 5) ?? 0,
                                            remoteGeneration: stmt.columnInt64(at: 6) ?? 0,
                                            dirtyGeneration: stmt.columnInt64(at: 7) ?? 0
                                        )
                                    }
                                    return nil
                                }

                                // Perform upload based on file size:≤ 8MB go Multipart,> 8MB go Resumable
                                if let existingTarget, existingTarget.hasBaseline, let remoteID = existingTarget.remoteID {
                                    throw DriveError.unsafeOverwrite(fileId: remoteID)
                                }
                                let receiptApplied = OSAllocatedUnfairLock(initialState: false)
                                let expectedRemoteGeneration = existingTarget?.remoteGeneration ?? 0
                                let expectedDirtyGeneration = max(existingTarget?.dirtyGeneration ?? 0, 1)
                                if fileSize <= limit8MB, let content = smallContent {
                                    let uploadedFile: DriveFile
                                    let committedItemID: Int64
                                    if let existing = existingTarget, let existingRemoteId = existing.remoteID,
                                       existing.phase == "committed" && existing.remoteStatus == "present" {
                                        // The content of the file submitted in the cloud has changed: call directly updateMultipart Update existing remote objects, never create new ones Drive Object!
                                        uploadedFile = try await self.client.updateMultipart(
                                            remoteId: existingRemoteId,
                                            content: content,
                                            expectedSha256: sha256Hex
                                        )
                                        committedItemID = existing.itemID
                                    } else {
                                        // Create new files or rerun unfinished files: Prioritize reusing existing files remote_file_id,avoid regeneration
                                        let targetRemoteId: String
                                        if let existingRemoteId = existingTarget?.remoteID {
                                            targetRemoteId = existingRemoteId
                                        } else {
                                            targetRemoteId = try await self.idPool.nextId()
                                        }
                                        let intent = try await DurableCreateIntentStore.prepareFileUpload(
                                            store: self.store,
                                            rootID: rootId,
                                            itemID: existingTarget?.itemID,
                                            parentItemID: parentDirItemId,
                                            name: name,
                                            targetParentRemoteID: remoteParentId,
                                            candidateRemoteID: targetRemoteId,
                                            device: dev,
                                            inode: ino,
                                            mtime: mtime,
                                            size: fileSize,
                                            sha256: sha256Hex,
                                            transport: .multipart
                                        )
                                        createIntent = intent

                                        do {
                                            uploadedFile = try await self.client.uploadMultipart(
                                                name: name,
                                                parentId: intent.targetParentRemoteID,
                                                remoteId: intent.targetRemoteID,
                                                content: content,
                                                expectedSha256: sha256Hex
                                            )
                                        } catch let error as DriveError {
                                            switch error {
                                            case .conflict:
                                                // The remote end may have successfully written the ID metadata, converted to update text
                                                uploadedFile = try await self.client.updateMultipart(
                                                    remoteId: intent.targetRemoteID,
                                                    content: content,
                                                    expectedSha256: sha256Hex
                                                )
                                            case .serverError(let code, _) where code == 400 || code == 409:
                                                uploadedFile = try await self.client.updateMultipart(
                                                    remoteId: intent.targetRemoteID,
                                                    content: content,
                                                    expectedSha256: sha256Hex
                                                )
                                            default:
                                                throw error
                                            }
                                        }
                                        committedItemID = intent.itemID
                                    }
                                    self.monitor.reportUploadProgress(id: fullPath, additionalBytes: fileSize)

                                    // After the upload is successful, pass batchWrite(Group Commit,merge 256 item or 5ms Flush the disk) and write the baseline
                                    try input.version.validate(at: fileURL)
                                    let completedCreateIntent = createIntent
                                    let expectedLocalGeneration = completedCreateIntent?.expectedLocalGeneration ?? existingTarget?.localGeneration ?? 1
                                    try await self.store.batchWrite { conn in
                                        guard (try? LocalFileVersion.read(at: fileURL)) == input.version else { return }
                                        let itemStmt = try conn.cachedStatement("""
                                        UPDATE items SET
                                            remote_file_id = ?,
                                            local_device = ?, local_inode = ?, local_mtime = ?,
                                            local_size = ?, local_sha256 = ?,
                                            base_sha256 = ?, base_size = ?,
                                            remote_sha256 = ?, remote_size = ?, remote_status = 'present',
                                            local_status = 'present', phase = 'committed', dirty_generation = 0,
                                            updated_at = ?
                                        WHERE item_id = ? AND local_generation = ? AND remote_generation = ?
                                            AND dirty_generation = ? AND is_tombstone = 0;
                                        """)
                                        itemStmt.bindText(uploadedFile.id, at: 1)
                                        itemStmt.bindInt64(dev, at: 2)
                                        itemStmt.bindInt64(ino, at: 3)
                                        itemStmt.bindInt64(mtime, at: 4)
                                        itemStmt.bindInt64(fileSize, at: 5)
                                        itemStmt.bindText(sha256Hex, at: 6)
                                        itemStmt.bindText(sha256Hex, at: 7)
                                        itemStmt.bindInt64(fileSize, at: 8)
                                        itemStmt.bindText(sha256Hex, at: 9)
                                        itemStmt.bindInt64(fileSize, at: 10)
                                        let ts = Date().timeIntervalSince1970
                                        itemStmt.bindDouble(ts, at: 11)
                                        itemStmt.bindInt64(committedItemID, at: 12)
                                        itemStmt.bindInt64(expectedLocalGeneration, at: 13)
                                        itemStmt.bindInt64(expectedRemoteGeneration, at: 14)
                                        itemStmt.bindInt64(expectedDirtyGeneration, at: 15)
                                        _ = try itemStmt.step()
                                        itemStmt.reset()
                                        guard conn.changes == 1 else { return }
                                        receiptApplied.withLock { $0 = true }
                                        if let createIntent = completedCreateIntent {
                                            try DurableCreateIntentStore.completeOperation(
                                                conn: conn,
                                                operationID: createIntent.operationID,
                                                now: ts
                                            )
                                        }
                                    }
                                } else {
                                    // large files (> 8MB):Reuse existing remote_file_id with breakpoints, no blind replacement ID!
                                    let remoteFileId: String
                                    if let existingRemoteId = existingTarget?.remoteID {
                                        remoteFileId = existingRemoteId
                                    } else {
                                        remoteFileId = try await self.idPool.nextId()
                                    }
                                    let isUpdate = existingTarget?.hasBaseline == true || (existingTarget?.phase == "committed" && existingTarget?.remoteStatus == "present")
                                    if isUpdate { throw DriveError.unsafeOverwrite(fileId: remoteFileId) }

                                    let currentItemId: Int64 = try await self.store.write { conn in
                                        let itemStmt = try conn.cachedStatement("""
                                        INSERT INTO items (
                                            root_id, parent_id, name, entry_kind, remote_file_id,
                                            local_device, local_inode, local_mtime, local_size, local_sha256,
                                            local_generation, local_status, phase, dirty_generation,
                                            created_at, updated_at
                                        ) VALUES (
                                            ?, ?, ?, 'file', ?,
                                            ?, ?, ?, ?, ?,
                                            1, 'present', 'inFlight', 1,
                                            ?, ?
                                        )
                                        ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                                        DO UPDATE SET
                                            remote_file_id = COALESCE(items.remote_file_id, excluded.remote_file_id),
                                            local_device = excluded.local_device,
                                            local_inode = excluded.local_inode,
                                            local_mtime = excluded.local_mtime,
                                            local_size = excluded.local_size,
                                            local_sha256 = excluded.local_sha256,
                                            phase = 'inFlight',
                                            dirty_generation = MAX(items.dirty_generation, 1),
                                            updated_at = excluded.updated_at;
                                        """)
                                        itemStmt.bindInt64(rootId, at: 1)
                                        itemStmt.bindInt64(parentDirItemId, at: 2)
                                        itemStmt.bindText(name, at: 3)
                                        itemStmt.bindText(remoteFileId, at: 4)
                                        itemStmt.bindInt64(dev, at: 5)
                                        itemStmt.bindInt64(ino, at: 6)
                                        itemStmt.bindInt64(mtime, at: 7)
                                        itemStmt.bindInt64(fileSize, at: 8)
                                        itemStmt.bindText(sha256Hex, at: 9)
                                        let ts = Date().timeIntervalSince1970
                                        itemStmt.bindDouble(ts, at: 10)
                                        itemStmt.bindDouble(ts, at: 11)
                                        _ = try itemStmt.step()
                                        itemStmt.reset()

                                        let qStmt = try conn.cachedStatement("""
                                        SELECT item_id FROM items
                                        WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;
                                        """)
                                        qStmt.bindInt64(rootId, at: 1)
                                        qStmt.bindInt64(parentDirItemId, at: 2)
                                        qStmt.bindText(name, at: 3)
                                        defer { qStmt.reset() }
                                        if try qStmt.step(), let id = qStmt.columnInt64(at: 0) {
                                            return id
                                        }
                                        return 0
                                    }

                                    // large files (> 8MB) Resumable 8MB Streaming block-based breakpoint resumption (each block is placed on the disk) offset)
                                    _ = try await self.performResumableUpload(
                                        rootId: rootId,
                                        itemId: currentItemId,
                                        fileURL: input.fileURL,
                                        fileSize: fileSize,
                                        expectedSha256: sha256Hex,
                                        remoteId: remoteFileId,
                                        parentId: remoteParentId,
                                        name: name,
                                        isUpdate: isUpdate
                                    )

                                    // Submit a common baseline B
                                    try input.version.validate(at: fileURL)
                                    let expectedLocalGeneration = existingTarget?.localGeneration ?? 1
                                    try await self.store.batchWrite { conn in
                                        guard (try? LocalFileVersion.read(at: fileURL)) == input.version else { return }
                                        let updateStmt = try conn.cachedStatement("""
                                        UPDATE items SET
                                            base_sha256 = ?,
                                            base_size = ?,
                                            remote_sha256 = ?,
                                            remote_size = ?,
                                            remote_status = 'present',
                                            phase = 'committed',
                                            dirty_generation = 0,
                                            updated_at = ?
                                        WHERE root_id = ? AND remote_file_id = ? AND local_generation = ?
                                            AND remote_generation = ? AND dirty_generation = ? AND is_tombstone = 0;
                                        """)
                                        updateStmt.bindText(sha256Hex, at: 1)
                                        updateStmt.bindInt64(fileSize, at: 2)
                                        updateStmt.bindText(sha256Hex, at: 3)
                                        updateStmt.bindInt64(fileSize, at: 4)
                                        updateStmt.bindDouble(Date().timeIntervalSince1970, at: 5)
                                        updateStmt.bindInt64(rootId, at: 6)
                                        updateStmt.bindText(remoteFileId, at: 7)
                                        updateStmt.bindInt64(expectedLocalGeneration, at: 8)
                                        updateStmt.bindInt64(expectedRemoteGeneration, at: 9)
                                        updateStmt.bindInt64(expectedDirtyGeneration, at: 10)
                                        _ = try updateStmt.step()
                                        updateStmt.reset()
                                        if conn.changes == 1 { receiptApplied.withLock { $0 = true } }
                                    }
                                }

                                guard receiptApplied.withLock({ $0 }) else {
                                    throw SyncEngineError.general("The upload receipt is stale; the newer generation remains pending: \(name)")
                                }
                                progress.recordSuccess(bytes: fileSize)
                            } catch {
                                if let createIntent {
                                    await DurableCreateIntentStore.markUnknownOutcome(
                                        store: self.store,
                                        operationID: createIntent.operationID,
                                        error: error
                                    )
                                }
                                progress.recordFailure()
                                if case DriveError.unsafeOverwrite = error {
                                    let parentID = localDirMap.get(parentRel) ?? rootItemId
                                    try? await self.store.batchWrite { conn in
                                        let stmt = try conn.cachedStatement("""
                                        UPDATE items SET phase = 'blocked', dirty_generation = MAX(dirty_generation, 1)
                                        WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;
                                        """)
                                        stmt.bindInt64(rootId, at: 1)
                                        stmt.bindInt64(parentID, at: 2)
                                        stmt.bindText(name, at: 3)
                                        _ = try stmt.step()
                                        stmt.reset()
                                    }
                                }
                                self.logger.error("Failed to upload file [\(relPath)]: \(error)")
                            }
                        }
                    }
            }
        }

        // 5. Wait for all concurrent directory creation and uploads to complete
        await withTaskCancellationHandler {
            await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                uploadGroup.notify(queue: .global(qos: .userInitiated)) {
                    cont.resume()
                }
            }
        } onCancel: {
            Task {
                await directoryTracker.cancelAll()
            }
        }

        // 6. Force the buffer to be written to disk and execute WAL checkpoint
        try await store.flush()
        try await store.checkpoint()

        if progress.filesFailed == 0 {
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                UPDATE roots SET bootstrap_state = 'existingKnown', updated_at = ? WHERE root_id = ?;
                """)
                stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                stmt.bindInt64(rootId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
        }

        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        stats.directoriesCreated = progress.dirsCreated
        stats.filesUploaded = progress.filesUploaded
        stats.filesSkipped = progress.filesSkipped
        stats.filesFailed = progress.filesFailed
        stats.bytesUploaded = progress.bytesUploaded
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }

    // MARK: - mode 2:remote directory -> Fast download of local empty directory (remoteToLocalEmpty)

    /// Synchronize full streaming download of remote non-empty directory to local empty directory
    @discardableResult
    public func syncRemoteToLocalEmpty(
        localPath: String,
        remoteRootId: String,
        maxDownloadConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        try await withRootSyncLock(localPath: localPath) {
            try await self.initializeRemoteToLocalEmpty(localPath: localPath, remoteRootId: remoteRootId,
                maxDownloadConcurrency: maxDownloadConcurrency, onProgress: onProgress, initialCursor: nil)
        }
    }

    private func initializeRemoteToLocalEmpty(
        localPath: String,
        remoteRootId: String,
        maxDownloadConcurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?,
        initialCursor: String?
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)

        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)

        // Verify that the remote root directory exists
        let remoteRoot = try await client.getFile(remoteId: remoteRootId)
        guard remoteRoot.isDirectory else {
            throw NSError(domain: "SyncEngine", code: 10, userInfo: [NSLocalizedDescriptionKey: "The remote target is not a valid directory: \(remoteRootId)"])
        }

        let downloadDirectory = try await downloadStagingDirectory(remoteRootID: remoteRootId, localRoot: rootURL)

        // Make sure the local directory exists and is empty
        var isDir: ObjCBool = false
        if FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir) {
            guard isDir.boolValue else {
                throw NSError(domain: "SyncEngine", code: 11, userInfo: [NSLocalizedDescriptionKey: "The local path already exists and is not a directory: \(resolvedLocalPath)"])
            }
            let contents = try FileManager.default.contentsOfDirectory(atPath: resolvedLocalPath)
            guard contents.isEmpty else {
                throw NSError(domain: "SyncEngine", code: 12, userInfo: [NSLocalizedDescriptionKey: "The local target directory must be empty: \(resolvedLocalPath)"])
            }
        } else {
            try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        }

        let rootExists = try await store.read { conn in
            let q = try conn.cachedStatement("SELECT 1 FROM roots WHERE remote_root_id = ? AND is_active = 1;")
            defer { q.reset() }
            q.bindText(remoteRootId, at: 1)
            return try q.step()
        }

        let now = Date().timeIntervalSince1970

        // 1. Register or get Root Records and root entries
        let (rootId, rootItemId): (Int64, Int64) = try await store.write { conn in
            let rootStmt = try conn.cachedStatement("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', ?, 1, 1, ?, 'remoteToLocalEmpty', 'freshCreated', ?, ?)
            ON CONFLICT (account_id, remote_root_id) DO UPDATE SET updated_at = excluded.updated_at;
            """)
            rootStmt.bindText(resolvedLocalPath, at: 1)
            rootStmt.bindText(remoteRootId, at: 2)
            rootStmt.bindDouble(now, at: 3)
            rootStmt.bindDouble(now, at: 4)
            _ = try rootStmt.step()
            rootStmt.reset()

            let rootQuery = try conn.cachedStatement("SELECT root_id FROM roots WHERE account_id = 'default' AND remote_root_id = ?;")
            rootQuery.bindText(remoteRootId, at: 1)
            guard try rootQuery.step(), let rId = rootQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 13, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root_id"])
            }
            rootQuery.reset()

            let itemStmt = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (?, NULL, ?, 'directory', ?, ?, ?)
            ON CONFLICT (root_id) WHERE parent_id IS NULL AND is_tombstone = 0 DO UPDATE SET updated_at = excluded.updated_at;
            """)
            itemStmt.bindInt64(rId, at: 1)
            itemStmt.bindText(rootURL.lastPathComponent, at: 2)
            itemStmt.bindText(remoteRootId, at: 3)
            itemStmt.bindDouble(now, at: 4)
            itemStmt.bindDouble(now, at: 5)
            _ = try itemStmt.step()
            itemStmt.reset()

            let itemQuery = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL AND is_tombstone = 0;")
            itemQuery.bindInt64(rId, at: 1)
            guard try itemQuery.step(), let rItemId = itemQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 14, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root item_id"])
            }
            itemQuery.reset()

            return (rId, rItemId)
        }

        try await RemoteChanges.saveInitialCursor(store: store, client: client, rootID: rootId, requireExisting: rootExists, initialToken: initialCursor)

        let effectiveDownloadConcurrency = max(1, min(64, maxDownloadConcurrency))
        let downloadSemaphore = AsyncSemaphore(count: effectiveDownloadConcurrency)
        let downloadGroup = DispatchGroup()

        final class DownloadTracker: @unchecked Sendable {
            var filesDownloaded = 0
            var bytesDownloaded: Int64 = 0
            var dirsCreated = 0
        }
        let progress = DownloadTracker()

        // 2. Recursive enumeration of remote files and streaming download
        func traverseRemote(parentRemoteId: String, currentLocalURL: URL, parentItemId: Int64) async throws {
            let children = try await self.client.listChildren(parentId: parentRemoteId)
            try RemoteNameMapping.validateSiblings(children)

            for item in children {
                let itemLocalURL = currentLocalURL.appendingPathComponent(item.name)
                try RemoteNameMapping.validateDestination(itemLocalURL, root: rootURL)

                if item.isDirectory {
                    // Create local directory
                    try FileManager.default.createDirectory(at: itemLocalURL, withIntermediateDirectories: true)
                    progress.dirsCreated += 1

                    // write SQLite
                    let dirItemId: Int64 = try await self.store.write { conn in
                        let stmt = try conn.cachedStatement("""
                        INSERT INTO items (
                            root_id, parent_id, name, entry_kind, remote_file_id,
                            phase, created_at, updated_at
                        ) VALUES (?, ?, ?, 'directory', ?, 'committed', ?, ?)
                        ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                        DO UPDATE SET updated_at = excluded.updated_at WHERE items.remote_file_id = excluded.remote_file_id;
                        """)
                        stmt.bindInt64(rootId, at: 1)
                        stmt.bindInt64(parentItemId, at: 2)
                        stmt.bindText(item.name, at: 3)
                        stmt.bindText(item.id, at: 4)
                        let ts = Date().timeIntervalSince1970
                        stmt.bindDouble(ts, at: 5)
                        stmt.bindDouble(ts, at: 6)
                        _ = try stmt.step()
                        stmt.reset()
                        guard conn.changes == 1 else {
                            throw SyncEngineError.general("The remote directory name belongs to another file ID: \(item.name)")
                        }

                        let qStmt = try conn.cachedStatement("""
                        SELECT item_id FROM items WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;
                        """)
                        qStmt.bindInt64(rootId, at: 1)
                        qStmt.bindInt64(parentItemId, at: 2)
                        qStmt.bindText(item.name, at: 3)
                        guard try qStmt.step(), let dId = qStmt.columnInt64(at: 0) else {
                            throw NSError(domain: "SyncEngine", code: 15, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain dir item_id"])
                        }
                        qStmt.reset()
                        return dId
                    }

                    // Recurse to the next level
                    try await traverseRemote(parentRemoteId: item.id, currentLocalURL: itemLocalURL, parentItemId: dirItemId)
                } else {
                    // File: join concurrent download queue
                    let downloadBytes = item.sizeBytes ?? 0
                    notifier.addDiscovered(files: 1, bytes: downloadBytes)
                    self.monitor.enqueueDownload(id: item.id, name: item.name, totalBytes: downloadBytes)
                    downloadGroup.enter()
                    Task {
                        await downloadSemaphore.wait()
                        defer {
                            self.monitor.finishDownload(id: item.id)
                            downloadSemaphore.signal()
                            downloadGroup.leave()
                            notifier.addCompleted(files: 1, bytes: downloadBytes)
                        }

                        do {
                            self.monitor.startDownload(id: item.id, name: item.name, totalBytes: item.sizeBytes ?? 0)
                            // Streaming download and verification SHA-256
                            let published = try await self.client.downloadFileSafely(
                                remoteId: item.id,
                                destinationURL: itemLocalURL,
                                expectedSha256: item.sha256Checksum,
                                expectedDestination: nil,
                                temporaryDirectory: downloadDirectory,
                                onProgress: { delta in
                                    self.monitor.reportDownloadProgress(id: item.id, additionalBytes: delta)
                                }
                            )

                            // Get metadata after local placement
                            let fileSize = published.size
                            let mtime = published.mtime

                            // write SQLite baseline B
                            let receiptApplied = OSAllocatedUnfairLock(initialState: false)
                            try await self.store.batchWrite { conn in
                                // A failed check leaves the file to the next scan; it
                                // must not abort unrelated writes in the group commit.
                                guard (try? LocalFileVersion.read(at: itemLocalURL)) == published else { return }
                                let stmt = try conn.cachedStatement("""
                                INSERT INTO items (
                                    root_id, parent_id, name, entry_kind, remote_file_id,
                                    local_mtime, local_size, local_sha256,
                                    base_sha256, base_size,
                                    remote_sha256, remote_size, remote_status,
                                    local_generation, local_status, phase,
                                    local_device, local_inode,
                                    created_at, updated_at
                                ) VALUES (
                                    ?, ?, ?, 'file', ?,
                                    ?, ?, ?,
                                    ?, ?,
                                    ?, ?, 'present',
                                    1, 'present', 'committed',
                                    ?, ?,
                                    ?, ?
                                )
                                ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                                DO NOTHING;
                                """)
                                stmt.bindInt64(rootId, at: 1)
                                stmt.bindInt64(parentItemId, at: 2)
                                stmt.bindText(item.name, at: 3)
                                stmt.bindText(item.id, at: 4)
                                stmt.bindInt64(Int64(mtime), at: 5)
                                stmt.bindInt64(fileSize, at: 6)
                                stmt.bindText(item.sha256Checksum, at: 7)
                                stmt.bindText(item.sha256Checksum, at: 8)
                                stmt.bindInt64(fileSize, at: 9)
                                stmt.bindText(item.sha256Checksum, at: 10)
                                stmt.bindInt64(fileSize, at: 11)
                                let ts = Date().timeIntervalSince1970
                                stmt.bindInt64(published.device, at: 12)
                                stmt.bindInt64(published.inode, at: 13)
                                stmt.bindDouble(ts, at: 14)
                                stmt.bindDouble(ts, at: 15)
                                _ = try stmt.step()
                                stmt.reset()
                                if conn.changes == 1 { receiptApplied.withLock { $0 = true } }
                            }
                            guard receiptApplied.withLock({ $0 }) else {
                                throw SyncEngineError.general("The initial download receipt has expired, retain the existing status: \(item.name)")
                            }

                            progress.filesDownloaded += 1
                            progress.bytesDownloaded += fileSize
                        } catch {
                            self.logger.error("Failed to download file [\(item.name)]: \(error)")
                        }
                    }
                }
            }
        }

        // Start recursive enumeration and downloading
        var traversalError: Error?
        do {
            try await traverseRemote(parentRemoteId: remoteRootId, currentLocalURL: rootURL, parentItemId: rootItemId)
        } catch { traversalError = error }

        // Wait for all in-flight download tasks to complete
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            downloadGroup.notify(queue: .global(qos: .userInitiated)) {
                cont.resume()
            }
        }

        try await store.flush()
        if let traversalError {
            // Preserve a durable recovery route for a partially downloaded bootstrap.
            try await store.write { conn in
                let q = try conn.cachedStatement("INSERT INTO remote_directory_scans(root_id, remote_id, scan_id, state) VALUES (?, ?, ?, 'pending') ON CONFLICT(root_id, remote_id) DO UPDATE SET state = 'pending', page_token = NULL;")
                q.bindInt64(rootId, at: 1)
                q.bindText(remoteRootId, at: 2)
                q.bindText(UUID().uuidString, at: 3)
                _ = try q.step()
                q.reset()
            }
            throw traversalError
        }
        try await store.checkpoint()

        try await store.write { conn in
            let stmt = try conn.cachedStatement("""
            UPDATE roots SET bootstrap_state = 'existingKnown', updated_at = ? WHERE root_id = ?;
            """)
            stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
            stmt.bindInt64(rootId, at: 2)
            _ = try stmt.step()
            stmt.reset()
        }

        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        stats.directoriesCreated = progress.dirsCreated
        stats.filesDownloaded = progress.filesDownloaded
        stats.bytesDownloaded = progress.bytesDownloaded
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }

    /// adopt 1MB Constant memory streaming file SHA-256 and byte length
    static func computeFileSha256(at url: URL) throws -> (sha256Hex: String, fileSize: Int64) {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var ctx = CC_SHA256_CTX()
        CC_SHA256_Init(&ctx)
        var totalSize: Int64 = 0
        let bufferSize = 1024 * 1024 // 1MB Streaming sharding to avoid large files occupying memory
        while let chunk = try handle.read(upToCount: bufferSize), !chunk.isEmpty {
            totalSize += Int64(chunk.count)
            _ = chunk.withUnsafeBytes { ptr in
                CC_SHA256_Update(&ctx, ptr.baseAddress, CC_LONG(chunk.count))
            }
        }
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        CC_SHA256_Final(&digest, &ctx)
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return (hex, totalSize)
    }

    /// Extremely fast calculations in memory Data of SHA-256 String (only for small files, time-consuming for a single time) ~10us)
    static func computeSha256(of data: Data) -> String {
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        data.withUnsafeBytes { ptr in
            _ = CC_SHA256(ptr.baseAddress, CC_LONG(data.count), &digest)
        }
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    /// Execute large files (> 8MB) Resume upload in chunks:
    /// - automatic check operations Whether there are unfinished sessions and breakpoints in the table
    /// - towards Google Drive The server detects the confirmed receipt offset (queryResumableOffset)
    /// - Using constant memory FileHandle Streaming slice reading (Default 8MB,must be 256KB an integer multiple of)
    /// - Each time a chunk is completed, the new offset persist to SQLite operations meter, power outage/Seamless transmission can be resumed after changing threads
    @discardableResult
    func performResumableUpload(
        rootId: Int64,
        itemId: Int64,
        fileURL: URL,
        fileSize: Int64,
        expectedSha256: String,
        remoteId: String,
        parentId: String,
        name: String,
        isUpdate: Bool,
        chunkSize: Int64 = 8 * 1024 * 1024 // 8MB chunked,256KB Integer multiple
    ) async throws -> DriveFile {
        if isUpdate { throw DriveError.unsafeOverwrite(fileId: remoteId) }
        let opId = "resumable_\(remoteId)"
        var sessionURL: URL? = nil
        var currentOffset: Int64 = 0
        var completedFile: DriveFile? = nil

        struct ExistingResumableOp {
            let sessionURL: URL
            let confirmedOffset: Int64
            let expectedSha256: String
            let totalBytes: Int64
        }

        // 1. Query whether there are outstanding breakpoint sessions
        let existingOp: ExistingResumableOp? = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT session_uri, confirmed_offset, expected_sha256, total_bytes
            FROM operations
            WHERE operation_id = ? AND state = 'inFlight';
            """)
            stmt.bindText(opId, at: 1)
            defer { stmt.reset() }
            if try stmt.step(), let uriStr = stmt.columnText(at: 0), let uri = URL(string: uriStr) {
                let off = stmt.columnInt64(at: 1) ?? 0
                let sha = stmt.columnText(at: 2) ?? ""
                let bytes = stmt.columnInt64(at: 3) ?? 0
                return ExistingResumableOp(sessionURL: uri, confirmedOffset: off, expectedSha256: sha, totalBytes: bytes)
            }
            return nil
        }

        if let existing = existingOp {
            // Key: Verify Breakpoint Session Expectations sha256 Whether the file size is strictly consistent with the current file!
            // If the file has been modified in the middle, the old session will be immediately invalidated and reset to prevent incorrect resumption of data from splicing dirty data in the cloud.
            let isSameFile = (existing.totalBytes == fileSize &&
                              existing.expectedSha256.caseInsensitiveCompare(expectedSha256) == .orderedSame)
            if isSameFile {
                // Detect the valid data actually received by the server from the cloud. offset
                do {
                    switch try await client.queryResumableOffset(sessionURL: existing.sessionURL, totalBytes: fileSize) {
                    case .complete(let file):
                        completedFile = file
                        sessionURL = existing.sessionURL
                        currentOffset = fileSize
                    case .incomplete(let serverOffset):
                        guard serverOffset >= 0, serverOffset < fileSize else {
                            throw DriveError.invalidResponse(message: "Resumable upload offset is out of bounds: \(serverOffset)/\(fileSize)")
                        }
                        sessionURL = existing.sessionURL
                        currentOffset = serverOffset
                    case .expired:
                        sessionURL = nil
                        currentOffset = 0
                    }
                } catch {
                    // Ad hoc network/Server-side failures must not discard sessions that may still be valid.
                    throw error
                }
            } else {
                // The file content or size has changed, the breakpoint will be invalidated, and a new upload will be initiated again.
                sessionURL = nil
                currentOffset = 0
            }
        }

        // 2. If there is no valid session, initiate a new Resumable Upload session and persist operation intent
        var activeSessionURL: URL
        if let sURL = sessionURL {
            activeSessionURL = sURL
        } else {
            if isUpdate {
                activeSessionURL = try await client.initiateResumableUpdate(remoteId: remoteId, totalBytes: fileSize)
            } else {
                activeSessionURL = try await client.initiateResumableUpload(name: name, parentId: parentId, remoteId: remoteId, totalBytes: fileSize)
            }
            currentOffset = 0

            let now = Date().timeIntervalSince1970
            let activeSessionURI = activeSessionURL.absoluteString
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                INSERT INTO operations (
                    operation_id, root_id, item_id, operation_type, state,
                    expected_sha256, target_remote_id, target_parent_remote_id,
                    session_uri, confirmed_offset, total_bytes, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, 'uploadResumable', 'inFlight',
                    ?, ?, ?,
                    ?, 0, ?, ?, ?
                )
                ON CONFLICT (operation_id) DO UPDATE SET
                    session_uri = excluded.session_uri,
                    expected_sha256 = excluded.expected_sha256,
                    confirmed_offset = 0,
                    total_bytes = excluded.total_bytes,
                    state = 'inFlight',
                    updated_at = excluded.updated_at;
                """)
                stmt.bindText(opId, at: 1)
                stmt.bindInt64(rootId, at: 2)
                stmt.bindInt64(itemId, at: 3)
                stmt.bindText(expectedSha256, at: 4)
                stmt.bindText(remoteId, at: 5)
                stmt.bindText(parentId, at: 6)
                stmt.bindText(activeSessionURI, at: 7)
                stmt.bindInt64(fileSize, at: 8)
                stmt.bindDouble(now, at: 9)
                stmt.bindDouble(now, at: 10)
                _ = try stmt.step()
                stmt.reset()
            }
        }

        // 3. Streaming chunked read and upload loop
        let fileHandle = try FileHandle(forReadingFrom: fileURL)
        defer { try? fileHandle.close() }

        // Get the initial file timestamp and size for detecting concurrent modifications in a chunked transfer loop
        let initialAttrs = try? FileManager.default.attributesOfItem(atPath: fileURL.path)
        let initialMtime = initialAttrs?[.modificationDate] as? Date

        var finalDriveFile: DriveFile? = completedFile
        var repeatedNoProgress = false

        while currentOffset < fileSize {
            // Detect whether the file is modified concurrently in the middle of uploading chunks
            if let currentAttrs = try? FileManager.default.attributesOfItem(atPath: fileURL.path) {
                let currentDiskSize = (currentAttrs[.size] as? NSNumber)?.int64Value ?? fileSize
                let currentMtime = currentAttrs[.modificationDate] as? Date
                if currentDiskSize != fileSize || (initialMtime != nil && currentMtime != initialMtime) {
                    // If the file is modified midway, the upload will be terminated immediately and marked operation for failed,Prevent corrupted data from being spliced and sent to the cloud
                    let now = Date().timeIntervalSince1970
                    try? await store.write { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE operations SET state = 'failed', updated_at = ? WHERE operation_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindText(opId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                    throw DriveError.fileModifiedDuringUpload(path: fileURL.path)
                }
            }

            try fileHandle.seek(toOffset: UInt64(currentOffset))
            let bytesToRead = min(chunkSize, fileSize - currentOffset)
            guard let chunkData = try fileHandle.read(upToCount: Int(bytesToRead)), !chunkData.isEmpty else {
                throw CocoaError(.fileReadUnknown)
            }

            let result = try await client.uploadResumableChunk(
                sessionURL: activeSessionURL,
                chunkData: chunkData,
                offset: currentOffset,
                totalBytes: fileSize
            )

            let previousOffset = currentOffset
            switch result {
            case .incomplete(let confirmedOffset):
                guard confirmedOffset >= previousOffset,
                      confirmedOffset <= previousOffset + Int64(chunkData.count),
                      confirmedOffset < fileSize else {
                    throw DriveError.invalidResponse(message: "Resumable upload confirmed an invalid offset: \(confirmedOffset); sent range \(previousOffset)-\(previousOffset + Int64(chunkData.count) - 1)")
                }
                if confirmedOffset == previousOffset {
                    guard !repeatedNoProgress else {
                        throw DriveError.invalidResponse(message: "Resumable upload repeatedly acknowledged no new bytes")
                    }
                    repeatedNoProgress = true
                } else {
                    repeatedNoProgress = false
                }
                currentOffset = confirmedOffset
            case .complete(let file):
                currentOffset = fileSize
                finalDriveFile = file
            case .expired:
                activeSessionURL = isUpdate
                    ? try await client.initiateResumableUpdate(remoteId: remoteId, totalBytes: fileSize)
                    : try await client.initiateResumableUpload(name: name, parentId: parentId, remoteId: remoteId, totalBytes: fileSize)
                currentOffset = 0
                repeatedNoProgress = false
            }
            let confirmedBytes = max(0, currentOffset - previousOffset)
            if confirmedBytes > 0 {
                self.monitor.reportUploadProgress(id: fileURL.path, additionalBytes: confirmedBytes)
            }

            // Each time a block is completed, it is immediately recorded in the database as confirmed offset with status
            let recordedOffset = currentOffset
            let now = Date().timeIntervalSince1970
            let activeSessionURI = activeSessionURL.absoluteString
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                UPDATE operations SET
                    session_uri = ?,
                    confirmed_offset = ?,
                    state = 'inFlight',
                    updated_at = ?
                WHERE operation_id = ?;
                """)
                stmt.bindText(activeSessionURI, at: 1)
                stmt.bindInt64(recordedOffset, at: 2)
                stmt.bindDouble(now, at: 3)
                stmt.bindText(opId, at: 4)
                _ = try stmt.step()
                stmt.reset()
            }

            if finalDriveFile != nil {
                break
            }
        }

        let file = try await (finalDriveFile != nil ? finalDriveFile! : client.getFile(remoteId: remoteId))
        guard file.sizeBytes == fileSize else {
            let now = Date().timeIntervalSince1970
            try? await store.write { conn in
                let stmt = try conn.cachedStatement("UPDATE operations SET state = 'failed', updated_at = ? WHERE operation_id = ?;")
                stmt.bindDouble(now, at: 1)
                stmt.bindText(opId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
            throw DriveError.sizeMismatch(expected: fileSize, actual: file.sizeBytes)
        }
        guard let checksum = file.sha256Checksum,
              checksum.caseInsensitiveCompare(expectedSha256) == .orderedSame else {
            // The hash calculated by the final stitching in the cloud does not match the expected one (indicating tampering or data corruption during transmission)
            let now = Date().timeIntervalSince1970
            try? await store.write { conn in
                let stmt = try conn.cachedStatement("""
                UPDATE operations SET state = 'failed', updated_at = ? WHERE operation_id = ?;
                """)
                stmt.bindDouble(now, at: 1)
                stmt.bindText(opId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
            throw DriveError.checksumMismatch(expected: expectedSha256, actual: file.sha256Checksum)
        }
        let now = Date().timeIntervalSince1970
        try await store.write { conn in
            let stmt = try conn.cachedStatement("""
            UPDATE operations SET state = 'completed', confirmed_offset = ?, updated_at = ? WHERE operation_id = ?;
            """)
            stmt.bindInt64(fileSize, at: 1)
            stmt.bindDouble(now, at: 2)
            stmt.bindText(opId, at: 3)
            _ = try stmt.step()
            stmt.reset()
        }
        return file
    }

    // MARK: - mode 3:Existing root directory incremental bidirectional synchronization (syncIncremental)

    /// Perform a two-way incremental synchronization on an existing sync root
    /// Combined with local DirectoryScanner Quick scan with Google Drive Changes Incremental changes, consisting of Reconciler Drive tripartite decision-making
    @discardableResult
    public func syncIncremental(
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        try await withRootSyncLock(localPath: localPath) {
            try await self.syncIncrementalUnlocked(
                localPath: localPath,
                remoteRootId: remoteRootId,
                maxConcurrency: maxConcurrency,
                onProgress: onProgress
            )
        }
    }

    private func syncIncrementalUnlocked(
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?
    ) async throws -> SyncStats {
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath

        // Find rootId with rootItemId
        let rootInfo: (rootId: Int64, rootItemId: Int64)? = try await store.read { conn in
            let stmt = try conn.cachedStatement("SELECT root_id FROM roots WHERE account_id = 'default' AND remote_root_id = ?;")
            stmt.bindText(remoteRootId, at: 1)
            guard try stmt.step(), let rId = stmt.columnInt64(at: 0) else {
                stmt.reset()
                return nil
            }
            stmt.reset()

            let itemStmt = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL AND is_tombstone = 0;")
            itemStmt.bindInt64(rId, at: 1)
            guard try itemStmt.step(), let rItemId = itemStmt.columnInt64(at: 0) else {
                itemStmt.reset()
                return nil
            }
            itemStmt.reset()
            return (rId, rItemId)
        }

        guard let rootInfo else {
            throw NSError(domain: "SyncEngine", code: 20, userInfo: [NSLocalizedDescriptionKey: "No matching sync root was found. Run initial sync first: \(remoteRootId)"])
        }

        return try await syncIncrementalUnlocked(
            rootId: rootInfo.rootId,
            rootItemId: rootInfo.rootItemId,
            localPath: resolvedLocalPath,
            remoteRootId: remoteRootId,
            maxConcurrency: maxConcurrency,
            onProgress: onProgress
        )
    }

    /// Perform bidirectional incremental synchronization
    @discardableResult
    public func syncIncremental(
        rootId: Int64,
        rootItemId: Int64,
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        try await withRootSyncLock(localPath: localPath) {
            try await self.syncIncrementalUnlocked(
                rootId: rootId,
                rootItemId: rootItemId,
                localPath: localPath,
                remoteRootId: remoteRootId,
                maxConcurrency: maxConcurrency,
                onProgress: onProgress
            )
        }
    }

    private func syncIncrementalUnlocked(
        rootId: Int64,
        rootItemId: Int64,
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)
        let now = Date().timeIntervalSince1970

        // -------------------------------------------------------------
        // 0. Root directory anti-proliferation security check (§7.2, §9.1)
        // -------------------------------------------------------------
        // A. Local root directory verification: If the local root directory disappears, deletion and diffusion are strictly prohibited, and an error will be reported and terminated immediately.
        var isDir: ObjCBool = false
        let localExists = FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir)
        guard localExists && isDir.boolValue else {
            logger.error("[Sync] The local sync root is missing or invalid: \(resolvedLocalPath). Stopping sync to protect remote files.")
            throw SyncEngineError.localRootNotFound(path: resolvedLocalPath)
        }

        // B. Remote root directory verification: If the remote root directory is moved to the recycle bin or completely deleted, deletion diffusion is strictly prohibited and an error will be reported and terminated immediately.
        let remoteRoot: DriveFile
        do {
            remoteRoot = try await client.getFile(remoteId: remoteRootId)
        } catch let error as DriveError {
            switch error {
            case .notFound:
                logger.error("[Sync] The remote sync root does not exist (404): \(remoteRootId). Stopping sync to protect local files.")
                throw SyncEngineError.remoteRootLost(remoteId: remoteRootId, reason: "notFound")
            default:
                throw error
            }
        } catch {
            let nsError = error as NSError
            if (nsError.domain == "SyncEngine" || nsError.domain == "DriveError") && nsError.code == 404 {
                logger.error("[Sync] The remote sync root does not exist (404): \(remoteRootId). Stopping sync to protect local files.")
                throw SyncEngineError.remoteRootLost(remoteId: remoteRootId, reason: "notFound")
            }
            throw error
        }

        if remoteRoot.trashed == true {
            logger.error("[Sync] The remote sync root is trashed: \(remoteRootId). Stopping sync to protect local files.")
            throw SyncEngineError.remoteRootLost(remoteId: remoteRootId, reason: "trashed")
        }
        guard remoteRoot.isDirectory else {
            logger.error("[Sync] The remote sync root is not a valid directory: \(remoteRootId). Stopping sync to protect local files.")
            throw SyncEngineError.remoteRootLost(remoteId: remoteRootId, reason: "notDirectory")
        }

        let downloadDirectory = try await downloadStagingDirectory(remoteRootID: remoteRootId, localRoot: rootURL)

        // Recover before Changes/scanning can mistake intermediate publications for new input.
        let pendingConflicts = try await ConflictOperation.pending(store: store, rootID: rootId)
        let recoveredConflicts = pendingConflicts.count
        if !pendingConflicts.isEmpty {
            try await withThrowingTaskGroup(of: Void.self) { group in
                let limit = max(1, min(64, maxConcurrency))
                for (index, op) in pendingConflicts.enumerated() {
                    if index >= limit { try await group.next() }
                    group.addTask { try await self.resolveConflict(op, temporaryDirectory: downloadDirectory) }
                }
                try await group.waitForAll()
            }
        }

        let remoteChanges = RemoteChanges(store: store, client: client, rootID: rootId,
            remoteRootID: remoteRootId, rootURL: rootURL)
        try await remoteChanges.consume()
        let remoteGate = try await remoteChanges.gate()

        // -------------------------------------------------------------
        // 1. Build directory topology mapping (Fast maintenance in memory,O(1) Path and parent resolution)
        // -------------------------------------------------------------
        final class DirectoryContext: @unchecked Sendable {
            private var dirPaths: [Int64: String] = [:]         // item_id -> relPath
            private var dirRemoteIds: [Int64: String] = [:]     // item_id -> remote_file_id
            private var dirIdByRemote: [String: Int64] = [:]    // remote_file_id -> item_id
            private var dirIdByRelPath: [String: Int64] = [:]   // relPath -> item_id
            private var readyDirectories: Set<Int64> = []
            private var lock = os_unfair_lock()

            init(rootItemId: Int64, remoteRootId: String) {
                readyDirectories.insert(rootItemId)
                dirPaths[rootItemId] = ""
                dirRemoteIds[rootItemId] = remoteRootId
                dirIdByRemote[remoteRootId] = rootItemId
                dirIdByRelPath[""] = rootItemId
            }

            func register(itemId: Int64, parentItemId: Int64, name: String, remoteId: String, remotePresent: Bool = true) {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                if remotePresent && readyDirectories.contains(parentItemId) {
                    readyDirectories.insert(itemId)
                } else { readyDirectories.remove(itemId) }
                let parentPath = dirPaths[parentItemId] ?? ""
                let relPath = parentPath.isEmpty ? name : "\(parentPath)/\(name)"
                dirPaths[itemId] = relPath
                dirRemoteIds[itemId] = remoteId
                dirIdByRemote[remoteId] = itemId
                dirIdByRelPath[relPath] = itemId
            }

            func isReady(_ itemID: Int64) -> Bool {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return readyDirectories.contains(itemID)
            }

            func getRelPath(for itemId: Int64) -> String? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return dirPaths[itemId]
            }

            func getRemoteId(for itemId: Int64) -> String? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return dirRemoteIds[itemId]
            }

            func getItemId(byRemote remoteId: String) -> Int64? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return dirIdByRemote[remoteId]
            }

            func getItemId(byRelPath path: String) -> Int64? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return dirIdByRelPath[path]
            }
        }

        let dirContext = DirectoryContext(rootItemId: rootItemId, remoteRootId: remoteRootId)

        // from SQLite Load all known directories
        try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT item_id, parent_id, name, remote_file_id, remote_status
            FROM items
            WHERE root_id = ? AND entry_kind = 'directory' AND is_tombstone = 0 AND parent_id IS NOT NULL;
            """)
            stmt.bindInt64(rootId, at: 1)
            var rawDirs: [(id: Int64, parentId: Int64, name: String, remoteId: String, present: Bool)] = []
            while try stmt.step() {
                if let id = stmt.columnInt64(at: 0),
                   let parentId = stmt.columnInt64(at: 1),
                   let name = stmt.columnText(at: 2),
                   let rId = stmt.columnText(at: 3) {
                    rawDirs.append((id, parentId, name, rId, stmt.columnText(at: 4) == "present"))
                }
            }
            stmt.reset()

            // Register in topological order dirContext
            var registered = Set<Int64>([rootItemId])
            var remaining = rawDirs
            while !remaining.isEmpty {
                let countBefore = remaining.count
                remaining.removeAll { dir in
                    if registered.contains(dir.parentId) {
                        dirContext.register(itemId: dir.id, parentItemId: dir.parentId, name: dir.name, remoteId: dir.remoteId, remotePresent: dir.present)
                        registered.insert(dir.id)
                        return true
                    }
                    return false
                }
                if remaining.count == countBefore {
                    // If there are orphan items, search according to the root directory.
                    for dir in remaining {
                        dirContext.register(itemId: dir.id, parentItemId: rootItemId, name: dir.name, remoteId: dir.remoteId, remotePresent: false)
                    }
                    break
                }
            }
        }

        struct DirtyRecord: Sendable {
            let itemId: Int64
            let parentId: Int64
            let name: String
            let remoteFileId: String?
            let entryKind: String
            let baseline: ItemBaseline?
            let local: LocalObservation?
            let remote: RemoteObservation?
            let pendingCreate: DurableCreateIntent?
            let localGeneration: Int64
            let remoteGeneration: Int64
            let dirtyGeneration: Int64
            let localDevice: Int64?
            let localInode: Int64?
            let localMtime: Int64?
        }

        @Sendable func loadDirtyItems(_ ids: [Int64]? = nil) async throws -> [DirtyRecord] {
            // CROSS JOIN fixes the driving side to this window. An IN filter can otherwise
            // choose idx_items_dirty and rescan every remaining dirty item for each batch.
            let source = ids == nil ? "items" : "json_each(?2) selected CROSS JOIN items ON items.item_id = selected.value"
            let encodedIDs = ids.map { "[" + $0.map(String.init).joined(separator: ",") + "]" }
            return try await store.read { conn in
                let stmt = try conn.cachedStatement("""
                SELECT items.item_id, items.parent_id, items.name, items.remote_file_id, items.entry_kind,
                       items.base_sha256, items.base_size,
                       items.local_sha256, items.local_size, items.local_status,
                       items.remote_sha256, items.remote_size, items.remote_status,
                       op.operation_id, op.target_remote_id, op.target_parent_remote_id,
                       op.expected_local_generation, op.expected_sha256, op.total_bytes,
                       items.local_generation, items.remote_generation, items.dirty_generation,
                       items.local_device, items.local_inode, items.local_mtime
                FROM \(source)
                LEFT JOIN operations op ON op.operation_id = (
                    SELECT candidate.operation_id
                    FROM operations candidate
                    WHERE candidate.item_id = items.item_id
                      AND candidate.operation_type IN ('createDirectory', 'uploadMultipart', 'createResumableUpload')
                      AND candidate.state IN ('ready', 'inFlight', 'verify', 'unknownOutcome')
                    ORDER BY candidate.created_at DESC
                    LIMIT 1
                )
                WHERE items.root_id = ?1 AND items.dirty_generation > 0 AND items.is_tombstone = 0;
                """)
                stmt.bindInt64(rootId, at: 1)
                if let encodedIDs { stmt.bindText(encodedIDs, at: 2) }
                var records: [DirtyRecord] = []
                while try stmt.step() {
                    guard let iId = stmt.columnInt64(at: 0),
                          let pId = stmt.columnInt64(at: 1),
                          let name = stmt.columnText(at: 2) else { continue }
                    let rFileId = stmt.columnText(at: 3)
                    let kind = stmt.columnText(at: 4) ?? "file"

                    let bSha = stmt.columnText(at: 5)
                    let bSize = stmt.columnInt64(at: 6)
                    let baseline: ItemBaseline? = (bSha != nil) ? ItemBaseline(sha256: bSha, size: bSize) : nil

                    let lSha = stmt.columnText(at: 7)
                    let lSize = stmt.columnInt64(at: 8)
                    let lStatusStr = stmt.columnText(at: 9) ?? "unknown"
                    let lStatus = LocalObservation.Status(rawValue: lStatusStr) ?? .unknown
                    let local = LocalObservation(status: lStatus, sha256: lSha, size: lSize)

                    let rSha = stmt.columnText(at: 10)
                    let rSize = stmt.columnInt64(at: 11)
                    let rStatusStr = stmt.columnText(at: 12) ?? "unknown"
                    let rStatus = RemoteObservation.Status(rawValue: rStatusStr) ?? .unknown
                    let remote = RemoteObservation(status: rStatus, sha256: rSha, size: rSize)

                    let pendingCreate: DurableCreateIntent?
                    if let operationID = stmt.columnText(at: 13),
                       let targetRemoteID = stmt.columnText(at: 14),
                       let targetParentRemoteID = stmt.columnText(at: 15) {
                        pendingCreate = DurableCreateIntent(
                            operationID: operationID,
                            itemID: iId,
                            targetRemoteID: targetRemoteID,
                            targetParentRemoteID: targetParentRemoteID,
                            expectedLocalGeneration: stmt.columnInt64(at: 16) ?? 0,
                            expectedSHA256: stmt.columnText(at: 17),
                            totalBytes: stmt.columnInt64(at: 18)
                        )
                    } else {
                        pendingCreate = nil
                    }

                    records.append(DirtyRecord(
                        itemId: iId, parentId: pId, name: name, remoteFileId: rFileId,
                        entryKind: kind, baseline: baseline, local: local, remote: remote,
                        pendingCreate: pendingCreate,
                        localGeneration: stmt.columnInt64(at: 19) ?? 0,
                        remoteGeneration: stmt.columnInt64(at: 20) ?? 0,
                        dirtyGeneration: stmt.columnInt64(at: 21) ?? 0,
                        localDevice: stmt.columnInt64(at: 22),
                        localInode: stmt.columnInt64(at: 23),
                        localMtime: stmt.columnInt64(at: 24)
                    ))
                }
                stmt.reset()
                return records
            }
        }

        let effectiveSyncConcurrency = max(1, min(64, maxConcurrency))
        let syncSemaphore = AsyncSemaphore(count: effectiveSyncConcurrency)
        let syncGroup = DispatchGroup()

        final class ActionTracker: @unchecked Sendable {
            struct Counts {
                var uploaded = 0
                var bytesUp: Int64 = 0
                var downloaded = 0
                var bytesDown: Int64 = 0
                var deleted = 0
            }
            let counts = OSAllocatedUnfairLock(initialState: Counts())
            var uploaded: Int { counts.withLock { $0.uploaded } }
            var bytesUp: Int64 { counts.withLock { $0.bytesUp } }
            var downloaded: Int { counts.withLock { $0.downloaded } }
            var bytesDown: Int64 { counts.withLock { $0.bytesDown } }
            var deleted: Int { counts.withLock { $0.deleted } }
            let conflicts = OSAllocatedUnfairLock(initialState: 0)
            let failures = OSAllocatedUnfairLock(initialState: 0)
        }
        let actionTracker = ActionTracker()

        // A failed early attempt remains dirty, but is not retried again by the final sweep.
        let scheduled = OSAllocatedUnfairLock(initialState: Set<Int64>())
        let startedTransfers = OSAllocatedUnfairLock(initialState: false)
        @Sendable func drainTransfers() async {
            // Keep no-change scans free of the new streaming phase's queue hops.
            guard startedTransfers.withLock({ $0 }) else { return }
            await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                syncGroup.notify(queue: .global(qos: .userInitiated)) { cont.resume() }
            }
        }

        @Sendable func acquireTransfer() async throws {
            try Task.checkCancellation()
            await syncSemaphore.wait()
            if Task.isCancelled {
                syncSemaphore.signal()
                throw CancellationError()
            }
            startedTransfers.withLock { $0 = true }
        }

        @Sendable func scheduleFiles(_ fileItems: [DirtyRecord], duringScan: Bool) async throws {
            var receipts: [@Sendable (SQLiteConnection) throws -> Void] = []
            for item in fileItems {
                let parent = dirContext.getRelPath(for: item.parentId) ?? ""
                let path = parent.isEmpty ? item.name : "\(parent)/\(item.name)"
                guard !remoteGate.blocks(path) else { continue }
                // Pending parent creation must recover before any child request is sent.
                if duringScan && !dirContext.isReady(item.parentId) { continue }
                let decision: ReconcileDecision
                if item.pendingCreate != nil {
                    decision = .upload(reason: "Restore persistent small file creation intent")
                } else {
                    decision = Reconciler.decide(baseline: item.baseline, local: item.local, remote: item.remote)
                }

                // Deletions and conflict copies require the completed local inventory.
                if duringScan {
                    switch decision {
                    case .upload, .download, .keepModified, .matchUpdateBaseline, .unchanged: break
                    default: continue
                    }
                }
                guard !scheduled.withLock({ $0.contains(item.itemId) }) else { continue }
                if duringScan {
                    switch decision {
                    case .upload, .download, .keepModified:
                        scheduled.withLock { _ = $0.insert(item.itemId) }
                    default: break
                    }
                }
                try Task.checkCancellation()
                switch decision {
                case .upload, .keepModified(preferLocal: true):
                    let upBytes = item.local?.size ?? 0
                    notifier.addDiscovered(files: 1, bytes: upBytes)
                    try await acquireTransfer()
                    self.monitor.enqueueUpload(id: item.name, name: item.name, totalBytes: upBytes)
                    syncGroup.enter()
                    Task {
                        var createIntent = item.pendingCreate
                        defer {
                            self.monitor.finishUpload(id: item.name)
                            syncSemaphore.signal()
                            syncGroup.leave()
                            notifier.addCompleted(files: 1, bytes: upBytes)
                        }

                        do {
                            let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                            let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                            let localFileURL = rootURL.appendingPathComponent(relPath)
                            let remoteParentId = dirContext.getRemoteId(for: item.parentId) ?? remoteRootId

                            if createIntent == nil, let remoteID = item.remoteFileId {
                                throw DriveError.unsafeOverwrite(fileId: remoteID)
                            }

                            // If the remote parent directory has been moved to the recycle bin before, the parent directory will be automatically restored before uploading the child files.
                            if let parentRId = dirContext.getRemoteId(for: item.parentId) {
                                let isParentTrashed: Bool = (try? await self.store.read { conn in
                                    let stmt = try conn.cachedStatement("SELECT remote_status FROM items WHERE item_id = ?;")
                                    stmt.bindInt64(item.parentId, at: 1)
                                    defer { stmt.reset() }
                                    if try stmt.step(), let st = stmt.columnText(at: 0) {
                                        return st == "trashed"
                                    }
                                    return false
                                }) ?? false

                                if isParentTrashed {
                                    do {
                                        try await self.client.untrash(remoteId: parentRId)
                                        try await self.store.write { conn in
                                            let stmt = try conn.cachedStatement("UPDATE items SET remote_status = 'present', updated_at = ? WHERE item_id = ?;")
                                            stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                                            stmt.bindInt64(item.parentId, at: 2)
                                            _ = try stmt.step()
                                        }
                                    } catch {
                                        self.logger.error("Failed to restore remote parent directory [\(parentRId)]: \(error)")
                                    }
                                }
                            }

                            let fSize: Int64
                            let sha256Hex: String
                            if let s = item.local?.sha256, let sz = item.local?.size {
                                sha256Hex = s
                                fSize = sz
                            } else {
                                let res = try SyncEngine.computeFileSha256(at: localFileURL)
                                sha256Hex = res.sha256Hex
                                fSize = res.fileSize
                            }
                            self.monitor.startUpload(id: item.name, name: item.name, totalBytes: fSize)

                            let input = try StableUploadInput.capture(at: localFileURL)
                            guard input.size == fSize, input.sha256 == sha256Hex else {
                                throw DriveError.fileModifiedDuringUpload(path: localFileURL.path)
                            }
                            let mtime = input.version.mtime
                            let dev = input.version.device
                            let ino = input.version.inode
                            let uploadedFile: DriveFile
                            if fSize > 8 * 1024 * 1024 {
                                let target: DurableCreateIntent
                                if let pending = createIntent {
                                    guard let expectedSHA256 = pending.expectedSHA256,
                                          expectedSHA256.caseInsensitiveCompare(sha256Hex) == .orderedSame,
                                          pending.totalBytes == fSize else {
                                        throw SyncEngineError.general("Unfinished large file creation intent input changed: \(item.name)")
                                    }
                                    target = pending
                                } else {
                                    let newRemoteId = try await self.idPool.nextId()
                                    target = try await DurableCreateIntentStore.prepareFileUpload(
                                        store: self.store,
                                        rootID: rootId,
                                        itemID: item.itemId,
                                        parentItemID: item.parentId,
                                        name: item.name,
                                        targetParentRemoteID: remoteParentId,
                                        candidateRemoteID: newRemoteId,
                                        device: dev,
                                        inode: ino,
                                        mtime: Int64(mtime),
                                        size: fSize,
                                        sha256: sha256Hex,
                                        transport: .resumable
                                    )
                                    createIntent = target
                                }
                                uploadedFile = try await self.performResumableUpload(
                                    rootId: rootId,
                                    itemId: target.itemID,
                                    fileURL: input.fileURL,
                                    fileSize: fSize,
                                    expectedSha256: sha256Hex,
                                    remoteId: target.targetRemoteID,
                                    parentId: target.targetParentRemoteID,
                                    name: item.name,
                                    isUpdate: false
                                )
                            } else if let pending = createIntent {
                                guard let expectedSHA256 = pending.expectedSHA256,
                                      expectedSHA256.caseInsensitiveCompare(sha256Hex) == .orderedSame,
                                      pending.totalBytes == fSize else {
                                    throw SyncEngineError.general("Unfinished small file creation intent input has changed: \(item.name)")
                                }
                                guard let fileData = input.data else {
                                    throw SyncEngineError.general("Stable small-file input is missing its in-memory body: \(item.name)")
                                }
                                uploadedFile = try await self.client.uploadMultipart(
                                    name: item.name,
                                    parentId: pending.targetParentRemoteID,
                                    remoteId: pending.targetRemoteID,
                                    content: fileData,
                                    expectedSha256: sha256Hex
                                )
                            } else if let existingRemoteId = item.remoteFileId {
                                guard let fileData = input.data else {
                                    throw SyncEngineError.general("Stable small-file input is missing its in-memory body: \(item.name)")
                                }
                                uploadedFile = try await self.client.updateMultipart(
                                    remoteId: existingRemoteId,
                                    content: fileData,
                                    expectedSha256: sha256Hex
                                )
                            } else {
                                guard let fileData = input.data else {
                                    throw SyncEngineError.general("Stable small-file input is missing its in-memory body: \(item.name)")
                                }
                                let newRemoteId = try await self.idPool.nextId()
                                let prepared = try await DurableCreateIntentStore.prepareFileUpload(
                                    store: self.store,
                                    rootID: rootId,
                                    itemID: item.itemId,
                                    parentItemID: item.parentId,
                                    name: item.name,
                                    targetParentRemoteID: remoteParentId,
                                    candidateRemoteID: newRemoteId,
                                    device: dev,
                                    inode: ino,
                                    mtime: Int64(mtime),
                                    size: fSize,
                                    sha256: sha256Hex,
                                    transport: .multipart
                                )
                                createIntent = prepared
                                uploadedFile = try await self.client.uploadMultipart(
                                    name: item.name,
                                    parentId: prepared.targetParentRemoteID,
                                    remoteId: prepared.targetRemoteID,
                                    content: fileData,
                                    expectedSha256: sha256Hex
                                )
                            }

                            let completedCreateIntent = createIntent
                            try input.version.validate(at: localFileURL)
                            let receiptApplied = OSAllocatedUnfairLock(initialState: false)
                            try await self.store.batchWrite { conn in
                                guard (try? LocalFileVersion.read(at: localFileURL)) == input.version else { return }
                                let stmt = try conn.cachedStatement("""
                                UPDATE items SET
                                    remote_file_id = ?,
                                    local_device = ?,
                                    local_inode = ?,
                                    local_mtime = ?,
                                    local_size = ?,
                                    base_sha256 = ?,
                                    base_size = ?,
                                    remote_sha256 = ?,
                                    remote_size = ?,
                                    remote_status = 'present',
                                    phase = 'committed',
                                    dirty_generation = 0,
                                    updated_at = ?
                                WHERE item_id = ? AND local_generation = ?
                                    AND remote_generation = ? AND dirty_generation = ? AND is_tombstone = 0;
                                """)
                                stmt.bindText(uploadedFile.id, at: 1)
                                stmt.bindInt64(dev, at: 2)
                                stmt.bindInt64(ino, at: 3)
                                stmt.bindInt64(Int64(mtime), at: 4)
                                stmt.bindInt64(fSize, at: 5)
                                stmt.bindText(sha256Hex, at: 6)
                                stmt.bindInt64(fSize, at: 7)
                                stmt.bindText(sha256Hex, at: 8)
                                stmt.bindInt64(fSize, at: 9)
                                stmt.bindDouble(now, at: 10)
                                stmt.bindInt64(item.itemId, at: 11)
                                stmt.bindInt64(item.localGeneration, at: 12)
                                stmt.bindInt64(item.remoteGeneration, at: 13)
                                stmt.bindInt64(item.dirtyGeneration, at: 14)
                                _ = try stmt.step()
                                stmt.reset()
                                guard conn.changes == 1 else { return }
                                receiptApplied.withLock { $0 = true }
                                if let createIntent = completedCreateIntent {
                                    try DurableCreateIntentStore.completeOperation(
                                        conn: conn,
                                        operationID: createIntent.operationID,
                                        now: now
                                    )
                                }
                            }

                            guard receiptApplied.withLock({ $0 }) else {
                                throw SyncEngineError.general("The upload receipt is stale; the newer generation remains pending: \(item.name)")
                            }

                            scheduled.withLock { _ = $0.remove(item.itemId) }
                            actionTracker.counts.withLock { $0.uploaded += 1; $0.bytesUp += fSize }
                        } catch {
                            actionTracker.failures.withLock { $0 += 1 }
                            if let createIntent {
                                await DurableCreateIntentStore.markUnknownOutcome(
                                    store: self.store,
                                    operationID: createIntent.operationID,
                                    error: error
                                )
                            }
                            if case DriveError.unsafeOverwrite = error {
                                try? await self.store.batchWrite { conn in
                                    let stmt = try conn.cachedStatement("""
                                    UPDATE items SET phase = 'blocked'
                                    WHERE item_id = ? AND local_generation = ? AND remote_generation = ? AND dirty_generation = ?;
                                    """)
                                    stmt.bindInt64(item.itemId, at: 1)
                                    stmt.bindInt64(item.localGeneration, at: 2)
                                    stmt.bindInt64(item.remoteGeneration, at: 3)
                                    stmt.bindInt64(item.dirtyGeneration, at: 4)
                                    _ = try stmt.step()
                                    stmt.reset()
                                }
                            }
                            self.logger.error("Incremental upload failed [\(item.name)]: \(error)")
                        }
                    }

                case .download, .keepModified(preferLocal: false):
                    let downId = item.remoteFileId ?? item.name
                    let downSize = item.remote?.size ?? 0
                    notifier.addDiscovered(files: 1, bytes: downSize)
                    try await acquireTransfer()
                    self.monitor.enqueueDownload(id: downId, name: item.name, totalBytes: downSize)
                    syncGroup.enter()
                    Task {
                        defer {
                            self.monitor.finishDownload(id: downId)
                            syncSemaphore.signal()
                            syncGroup.leave()
                            notifier.addCompleted(files: 1, bytes: downSize)
                        }

                        do {
                            guard let remoteFileId = item.remoteFileId else { return }
                            let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                            let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                            let localFileURL = rootURL.appendingPathComponent(relPath)

                            // Make sure the local target parent directory exists
                            try? FileManager.default.createDirectory(at: localFileURL.deletingLastPathComponent(), withIntermediateDirectories: true)

                            let expectedDestination = try LocalFileVersion.read(at: localFileURL)
                            if item.local?.status == .present {
                                guard let expectedDestination,
                                      expectedDestination.device == item.localDevice,
                                      expectedDestination.inode == item.localInode,
                                      expectedDestination.mtime == item.localMtime,
                                      expectedDestination.size == item.local?.size else {
                                    throw DriveError.fileModifiedDuringUpload(path: localFileURL.path)
                                }
                            } else if expectedDestination != nil {
                                throw DriveError.fileModifiedDuringUpload(path: localFileURL.path)
                            }
                            self.monitor.startDownload(id: downId, name: item.name, totalBytes: downSize)
                            let published = try await self.client.downloadFileSafely(
                                remoteId: remoteFileId,
                                destinationURL: localFileURL,
                                expectedSha256: item.remote?.sha256,
                                expectedDestination: expectedDestination,
                                temporaryDirectory: downloadDirectory,
                                beforePublish: {
                                    let current = try await self.store.read { conn in
                                        let stmt = try conn.cachedStatement("""
                                        SELECT 1 FROM items WHERE item_id = ? AND local_generation = ?
                                            AND remote_generation = ? AND dirty_generation = ? AND is_tombstone = 0;
                                        """)
                                        defer { stmt.reset() }
                                        stmt.bindInt64(item.itemId, at: 1)
                                        stmt.bindInt64(item.localGeneration, at: 2)
                                        stmt.bindInt64(item.remoteGeneration, at: 3)
                                        stmt.bindInt64(item.dirtyGeneration, at: 4)
                                        return try stmt.step()
                                    }
                                    guard current else {
                                        throw SyncEngineError.general("Download plan has expired, keep local files: \(item.name)")
                                    }
                                },
                                onProgress: { delta in
                                    self.monitor.reportDownloadProgress(id: downId, additionalBytes: delta)
                                }
                            )

                            let fSize = published.size
                            let mtime = published.mtime

                            let receiptApplied = OSAllocatedUnfairLock(initialState: false)
                            try await self.store.batchWrite { conn in
                                guard (try? LocalFileVersion.read(at: localFileURL)) == published else { return }
                                let stmt = try conn.cachedStatement("""
                                UPDATE items SET
                                    local_sha256 = remote_sha256,
                                    local_size = remote_size,
                                    local_mtime = ?,
                                    local_device = ?,
                                    local_inode = ?,
                                    local_status = 'present',
                                    remote_status = 'present',
                                    base_sha256 = remote_sha256,
                                    base_size = remote_size,
                                    phase = 'committed',
                                    dirty_generation = 0,
                                    updated_at = ?
                                WHERE item_id = ? AND local_generation = ?
                                    AND remote_generation = ? AND dirty_generation = ? AND is_tombstone = 0;
                                """)
                                stmt.bindInt64(Int64(mtime), at: 1)
                                stmt.bindInt64(published.device, at: 2)
                                stmt.bindInt64(published.inode, at: 3)
                                stmt.bindDouble(Date().timeIntervalSince1970, at: 4)
                                stmt.bindInt64(item.itemId, at: 5)
                                stmt.bindInt64(item.localGeneration, at: 6)
                                stmt.bindInt64(item.remoteGeneration, at: 7)
                                stmt.bindInt64(item.dirtyGeneration, at: 8)
                                _ = try stmt.step()
                                stmt.reset()
                                guard conn.changes == 1 else { return }
                                receiptApplied.withLock { $0 = true }
                            }
                            guard receiptApplied.withLock({ $0 }) else {
                                throw SyncEngineError.general("The download receipt is stale; the newer generation remains pending: \(item.name)")
                            }

                            scheduled.withLock { _ = $0.remove(item.itemId) }
                            actionTracker.counts.withLock { $0.downloaded += 1; $0.bytesDown += fSize }
                        } catch {
                            actionTracker.failures.withLock { $0 += 1 }
                            self.logger.error("Incremental download failed [\(item.name)]: \(error)")
                        }
                    }

                case .matchUpdateBaseline(let sha, let size):
                    receipts.append { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE items SET
                            base_sha256 = ?,
                            base_size = ?,
                            remote_status = 'present',
                            local_status = 'present',
                            phase = 'committed',
                            dirty_generation = 0,
                            updated_at = ?
                        WHERE item_id = ? AND local_generation = ? AND remote_generation = ? AND dirty_generation = ?;
                        """)
                        stmt.bindText(sha, at: 1)
                        stmt.bindInt64(size, at: 2)
                        stmt.bindDouble(now, at: 3)
                        stmt.bindInt64(item.itemId, at: 4)
                        stmt.bindInt64(item.localGeneration, at: 5)
                        stmt.bindInt64(item.remoteGeneration, at: 6)
                        stmt.bindInt64(item.dirtyGeneration, at: 7)
                        _ = try stmt.step()
                        stmt.reset()
                    }

                case .trashRemote:
                    try await acquireTransfer()
                    syncGroup.enter()
                    Task {
                        defer {
                            syncSemaphore.signal()
                            syncGroup.leave()
                        }
                        do {
                            if let rId = item.remoteFileId {
                                try await self.client.trash(remoteId: rId)
                            }
                            try await self.store.batchWrite { conn in
                                let stmt = try conn.cachedStatement("""
                                UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                                """)
                                stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                                stmt.bindInt64(item.itemId, at: 2)
                                _ = try stmt.step()
                                stmt.reset()
                            }
                            actionTracker.counts.withLock { $0.deleted += 1 }
                        } catch {
                            self.logger.error("Remote file deletion failed [\(item.name)]: \(error)")
                        }
                    }

                case .deleteLocal:
                    let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                    let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                    let localFileURL = rootURL.appendingPathComponent(relPath)
                    var trashSucceeded = true

                    if FileManager.default.fileExists(atPath: localFileURL.path) {
                        var trashURL: NSURL?
                        do {
                            try FileManager.default.trashItem(at: localFileURL, resultingItemURL: &trashURL)
                        } catch {
                            trashSucceeded = false
                            self.logger.warning("Unable to move local file to Trash [\(relPath)]: \(error). Keeping it locally and marking the operation blocked; permanent deletion is disabled.")
                        }
                    }

                    if trashSucceeded {
                        try await store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(item.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        actionTracker.counts.withLock { $0.deleted += 1 }
                    } else {
                        try await store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET phase = 'blocked', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(item.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                    }

                case .conflict(let winner, let conflictId):
                    try await acquireTransfer()
                    syncGroup.enter()
                    Task {
                        defer { syncSemaphore.signal(); syncGroup.leave() }
                        do {
                            guard winner == .remote, let remoteID = item.remoteFileId,
                                  let localSHA = item.local?.sha256?.lowercased(),
                                  let remoteSHA = item.remote?.sha256?.lowercased() else {
                                throw SyncEngineError.general("The conflict lacks valid evidence of the content of both parties")
                            }
                            let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                            let original = rootURL.appendingPathComponent(parentRel).appendingPathComponent(item.name)
                            let op = try await ConflictOperation.prepare(store: self.store,
                                rootID: rootId, itemID: item.itemId, parentID: item.parentId,
                                original: original, remoteID: remoteID,
                                parentRemoteID: dirContext.getRemoteId(for: item.parentId) ?? remoteRootId,
                                copyRemoteID: self.idPool.nextId(), conflictID: conflictId,
                                localSHA: localSHA, remoteSHA: remoteSHA,
                                localGeneration: item.localGeneration, remoteGeneration: item.remoteGeneration,
                                dirtyGeneration: item.dirtyGeneration)
                            try await self.resolveConflict(op, temporaryDirectory: downloadDirectory)
                            actionTracker.conflicts.withLock { $0 += 1 }
                        } catch {
                            actionTracker.failures.withLock { $0 += 1 }
                            self.logger.error("Failed to handle file conflicts [\(item.name)]: \(error)")
                        }
                    }

                case .unchanged:
                    receipts.append { conn in
                        let stmt = try conn.cachedStatement("UPDATE items SET dirty_generation = 0 WHERE item_id = ? AND local_generation = ? AND remote_generation = ? AND dirty_generation = ?;")
                        stmt.bindInt64(item.itemId, at: 1)
                        stmt.bindInt64(item.localGeneration, at: 2)
                        stmt.bindInt64(item.remoteGeneration, at: 3)
                        stmt.bindInt64(item.dirtyGeneration, at: 4)
                        _ = try stmt.step()
                        stmt.reset()
                    }

                case .waitingEvidence:
                    break
                }
                if receipts.count >= 64 {
                    let batch = receipts
                    try await store.write { conn in
                        for receipt in batch { try receipt(conn) }
                    }
                    receipts.removeAll(keepingCapacity: true)
                }
            }

            if !receipts.isEmpty {
                let batch = receipts
                try await store.write { conn in
                    for receipt in batch { try receipt(conn) }
                }
            }
        }

        // -------------------------------------------------------------
        // 3. local DirectoryScanner Scanning and rapid change detection (§6.2)
        // -------------------------------------------------------------
        let baselineCache = try await LocalBaselineCache.load(store: store, rootId: rootId)
        final class SeenItemsTracker: @unchecked Sendable {
            private var seen = Set<String>()
            private var lock = os_unfair_lock()

            func markSeen(parentId: Int64, name: String) {
                os_unfair_lock_lock(&lock)
                seen.insert("\(parentId):\(name)")
                os_unfair_lock_unlock(&lock)
            }

            func contains(parentId: Int64, name: String) -> Bool {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return seen.contains("\(parentId):\(name)")
            }
        }
        let seenTracker = SeenItemsTracker()
        let seenDirTracker = SeenItemsTracker()

        final class ScanProgress: @unchecked Sendable {
            var scanned = 0
            var skipped = 0
            var dirsCreated = 0
            var failed = 0
            private var lock = os_unfair_lock()

            func incScanned() {
                os_unfair_lock_lock(&lock)
                scanned += 1
                os_unfair_lock_unlock(&lock)
            }

            func incSkipped() {
                os_unfair_lock_lock(&lock)
                skipped += 1
                os_unfair_lock_unlock(&lock)
            }

            func incDirs() {
                os_unfair_lock_lock(&lock)
                dirsCreated += 1
                os_unfair_lock_unlock(&lock)
            }

            func incFailed() {
                os_unfair_lock_lock(&lock)
                failed += 1
                os_unfair_lock_unlock(&lock)
            }
        }
        let scanProgress = ScanProgress()

        let staticPrefix = resolvedLocalPath.hasSuffix("/") ? resolvedLocalPath : resolvedLocalPath + "/"
        let prefixBytes = Array(staticPrefix.utf8)

        let scanOptions = ScanOptions(
            mode: .basic,
            emission: .all,
            includeHidden: true,
            workers: min(6, ProcessInfo.processInfo.activeProcessorCount),
            batchCapacity: 512,
            delimiter: 0x00,
            pathPrefix: prefixBytes
        )
        let scanFilters: [FilterRule] = [.excludeDirectory(".git")]
        let request = ScanRequest(root: resolvedLocalPath, filters: scanFilters, options: scanOptions)

        struct DiscoveredRecord {
            let type: EntryType
            let fullPath: String
            let dev: Int64
            let ino: Int64
            let mtime: Int64
            let fileSize: Int64
        }

        @Sendable func commitObservations(_ pending: [IncrementalLocalObservation]) async throws {
            guard !pending.isEmpty else { return }
            let results = try await IncrementalLocalObservation.hash(pending, concurrency: min(6, effectiveSyncConcurrency))
            var observations: [IncrementalLocalObservation.Hashed] = []
            var failures: [IncrementalLocalObservation.Failure] = []
            observations.reserveCapacity(results.count)
            failures.reserveCapacity(results.count)
            for result in results {
                switch result {
                case .hashed(let observation):
                    observations.append(observation)
                case .failed(let failure):
                    failures.append(failure)
                    scanProgress.incFailed()
                    self.logger.error("Local file reading or hashing failed, retain the existing state and continue synchronization [\(failure.observation.url.path)]: \(failure.message)")
                }
            }
            let successfulObservations = observations
            let failedObservations = failures
            let ids = try await self.store.write { conn -> [Int64] in
                var ids: [Int64] = []
                for result in successfulObservations {
                    let observation = result.observation
                    let parentItemId = observation.parentID
                    let name = observation.name
                    let dev = observation.device
                    let ino = observation.inode
                    let mtime = observation.mtime
                    let fileSize = observation.size
                    let sha256Hex = result.sha256
                    let stmt = try conn.cachedStatement("""
                    INSERT INTO items (
                        root_id, parent_id, name, entry_kind,
                        local_device, local_inode, local_mtime, local_size, local_sha256,
                        remote_status,
                        local_generation, local_status, phase, dirty_generation,
                        created_at, updated_at
                    ) VALUES (
                        ?, ?, ?, 'file',
                        ?, ?, ?, ?, ?,
                        'absent',
                        1, 'present', 'ready', 1,
                        ?, ?
                    )
                    ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                    DO UPDATE SET
                        local_device = excluded.local_device,
                        local_inode = excluded.local_inode,
                        local_mtime = excluded.local_mtime,
                        local_size = excluded.local_size,
                        local_sha256 = excluded.local_sha256,
                        local_status = 'present',
                        local_generation = items.local_generation + 1,
                        dirty_generation = items.dirty_generation + 1,
                        phase = 'ready',
                        updated_at = excluded.updated_at
                    RETURNING item_id;
                    """)
                    stmt.bindInt64(rootId, at: 1)
                    stmt.bindInt64(parentItemId, at: 2)
                    stmt.bindText(name, at: 3)
                    stmt.bindInt64(dev, at: 4)
                    stmt.bindInt64(ino, at: 5)
                    stmt.bindInt64(mtime, at: 6)
                    stmt.bindInt64(fileSize, at: 7)
                    stmt.bindText(sha256Hex, at: 8)
                    stmt.bindDouble(now, at: 9)
                    stmt.bindDouble(now, at: 10)
                    if try stmt.step(), let id = stmt.columnInt64(at: 0) { ids.append(id) }
                    _ = try stmt.step()
                    stmt.reset()
                }
                for failure in failedObservations {
                    let observation = failure.observation
                    let stmt = try conn.cachedStatement("""
                    INSERT INTO items (
                        root_id, parent_id, name, entry_kind,
                        local_device, local_inode, local_mtime, local_size,
                        local_status, remote_status, local_generation,
                        phase, dirty_generation, created_at, updated_at
                    ) VALUES (?, ?, ?, 'file', ?, ?, ?, ?,
                        'unknown', 'absent', 1, 'waitingEvidence', 1, ?, ?)
                    ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                    DO UPDATE SET
                        local_device = excluded.local_device,
                        local_inode = excluded.local_inode,
                        local_mtime = excluded.local_mtime,
                        local_size = excluded.local_size,
                        local_status = 'unknown',
                        local_generation = items.local_generation + 1,
                        dirty_generation = items.dirty_generation + 1,
                        phase = 'waitingEvidence',
                        updated_at = excluded.updated_at;
                    """)
                    stmt.bindInt64(rootId, at: 1)
                    stmt.bindInt64(observation.parentID, at: 2)
                    stmt.bindText(observation.name, at: 3)
                    stmt.bindInt64(observation.device, at: 4)
                    stmt.bindInt64(observation.inode, at: 5)
                    stmt.bindInt64(observation.mtime, at: 6)
                    stmt.bindInt64(observation.size, at: 7)
                    stmt.bindDouble(now, at: 8)
                    stmt.bindDouble(now, at: 9)
                    _ = try stmt.step()
                    stmt.reset()
                }
                return ids
            }
            if !ids.isEmpty { try await scheduleFiles(loadDirtyItems(ids), duringScan: true) }
        }
        let sentFirstObservation = OSAllocatedUnfairLock(initialState: false)

        do {
            try await incrementalScan(request) { batch in
                var pendingObservations: [IncrementalLocalObservation] = []
                var firstObservationSent = sentFirstObservation.withLock { $0 }
                var itemsInBatch: [DiscoveredRecord] = []
                batch.withRawData { rawBuf in
                    guard let basePtr = rawBuf.baseAddress else { return }

                    for idx in 0..<batch.count {
                        let record = batch.records[idx]
                        let rawPtr = UnsafeRawPointer(basePtr + Int(record.offset))
                        let cPath = rawPtr.assumingMemoryBound(to: CChar.self)
                        let fullPath = String(cString: cPath)

                        let dev = Int64(record.metadata?.identity.device ?? 1)
                        let ino = Int64(record.metadata?.identity.inode ?? 0)
                        let mtime = (record.metadata?.modificationTime.seconds ?? 0) * 1_000_000_000 + Int64(record.metadata?.modificationTime.nanoseconds ?? 0)
                        let fileSize = record.metadata?.fileSize ?? 0

                        itemsInBatch.append(DiscoveredRecord(
                            type: record.type,
                            fullPath: fullPath,
                            dev: dev,
                            ino: ino,
                            mtime: mtime,
                            fileSize: fileSize
                        ))
                    }
                }

                for record in itemsInBatch {
                    let fullPath = record.fullPath
                    let relPath: String
                    if fullPath.hasPrefix(staticPrefix) {
                        relPath = String(fullPath.dropFirst(staticPrefix.count))
                    } else {
                        relPath = (fullPath as NSString).lastPathComponent
                    }

                    if relPath.isEmpty || relPath == ".git" || relPath.hasPrefix(".git/") { continue }

                    if remoteGate.blocks(relPath) { continue }
                    let name = (relPath as NSString).lastPathComponent
                    let parentRel = (relPath as NSString).deletingLastPathComponent
                    let parentRelNormalized = (parentRel == "." || parentRel.isEmpty) ? "" : parentRel

                    if !remoteGate.paths.isEmpty,
                       dirContext.getItemId(byRelPath: parentRelNormalized) == nil,
                       try remoteGate.blocksLocalAncestors(URL(fileURLWithPath: fullPath).deletingLastPathComponent(), root: rootURL) {
                        continue
                    }

                    if record.type == .directory, !pendingObservations.isEmpty {
                        try await commitObservations(pendingObservations)
                        pendingObservations.removeAll(keepingCapacity: true)
                        firstObservationSent = true
                    }
                    if record.type == .directory {
                        let parentItemId = dirContext.getItemId(byRelPath: parentRelNormalized) ?? rootItemId
                        let dev = record.dev
                        let ino = record.ino

                        // Check whether the local directory has been renamed or moved (press dev + ino Find)
                        let existingDir: (itemId: Int64, parentId: Int64, name: String, remoteId: String?)? = try await self.store.read { conn in
                            let stmt = try conn.cachedStatement("""
                            SELECT item_id, parent_id, name, remote_file_id
                            FROM items
                            WHERE root_id = ? AND entry_kind = 'directory' AND local_device = ? AND local_inode = ? AND is_tombstone = 0;
                            """)
                            stmt.bindInt64(rootId, at: 1)
                            stmt.bindInt64(dev, at: 2)
                            stmt.bindInt64(ino, at: 3)
                            defer { stmt.reset() }
                            if try stmt.step(),
                               let iId = stmt.columnInt64(at: 0),
                               let pId = stmt.columnInt64(at: 1),
                               let nm = stmt.columnText(at: 2) {
                                let rId = stmt.columnText(at: 3)
                                return (iId, pId, nm, rId)
                            }
                            return nil
                        }

                        if let existing = existingDir,
                           remoteGate.blocks(dirContext.getRelPath(for: existing.itemId) ?? "") {
                            remoteGate.addAlias(relPath)
                            continue
                        }
                        if let existing = existingDir, (existing.name != name || existing.parentId != parentItemId) {
                            // The local directory is renamed or moved
                            seenDirTracker.markSeen(parentId: existing.parentId, name: existing.name)
                            do {
                                if let rId = existing.remoteId {
                                    let oldPRemote = dirContext.getRemoteId(for: existing.parentId)
                                    let newPRemote = dirContext.getRemoteId(for: parentItemId)
                                    let addP = (parentItemId != existing.parentId) ? newPRemote : nil
                                    let remP = (parentItemId != existing.parentId) ? oldPRemote : nil
                                    _ = try await self.client.updateMetadata(remoteId: rId, newName: name, addParentId: addP, removeParentId: remP)
                                }
                                try await self.store.write { conn in
                                    let stmt = try conn.cachedStatement("""
                                    UPDATE items SET name = ?, parent_id = ?, updated_at = ? WHERE item_id = ?;
                                    """)
                                    stmt.bindText(name, at: 1)
                                    stmt.bindInt64(parentItemId, at: 2)
                                    stmt.bindDouble(now, at: 3)
                                    stmt.bindInt64(existing.itemId, at: 4)
                                    _ = try stmt.step()
                                    stmt.reset()
                                }
                                seenDirTracker.markSeen(parentId: parentItemId, name: name)
                                dirContext.register(itemId: existing.itemId, parentItemId: parentItemId, name: name, remoteId: existing.remoteId ?? "")
                            } catch {
                                self.logger.error("Failed to rename or move remote directory [\(existing.name) -> \(name)]: \(error)")
                            }
                        } else if dirContext.getItemId(byRelPath: relPath) == nil {
                            // Create a new local directory
                            let remoteParentId = dirContext.getRemoteId(for: parentItemId) ?? remoteRootId
                            var intent: DurableCreateIntent?
                            do {
                                let candidateRemoteID = try await self.idPool.nextId()
                                let prepared = try await DurableCreateIntentStore.prepareDirectory(
                                    store: self.store,
                                    rootID: rootId,
                                    parentItemID: parentItemId,
                                    name: name,
                                    targetParentRemoteID: remoteParentId,
                                    candidateRemoteID: candidateRemoteID,
                                    device: dev,
                                    inode: ino
                                )
                                intent = prepared
                                _ = try await self.client.createDirectory(
                                    name: name,
                                    parentId: prepared.targetParentRemoteID,
                                    remoteId: prepared.targetRemoteID
                                )
                                try await self.store.write { conn in
                                    let ts = Date().timeIntervalSince1970
                                    let stmt = try conn.cachedStatement("""
                                    UPDATE items SET
                                        remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                                    WHERE item_id = ?;
                                    """)
                                    stmt.bindDouble(ts, at: 1)
                                    stmt.bindInt64(prepared.itemID, at: 2)
                                    _ = try stmt.step()
                                    stmt.reset()
                                    try DurableCreateIntentStore.completeOperation(conn: conn, operationID: prepared.operationID, now: ts)
                                }
                                dirContext.register(
                                    itemId: prepared.itemID,
                                    parentItemId: parentItemId,
                                    name: name,
                                    remoteId: prepared.targetRemoteID
                                )
                                seenDirTracker.markSeen(parentId: parentItemId, name: name)
                                scanProgress.incDirs()
                            } catch {
                                if let intent {
                                    await DurableCreateIntentStore.markUnknownOutcome(
                                        store: self.store,
                                        operationID: intent.operationID,
                                        error: error
                                    )
                                }
                                self.logger.error("Failed to create remote directory [\(relPath)]: \(error)")
                            }
                        } else {
                            seenDirTracker.markSeen(parentId: parentItemId, name: name)
                        }
                    } else if record.type == .file {
                        scanProgress.incScanned()
                        let parentItemId = dirContext.getItemId(byRelPath: parentRelNormalized) ?? rootItemId
                        let dev = record.dev
                        let ino = record.ino
                        let mtime = record.mtime
                        let fileSize = record.fileSize

                        // The cache only contains committed, clean remote-present baselines.
                        // Matching the path as well as identity preserves the rename path below,
                        // while an ordinary unchanged file needs no per-item SQLite round trip.
                        if let cached = baselineCache.lookupUnchanged(device: dev, inode: ino, mtime: mtime, size: fileSize),
                           cached.parentId == parentItemId, cached.name == name {
                            seenTracker.markSeen(parentId: parentItemId, name: name)
                            scanProgress.incSkipped()
                            continue
                        }

                        // Check if local files have been renamed or moved (press dev + ino Find)
                        let existingFile: (itemId: Int64, parentId: Int64, name: String, remoteId: String?)? = try await self.store.read { conn in
                            let stmt = try conn.cachedStatement("""
                            SELECT item_id, parent_id, name, remote_file_id
                            FROM items
                            WHERE root_id = ? AND entry_kind = 'file' AND local_device = ? AND local_inode = ? AND is_tombstone = 0;
                            """)
                            stmt.bindInt64(rootId, at: 1)
                            stmt.bindInt64(dev, at: 2)
                            stmt.bindInt64(ino, at: 3)
                            defer { stmt.reset() }
                            if try stmt.step(),
                               let iId = stmt.columnInt64(at: 0),
                               let pId = stmt.columnInt64(at: 1),
                               let nm = stmt.columnText(at: 2) {
                                let rId = stmt.columnText(at: 3)
                                return (iId, pId, nm, rId)
                            }
                            return nil
                        }

                        if let existing = existingFile {
                            let parentPath = dirContext.getRelPath(for: existing.parentId) ?? ""
                            let oldPath = parentPath.isEmpty ? existing.name : "\(parentPath)/\(existing.name)"
                            if remoteGate.blocks(oldPath) { continue }
                        }
                        if let existing = existingFile, (existing.name != name || existing.parentId != parentItemId) {
                            // Local files are renamed or moved
                            seenTracker.markSeen(parentId: existing.parentId, name: existing.name)
                            do {
                                if let rId = existing.remoteId {
                                    let oldPRemote = dirContext.getRemoteId(for: existing.parentId)
                                    let newPRemote = dirContext.getRemoteId(for: parentItemId)
                                    let addP = (parentItemId != existing.parentId) ? newPRemote : nil
                                    let remP = (parentItemId != existing.parentId) ? oldPRemote : nil
                                    _ = try await self.client.updateMetadata(remoteId: rId, newName: name, addParentId: addP, removeParentId: remP)
                                }

                                try await self.store.write { conn in
                                    let stmt = try conn.cachedStatement("""
                                    UPDATE items SET
                                        name = ?,
                                        parent_id = ?,
                                        updated_at = ?
                                    WHERE item_id = ?;
                                    """)
                                    stmt.bindText(name, at: 1)
                                    stmt.bindInt64(parentItemId, at: 2)
                                    stmt.bindDouble(now, at: 3)
                                    stmt.bindInt64(existing.itemId, at: 4)
                                    _ = try stmt.step()
                                    stmt.reset()
                                }
                                seenTracker.markSeen(parentId: parentItemId, name: name)
                            } catch {
                                self.logger.error("Failed to rename or move remote file [\(existing.name) -> \(name)]: \(error)")
                                continue
                            }
                            // Path updates do not mean that the text has been verified; retain the old metadata and continue content comparison.
                            // A pure name change will still hit the cache below, while a text change will reuse the existing summary and decision process.
                        }

                        seenTracker.markSeen(parentId: parentItemId, name: name)

                        // Quick change comparison (§6.2)
                        if let _ = baselineCache.lookupUnchanged(device: dev, inode: ino, mtime: mtime, size: fileSize) {
                            scanProgress.incSkipped()
                            continue
                        }

                        pendingObservations.append(IncrementalLocalObservation(
                            parentID: parentItemId, name: name, url: URL(fileURLWithPath: fullPath),
                            device: dev, inode: ino, mtime: mtime, size: fileSize))
                        // First ready file goes immediately; subsequent work uses bounded natural chunks.
                        if !firstObservationSent || pendingObservations.count >= 64 {
                            try await commitObservations(pendingObservations)
                            pendingObservations.removeAll(keepingCapacity: true)
                            firstObservationSent = true
                        }
                    }
                }
                try await commitObservations(pendingObservations)
                let sent = firstObservationSent || !pendingObservations.isEmpty
                sentFirstObservation.withLock { $0 = sent }
            }
        } catch {
            // No transfer may escape a failed/cancelled scan and mutate state after return.
            await drainTransfers()
            throw error
        }
        await drainTransfers()

        // Identify local deleted files and directories (§9.1)
        // Verify again whether the local root directory exists to avoid all misjudgments caused by the local directory being removed during the scan. absent diffuse deletion
        var isStillDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isStillDir), isStillDir.boolValue else {
            logger.error("[Sync] The local sync root disappeared during scanning: \(resolvedLocalPath). Stopping sync to protect remote files.")
            throw SyncEngineError.localRootNotFound(path: resolvedLocalPath)
        }

        try await store.write { conn in
            // 1. File deletion detection
            let stmt = try conn.cachedStatement("""
            SELECT item_id, parent_id, name
            FROM items
            WHERE root_id = ? AND entry_kind = 'file' AND local_status = 'present' AND is_tombstone = 0;
            """)
            stmt.bindInt64(rootId, at: 1)
            var deletedIds: [Int64] = []
            while try stmt.step() {
                if let iId = stmt.columnInt64(at: 0),
                   let pId = stmt.columnInt64(at: 1),
                   let name = stmt.columnText(at: 2) {
                    let parent = dirContext.getRelPath(for: pId) ?? ""
                    let path = parent.isEmpty ? name : "\(parent)/\(name)"
                    if !remoteGate.blocks(path) && !seenTracker.contains(parentId: pId, name: name) {
                        deletedIds.append(iId)
                    }
                }
            }
            stmt.reset()

            // 2. Directory deletion detection (excluding root directory entries)
            let dirStmt = try conn.cachedStatement("""
            SELECT item_id, parent_id, name
            FROM items
            WHERE root_id = ? AND entry_kind = 'directory' AND parent_id IS NOT NULL AND local_status = 'present' AND is_tombstone = 0;
            """)
            dirStmt.bindInt64(rootId, at: 1)
            while try dirStmt.step() {
                if let iId = dirStmt.columnInt64(at: 0),
                   let pId = dirStmt.columnInt64(at: 1),
                   let name = dirStmt.columnText(at: 2) {
                    let parent = dirContext.getRelPath(for: pId) ?? ""
                    let path = parent.isEmpty ? name : "\(parent)/\(name)"
                    if !remoteGate.blocks(path) && !seenDirTracker.contains(parentId: pId, name: name) {
                        deletedIds.append(iId)
                    }
                }
            }
            dirStmt.reset()

            for dId in deletedIds {
                let u = try conn.cachedStatement("""
                UPDATE items SET
                    local_status = 'absent',
                    local_generation = local_generation + 1,
                    dirty_generation = dirty_generation + 1,
                    phase = 'ready',
                    updated_at = ?
                WHERE item_id = ?;
                """)
                u.bindDouble(now, at: 1)
                u.bindInt64(dId, at: 2)
                _ = try u.step()
                u.reset()
            }
        }

        // Refresh all uncommitted Changes and scan the batch to disk, ensuring immediate visibility for subsequent queries
        try await store.flush()

        // -------------------------------------------------------------
        // 4. Reconciler decision making and execution (§9.1)
        // -------------------------------------------------------------
        let dirtyItems = try await loadDirtyItems()

        // -------------------------------------------------------------
        // 5A. Phase 1: Execute on all file items first Reconciler Adjudication and Scheduling Execution (§9.1, A04)
        // Ensure that all file-level uploads, downloads, modification retention, and deletions are completely completed as a barrier to directory deletion
        // -------------------------------------------------------------
        let eligibleItems = dirtyItems.filter { item in
            let parent = dirContext.getRelPath(for: item.parentId) ?? ""
            let path = parent.isEmpty ? item.name : "\(parent)/\(item.name)"
            return !remoteGate.blocks(path)
        }
        let fileItems = eligibleItems.filter { $0.entryKind == "file" }
        let dirItems = eligibleItems.filter { $0.entryKind == "directory" }

        // Resumes the last creation of a directory that was persisted before the request but for which a completion receipt has not yet been submitted.
        // createDirectory Use the same pregenerated ID Retry; if the server succeeded last time,DriveClient will be in 409 Then verify the same object.
        let pendingDirectoryCreates = dirItems.compactMap { item -> (DirtyRecord, DurableCreateIntent)? in
            guard let intent = item.pendingCreate else { return nil }
            return (item, intent)
        }.sorted { lhs, rhs in
            let left = dirContext.getRelPath(for: lhs.0.itemId) ?? lhs.0.name
            let right = dirContext.getRelPath(for: rhs.0.itemId) ?? rhs.0.name
            return left.split(separator: "/").count < right.split(separator: "/").count
        }

        for (item, intent) in pendingDirectoryCreates {
            do {
                _ = try await client.createDirectory(
                    name: item.name,
                    parentId: intent.targetParentRemoteID,
                    remoteId: intent.targetRemoteID
                )
                try await store.batchWrite { conn in
                    let ts = Date().timeIntervalSince1970
                    let stmt = try conn.cachedStatement("""
                    UPDATE items SET
                        remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                    WHERE item_id = ?;
                    """)
                    stmt.bindDouble(ts, at: 1)
                    stmt.bindInt64(intent.itemID, at: 2)
                    _ = try stmt.step()
                    stmt.reset()
                    try DurableCreateIntentStore.completeOperation(conn: conn, operationID: intent.operationID, now: ts)
                }
            } catch {
                await DurableCreateIntentStore.markUnknownOutcome(
                    store: store,
                    operationID: intent.operationID,
                    error: error
                )
                logger.error("Failed to restore remote directory creation [\(item.name)]: \(error)")
            }
        }

        do {
            try await scheduleFiles(fileItems, duringScan: false)
        } catch {
            await drainTransfers()
            throw error
        }

        // Wait for all file-level uploads, downloads, modifications, retention, and deletions to be completed and dropped into the database.
        await drainTransfers()
        try await store.flush()

        // -------------------------------------------------------------
        // 5B. Phase 2: Descendant conflict barrier and controlled bottom-up directory processing (A04)
        // Depends on the results of all descendant rulings, any reserved, added, and pending uploads/Downloads or conflicts will block directory deletion
        // -------------------------------------------------------------
        // 1. For ordinary directories in a non-deleted state, submit and clear them directly. dirty_generation
        if !dirItems.isEmpty {
            try await store.write { conn in
                for item in dirItems {
                    let isCandidate = (item.local?.status == .absent && item.remote?.status == .present) ||
                                      (item.remote?.status == .trashed && item.local?.status == .present) ||
                                      (item.local?.status == .absent && item.remote?.status == .trashed)
                    if !isCandidate {
                        if item.local?.status != .unknown && item.remote?.status != .unknown {
                            do {
                                let stmt = try conn.cachedStatement("""
                                UPDATE items SET phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                                """)
                                stmt.bindDouble(now, at: 1)
                                stmt.bindInt64(item.itemId, at: 2)
                                _ = try stmt.step()
                                stmt.reset()
                            }
                        }
                    } else if item.local?.status == .absent && item.remote?.status == .trashed {
                        // Both ends have been deleted
                        do {
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(item.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        actionTracker.counts.withLock { $0.deleted += 1 }
                    }
                }

            }
        }

        // 2. For directories with unilateral deletion intentions, perform barrier verification in reverse order of tree depth (bottom-up, leaf directories first)
        let dirCandidates = dirItems.filter {
            ($0.local?.status == .absent && $0.remote?.status == .present) ||
            ($0.remote?.status == .trashed && $0.local?.status == .present)
        }.sorted { a, b in
            let pathA = dirContext.getRelPath(for: a.itemId) ?? ""
            let pathB = dirContext.getRelPath(for: b.itemId) ?? ""
            let depthA = pathA.isEmpty ? 0 : pathA.split(separator: "/").count
            let depthB = pathB.isEmpty ? 0 : pathB.split(separator: "/").count
            return depthA > depthB
        }

        for dirItem in dirCandidates {
            let parentRel = dirContext.getRelPath(for: dirItem.parentId) ?? ""
            let relPath = parentRel.isEmpty ? dirItem.name : "\(parentRel)/\(dirItem.name)"
            let localDirURL = rootURL.appendingPathComponent(relPath)

            // Recursively query the status of all descendants of the current directory
            let barrier = try await store.read { conn in
                let stmt = try conn.cachedStatement("""
                WITH RECURSIVE subtree AS (
                    SELECT item_id, entry_kind, local_status, remote_status, phase, is_tombstone
                    FROM items
                    WHERE parent_id = ? AND root_id = ?
                    UNION ALL
                    SELECT i.item_id, i.entry_kind, i.local_status, i.remote_status, i.phase, i.is_tombstone
                    FROM items i
                    JOIN subtree s ON i.parent_id = s.item_id
                    WHERE i.root_id = ?
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN is_tombstone = 0 AND local_status = 'present' THEN 1 ELSE 0 END) AS local_present,
                    SUM(CASE WHEN is_tombstone = 0 AND remote_status = 'present' THEN 1 ELSE 0 END) AS remote_present,
                    SUM(CASE WHEN is_tombstone = 0 AND (phase IN ('waitingEvidence', 'conflict', 'blocked') OR local_status = 'unknown' OR remote_status = 'unknown') THEN 1 ELSE 0 END) AS pending_count
                FROM subtree;
                """)
                stmt.bindInt64(dirItem.itemId, at: 1)
                stmt.bindInt64(rootId, at: 2)
                stmt.bindInt64(rootId, at: 3)
                defer { stmt.reset() }
                if try stmt.step() {
                    let total = Int(stmt.columnInt64(at: 0) ?? 0)
                    let localPresent = Int(stmt.columnInt64(at: 1) ?? 0)
                    let remotePresent = Int(stmt.columnInt64(at: 2) ?? 0)
                    let pending = Int(stmt.columnInt64(at: 3) ?? 0)
                    return (total: total, localPresent: localPresent, remotePresent: remotePresent, pending: pending)
                }
                return (total: 0, localPresent: 0, remotePresent: 0, pending: 0)
            }

            if dirItem.local?.status == .absent && dirItem.remote?.status == .present {
                // The directory is deleted locally, but the remote directory is still there (original intention: trashRemote)
                // Barrier check: if there are any remote files in the descendants that need to be preserved/Download files locally/conflict/Pending items, deletion of remote directories is absolutely prohibited
                if barrier.remotePresent > 0 || barrier.localPresent > 0 || barrier.pending > 0 {
                    self.logger.info("Descendant barrier blocked deletion of remote directory [\(relPath)]: descendants must be retained or added (remotePresent: \(barrier.remotePresent), localPresent: \(barrier.localPresent)); restoring the local directory")
                    try? FileManager.default.createDirectory(at: localDirURL, withIntermediateDirectories: true)
                    try await store.batchWrite { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE items SET local_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindInt64(dirItem.itemId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                } else {
                    // All descendants have been safely deleted and sent to the cloud trashRemote
                    do {
                        if let rId = dirItem.remoteFileId {
                            try await self.client.trash(remoteId: rId)
                        }
                        try await store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        actionTracker.counts.withLock { $0.deleted += 1 }
                    } catch {
                        self.logger.error("Failed to delete remote directory [\(relPath)]: \(error)")
                    }
                }
            } else if dirItem.remote?.status == .trashed && dirItem.local?.status == .present {
                // The remote end deleted the directory, but the local directory is still there (original intention: deleteLocal)
                // Barrier check: If there are local new, modified or conflicting files in the descendants, deletion of the local directory is absolutely prohibited
                if barrier.localPresent > 0 || barrier.remotePresent > 0 || barrier.pending > 0 {
                    self.logger.info("Descendant barrier blocked deletion of local directory [\(relPath)]: it contains locally added or modified descendants (localPresent: \(barrier.localPresent))")
                    do {
                        if let rId = dirItem.remoteFileId {
                            try await self.client.untrash(remoteId: rId)
                        }
                        try await store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                    } catch {
                        self.logger.error("Descendant barrier failed to restore remote directory [\(relPath)]: \(error)")
                    }
                } else {
                    // All descendants have been cleaned up. Verify that the local directory is empty and safely move it to the trash.
                    var trashSucceeded = true
                    if FileManager.default.fileExists(atPath: localDirURL.path) {
                        let contents = (try? FileManager.default.contentsOfDirectory(atPath: localDirURL.path)) ?? []
                        let nonHidden = contents.filter { !$0.hasPrefix(".") }
                        if nonHidden.isEmpty {
                            var trashURL: NSURL?
                            do {
                                try FileManager.default.trashItem(at: localDirURL, resultingItemURL: &trashURL)
                            } catch {
                                trashSucceeded = false
                                self.logger.warning("Unable to move local directory to Trash [\(relPath)]: \(error). Keeping it and marking the operation blocked.")
                            }
                        } else {
                            trashSucceeded = false
                            self.logger.warning("Local directory [\(relPath)] is not empty; blocking deletion")
                        }
                    }

                    if trashSucceeded {
                        try await store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        actionTracker.counts.withLock { $0.deleted += 1 }
                    } else {
                        try await store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET phase = 'blocked', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                    }
                }
            }
        }

        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            syncGroup.notify(queue: .global(qos: .userInitiated)) {
                cont.resume()
            }
        }

        // Local ready work finishes before any subtree enumeration request is issued.
        let enumerated = try await remoteChanges.enumeratePending(limit: maxConcurrency)
        // Even an empty completed listing releases local paths which were gated earlier
        // in this round. Schedule one follow-up discovery instead of claiming convergence.
        stats.remoteWorkPending = max(try await remoteChanges.pendingCount(), enumerated ? 1 : 0)
        if stats.remoteWorkPending > 0 { stats.remoteNameConflicts = try await remoteChanges.nameConflictCount() }

        try await store.flush()
        try await store.checkpoint()

        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        stats.directoriesCreated = scanProgress.dirsCreated
        stats.filesScanned = scanProgress.scanned
        stats.filesSkipped = scanProgress.skipped
        stats.filesUploaded = actionTracker.uploaded
        stats.bytesUploaded = actionTracker.bytesUp
        stats.filesDownloaded = actionTracker.downloaded
        stats.bytesDownloaded = actionTracker.bytesDown
        stats.filesDeleted = actionTracker.deleted
        stats.conflictsResolved = recoveredConflicts + actionTracker.conflicts.withLock { $0 }
        stats.filesFailed = scanProgress.failed + actionTracker.failures.withLock { $0 }
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }
}
