import Foundation
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

/// Public entry points for bidirectional synchronization using SQLite baselines and SHA-256.
/// Synchronization routing and mode implementations are split across SyncEngine extensions.
public final class SyncEngine: Sendable {
    private struct PendingRoot: Sendable {
        let id: Int64
        let item: Int64
        let remote: String
    }
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
    let logger = Logger(label: "gdrive.engine")
    typealias DirectoryScan = @Sendable (ScanRequest, @escaping @Sendable (ScanBatch) async throws -> Void) async throws -> Void
    typealias IncrementalScan = DirectoryScan
    static let defaultDirectoryScan: DirectoryScan = { request, consume in
        _ = try await DirectoryScanner().scan(request, consume: consume)
    }
    let directoryScan: DirectoryScan

    public convenience init(
        auth: Auth,
        store: StateStore? = nil,
        client: DriveClient? = nil,
        idPool: IDPool? = nil,
        downloadTemporaryDirectory: URL = DriveClient.defaultDownloadTemporaryDirectory
    ) async throws {
        try await self.init(auth: auth, store: store, client: client, idPool: idPool,
            downloadTemporaryDirectory: downloadTemporaryDirectory, incrementalScan: Self.defaultDirectoryScan)
    }

    init(auth: Auth, store: StateStore? = nil, client: DriveClient? = nil, idPool: IDPool? = nil,
         downloadTemporaryDirectory: URL = DriveClient.defaultDownloadTemporaryDirectory,
         incrementalScan: @escaping IncrementalScan) async throws {
        guard downloadTemporaryDirectory.isFileURL else {
            throw SyncEngineError.general("The temporary download directory must be a local file path")
        }
        self.downloadTemporaryDirectoryStorage = OSAllocatedUnfairLock(initialState: downloadTemporaryDirectory)
        self.directoryScan = incrementalScan
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

    // MARK: - Synchronization entry points

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
        let normalizedLocalPath = Self.normalizedPath(localPath)
        return try await withRootSyncLock(localPath: normalizedLocalPath) {
            try await self.syncUnlocked(
                localPath: normalizedLocalPath,
                remoteFolderId: remoteFolderId,
                concurrency: concurrency,
                onProgress: onProgress
            )
        }
    }

    /// Full streaming synchronization of local non-empty directories to remote empty directories
    @discardableResult
    public func syncLocalToRemoteEmpty(
        localPath: String,
        remoteRootId: String,
        maxUploadConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        let normalizedLocalPath = Self.normalizedPath(localPath)
        return try await withRootSyncLock(localPath: normalizedLocalPath) {
            try await self.syncLocalToRemoteEmptyUnlocked(
                localPath: normalizedLocalPath,
                remoteRootId: remoteRootId,
                maxUploadConcurrency: maxUploadConcurrency,
                onProgress: onProgress
            )
        }
    }

    /// Synchronize full streaming download of remote non-empty directory to local empty directory
    @discardableResult
    public func syncRemoteToLocalEmpty(
        localPath: String,
        remoteRootId: String,
        maxDownloadConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        let normalizedLocalPath = Self.normalizedPath(localPath)
        return try await withRootSyncLock(localPath: normalizedLocalPath) {
            try await self.initializeRemoteToLocalEmpty(localPath: normalizedLocalPath, remoteRootId: remoteRootId,
                maxDownloadConcurrency: maxDownloadConcurrency, onProgress: onProgress, initialCursor: nil)
        }
    }

    /// Perform a two-way incremental synchronization on an existing sync root
    /// Combined with local DirectoryScanner Quick scan with Google Drive Changes Incremental changes, consisting of Reconciler Drive tripartite decision-making
    @discardableResult
    public func syncIncremental(
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        let normalizedLocalPath = Self.normalizedPath(localPath)
        return try await withRootSyncLock(localPath: normalizedLocalPath) {
            try await self.syncIncrementalUnlocked(
                localPath: normalizedLocalPath,
                remoteRootId: remoteRootId,
                maxConcurrency: maxConcurrency,
                onProgress: onProgress
            )
        }
    }

    /// Registers watcher observations and returns after admission. Transfers run
    /// after the current whole-root round, or in a new background round when idle.
    public func notifyLocalChanges(_ changes: [LocalChange]) async throws {
        guard !changes.isEmpty else { return }
        let normalized = changes.map(Self.normalizedLocalChange)
        let roots = try await resolveNotificationRoots(for: normalized)
        for (root, rootChanges) in roots {
            let shouldStart = await RootSyncCoordinator.shared.enqueue(rootChanges, for: root)
            if shouldStart {
                Task { await self.drainPendingLocalChanges(localRootPath: root) }
            }
        }
    }

    /// Returns process-local watcher queue state.
    public func pendingLocalChangeStatus() async -> [PendingLocalChangeStatus] {
        await RootSyncCoordinator.shared.statuses()
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
        let normalizedLocalPath = Self.normalizedPath(localPath)
        return try await withRootSyncLock(localPath: normalizedLocalPath) {
            try await self.syncIncrementalUnlocked(
                rootId: rootId,
                rootItemId: rootItemId,
                localPath: normalizedLocalPath,
                remoteRootId: remoteRootId,
                maxConcurrency: maxConcurrency,
                onProgress: onProgress
            )
        }
    }

    // MARK: - Root synchronization lock

    private func withRootSyncLock<T: Sendable>(
        localPath: String,
        operation: @Sendable () async throws -> T
    ) async throws -> T {
        let resolvedLocalPath = Self.normalizedPath(localPath)
        try await RootSyncCoordinator.shared.acquire(localRootPath: resolvedLocalPath)
        do {
            let result = try await operation()
            await drainPendingLocalChanges(localRootPath: resolvedLocalPath)
            return result
        } catch {
            if !(await RootSyncCoordinator.shared.finishIfIdle(localRootPath: resolvedLocalPath)) {
                Task { await self.drainPendingLocalChanges(localRootPath: resolvedLocalPath) }
            }
            throw error
        }
    }

    private static func normalizedPath(_ path: String) -> String {
        URL(fileURLWithPath: (path as NSString).expandingTildeInPath).standardizedFileURL.path
    }

    private static func normalizedLocalChange(_ change: LocalChange) -> LocalChange {
        switch change {
        case .created(let path, let isDirectory): return .created(path: normalizedPath(path), isDirectory: isDirectory)
        case .modified(let path, let isDirectory): return .modified(path: normalizedPath(path), isDirectory: isDirectory)
        case .deleted(let path, let isDirectory): return .deleted(path: normalizedPath(path), isDirectory: isDirectory)
        case .moved(let from, let destination, let isDirectory):
            return .moved(from: normalizedPath(from), to: normalizedPath(destination), isDirectory: isDirectory)
        }
    }

    private func resolveNotificationRoots(for changes: [LocalChange]) async throws -> [(String, [LocalChange])] {
        let paths = changes.flatMap(\.paths)
        let active = await RootSyncCoordinator.shared.activeRoots(containing: paths)
        let databaseRoots: [String] = try await store.read { conn in
            let stmt = try conn.cachedStatement("SELECT local_root_path FROM roots WHERE is_active = 1;")
            defer { stmt.reset() }
            var result: [String] = []
            while try stmt.step(), let path = stmt.columnText(at: 0) { result.append(Self.normalizedPath(path)) }
            return result
        }
        let candidates = Set(active + databaseRoots)
        var routed: [String: [LocalChange]] = [:]
        for change in changes {
            let matching = candidates.filter { root in change.paths.allSatisfy { RootSyncCoordinator.contains($0, in: root) } }
            guard let root = matching.max(by: { $0.count < $1.count }) else {
                throw SyncEngineError.general("No single configured local root contains local change paths: \(change.paths.joined(separator: ", "))")
            }
            routed[root, default: []].append(change)
        }
        return routed.map { ($0.key, $0.value) }
    }

    private func drainPendingLocalChanges(localRootPath: String) async {
        while true {
            guard let batch = await RootSyncCoordinator.shared.takePending(for: localRootPath) else {
                if await RootSyncCoordinator.shared.finishIfIdle(localRootPath: localRootPath) { return }
                continue
            }
            do {
                try await runPendingLocalChanges(batch.changes, localRootPath: localRootPath)
            } catch is CancellationError {
                logger.warning("Discarded cancelled pending local changes for \(localRootPath)")
                Task { await self.drainPendingLocalChanges(localRootPath: localRootPath) }
                return
            } catch {
                logger.error("Discarded failed pending local changes for \(localRootPath): \(error)")
            }
        }
    }

    private func runPendingLocalChanges(_ changes: [LocalChange], localRootPath: String) async throws {
        guard let root = try await pendingRoot(localRootPath: localRootPath) else {
            logger.warning(
                "Discard pending local changes because no active root binding exists [\(localRootPath), changes=\(changes.count)]")
            return
        }
        _ = try await syncIncrementalUnlocked(rootId: root.id, rootItemId: root.item, localPath: localRootPath,
            remoteRootId: root.remote, maxConcurrency: 64, onProgress: nil, localChanges: changes)
    }

    private func pendingRoot(localRootPath: String) async throws -> PendingRoot? {
        let root: PendingRoot? = try await store.read { conn in
            let stmt = try conn.cachedStatement("SELECT root_id, remote_root_id, local_root_path FROM roots WHERE is_active = 1;")
            defer { stmt.reset() }
            var record: (Int64, String)?
            while try stmt.step() {
                if let id = stmt.columnInt64(at: 0), let remote = stmt.columnText(at: 1),
                   let path = stmt.columnText(at: 2), Self.normalizedPath(path) == localRootPath {
                    record = (id, remote)
                    break
                }
            }
            guard let record else { return nil }
            let item = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL AND is_tombstone = 0;")
            item.bindInt64(record.0, at: 1)
            defer { item.reset() }
            guard try item.step(), let rootItem = item.columnInt64(at: 0) else { return nil }
            return PendingRoot(id: record.0, item: rootItem, remote: record.1)
        }
        return root
    }
}
