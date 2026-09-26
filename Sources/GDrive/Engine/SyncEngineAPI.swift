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
    public var conflicts: [SyncConflict] = []
    /// Durable remote observations/pages still awaiting a later reconcile round.
    public var remoteWorkPending: Int = 0
    /// Remote identities blocked by equivalent local names; rename remotely to resolve.
    public var remoteNameConflicts: Int = 0
    public var filesFailed: Int = 0
    public var issueCount: Int = 0
    public var elapsedSeconds: Double = 0
}

public struct SyncConflict: Sendable, Equatable {
    public enum RemoteStatus: String, Sendable {
        case present, trashed, removed
    }

    public let id: String
    public let remoteFileId: String
    public let relativePath: String
    public let localPath: String
    public let conflictPath: String?
    public let remoteSHA256: String
    public let remoteSize: Int64
    public let remoteVersion: Int64?
    public let remoteStatus: RemoteStatus
}

public struct SyncConflictEntry: Sendable, Equatable {
    public enum Kind: Sendable, Equatable {
        case sync
        case parentRemoved
    }

    public let id: String
    public let kind: Kind
    public let localFilePath: String
    public let remoteFilePath: String?
    public let localStagedPath: String?
    public let error: String?

    public init(
        id: String,
        kind: Kind,
        localFilePath: String,
        remoteFilePath: String? = nil,
        localStagedPath: String? = nil,
        error: String? = nil
    ) {
        self.id = id
        self.kind = kind
        self.localFilePath = localFilePath
        self.remoteFilePath = remoteFilePath
        self.localStagedPath = localStagedPath
        self.error = error
    }
}

public enum SyncConflictResolution: Sendable {
    case local
    case remote
}

/// SyncEngine Exception type definition
public enum SyncEngineError: Error, LocalizedError, CustomStringConvertible, Sendable, Equatable {
    case localRootNotFound(path: String)
    case localRootChanged(path: String)
    case rootBindingConflict(
        localPath: String,
        remoteRootId: String,
        existingLocalPath: String,
        existingRemoteRootId: String
    )
    case rootBusy(path: String)
    case remoteRootLost(remoteId: String, reason: String)
    case localFileModified(path: String)
    case localFilePublicationFailed(path: String)
    case general(String)

    public var errorDescription: String? {
        description
    }

    public var description: String {
        switch self {
        case .localRootNotFound(let path):
            return "The local sync root no longer exists or is not a valid directory: \(path). Sync stopped to prevent remote deletion propagation."
        case .localRootChanged(let path):
            return "The local sync root was replaced by a different directory: \(path). Sync stopped to prevent remote deletion propagation."
        case .rootBindingConflict(
            let localPath, let remoteRootId, let existingLocalPath, let existingRemoteRootId):
            return "The requested sync root binding \(localPath) <-> \(remoteRootId) conflicts with the existing binding \(existingLocalPath) <-> \(existingRemoteRootId)."
        case .rootBusy(let path):
            return "The local sync root is already being synchronized in this process: \(path)"
        case .remoteRootLost(let remoteId, let reason):
            return "The remote sync root was removed or trashed (\(reason)): \(remoteId). Sync stopped to prevent local deletion propagation."
        case .localFileModified(let path):
            return "Local file changed during synchronization: \(path)"
        case .localFilePublicationFailed(let path):
            return "Local file publication could not be verified after writing began: \(path)"
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
    let conflictDirectory: URL

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
    typealias StableFileDigestCapture = @Sendable (URL) throws -> StableLocalFileDigest
    let stableFileDigestCapture: StableFileDigestCapture
    typealias FilePublisher = @Sendable (
        URL, URL, LocalFileVersion?, String, String?
    ) throws -> LocalFilePublication.Result
    let filePublisher: FilePublisher

    public convenience init(
        auth: Auth,
        store: StateStore? = nil,
        client: DriveClient? = nil,
        idPool: IDPool? = nil,
        downloadTemporaryDirectory: URL = DriveClient.defaultDownloadTemporaryDirectory,
        conflictDirectory: URL = DriveClient.defaultConflictDirectory
    ) async throws {
        try await self.init(auth: auth, store: store, client: client, idPool: idPool,
            downloadTemporaryDirectory: downloadTemporaryDirectory,
            conflictDirectory: conflictDirectory, incrementalScan: Self.defaultDirectoryScan,
            stableFileDigestCapture: { try StableLocalFileDigest.capture(at: $0) },
            filePublisher: {
                try LocalFilePublication.publish($0, to: $1, expected: $2, expectedSHA256: $3, expectedLocalSHA256: $4)
            })
    }

    init(auth: Auth, store: StateStore? = nil, client: DriveClient? = nil, idPool: IDPool? = nil,
         downloadTemporaryDirectory: URL = DriveClient.defaultDownloadTemporaryDirectory,
         conflictDirectory: URL = DriveClient.defaultConflictDirectory,
         incrementalScan: @escaping IncrementalScan,
         stableFileDigestCapture: @escaping StableFileDigestCapture = {
             try StableLocalFileDigest.capture(at: $0)
         },
         filePublisher: @escaping FilePublisher = {
             try LocalFilePublication.publish($0, to: $1, expected: $2, expectedSHA256: $3, expectedLocalSHA256: $4)
         }) async throws {
        guard downloadTemporaryDirectory.isFileURL else {
            throw SyncEngineError.general("The temporary download directory must be a local file path")
        }
        self.downloadTemporaryDirectoryStorage = OSAllocatedUnfairLock(initialState: downloadTemporaryDirectory)
        guard conflictDirectory.isFileURL else {
            throw SyncEngineError.general("The conflict directory must be a local file path")
        }
        self.conflictDirectory = conflictDirectory
        self.directoryScan = incrementalScan
        self.stableFileDigestCapture = stableFileDigestCapture
        self.filePublisher = filePublisher
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
        return try await withRootSyncLock(localPath: normalizedLocalPath, cancellable: true) {
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
        return try await withRootSyncLock(localPath: normalizedLocalPath, cancellable: true) {
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
        return try await withRootSyncLock(localPath: normalizedLocalPath, cancellable: true) {
            try await self.initializeRemoteToLocalEmpty(localPath: normalizedLocalPath, remoteRootId: remoteRootId,
                maxDownloadConcurrency: maxDownloadConcurrency, onProgress: onProgress, initialCursor: nil)
        }
    }

    /// Perform a two-way incremental synchronization on an existing sync root
    /// Combined with local DirectoryScanner Quick scan with Google Drive Changes Incremental changes, consisting of Reconciler Drive tripartite decision-making
    @discardableResult
    public func syncIncremental(
        localPath: String,
        maxConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        let normalizedLocalPath = Self.normalizedPath(localPath)
        return try await withRootSyncLock(localPath: normalizedLocalPath, cancellable: true) {
            guard let root = try await self.activeRootBinding(localRootPath: normalizedLocalPath) else {
                throw SyncEngineError.general(
                    "Directory has no available remote root ID: \(normalizedLocalPath)"
                )
            }
            return try await self.syncIncrementalUnlocked(
                rootId: root.id,
                rootItemId: root.item,
                localPath: normalizedLocalPath,
                remoteRootId: root.remote,
                maxConcurrency: maxConcurrency,
                onProgress: onProgress
            )
        }
    }

    /// Stops admission of new work for the active sync of this exact root and
    /// waits until its already admitted work has drained.
    public func cancelSync(localPath: String) async {
        await RootSyncCoordinator.shared.cancelSync(localRootPath: Self.normalizedPath(localPath))
    }

    // MARK: - Root synchronization lock

    func withRootSyncLock<T: Sendable>(
        localPath: String,
        cancellable: Bool = false,
        operation: @Sendable () async throws -> T
    ) async throws -> T {
        let resolvedLocalPath = Self.normalizedPath(localPath)
        let control = cancellable ? SyncRunControl() : nil
        let token = try await RootSyncCoordinator.shared.acquire(
            localRootPath: resolvedLocalPath, control: control)
        let result: Result<T, any Error>
        do {
            let value = try await SyncRunControl.$current.withValue(control) {
                try control?.checkCancellation()
                try await verifyDatabaseConnection()
                try await validateOperationalStateIsOutsideSyncRoots(resolvedLocalPath)
                try control?.checkCancellation()
                return try await operation()
            }
            result = .success(value)
        } catch {
            result = .failure(error)
        }
        monitor.refreshSnapshot()
        await RootSyncCoordinator.shared.release(token)
        let value = try result.get()
        try control?.checkCancellation()
        return value
    }

    private func validateOperationalStateIsOutsideSyncRoots(_ localPath: String) async throws {
        try await store.read { conn in
            var rootPaths = Set([localPath])
            let roots = try conn.cachedStatement("""
                SELECT local_root_path FROM roots WHERE account_id = 'default';
                """)
            defer { roots.reset() }
            while try roots.step() {
                guard let rootPath = roots.columnText(at: 0) else {
                    throw SyncEngineError.general("Stored root binding is incomplete")
                }
                rootPaths.insert(Self.normalizedPath(rootPath))
            }

            for rootPath in rootPaths {
                try self.validateOperationalStateIsOutsideSyncRoot(rootPath)
            }

            let storageDirectories = try conn.cachedStatement("""
                SELECT path FROM root_storage_directories;
                """)
            defer { storageDirectories.reset() }
            while try storageDirectories.step() {
                guard let storagePath = storageDirectories.columnText(at: 0) else { continue }
                for rootPath in rootPaths
                where RootSyncCoordinator.contains(storagePath, in: rootPath)
                    || RootSyncCoordinator.contains(rootPath, in: storagePath) {
                    throw SyncEngineError.general(
                        "The local sync root overlaps registered storage: \(storagePath). "
                            + "Configure operational state outside the sync root.")
                }
            }

            let conflicts = try conn.cachedStatement("""
                SELECT conflict_path FROM sync_conflicts WHERE conflict_path IS NOT NULL;
                """)
            defer { conflicts.reset() }
            while try conflicts.step() {
                guard let conflictPath = conflicts.columnText(at: 0) else { continue }
                if RootSyncCoordinator.contains(conflictPath, in: localPath) {
                    throw SyncEngineError.general(
                        "The sync root contains a persisted conflict copy: \(conflictPath). "
                            + "Move conflict storage outside the sync root: \(localPath).")
                }
            }
        }
    }

    private func validateOperationalStateIsOutsideSyncRoot(_ localPath: String) throws {
        let protectedFiles = [
            ("Google Drive credential file", auth.fileURL.path),
            ("SQLite state database", store.path)
        ]
        for (description, path) in protectedFiles
        where RootSyncCoordinator.contains(path, in: localPath) {
            throw SyncEngineError.general(
                "The local sync root contains the \(description): \(path). "
                    + "Configure operational state outside the sync root.")
        }

        let protectedDirectories = [
            ("download temporary directory", downloadTemporaryDirectory.path),
            ("conflict directory", conflictDirectory.path),
            ("parent-removed directory", conflictDirectory.deletingLastPathComponent()
                .appendingPathComponent("parent_removed").path)
        ]
        for (description, path) in protectedDirectories
        where RootSyncCoordinator.contains(path, in: localPath)
            || RootSyncCoordinator.contains(localPath, in: path) {
            throw SyncEngineError.general(
                "The local sync root overlaps the \(description): \(path). "
                    + "Configure operational state outside the sync root.")
        }
    }

    private func verifyDatabaseConnection() async throws {
        try await store.read { conn in
            let statement = try conn.cachedStatement("SELECT 1;")
            defer { statement.reset() }
            guard try statement.step() else {
                throw SyncEngineError.general("Database connection check returned no result")
            }
        }
    }

    static func normalizedPath(_ path: String) -> String {
        RootSyncCoordinator.normalizedPath(path)
    }

    private func activeRootBinding(localRootPath: String) async throws -> PendingRoot? {
        let root: PendingRoot? = try await store.read { conn in
            let stmt = try conn.cachedStatement(
                """
                SELECT root_id, remote_root_id
                FROM roots
                WHERE account_id = 'default' AND local_root_path = ? AND is_active = 1;
                """
            )
            stmt.bindText(localRootPath, at: 1)
            defer { stmt.reset() }
            guard try stmt.step(), let rootID = stmt.columnInt64(at: 0),
                  let remoteRootID = stmt.columnText(at: 1) else { return nil }
            let item = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL;")
            item.bindInt64(rootID, at: 1)
            defer { item.reset() }
            guard try item.step(), let rootItem = item.columnInt64(at: 0) else { return nil }
            return PendingRoot(id: rootID, item: rootItem, remote: remoteRootID)
        }
        return root
    }
}
