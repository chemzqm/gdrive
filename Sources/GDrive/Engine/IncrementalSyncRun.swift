import Foundation
import os

/// Per-invocation references and synchronization state for incremental work.
final class IncrementalSyncRun: Sendable {
    let engine: SyncEngine
    let rootID: Int64
    let rootItemID: Int64
    let localPath: String
    let rootURL: URL
    let remoteRootID: String
    let localRootIdentity: LocalDirectoryIdentity
    let downloadDirectory: URL
    let now: TimeInterval
    let maxConcurrency: Int
    let effectiveSyncConcurrency: Int
    let notifier: ProgressNotifier
    let remoteChanges: RemoteChanges
    let remoteGate: RemoteChanges.Gate
    let directoryContext: DirectoryContext
    let syncSemaphore: AsyncSemaphore
    let itemTaskRegistry: ItemTaskRegistry
    let actionTracker: ActionTracker
    let scheduled: OSAllocatedUnfairLock<Set<Int64>>
    let seenTracker: SeenItemsTracker
    let seenDirTracker: SeenItemsTracker
    let failedDirectorySubtrees: FailedDirectorySubtrees
    let scanProgress: ScanProgress
    let startTime: DispatchTime
    let recoveredCleanups: Int
    let downloadCache: DownloadCache
    let collidedDownloads: OSAllocatedUnfairLock<[CollidedDownload]>

    private init(
        engine: SyncEngine,
        rootID: Int64,
        rootItemID: Int64,
        localPath: String,
        rootURL: URL,
        remoteRootID: String,
        localRootIdentity: LocalDirectoryIdentity,
        downloadDirectory: URL,
        now: TimeInterval,
        maxConcurrency: Int,
        effectiveSyncConcurrency: Int,
        notifier: ProgressNotifier,
        remoteChanges: RemoteChanges,
        remoteGate: RemoteChanges.Gate,
        directoryContext: DirectoryContext,
        syncSemaphore: AsyncSemaphore,
        itemTaskRegistry: ItemTaskRegistry,
        actionTracker: ActionTracker,
        scheduled: OSAllocatedUnfairLock<Set<Int64>>,
        seenTracker: SeenItemsTracker,
        seenDirTracker: SeenItemsTracker,
        scanProgress: ScanProgress,
        startTime: DispatchTime,
        recoveredCleanups: Int
    ) {
        self.engine = engine
        self.rootID = rootID
        self.rootItemID = rootItemID
        self.localPath = localPath
        self.rootURL = rootURL
        self.remoteRootID = remoteRootID
        self.localRootIdentity = localRootIdentity
        self.downloadDirectory = downloadDirectory
        self.now = now
        self.maxConcurrency = maxConcurrency
        self.effectiveSyncConcurrency = effectiveSyncConcurrency
        self.notifier = notifier
        self.remoteChanges = remoteChanges
        self.remoteGate = remoteGate
        self.directoryContext = directoryContext
        self.syncSemaphore = syncSemaphore
        self.itemTaskRegistry = itemTaskRegistry
        self.actionTracker = actionTracker
        self.scheduled = scheduled
        self.seenTracker = seenTracker
        self.seenDirTracker = seenDirTracker
        self.failedDirectorySubtrees = FailedDirectorySubtrees()
        self.scanProgress = scanProgress
        self.startTime = startTime
        self.recoveredCleanups = recoveredCleanups
        self.downloadCache = DownloadCache()
        self.collidedDownloads = OSAllocatedUnfairLock(initialState: [])
    }
}

extension SyncEngine {
    func syncIncrementalUnlocked(
        rootId: Int64,
        rootItemId: Int64,
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?
    ) async throws -> SyncStats {
        try SyncRunControl.current?.checkCancellation()
        try await SyncIssueStore.clear(store: store, rootID: rootId)
        let run = try await IncrementalSyncRun.prepare(
            engine: self,
            rootID: rootId,
            rootItemID: rootItemId,
            localPath: localPath,
            remoteRootID: remoteRootId,
            maxConcurrency: maxConcurrency,
            onProgress: onProgress
        )
        return try await run.execute()
    }
}

extension IncrementalSyncRun {
    static func prepare(
        engine: SyncEngine,
        rootID: Int64,
        rootItemID: Int64,
        localPath: String,
        remoteRootID: String,
        maxConcurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?
    ) async throws -> IncrementalSyncRun {
        let startTime = DispatchTime.now()
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)
        let now = Date().timeIntervalSince1970
        guard let currentIdentity = try LocalDirectoryIdentity.read(at: rootURL) else {
            engine.logger.error(
                "[Sync] The local sync root is missing or invalid: \(resolvedLocalPath). Stopping sync to protect remote files."
            )
            throw SyncEngineError.localRootNotFound(path: resolvedLocalPath)
        }
        let storedIdentity = try await engine.store.read { conn in
            let statement = try conn.cachedStatement(
                "SELECT local_root_device, local_root_inode FROM roots WHERE root_id = ? AND is_active = 1;")
            defer { statement.reset() }
            statement.bindInt64(rootID, at: 1)
            guard try statement.step(),
                  let device = statement.columnInt64(at: 0),
                  let inode = statement.columnInt64(at: 1) else {
                throw SyncEngineError.general("The active local root binding is missing: \(resolvedLocalPath)")
            }
            return LocalDirectoryIdentity(device: device, inode: inode)
        }
        guard currentIdentity == storedIdentity else {
            engine.logger.error(
                "[Sync] The local sync root identity changed: \(resolvedLocalPath). Stopping sync to protect remote files."
            )
            throw SyncEngineError.localRootChanged(path: resolvedLocalPath)
        }
        try await validateRemoteRoot(engine: engine, remoteRootID: remoteRootID)
        let downloadDirectory = try await engine.downloadStagingDirectory(
            remoteRootID: remoteRootID, localRoot: rootURL)
        try SyncRunControl.current?.checkCancellation()
        let itemTaskRegistry = ItemTaskRegistry()
        var prepared = false
        defer {
            if !prepared { engine.cleanupDownloadStagingDirectory(downloadDirectory) }
        }
        guard let currentIdentity = try LocalDirectoryIdentity.read(at: rootURL) else {
            throw SyncEngineError.localRootNotFound(path: resolvedLocalPath)
        }
        guard currentIdentity == storedIdentity else {
            throw SyncEngineError.localRootChanged(path: resolvedLocalPath)
        }
        let recoveredCleanups = try await engine.recoverPendingItemCleanups(
            rootID: rootID, taskRegistry: itemTaskRegistry)
        let remoteChanges = RemoteChanges(
            store: engine.store, client: engine.client, rootID: rootID, remoteRootID: remoteRootID,
            rootURL: rootURL)
        try await remoteChanges.consume()
        try await engine.refreshSyncConflicts(rootID: rootID)
        let remoteGate = try await remoteChanges.gate()
        let directoryContext = try await loadDirectoryContext(
            engine: engine, rootID: rootID, rootItemID: rootItemID, remoteRootID: remoteRootID)
        let effectiveSyncConcurrency = max(1, min(64, maxConcurrency))
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)
        let run = IncrementalSyncRun(
            engine: engine, rootID: rootID, rootItemID: rootItemID,
            localPath: resolvedLocalPath, rootURL: rootURL, remoteRootID: remoteRootID,
            localRootIdentity: storedIdentity,
            downloadDirectory: downloadDirectory, now: now, maxConcurrency: maxConcurrency,
            effectiveSyncConcurrency: effectiveSyncConcurrency, notifier: notifier,
            remoteChanges: remoteChanges, remoteGate: remoteGate,
            directoryContext: directoryContext,
            syncSemaphore: AsyncSemaphore(count: effectiveSyncConcurrency),
            itemTaskRegistry: itemTaskRegistry,
            actionTracker: ActionTracker(),
            scheduled: OSAllocatedUnfairLock(initialState: Set<Int64>()),
            seenTracker: SeenItemsTracker(), seenDirTracker: SeenItemsTracker(),
            scanProgress: ScanProgress(),
            startTime: startTime, recoveredCleanups: recoveredCleanups
        )
        prepared = true
        return run
    }

    func execute() async throws -> SyncStats {
        defer {
            notifier.stop()
            downloadCache.clear()
            engine.cleanupDownloadStagingDirectory(downloadDirectory)
        }
        return try await withTaskCancellationHandler {
            do {
                return try await executeStages()
            } catch {
                await itemTaskRegistry.drainAll()
                throw error
            }
        } onCancel: {
            Task { await self.itemTaskRegistry.cancelAll() }
        }
    }

    private func executeStages() async throws -> SyncStats {
        try SyncRunControl.current?.checkCancellation()
        let pendingDirectories = try await loadDirtyItems().filter { item in
            guard item.entryKind == "directory", item.pendingCreate != nil else { return false }
            let parent = directoryContext.getRelPath(for: item.parentId) ?? ""
            let path = parent.isEmpty ? item.name : "\(parent)/\(item.name)"
            return !remoteGate.blocks(path)
        }
        try await recoverPendingDirectories(pendingDirectories)
        try SyncRunControl.current?.checkCancellation()
        try await scanLocal()
        try actionTracker.throwIfDatabaseFailure()
        try validateLocalRootIdentity()
        try SyncRunControl.current?.checkCancellation()
        try await markMissingAfterSuccessfulScan()
        try SyncRunControl.current?.checkCancellation()
        let dirtyItems = try await loadDirtyItems()
        let eligibleItems = dirtyItems.filter { item in
            let parent = directoryContext.getRelPath(for: item.parentId) ?? ""
            let path = parent.isEmpty ? item.name : "\(parent)/\(item.name)"
            return !remoteGate.blocks(path)
        }
        let dirItems = eligibleItems.filter { $0.entryKind == "directory" }
        let deletionRoots = dirItems.compactMap { item -> String? in
            let unilateralDeletion =
                (item.local?.status == .absent && item.remote?.status == .present)
                || (item.remote?.status == .trashed && item.local?.status == .present)
            guard unilateralDeletion else { return nil }
            return directoryContext.getRelPath(for: item.itemId)
        }
        let fileItems = eligibleItems.filter { item in
            guard item.entryKind == "file" else { return false }
            let parent = directoryContext.getRelPath(for: item.parentId) ?? ""
            let path = parent.isEmpty ? item.name : "\(parent)/\(item.name)"
            return !deletionRoots.contains { root in path == root || path.hasPrefix(root + "/") }
        }
        try validateLocalRootIdentity()
        do {
            try await scheduleFiles(fileItems, duringScan: false)
        } catch {
            await drainTransfers()
            try actionTracker.throwIfDatabaseFailure()
            throw error
        }
        await drainTransfers()
        try actionTracker.throwIfDatabaseFailure()
        try SyncRunControl.current?.checkCancellation()
        try await processCollidedDownloads()
        try actionTracker.throwIfDatabaseFailure()
        try await engine.store.flush()
        try validateLocalRootIdentity()
        try SyncRunControl.current?.checkCancellation()
        try await reconcileDirectories(dirItems)
        return try await finish()
    }

    func validateLocalRootIdentity() throws {
        guard let currentIdentity = try LocalDirectoryIdentity.read(at: rootURL) else {
            throw SyncEngineError.localRootNotFound(path: localPath)
        }
        guard currentIdentity == localRootIdentity else {
            throw SyncEngineError.localRootChanged(path: localPath)
        }
    }

    func shouldAbortRun(for error: any Error) -> Bool {
        if DatabaseFailure.isSQLite(error) || error is CancellationError { return true }
        guard let syncError = error as? SyncEngineError else { return false }
        switch syncError {
        case .localRootNotFound, .localRootChanged, .remoteRootLost:
            return true
        default:
            return false
        }
    }

    private func finish() async throws -> SyncStats {
        await itemTaskRegistry.drainAll()
        let enumerated = try await remoteChanges.enumeratePending(limit: maxConcurrency)
        var stats = SyncStats()
        stats.remoteWorkPending = max(try await remoteChanges.pendingCount(), enumerated ? 1 : 0)
        if stats.remoteWorkPending > 0 {
            let nameConflicts = try await remoteChanges.nameConflicts()
            stats.remoteNameConflicts = nameConflicts.count
            for conflict in nameConflicts {
                await recordIssue(
                    RemoteNameConflictIssueError(description: conflict.message),
                    stage: .pathUpdate,
                    subject: SyncIssueSubject(
                        itemID: nil, remoteFileID: conflict.remoteID,
                        relativePath: conflict.relativePath))
            }
        }
        try await engine.store.flush()
        try await engine.store.checkpoint()
        let elapsed =
            Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds)
            / 1_000_000_000
        stats.directoriesCreated = scanProgress.dirsCreated
        stats.filesScanned = scanProgress.scanned
        stats.filesSkipped = scanProgress.skipped
        stats.filesUploaded = actionTracker.uploaded
        stats.bytesUploaded = actionTracker.bytesUp
        stats.filesDownloaded = actionTracker.downloaded
        stats.bytesDownloaded = actionTracker.bytesDown
        stats.filesDeleted = recoveredCleanups + actionTracker.deleted
        stats.conflicts = try await SyncConflictStore.list(store: engine.store, rootID: rootID)
        stats.remoteWorkPending += stats.conflicts.count
        stats.filesFailed = scanProgress.failed + actionTracker.failures.withLock { $0 }
        stats.issueCount = try await SyncIssueStore.count(store: engine.store, rootID: rootID)
        stats.elapsedSeconds = elapsed
        notifier.finish()
        return stats
    }
}

extension IncrementalSyncRun {
    private static func validateRemoteRoot(engine: SyncEngine, remoteRootID: String) async throws {
        let remoteRoot: DriveFile
        do {
            remoteRoot = try await engine.client.getFile(remoteId: remoteRootID)
        } catch let error as DriveError {
            switch error {
            case .notFound:
                engine.logger.error(
                    "[Sync] The remote sync root does not exist (404): \(remoteRootID). Stopping sync to protect local files."
                )
                throw SyncEngineError.remoteRootLost(remoteId: remoteRootID, reason: "notFound")
            default: throw error
            }
        } catch {
            let nsError = error as NSError
            if (nsError.domain == "SyncEngine" || nsError.domain == "DriveError")
                && nsError.code == 404 {
                engine.logger.error(
                    "[Sync] The remote sync root does not exist (404): \(remoteRootID). Stopping sync to protect local files."
                )
                throw SyncEngineError.remoteRootLost(remoteId: remoteRootID, reason: "notFound")
            }
            throw error
        }
        if remoteRoot.trashed == true {
            engine.logger.error(
                "[Sync] The remote sync root is trashed: \(remoteRootID). Stopping sync to protect local files."
            )
            throw SyncEngineError.remoteRootLost(remoteId: remoteRootID, reason: "trashed")
        }
        guard remoteRoot.isDirectory else {
            engine.logger.error(
                "[Sync] The remote sync root is not a valid directory: \(remoteRootID). Stopping sync to protect local files."
            )
            throw SyncEngineError.remoteRootLost(remoteId: remoteRootID, reason: "notDirectory")
        }
    }
}
