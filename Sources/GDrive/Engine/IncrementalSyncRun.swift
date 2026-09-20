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
    let downloadDirectory: URL
    let now: TimeInterval
    let maxConcurrency: Int
    let effectiveSyncConcurrency: Int
    let notifier: ProgressNotifier
    let remoteChanges: RemoteChanges
    let remoteGate: RemoteChanges.Gate
    let directoryContext: DirectoryContext
    let syncSemaphore: AsyncSemaphore
    let syncGroup: DispatchGroup
    let actionTracker: ActionTracker
    let scheduled: OSAllocatedUnfairLock<Set<Int64>>
    let startedTransfers: OSAllocatedUnfairLock<Bool>
    let seenTracker: SeenItemsTracker
    let seenDirTracker: SeenItemsTracker
    let scanProgress: ScanProgress
    let startTime: DispatchTime
    let recoveredConflicts: Int
    let localChangeScope: LocalChangeScope?

    private init(
        engine: SyncEngine,
        rootID: Int64,
        rootItemID: Int64,
        localPath: String,
        rootURL: URL,
        remoteRootID: String,
        downloadDirectory: URL,
        now: TimeInterval,
        maxConcurrency: Int,
        effectiveSyncConcurrency: Int,
        notifier: ProgressNotifier,
        remoteChanges: RemoteChanges,
        remoteGate: RemoteChanges.Gate,
        directoryContext: DirectoryContext,
        syncSemaphore: AsyncSemaphore,
        syncGroup: DispatchGroup,
        actionTracker: ActionTracker,
        scheduled: OSAllocatedUnfairLock<Set<Int64>>,
        startedTransfers: OSAllocatedUnfairLock<Bool>,
        seenTracker: SeenItemsTracker,
        seenDirTracker: SeenItemsTracker,
        scanProgress: ScanProgress,
        startTime: DispatchTime,
        recoveredConflicts: Int,
        localChangeScope: LocalChangeScope?
    ) {
        self.engine = engine
        self.rootID = rootID
        self.rootItemID = rootItemID
        self.localPath = localPath
        self.rootURL = rootURL
        self.remoteRootID = remoteRootID
        self.downloadDirectory = downloadDirectory
        self.now = now
        self.maxConcurrency = maxConcurrency
        self.effectiveSyncConcurrency = effectiveSyncConcurrency
        self.notifier = notifier
        self.remoteChanges = remoteChanges
        self.remoteGate = remoteGate
        self.directoryContext = directoryContext
        self.syncSemaphore = syncSemaphore
        self.syncGroup = syncGroup
        self.actionTracker = actionTracker
        self.scheduled = scheduled
        self.startedTransfers = startedTransfers
        self.seenTracker = seenTracker
        self.seenDirTracker = seenDirTracker
        self.scanProgress = scanProgress
        self.startTime = startTime
        self.recoveredConflicts = recoveredConflicts
        self.localChangeScope = localChangeScope
    }
}

extension SyncEngine {
    func syncIncrementalUnlocked(
        rootId: Int64,
        rootItemId: Int64,
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?,
        localChanges: [LocalChange]? = nil
    ) async throws -> SyncStats {
        let run = try await IncrementalSyncRun.prepare(
            engine: self,
            rootID: rootId,
            rootItemID: rootItemId,
            localPath: localPath,
            remoteRootID: remoteRootId,
            maxConcurrency: maxConcurrency,
            onProgress: onProgress,
            localChanges: localChanges
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
        onProgress: (@Sendable (SyncProgress) -> Void)?,
        localChanges: [LocalChange]?
    ) async throws -> IncrementalSyncRun {
        let startTime = DispatchTime.now()
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)
        let now = Date().timeIntervalSince1970
        var isDir: ObjCBool = false
        let localExists = FileManager.default.fileExists(
            atPath: resolvedLocalPath, isDirectory: &isDir)
        guard localExists && isDir.boolValue else {
            engine.logger.error(
                "[Sync] The local sync root is missing or invalid: \(resolvedLocalPath). Stopping sync to protect remote files."
            )
            throw SyncEngineError.localRootNotFound(path: resolvedLocalPath)
        }
        try await validateRemoteRoot(engine: engine, remoteRootID: remoteRootID)
        let downloadDirectory = try await engine.downloadStagingDirectory(
            remoteRootID: remoteRootID, localRoot: rootURL)
        var prepared = false
        defer {
            if !prepared { engine.cleanupDownloadStagingDirectory(downloadDirectory) }
        }
        let pendingConflicts = try await ConflictOperation.pending(
            store: engine.store, rootID: rootID)
        let recoveredConflicts = pendingConflicts.count
        if !pendingConflicts.isEmpty {
            try await withThrowingTaskGroup(of: Void.self) { group in
                let limit = max(1, min(64, maxConcurrency))
                for (index, pendingOperation) in pendingConflicts.enumerated() {
                    if index >= limit { try await group.next() }
                    group.addTask {
                        try await engine.resolveConflict(pendingOperation, temporaryDirectory: downloadDirectory)
                    }
                }
                try await group.waitForAll()
            }
        }
        let remoteChanges = RemoteChanges(
            store: engine.store, client: engine.client, rootID: rootID, remoteRootID: remoteRootID,
            rootURL: rootURL)
        try await remoteChanges.consume()
        try await engine.refreshSyncConflicts(rootID: rootID)
        let remoteGate = try await remoteChanges.gate()
        let directoryContext = try await loadDirectoryContext(
            engine: engine, rootID: rootID, rootItemID: rootItemID, remoteRootID: remoteRootID)
        let effectiveSyncConcurrency = max(1, min(64, maxConcurrency))
        let run = IncrementalSyncRun(
            engine: engine, rootID: rootID, rootItemID: rootItemID,
            localPath: resolvedLocalPath, rootURL: rootURL, remoteRootID: remoteRootID,
            downloadDirectory: downloadDirectory, now: now, maxConcurrency: maxConcurrency,
            effectiveSyncConcurrency: effectiveSyncConcurrency, notifier: notifier,
            remoteChanges: remoteChanges, remoteGate: remoteGate,
            directoryContext: directoryContext,
            syncSemaphore: AsyncSemaphore(count: effectiveSyncConcurrency),
            syncGroup: DispatchGroup(), actionTracker: ActionTracker(),
            scheduled: OSAllocatedUnfairLock(initialState: Set<Int64>()),
            startedTransfers: OSAllocatedUnfairLock(initialState: false),
            seenTracker: SeenItemsTracker(), seenDirTracker: SeenItemsTracker(),
            scanProgress: ScanProgress(),
            startTime: startTime, recoveredConflicts: recoveredConflicts,
            localChangeScope: localChanges.map { LocalChangeScope(rootURL: rootURL, changes: $0) }
        )
        prepared = true
        return run
    }

    func execute() async throws -> SyncStats {
        defer { engine.cleanupDownloadStagingDirectory(downloadDirectory) }
        try await scanLocal()
        try actionTracker.throwIfDatabaseFailure()
        try await markMissingAfterSuccessfulScan()
        let dirtyItems = try await loadDirtyItems()
        let eligibleItems = dirtyItems.filter { item in
            let parent = directoryContext.getRelPath(for: item.parentId) ?? ""
            let path = parent.isEmpty ? item.name : "\(parent)/\(item.name)"
            return !remoteGate.blocks(path) && isInLocalScope(path)
        }
        let fileItems = eligibleItems.filter { $0.entryKind == "file" }
        let dirItems = eligibleItems.filter { $0.entryKind == "directory" }
        try await recoverPendingDirectories(dirItems)
        do {
            try await scheduleFiles(fileItems, duringScan: false)
        } catch {
            await drainTransfers()
            try actionTracker.throwIfDatabaseFailure()
            throw error
        }
        await drainTransfers()
        try actionTracker.throwIfDatabaseFailure()
        try await engine.store.flush()
        try await reconcileDirectories(dirItems)
        return try await finish()
    }

    func isInLocalScope(_ relativePath: String) -> Bool {
        localChangeScope?.includes(relativePath) ?? true
    }

    private func finish() async throws -> SyncStats {
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            syncGroup.notify(queue: .global(qos: .userInitiated)) { cont.resume() }
        }
        let enumerated = try await remoteChanges.enumeratePending(limit: maxConcurrency)
        var stats = SyncStats()
        stats.remoteWorkPending = max(try await remoteChanges.pendingCount(), enumerated ? 1 : 0)
        if stats.remoteWorkPending > 0 {
            stats.remoteNameConflicts = try await remoteChanges.nameConflictCount()
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
        stats.filesDeleted = actionTracker.deleted
        stats.conflictsResolved = recoveredConflicts + actionTracker.conflicts.withLock { $0 }
        stats.conflicts = try await SyncConflictStore.list(store: engine.store, rootID: rootID)
        stats.remoteWorkPending += stats.conflicts.count
        stats.filesFailed = scanProgress.failed + actionTracker.failures.withLock { $0 }
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
