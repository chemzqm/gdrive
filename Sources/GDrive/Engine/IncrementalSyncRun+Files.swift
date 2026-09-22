import Darwin
import Foundation
import os

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
    let failures = OSAllocatedUnfairLock(initialState: 0)
    private let databaseFailure = OSAllocatedUnfairLock<Error?>(initialState: nil)

    func recordDatabaseFailure(_ error: Error) {
        guard DatabaseFailure.isSQLite(error) else { return }
        databaseFailure.withLock { if $0 == nil { $0 = error } }
    }

    func throwIfDatabaseFailure() throws {
        if let error = databaseFailure.withLock({ $0 }) { throw error }
    }
}

struct CollidedDownload: Sendable {
    let item: DirtyRecord
    let localFileURL: URL
    let relPath: String
}

extension IncrementalSyncRun {
    func issueSubject(for item: DirtyRecord, relativePath: String? = nil) -> SyncIssueSubject {
        let parent = directoryContext.getRelPath(for: item.parentId) ?? ""
        let path = relativePath ?? (parent.isEmpty ? item.name : "\(parent)/\(item.name)")
        return SyncIssueSubject(
            itemID: item.itemId, remoteFileID: item.remoteFileId, relativePath: path)
    }

    func recordIssue(
        _ error: Error, stage: SyncIssue.Stage, subject: SyncIssueSubject
    ) async {
        guard !(error is CancellationError) else { return }
        do {
            try await SyncIssueStore.record(
                store: engine.store, rootID: rootID, subject: subject,
                stage: stage, error: error)
        } catch {
            actionTracker.recordDatabaseFailure(error)
        }
    }

    private func downloadPlanIsCurrent(_ item: DirtyRecord) async throws -> Bool {
        try await engine.store.read { conn in
            let stmt = try conn.cachedStatement(
                """
                SELECT 1 FROM items WHERE item_id = ? AND local_generation = ?
                    AND remote_generation = ? AND dirty_generation = ?;
                """)
            defer { stmt.reset() }
            stmt.bindInt64(item.itemId, at: 1)
            stmt.bindInt64(item.localGeneration, at: 2)
            stmt.bindInt64(item.remoteGeneration, at: 3)
            stmt.bindInt64(item.dirtyGeneration, at: 4)
            return try stmt.step()
        }
    }

    func processCollidedDownloads() async throws {
        let collided = collidedDownloads.withLock { state -> [CollidedDownload] in
            let all = state
            state.removeAll()
            return all
        }
        guard !collided.isEmpty else { return }

        var receipts: [@Sendable (SQLiteConnection) throws -> Void] = []
        for entry in collided {
            try actionTracker.throwIfDatabaseFailure()
            let item = entry.item
            let localFileURL = entry.localFileURL

            do {
                guard try await downloadPlanIsCurrent(item) else {
                    engine.logger.warning("Download plan expired for collided item: \(item.name)")
                    continue
                }

                let currentVersion = try LocalFileVersion.read(at: localFileURL)
                let localObs: LocalObservation
                if let currentVersion {
                    let digest = try SyncEngine.computeFileSha256(at: localFileURL)
                    try currentVersion.validate(at: localFileURL)
                    localObs = LocalObservation(
                        status: .present, sha256: digest.sha256Hex,
                        size: digest.fileSize, mtime: currentVersion.mtime)
                } else {
                    localObs = LocalObservation(status: .absent)
                }

                let decision = Reconciler.decide(
                    baseline: item.baseline, local: localObs, remote: item.remote)

                try await applyDecision(decision, to: item, receipts: &receipts)
            } catch {
                actionTracker.failures.withLock { $0 += 1 }
                actionTracker.recordDatabaseFailure(error)
                await recordIssue(
                    error, stage: .download,
                    subject: issueSubject(for: item, relativePath: entry.relPath))
                engine.logger.error("Failed to re-reconcile collided download [\(item.name)]: \(error)")
            }
        }

        if !receipts.isEmpty {
            let batch = receipts
            try await engine.store.write { conn in
                for receipt in batch { try receipt(conn) }
            }
        }

        await drainTransfers()
        try actionTracker.throwIfDatabaseFailure()
    }

    func drainTransfers() async {
        await itemTaskRegistry.drainAll()
    }

    private func acquireTransfer() async throws {
        try Task.checkCancellation()
        await syncSemaphore.wait()
        if Task.isCancelled {
            syncSemaphore.signal()
            throw CancellationError()
        }
        do {
            try actionTracker.throwIfDatabaseFailure()
        } catch {
            syncSemaphore.signal()
            throw error
        }
    }

    func scheduleFiles(_ fileItems: [DirtyRecord], duringScan: Bool) async throws {
        var receipts: [@Sendable (SQLiteConnection) throws -> Void] = []
        for item in fileItems {
            try actionTracker.throwIfDatabaseFailure()
            guard let decision = decisionForScheduling(item, duringScan: duringScan) else { continue }
            try Task.checkCancellation()
            do {
                try await applyDecision(decision, to: item, receipts: &receipts)
            } catch {
                switch decision {
                case .trashRemote, .deleteLocal:
                    await recordIssue(error, stage: .delete, subject: issueSubject(for: item))
                case .unchanged
                    where item.local?.status == .absent && item.remote?.status == .trashed:
                    await recordIssue(error, stage: .delete, subject: issueSubject(for: item))
                default:
                    break
                }
                throw error
            }
            if receipts.count >= 64 {
                let batch = receipts
                try await engine.store.write { conn in
                    for receipt in batch { try receipt(conn) }
                }
                receipts.removeAll(keepingCapacity: true)
            }
        }
        try actionTracker.throwIfDatabaseFailure()

        if !receipts.isEmpty {
            let batch = receipts
            try await engine.store.write { conn in
                for receipt in batch { try receipt(conn) }
            }
        }
    }

    private func restoreRemoteParent(for item: DirtyRecord) async {
        guard let parentRemoteID = directoryContext.getRemoteId(for: item.parentId) else { return }
        let isParentTrashed: Bool = (try? await engine.store.read { conn in
            let stmt = try conn.cachedStatement(
                "SELECT remote_status FROM items WHERE item_id = ?;")
            stmt.bindInt64(item.parentId, at: 1)
            defer { stmt.reset() }
            if try stmt.step(), let parentStatus = stmt.columnText(at: 0) {
                return parentStatus == "trashed"
            }
            return false
        }) ?? false
        guard isParentTrashed else { return }
        do {
            try await engine.client.untrash(remoteId: parentRemoteID)
        } catch {
            engine.logger.error(
                "Failed to restore remote parent directory [\(parentRemoteID)]: \(error)"
            )
            return
        }
        do {
            try await engine.store.write { conn in
                let stmt = try conn.cachedStatement(
                    "UPDATE items SET remote_status = 'present', updated_at = ? WHERE item_id = ?;")
                stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                stmt.bindInt64(item.parentId, at: 2)
                _ = try stmt.step()
            }
            directoryContext.markReady(item.parentId)
        } catch {
            engine.logger.error("Failed to record restored remote parent [\(parentRemoteID)]: \(error)")
        }
    }

    private func handleUploadFailure(
        _ error: Error, item: DirtyRecord, createIntent: DurableCreateIntent?
    ) async {
        actionTracker.failures.withLock { $0 += 1 }
        actionTracker.recordDatabaseFailure(error)
        await recordIssue(error, stage: .upload, subject: issueSubject(for: item))
        if let createIntent {
            do {
                try await DurableCreateIntentStore.markUnknownOutcome(
                    store: engine.store, operationID: createIntent.operationID, error: error)
            } catch {
                actionTracker.recordDatabaseFailure(error)
            }
        }
        if case DriveError.unsafeOverwrite = error {
            do {
                try await engine.store.batchWrite { conn in
                    let stmt = try conn.cachedStatement(
                        """
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
            } catch {
                actionTracker.recordDatabaseFailure(error)
            }
        }
        engine.logger.error("Incremental upload failed [\(item.name)]: \(error)")
    }

    private func scheduleUpload(_ item: DirtyRecord) async throws {
        let upBytes = item.local?.size ?? 0
        notifier.addDiscovered(files: 1, bytes: upBytes)
        try await acquireTransfer()
        engine.monitor.enqueueUpload(id: item.name, name: item.name, totalBytes: upBytes)
        do {
            _ = try await itemTaskRegistry.start(itemIDs: [item.itemId]) { [self] in
            var createIntent = item.pendingCreate
            defer {
                engine.monitor.finishUpload(id: item.name)
                syncSemaphore.signal()
                notifier.addCompleted(files: 1, bytes: upBytes)
            }

            do {
                try Task.checkCancellation()
                let parentRel = directoryContext.getRelPath(for: item.parentId) ?? ""
                let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                let localFileURL = rootURL.appendingPathComponent(relPath)
                if createIntent == nil, let remoteID = item.remoteFileId {
                    throw DriveError.unsafeOverwrite(fileId: remoteID)
                }

                await restoreRemoteParent(for: item)
                guard let remoteParentId = directoryContext.getRemoteId(for: item.parentId),
                      directoryContext.isReady(item.parentId) else {
                    throw SyncEngineError.general(
                        "Incremental upload parent became unavailable: \(item.name)")
                }

                let fSize: Int64
                let sha256Hex: String
                if let localSHA256 = item.local?.sha256, let localByteCount = item.local?.size {
                    sha256Hex = localSHA256
                    fSize = localByteCount
                } else {
                    let res = try SyncEngine.computeFileSha256(at: localFileURL)
                    sha256Hex = res.sha256Hex
                    fSize = res.fileSize
                }
                engine.monitor.startUpload(
                    id: item.name, name: item.name, totalBytes: fSize)

                let input = try StableUploadInput.capture(at: localFileURL)
                guard input.size == fSize, input.sha256 == sha256Hex else {
                    throw SyncEngineError.localFileModified(path: localFileURL.path)
                }
                let intent = try await engine.prepareNewFileUpload(
                    rootID: rootID,
                    itemID: item.itemId,
                    parentItemID: item.parentId,
                    name: item.name,
                    parentRemoteID: remoteParentId,
                    input: input,
                    pendingIntent: createIntent
                )
                createIntent = intent
                let uploadedFile = try await engine.performNewFileUpload(
                    rootID: rootID,
                    intent: intent,
                    input: input,
                    name: item.name,
                    multipartRecovery: .verifyExisting
                )
                let receipt = try await DurableCreateIntentStore.commitFileUploadReceipt(
                    store: engine.store,
                    expectation: FileUploadReceiptExpectation(
                        intent: intent,
                        localGeneration: item.localGeneration,
                        remoteGeneration: item.remoteGeneration,
                        dirtyGeneration: item.dirtyGeneration
                    ),
                    uploadedFile: uploadedFile,
                    input: input,
                    now: self.now
                )

                guard case .applied = receipt else {
                    throw SyncEngineError.general(
                        "The upload receipt is stale; the newer generation remains pending: \(item.name)"
                    )
                }

                scheduled.withLock { _ = $0.remove(item.itemId) }
                actionTracker.counts.withLock {
                    $0.uploaded += 1
                    $0.bytesUp += fSize
                }
            } catch {
                await handleUploadFailure(error, item: item, createIntent: createIntent)
            }
            }
        } catch {
            engine.monitor.finishUpload(id: item.name)
            syncSemaphore.signal()
            notifier.addCompleted(files: 1, bytes: upBytes)
            throw error
        }
    }

    private func validateExpectedDestination(at url: URL, item: DirtyRecord) throws -> LocalFileVersion? {
        let expected = try LocalFileVersion.read(at: url)
        if item.local?.status == .present {
            guard let expected,
                expected.device == item.localDevice,
                expected.inode == item.localInode,
                expected.mtime == item.localMtime,
                expected.size == item.local?.size
            else {
                throw SyncEngineError.localFileModified(path: url.path)
            }
            return expected
        }
        if expected != nil {
            throw SyncEngineError.localFileModified(path: url.path)
        }
        return nil
    }

    private func scheduleDownload(_ item: DirtyRecord) async throws {
        let downId = item.remoteFileId ?? item.name
        let downSize = item.remote?.size ?? 0
        notifier.addDiscovered(files: 1, bytes: downSize)
        try await acquireTransfer()
        engine.monitor.enqueueDownload(id: downId, name: item.name, totalBytes: downSize)
        do {
            _ = try await itemTaskRegistry.start(itemIDs: [item.itemId]) { [self] in
            defer {
                engine.monitor.finishDownload(id: downId)
                syncSemaphore.signal()
                notifier.addCompleted(files: 1, bytes: downSize)
            }

            do {
                try Task.checkCancellation()
                guard let remoteFileId = item.remoteFileId else { return }
                let parentRel = directoryContext.getRelPath(for: item.parentId) ?? ""
                let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                let localFileURL = rootURL.appendingPathComponent(relPath)

                // Make sure the local target parent directory exists
                try? FileManager.default.createDirectory(
                    at: localFileURL.deletingLastPathComponent(),
                    withIntermediateDirectories: true)

                let expectedDestination = try validateExpectedDestination(at: localFileURL, item: item)
                engine.monitor.startDownload(
                    id: downId, name: item.name, totalBytes: downSize)
                let cached = downloadCache.take(
                    remoteID: remoteFileId, sha256: item.remote?.sha256 ?? ""
                ).map {
                    DriveClient.VerifiedDownload(url: $0.url, sha256: $0.sha256, size: $0.size)
                }
                defer {
                    if let cached { try? FileManager.default.removeItem(at: cached.url) }
                }
                let execution = try await engine.executeFileDownload(
                    remoteID: remoteFileId,
                    expectedSHA256: item.remote?.sha256,
                    destination: localFileURL,
                    expectedDestination: expectedDestination,
                    temporaryDirectory: downloadDirectory,
                    cached: cached,
                    onProgress: { delta in
                        self.engine.monitor.reportDownloadProgress(
                            id: downId, additionalBytes: delta)
                    },
                    beforePublish: {
                        guard try await self.downloadPlanIsCurrent(item) else {
                            throw SyncEngineError.general(
                                "Download plan has expired, keep local files: \(item.name)")
                        }
                    }
                )
                switch execution {
                case .published(let published):
                    let receipt = try await engine.store.commitFileDownloadReceipt(
                        expectation: .incremental(
                            itemID: item.itemId,
                            localGeneration: item.localGeneration,
                            remoteGeneration: item.remoteGeneration,
                            dirtyGeneration: item.dirtyGeneration
                        ),
                        localURL: localFileURL,
                        published: published
                    )
                    guard receipt == .applied else {
                        throw SyncEngineError.general(
                            "The download receipt is stale; the newer generation remains pending: \(item.name)"
                        )
                    }
                    scheduled.withLock { _ = $0.remove(item.itemId) }
                    actionTracker.counts.withLock {
                        $0.downloaded += 1
                        $0.bytesDown += published.size
                    }
                case .destinationChanged(let download):
                    if downloadCache.store(download, remoteID: remoteFileId, temporaryDirectory: downloadDirectory) != nil {
                        collidedDownloads.withLock {
                            $0.append(CollidedDownload(item: item, localFileURL: localFileURL, relPath: relPath))
                        }
                    } else {
                        try? FileManager.default.removeItem(at: download.url)
                        scheduled.withLock { _ = $0.remove(item.itemId) }
                        throw SyncEngineError.localFileModified(path: localFileURL.path)
                    }
                }
            } catch {
                actionTracker.failures.withLock { $0 += 1 }
                actionTracker.recordDatabaseFailure(error)
                await recordIssue(error, stage: .download, subject: issueSubject(for: item))
                engine.logger.error("Incremental download failed [\(item.name)]: \(error)")
            }
            }
        } catch {
            engine.monitor.finishDownload(id: downId)
            syncSemaphore.signal()
            notifier.addCompleted(files: 1, bytes: downSize)
            throw error
        }
    }

    private func scheduleRemoteDeletion(_ item: DirtyRecord) async throws {
        try await engine.cleanupLocalDeletionToRemoteUnlocked(
            itemID: item.itemId,
            expected: ItemCleanupGenerations(
                local: item.localGeneration, remote: item.remoteGeneration,
                dirty: item.dirtyGeneration),
            taskRegistry: itemTaskRegistry)
        actionTracker.counts.withLock { $0.deleted += 1 }
    }

    private func deleteLocalFile(_ item: DirtyRecord) async throws {
        try await engine.cleanupRemoteDeletionToLocalUnlocked(
            itemID: item.itemId,
            expected: ItemCleanupGenerations(
                local: item.localGeneration, remote: item.remoteGeneration,
                dirty: item.dirtyGeneration),
            taskRegistry: itemTaskRegistry)
        actionTracker.counts.withLock { $0.deleted += 1 }
    }

    private func scheduleConflict(_ item: DirtyRecord) async throws {
        try await acquireTransfer()
        do {
            _ = try await itemTaskRegistry.start(itemIDs: [item.itemId]) { [self] in
            defer {
                syncSemaphore.signal()
            }
            do {
                try Task.checkCancellation()
                guard let remoteID = item.remoteFileId else {
                    throw SyncEngineError.general(
                        "The conflict lacks valid evidence of the content of both parties")
                }
                let parentRel = directoryContext.getRelPath(for: item.parentId) ?? ""
                let relativePath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                let original = rootURL.appendingPathComponent(relativePath)
                let remotePresent = item.remote?.status == .present
                let remoteSHA = remotePresent ? item.remote?.sha256 : item.baseline?.sha256
                let remoteSize = remotePresent ? item.remote?.size : item.baseline?.size
                guard let remoteSHA = remoteSHA?.lowercased(), let remoteSize else {
                    throw SyncEngineError.general(
                        "The conflict lacks a durable remote content identity: \(relativePath)")
                }
                var stored: URL?
                if remotePresent {
                    let conflictRoot = try SyncConflictStore.directory(
                        base: engine.conflictDirectory, remoteRootID: remoteRootID)
                    let destination = conflictRoot.appendingPathComponent(relativePath)
                    try FileManager.default.createDirectory(
                        at: destination.deletingLastPathComponent(), withIntermediateDirectories: true)
                    if let cached = downloadCache.take(remoteID: remoteID, sha256: remoteSHA) {
                        defer { try? FileManager.default.removeItem(at: cached.url) }
                        let expected = try LocalFileVersion.read(at: destination)
                        let result = try LocalFilePublication.publish(
                            cached.url, to: destination, expected: expected,
                            expectedSHA256: remoteSHA)
                        guard case .published = result else {
                            throw SyncEngineError.localFilePublicationFailed(path: destination.path)
                        }
                    } else {
                        _ = try await engine.client.downloadFileSafely(
                            remoteId: remoteID, destinationURL: destination, expectedSha256: remoteSHA,
                            expectedDestination: try LocalFileVersion.read(at: destination),
                            temporaryDirectory: conflictRoot)
                    }
                    stored = destination
                }
                let status: SyncConflict.RemoteStatus = item.remote?.status == .trashed ? .trashed :
                    (remotePresent ? .present : .removed)
                try await SyncConflictStore.commitIncremental(
                    store: engine.store, rootID: rootID, itemID: item.itemId,
                    remoteFileID: remoteID, relativePath: relativePath,
                    localURL: original, conflictURL: stored,
                    remoteSHA: remoteSHA, remoteSize: remoteSize, remoteStatus: status,
                    expectedLocalGeneration: item.localGeneration,
                    expectedRemoteGeneration: item.remoteGeneration,
                    expectedDirtyGeneration: item.dirtyGeneration)
            } catch {
                actionTracker.failures.withLock { $0 += 1 }
                actionTracker.recordDatabaseFailure(error)
                await recordIssue(
                    error, stage: .conflictRefresh, subject: issueSubject(for: item))
                engine.logger.error(
                    "Failed to handle file conflicts [\(item.name)]: \(error)")
            }
            }
        } catch {
            syncSemaphore.signal()
            throw error
        }
    }

    private func applyDecision(_ decision: ReconcileDecision, to item: DirtyRecord,
                               receipts: inout [@Sendable (SQLiteConnection) throws -> Void]) async throws {
        switch decision {
        case .upload, .keepModified(preferLocal: true):
            try await scheduleUpload(item)

        case .download, .keepModified(preferLocal: false):
            try await scheduleDownload(item)

        case .matchUpdateBaseline(let sha, let size):
            receipts.append { conn in
                let stmt = try conn.cachedStatement(
                    """
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
                stmt.bindDouble(self.now, at: 3)
                stmt.bindInt64(item.itemId, at: 4)
                stmt.bindInt64(item.localGeneration, at: 5)
                stmt.bindInt64(item.remoteGeneration, at: 6)
                stmt.bindInt64(item.dirtyGeneration, at: 7)
                _ = try stmt.step()
                stmt.reset()
            }

        case .trashRemote:
            try await scheduleRemoteDeletion(item)

        case .deleteLocal:
            try await deleteLocalFile(item)

        case .conflict:
            try await scheduleConflict(item)

        case .unchanged:
            if item.local?.status == .absent && item.remote?.status == .trashed {
                try await engine.cleanupRemoteDeletionToLocalUnlocked(
                    itemID: item.itemId,
                    expected: ItemCleanupGenerations(
                        local: item.localGeneration, remote: item.remoteGeneration,
                        dirty: item.dirtyGeneration),
                    taskRegistry: itemTaskRegistry)
                actionTracker.counts.withLock { $0.deleted += 1 }
                break
            }
            receipts.append { conn in
                let stmt = try conn.cachedStatement(
                    "UPDATE items SET dirty_generation = 0 WHERE item_id = ? AND local_generation = ? AND remote_generation = ? AND dirty_generation = ?;"
                )
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
    }

    private func decisionForScheduling(_ item: DirtyRecord, duringScan: Bool) -> ReconcileDecision? {
        let parent = directoryContext.getRelPath(for: item.parentId) ?? ""
        let path = parent.isEmpty ? item.name : "\(parent)/\(item.name)"
        guard !remoteGate.blocks(path) else { return nil }
        // Pending parent creation must recover before any child request is sent.
        if duringScan && !directoryContext.isReady(item.parentId) { return nil }
        let decision: ReconcileDecision
        if item.pendingCreate != nil {
            decision = .upload(reason: "Restore persistent small file creation intent")
        } else {
            decision = Reconciler.decide(
                baseline: item.baseline, local: item.local, remote: item.remote)
        }

        // Deletions and conflict copies require the completed local inventory.
        if duringScan {
            switch decision {
            case .upload, .download, .keepModified, .matchUpdateBaseline, .unchanged: break
            default: return nil
            }
        }
        guard !scheduled.withLock({ $0.contains(item.itemId) }) else { return nil }
        if duringScan {
            switch decision {
            case .upload, .download, .keepModified:
                scheduled.withLock { _ = $0.insert(item.itemId) }
            default: break
            }
        }
        return decision
    }
}
