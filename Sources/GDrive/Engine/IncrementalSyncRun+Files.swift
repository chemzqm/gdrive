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
    let conflicts = OSAllocatedUnfairLock(initialState: 0)
    let failures = OSAllocatedUnfairLock(initialState: 0)
}

extension IncrementalSyncRun {
    func drainTransfers() async {
        // Keep no-change scans free of the new streaming phase's queue hops.
        guard startedTransfers.withLock({ $0 }) else { return }
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            syncGroup.notify(queue: .global(qos: .userInitiated)) { cont.resume() }
        }
    }

    private func acquireTransfer() async throws {
        try Task.checkCancellation()
        await syncSemaphore.wait()
        if Task.isCancelled {
            syncSemaphore.signal()
            throw CancellationError()
        }
        startedTransfers.withLock { $0 = true }
    }

    func scheduleFiles(_ fileItems: [DirtyRecord], duringScan: Bool) async throws {
        var receipts: [@Sendable (SQLiteConnection) throws -> Void] = []
        for item in fileItems {
            guard let decision = decisionForScheduling(item, duringScan: duringScan) else { continue }
            try Task.checkCancellation()
            try await applyDecision(decision, to: item, receipts: &receipts)
            if receipts.count >= 64 {
                let batch = receipts
                try await engine.store.write { conn in
                    for receipt in batch { try receipt(conn) }
                }
                receipts.removeAll(keepingCapacity: true)
            }
        }

        if !receipts.isEmpty {
            let batch = receipts
            try await engine.store.write { conn in
                for receipt in batch { try receipt(conn) }
            }
        }
    }

    private func scheduleUpload(_ item: DirtyRecord) async throws {
        let upBytes = item.local?.size ?? 0
        notifier.addDiscovered(files: 1, bytes: upBytes)
        try await acquireTransfer()
        engine.monitor.enqueueUpload(id: item.name, name: item.name, totalBytes: upBytes)
        syncGroup.enter()
        Task {
            var createIntent = item.pendingCreate
            defer {
                engine.monitor.finishUpload(id: item.name)
                syncSemaphore.signal()
                syncGroup.leave()
                notifier.addCompleted(files: 1, bytes: upBytes)
            }

            do {
                let parentRel = directoryContext.getRelPath(for: item.parentId) ?? ""
                let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                let localFileURL = rootURL.appendingPathComponent(relPath)
                let remoteParentId =
                    directoryContext.getRemoteId(for: item.parentId) ?? remoteRootID

                if createIntent == nil, let remoteID = item.remoteFileId {
                    throw DriveError.unsafeOverwrite(fileId: remoteID)
                }

                // If the remote parent directory has been moved to the recycle bin before, the parent directory will be automatically restored before uploading the child files.
                func restoreRemoteParent() async {
                    if let parentRId = directoryContext.getRemoteId(for: item.parentId) {
                        let isParentTrashed: Bool =
                            (try? await engine.store.read { conn in
                                let stmt = try conn.cachedStatement(
                                    "SELECT remote_status FROM items WHERE item_id = ?;")
                                stmt.bindInt64(item.parentId, at: 1)
                                defer { stmt.reset() }
                                if try stmt.step(), let parentStatus = stmt.columnText(at: 0) {
                                    return parentStatus == "trashed"
                                }
                                return false
                            }) ?? false

                        if isParentTrashed {
                            do {
                                try await engine.client.untrash(remoteId: parentRId)
                                try await engine.store.write { conn in
                                    let stmt = try conn.cachedStatement(
                                        "UPDATE items SET remote_status = 'present', updated_at = ? WHERE item_id = ?;"
                                    )
                                    stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                                    stmt.bindInt64(item.parentId, at: 2)
                                    _ = try stmt.step()
                                }
                            } catch {
                                engine.logger.error(
                                    "Failed to restore remote parent directory [\(parentRId)]: \(error)"
                                )
                            }
                        }
                    }
                }
                await restoreRemoteParent()

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
                    throw DriveError.fileModifiedDuringUpload(path: localFileURL.path)
                }
                let mtime = input.version.mtime
                let dev = input.version.device
                let ino = input.version.inode
                func uploadBody() async throws -> DriveFile {
                    if fSize > 8 * 1024 * 1024 {
                        let target: DurableCreateIntent
                        if let pending = createIntent {
                            guard let expectedSHA256 = pending.expectedSHA256,
                                expectedSHA256.caseInsensitiveCompare(sha256Hex)
                                    == .orderedSame,
                                pending.totalBytes == fSize
                            else {
                                throw SyncEngineError.general(
                                    "Unfinished large file creation intent input changed: \(item.name)"
                                )
                            }
                            target = pending
                        } else {
                            let newRemoteId = try await engine.idPool.nextId()
                            target = try await DurableCreateIntentStore.prepareFileUpload(
                                store: engine.store,
                                rootID: rootID,
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
                        return try await engine.performResumableUpload(
                            rootId: rootID,
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
                            pending.totalBytes == fSize
                        else {
                            throw SyncEngineError.general(
                                "Unfinished small file creation intent input has changed: \(item.name)"
                            )
                        }
                        guard let fileData = input.data else {
                            throw SyncEngineError.general(
                                "Stable small-file input is missing its in-memory body: \(item.name)"
                            )
                        }
                        return try await engine.client.uploadMultipart(
                            name: item.name,
                            parentId: pending.targetParentRemoteID,
                            remoteId: pending.targetRemoteID,
                            content: fileData,
                            expectedSha256: sha256Hex
                        )
                    } else if let existingRemoteId = item.remoteFileId {
                        guard let fileData = input.data else {
                            throw SyncEngineError.general(
                                "Stable small-file input is missing its in-memory body: \(item.name)"
                            )
                        }
                        return try await engine.client.updateMultipart(
                            remoteId: existingRemoteId,
                            content: fileData,
                            expectedSha256: sha256Hex
                        )
                    } else {
                        guard let fileData = input.data else {
                            throw SyncEngineError.general(
                                "Stable small-file input is missing its in-memory body: \(item.name)"
                            )
                        }
                        let newRemoteId = try await engine.idPool.nextId()
                        let prepared = try await DurableCreateIntentStore.prepareFileUpload(
                            store: engine.store,
                            rootID: rootID,
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
                        return try await engine.client.uploadMultipart(
                            name: item.name,
                            parentId: prepared.targetParentRemoteID,
                            remoteId: prepared.targetRemoteID,
                            content: fileData,
                            expectedSha256: sha256Hex
                        )
                    }
                }
                let uploadedFile = try await uploadBody()

                let completedCreateIntent = createIntent
                try input.version.validate(at: localFileURL)
                let receiptApplied = OSAllocatedUnfairLock(initialState: false)
                try await engine.store.batchWrite { conn in
                    guard (try? LocalFileVersion.read(at: localFileURL)) == input.version
                    else { return }
                    let stmt = try conn.cachedStatement(
                        """
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
                    stmt.bindDouble(self.now, at: 10)
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
                            now: self.now
                        )
                    }
                }

                guard receiptApplied.withLock({ $0 }) else {
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
                actionTracker.failures.withLock { $0 += 1 }
                if let createIntent {
                    try? await DurableCreateIntentStore.markUnknownOutcome(
                        store: engine.store,
                        operationID: createIntent.operationID,
                        error: error
                    )
                }
                if case DriveError.unsafeOverwrite = error {
                    try? await engine.store.batchWrite { conn in
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
                }
                engine.logger.error("Incremental upload failed [\(item.name)]: \(error)")
            }
        }
    }

    private func scheduleDownload(_ item: DirtyRecord) async throws {
        let downId = item.remoteFileId ?? item.name
        let downSize = item.remote?.size ?? 0
        notifier.addDiscovered(files: 1, bytes: downSize)
        try await acquireTransfer()
        engine.monitor.enqueueDownload(id: downId, name: item.name, totalBytes: downSize)
        syncGroup.enter()
        Task {
            defer {
                engine.monitor.finishDownload(id: downId)
                syncSemaphore.signal()
                syncGroup.leave()
                notifier.addCompleted(files: 1, bytes: downSize)
            }

            do {
                guard let remoteFileId = item.remoteFileId else { return }
                let parentRel = directoryContext.getRelPath(for: item.parentId) ?? ""
                let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                let localFileURL = rootURL.appendingPathComponent(relPath)

                // Make sure the local target parent directory exists
                try? FileManager.default.createDirectory(
                    at: localFileURL.deletingLastPathComponent(),
                    withIntermediateDirectories: true)

                let expectedDestination = try LocalFileVersion.read(at: localFileURL)
                if item.local?.status == .present {
                    guard let expectedDestination,
                        expectedDestination.device == item.localDevice,
                        expectedDestination.inode == item.localInode,
                        expectedDestination.mtime == item.localMtime,
                        expectedDestination.size == item.local?.size
                    else {
                        throw DriveError.fileModifiedDuringUpload(path: localFileURL.path)
                    }
                } else if expectedDestination != nil {
                    throw DriveError.fileModifiedDuringUpload(path: localFileURL.path)
                }
                engine.monitor.startDownload(
                    id: downId, name: item.name, totalBytes: downSize)
                let published = try await engine.client.downloadFileSafely(
                    remoteId: remoteFileId,
                    destinationURL: localFileURL,
                    expectedSha256: item.remote?.sha256,
                    expectedDestination: expectedDestination,
                    temporaryDirectory: downloadDirectory,
                    beforePublish: {
                        let current = try await self.engine.store.read { conn in
                            let stmt = try conn.cachedStatement(
                                """
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
                            throw SyncEngineError.general(
                                "Download plan has expired, keep local files: \(item.name)")
                        }
                    },
                    onProgress: { delta in
                        self.engine.monitor.reportDownloadProgress(
                            id: downId, additionalBytes: delta)
                    }
                )

                let fSize = published.size
                let mtime = published.mtime

                let receiptApplied = OSAllocatedUnfairLock(initialState: false)
                try await engine.store.batchWrite { conn in
                    guard (try? LocalFileVersion.read(at: localFileURL)) == published else {
                        return
                    }
                    let stmt = try conn.cachedStatement(
                        """
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
                    throw SyncEngineError.general(
                        "The download receipt is stale; the newer generation remains pending: \(item.name)"
                    )
                }

                scheduled.withLock { _ = $0.remove(item.itemId) }
                actionTracker.counts.withLock {
                    $0.downloaded += 1
                    $0.bytesDown += fSize
                }
            } catch {
                actionTracker.failures.withLock { $0 += 1 }
                engine.logger.error("Incremental download failed [\(item.name)]: \(error)")
            }
        }
    }

    private func scheduleRemoteDeletion(_ item: DirtyRecord) async throws {
        try await acquireTransfer()
        syncGroup.enter()
        Task {
            defer {
                syncSemaphore.signal()
                syncGroup.leave()
            }
            do {
                if let rId = item.remoteFileId {
                    try await engine.client.trash(remoteId: rId)
                }
                try await engine.store.batchWrite { conn in
                    let stmt = try conn.cachedStatement(
                        """
                        UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                        """)
                    stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                    stmt.bindInt64(item.itemId, at: 2)
                    _ = try stmt.step()
                    stmt.reset()
                }
                actionTracker.counts.withLock { $0.deleted += 1 }
            } catch {
                engine.logger.error("Remote file deletion failed [\(item.name)]: \(error)")
            }
        }
    }

    private func deleteLocalFile(_ item: DirtyRecord) async throws {
        let parentRel = directoryContext.getRelPath(for: item.parentId) ?? ""
        let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
        let localFileURL = rootURL.appendingPathComponent(relPath)
        var trashSucceeded = true

        if FileManager.default.fileExists(atPath: localFileURL.path) {
            var trashURL: NSURL?
            do {
                try FileManager.default.trashItem(
                    at: localFileURL, resultingItemURL: &trashURL)
            } catch {
                trashSucceeded = false
                engine.logger.warning(
                    "Unable to move local file to Trash [\(relPath)]: \(error). Keeping it locally and marking the operation blocked; permanent deletion is disabled."
                )
            }
        }

        if trashSucceeded {
            try await engine.store.batchWrite { conn in
                let stmt = try conn.cachedStatement(
                    """
                    UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                    """)
                stmt.bindDouble(self.now, at: 1)
                stmt.bindInt64(item.itemId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
            actionTracker.counts.withLock { $0.deleted += 1 }
        } else {
            try await engine.store.batchWrite { conn in
                let stmt = try conn.cachedStatement(
                    """
                    UPDATE items SET phase = 'blocked', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                    """)
                stmt.bindDouble(self.now, at: 1)
                stmt.bindInt64(item.itemId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
        }
    }

    private func scheduleConflict(_ item: DirtyRecord, winner: ConflictWinner, conflictId: String) async throws {
        try await acquireTransfer()
        syncGroup.enter()
        Task {
            defer {
                syncSemaphore.signal()
                syncGroup.leave()
            }
            do {
                guard winner == .remote, let remoteID = item.remoteFileId,
                    let localSHA = item.local?.sha256?.lowercased(),
                    let remoteSHA = item.remote?.sha256?.lowercased()
                else {
                    throw SyncEngineError.general(
                        "The conflict lacks valid evidence of the content of both parties")
                }
                let parentRel = directoryContext.getRelPath(for: item.parentId) ?? ""
                let original = rootURL.appendingPathComponent(parentRel)
                    .appendingPathComponent(
                        item.name)
                let conflictOperation = try await ConflictOperation.prepare(
                    store: engine.store,
                    rootID: rootID, itemID: item.itemId, parentID: item.parentId,
                    original: original, remoteID: remoteID,
                    parentRemoteID: directoryContext.getRemoteId(for: item.parentId)
                        ?? remoteRootID,
                    copyRemoteID: engine.idPool.nextId(), conflictID: conflictId,
                    localSHA: localSHA, remoteSHA: remoteSHA,
                    localGeneration: item.localGeneration,
                    remoteGeneration: item.remoteGeneration,
                    dirtyGeneration: item.dirtyGeneration)
                try await engine.resolveConflict(conflictOperation, temporaryDirectory: downloadDirectory)
                actionTracker.conflicts.withLock { $0 += 1 }
            } catch {
                actionTracker.failures.withLock { $0 += 1 }
                engine.logger.error(
                    "Failed to handle file conflicts [\(item.name)]: \(error)")
            }
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

        case .conflict(let winner, let conflictId):
            try await scheduleConflict(item, winner: winner, conflictId: conflictId)

        case .unchanged:
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
