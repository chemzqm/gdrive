import DirectoryScanner
import Foundation
import os

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

extension IncrementalSyncRun {
    func scanLocal() async throws {
        let baselineCache = try await LocalBaselineCache.load(store: engine.store, rootId: rootID)
        let staticPrefix = localPath.hasSuffix("/") ? localPath : localPath + "/"
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
        let request = ScanRequest(root: localPath, filters: scanFilters, options: scanOptions)

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
            let results = try await IncrementalLocalObservation.hash(
                pending, concurrency: min(6, effectiveSyncConcurrency))
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
                    engine.logger.error(
                        "Local file reading or hashing failed, retain the existing state and continue synchronization [\(failure.observation.url.path)]: \(failure.message)"
                    )
                }
            }
            let successfulObservations = observations
            let failedObservations = failures
            let ids = try await engine.store.write { conn -> [Int64] in
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
                    let stmt = try conn.cachedStatement(
                        """
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
                    stmt.bindInt64(rootID, at: 1)
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
                    let stmt = try conn.cachedStatement(
                        """
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
                    stmt.bindInt64(rootID, at: 1)
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
            if !ids.isEmpty {
                try await self.scheduleFiles(
                    try await self.loadDirtyItems(ids), duringScan: true)
            }
        }
        let sentFirstObservation = OSAllocatedUnfairLock(initialState: false)

        let engine = self.engine
        let rootID = self.rootID
        let rootItemID = self.rootItemID
        let remoteRootID = self.remoteRootID
        let rootURL = self.rootURL
        let now = self.now
        let remoteGate = self.remoteGate
        let directoryContext = self.directoryContext
        let seenTracker = self.seenTracker
        let seenDirTracker = self.seenDirTracker
        let scanProgress = self.scanProgress

        do {
            try await engine.directoryScan(request) { batch in
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
                        let mtime =
                            (record.metadata?.modificationTime.seconds ?? 0) * 1_000_000_000
                            + Int64(record.metadata?.modificationTime.nanoseconds ?? 0)
                        let fileSize = record.metadata?.fileSize ?? 0

                        itemsInBatch.append(
                            DiscoveredRecord(
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

                    if relPath.isEmpty || relPath == ".git" || relPath.hasPrefix(".git/") {
                        continue
                    }

                    if remoteGate.blocks(relPath) { continue }
                    let name = (relPath as NSString).lastPathComponent
                    let parentRel = (relPath as NSString).deletingLastPathComponent
                    let parentRelNormalized =
                        (parentRel == "." || parentRel.isEmpty) ? "" : parentRel

                    if !remoteGate.paths.isEmpty,
                        directoryContext.getItemId(byRelPath: parentRelNormalized) == nil,
                        try remoteGate.blocksLocalAncestors(
                            URL(fileURLWithPath: fullPath).deletingLastPathComponent(),
                            root: rootURL)
                    {
                        continue
                    }

                    if record.type == .directory, !pendingObservations.isEmpty {
                        try await commitObservations(pendingObservations)
                        pendingObservations.removeAll(keepingCapacity: true)
                        firstObservationSent = true
                    }
                    if record.type == .directory {
                        let parentItemId =
                            directoryContext.getItemId(byRelPath: parentRelNormalized) ?? rootItemID
                        let dev = record.dev
                        let ino = record.ino

                        // Check whether the local directory has been renamed or moved (press dev + ino Find)
                        let existingDir:
                            (itemId: Int64, parentId: Int64, name: String, remoteId: String?)? =
                                try await engine.store.read { conn in
                                    let stmt = try conn.cachedStatement(
                                        """
                                        SELECT item_id, parent_id, name, remote_file_id
                                        FROM items
                                        WHERE root_id = ? AND entry_kind = 'directory' AND local_device = ? AND local_inode = ? AND is_tombstone = 0;
                                        """)
                                    stmt.bindInt64(rootID, at: 1)
                                    stmt.bindInt64(dev, at: 2)
                                    stmt.bindInt64(ino, at: 3)
                                    defer { stmt.reset() }
                                    if try stmt.step(),
                                        let iId = stmt.columnInt64(at: 0),
                                        let pId = stmt.columnInt64(at: 1),
                                        let nm = stmt.columnText(at: 2)
                                    {
                                        let rId = stmt.columnText(at: 3)
                                        return (iId, pId, nm, rId)
                                    }
                                    return nil
                                }

                        if let existing = existingDir,
                            remoteGate.blocks(
                                directoryContext.getRelPath(for: existing.itemId) ?? "")
                        {
                            remoteGate.addAlias(relPath)
                            continue
                        }
                        if let existing = existingDir,
                            existing.name != name || existing.parentId != parentItemId
                        {
                            // The local directory is renamed or moved
                            seenDirTracker.markSeen(
                                parentId: existing.parentId, name: existing.name)
                            do {
                                if let rId = existing.remoteId {
                                    let oldPRemote = directoryContext.getRemoteId(
                                        for: existing.parentId)
                                    let newPRemote = directoryContext.getRemoteId(for: parentItemId)
                                    let addP =
                                        (parentItemId != existing.parentId) ? newPRemote : nil
                                    let remP =
                                        (parentItemId != existing.parentId) ? oldPRemote : nil
                                    _ = try await engine.client.updateMetadata(
                                        remoteId: rId, newName: name, addParentId: addP,
                                        removeParentId: remP)
                                }
                                try await engine.store.write { conn in
                                    let stmt = try conn.cachedStatement(
                                        """
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
                                directoryContext.register(
                                    itemId: existing.itemId, parentItemId: parentItemId, name: name,
                                    remoteId: existing.remoteId ?? "")
                            } catch {
                                engine.logger.error(
                                    "Failed to rename or move remote directory [\(existing.name) -> \(name)]: \(error)"
                                )
                            }
                        } else if directoryContext.getItemId(byRelPath: relPath) == nil {
                            // Create a new local directory
                            let remoteParentId =
                                directoryContext.getRemoteId(for: parentItemId) ?? remoteRootID
                            var intent: DurableCreateIntent?
                            do {
                                let candidateRemoteID = try await engine.idPool.nextId()
                                let prepared = try await DurableCreateIntentStore.prepareDirectory(
                                    store: engine.store,
                                    rootID: rootID,
                                    parentItemID: parentItemId,
                                    name: name,
                                    targetParentRemoteID: remoteParentId,
                                    candidateRemoteID: candidateRemoteID,
                                    device: dev,
                                    inode: ino
                                )
                                intent = prepared
                                _ = try await engine.client.createDirectory(
                                    name: name,
                                    parentId: prepared.targetParentRemoteID,
                                    remoteId: prepared.targetRemoteID
                                )
                                try await engine.store.write { conn in
                                    let ts = Date().timeIntervalSince1970
                                    let stmt = try conn.cachedStatement(
                                        """
                                        UPDATE items SET
                                            remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                                        WHERE item_id = ?;
                                        """)
                                    stmt.bindDouble(ts, at: 1)
                                    stmt.bindInt64(prepared.itemID, at: 2)
                                    _ = try stmt.step()
                                    stmt.reset()
                                    try DurableCreateIntentStore.completeOperation(
                                        conn: conn, operationID: prepared.operationID, now: ts)
                                }
                                directoryContext.register(
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
                                        store: engine.store,
                                        operationID: intent.operationID,
                                        error: error
                                    )
                                }
                                engine.logger.error(
                                    "Failed to create remote directory [\(relPath)]: \(error)")
                            }
                        } else {
                            seenDirTracker.markSeen(parentId: parentItemId, name: name)
                        }
                    } else if record.type == .file {
                        scanProgress.incScanned()
                        let parentItemId =
                            directoryContext.getItemId(byRelPath: parentRelNormalized) ?? rootItemID
                        let dev = record.dev
                        let ino = record.ino
                        let mtime = record.mtime
                        let fileSize = record.fileSize

                        // The cache only contains committed, clean remote-present baselines.
                        // Matching the path as well as identity preserves the rename path below,
                        // while an ordinary unchanged file needs no per-item SQLite round trip.
                        if let cached = baselineCache.lookupUnchanged(
                            device: dev, inode: ino, mtime: mtime, size: fileSize),
                            cached.parentId == parentItemId, cached.name == name
                        {
                            seenTracker.markSeen(parentId: parentItemId, name: name)
                            scanProgress.incSkipped()
                            continue
                        }

                        // Check if local files have been renamed or moved (press dev + ino Find)
                        let existingFile:
                            (itemId: Int64, parentId: Int64, name: String, remoteId: String?)? =
                                try await engine.store.read { conn in
                                    let stmt = try conn.cachedStatement(
                                        """
                                        SELECT item_id, parent_id, name, remote_file_id
                                        FROM items
                                        WHERE root_id = ? AND entry_kind = 'file' AND local_device = ? AND local_inode = ? AND is_tombstone = 0;
                                        """)
                                    stmt.bindInt64(rootID, at: 1)
                                    stmt.bindInt64(dev, at: 2)
                                    stmt.bindInt64(ino, at: 3)
                                    defer { stmt.reset() }
                                    if try stmt.step(),
                                        let iId = stmt.columnInt64(at: 0),
                                        let pId = stmt.columnInt64(at: 1),
                                        let nm = stmt.columnText(at: 2)
                                    {
                                        let rId = stmt.columnText(at: 3)
                                        return (iId, pId, nm, rId)
                                    }
                                    return nil
                                }

                        if let existing = existingFile {
                            let parentPath =
                                directoryContext.getRelPath(for: existing.parentId) ?? ""
                            let oldPath =
                                parentPath.isEmpty
                                ? existing.name : "\(parentPath)/\(existing.name)"
                            if remoteGate.blocks(oldPath) { continue }
                        }
                        if let existing = existingFile,
                            existing.name != name || existing.parentId != parentItemId
                        {
                            // Local files are renamed or moved
                            seenTracker.markSeen(parentId: existing.parentId, name: existing.name)
                            do {
                                if let rId = existing.remoteId {
                                    let oldPRemote = directoryContext.getRemoteId(
                                        for: existing.parentId)
                                    let newPRemote = directoryContext.getRemoteId(for: parentItemId)
                                    let addP =
                                        (parentItemId != existing.parentId) ? newPRemote : nil
                                    let remP =
                                        (parentItemId != existing.parentId) ? oldPRemote : nil
                                    _ = try await engine.client.updateMetadata(
                                        remoteId: rId, newName: name, addParentId: addP,
                                        removeParentId: remP)
                                }

                                try await engine.store.write { conn in
                                    let stmt = try conn.cachedStatement(
                                        """
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
                                engine.logger.error(
                                    "Failed to rename or move remote file [\(existing.name) -> \(name)]: \(error)"
                                )
                                continue
                            }
                            // Path updates do not mean that the text has been verified; retain the old metadata and continue content comparison.
                            // A pure name change will still hit the cache below, while a text change will reuse the existing summary and decision process.
                        }

                        seenTracker.markSeen(parentId: parentItemId, name: name)

                        // Quick change comparison (§6.2)
                        if baselineCache.lookupUnchanged(
                            device: dev, inode: ino, mtime: mtime, size: fileSize)
                            != nil
                        {
                            scanProgress.incSkipped()
                            continue
                        }

                        pendingObservations.append(
                            IncrementalLocalObservation(
                                parentID: parentItemId, name: name,
                                url: URL(fileURLWithPath: fullPath),
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
            await self.drainTransfers()
            throw error
        }
        await self.drainTransfers()

    }
}
