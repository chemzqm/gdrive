import Darwin
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

        struct ExistingLocalItem: Sendable {
            let itemId: Int64
            let parentId: Int64
            let name: String
            let remoteId: String?
        }

        struct DiscoveredRecord: Sendable {
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
        let rootURL = self.rootURL
        let now = self.now
        let remoteGate = self.remoteGate
        let directoryContext = self.directoryContext
        let seenTracker = self.seenTracker
        let seenDirTracker = self.seenDirTracker
        let scanProgress = self.scanProgress
        let localChangeScope = self.localChangeScope

        @Sendable func directoryParentID(
            relPath: String, parentRelPath: String, name: String
        ) async throws -> Int64 {
            guard let parentItemID = directoryContext.getItemId(byRelPath: parentRelPath) else {
                throw SyncEngineError.general("Missing local directory parent while observing \(relPath)")
            }
            guard localChangeScope != nil else { return parentItemID }
            let kind: String? = try await engine.store.read { conn in
                let stmt = try conn.cachedStatement(
                    "SELECT entry_kind FROM items WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;")
                stmt.bindInt64(rootID, at: 1)
                stmt.bindInt64(parentItemID, at: 2)
                stmt.bindText(name, at: 3)
                defer { stmt.reset() }
                guard try stmt.step() else { return nil }
                return stmt.columnText(at: 0)
            }
            if kind == "file" {
                throw SyncEngineError.general("Local directory replaces an existing file at \(relPath); preserving the baseline")
            }
            return parentItemID
        }

        @Sendable func fileParentID(relPath: String, parentRelPath: String) throws -> Int64 {
            guard let parentItemID = directoryContext.getItemId(byRelPath: parentRelPath) else {
                throw SyncEngineError.general("Missing local directory parent while observing \(relPath)")
            }
            guard localChangeScope != nil else { return parentItemID }
            guard directoryContext.getItemId(byRelPath: relPath) == nil else {
                throw SyncEngineError.general("Local file replaces an existing directory at \(relPath); preserving the baseline")
            }
            return parentItemID
        }

        @Sendable func remoteParentID(parentItemID: Int64, relPath: String) throws -> String {
            guard directoryContext.isReady(parentItemID) else {
                throw SyncEngineError.general(
                    "Remote directory parent is not ready while creating \(relPath)")
            }
            guard let remoteID = directoryContext.getRemoteId(for: parentItemID) else {
                throw SyncEngineError.general("Missing remote directory parent while creating \(relPath)")
            }
            return remoteID
        }

        @Sendable func observeDirectory(_ record: DiscoveredRecord, relPath: String, parentRelNormalized: String, name: String) async throws {
            let parentItemId = try await directoryParentID(
                relPath: relPath, parentRelPath: parentRelNormalized, name: name)
            let dev = record.dev
            let ino = record.ino

            // Check whether the local directory has been renamed or moved (press dev + ino Find)
            let existingDir:
                ExistingLocalItem? =
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
                            let itemName = stmt.columnText(at: 2) {
                            let rId = stmt.columnText(at: 3)
                            return ExistingLocalItem(
                                itemId: iId, parentId: pId, name: itemName, remoteId: rId)
                        }
                        return nil
                    }

            if let existing = existingDir,
                remoteGate.blocks(
                    directoryContext.getRelPath(for: existing.itemId) ?? "") {
                remoteGate.addAlias(relPath)
                return
            }
            if let existing = existingDir,
                existing.name != name || existing.parentId != parentItemId {
                // The local directory is renamed or moved
                seenDirTracker.markSeen(
                    parentId: existing.parentId, name: existing.name)
                do {
                    if let remoteID = existing.remoteId {
                        let oldParentRemoteID = directoryContext.getRemoteId(for: existing.parentId)
                        let newParentRemoteID = directoryContext.getRemoteId(for: parentItemId)
                        let moving = parentItemId != existing.parentId
                        _ = try await engine.client.updateMetadata(
                            remoteId: remoteID, newName: name,
                            addParentId: moving ? newParentRemoteID : nil,
                            removeParentId: moving ? oldParentRemoteID : nil)
                    }
                    try await engine.store.write { conn in
                        let statement = try conn.cachedStatement(
                            "UPDATE items SET name = ?, parent_id = ?, updated_at = ? WHERE item_id = ?;")
                        statement.bindText(name, at: 1)
                        statement.bindInt64(parentItemId, at: 2)
                        statement.bindDouble(now, at: 3)
                        statement.bindInt64(existing.itemId, at: 4)
                        _ = try statement.step()
                        statement.reset()
                    }
                    seenDirTracker.markSeen(parentId: parentItemId, name: name)
                    directoryContext.register(
                        itemId: existing.itemId, parentItemId: parentItemId, name: name,
                        remoteId: existing.remoteId ?? "", updateDescendantPaths: true)
                } catch {
                    if DatabaseFailure.isSQLite(error) { throw error }
                    engine.logger.error(
                        "Failed to rename or move remote directory [\(existing.name) -> \(name)]: \(error)")
                    return
                }
            } else if directoryContext.getItemId(byRelPath: relPath) == nil {
                // Create a new local directory
                let remoteParentId = try remoteParentID(parentItemID: parentItemId, relPath: relPath)
                var intent: DurableCreateIntent?
                do {
                    try self.actionTracker.throwIfDatabaseFailure()
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
                        let timestamp = Date().timeIntervalSince1970
                        let stmt = try conn.cachedStatement(
                            """
                            UPDATE items SET
                                remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                            WHERE item_id = ?;
                            """)
                        stmt.bindDouble(timestamp, at: 1)
                        stmt.bindInt64(prepared.itemID, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                        try DurableCreateIntentStore.completeOperation(
                            conn: conn, operationID: prepared.operationID, now: timestamp)
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
                    if DatabaseFailure.isSQLite(error) { throw error }
                    if let intent {
                        try await DurableCreateIntentStore.markUnknownOutcome(
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
        }

        @Sendable func observeFile(_ record: DiscoveredRecord, fullPath: String, parentRelNormalized: String, name: String,
                                   pendingObservations: inout [IncrementalLocalObservation], firstObservationSent: inout Bool) async throws {
            scanProgress.incScanned()
            let relativePath = parentRelNormalized.isEmpty ? name : "\(parentRelNormalized)/\(name)"
            let parentItemId = try fileParentID(
                relPath: relativePath, parentRelPath: parentRelNormalized)
            let dev = record.dev
            let ino = record.ino
            let mtime = record.mtime
            let fileSize = record.fileSize

            // The cache only contains committed, clean remote-present baselines.
            // Matching the path as well as identity preserves the rename path below,
            // while an ordinary unchanged file needs no per-item SQLite round trip.
            if !(localChangeScope?.includes(relativePath) ?? false),
                let cached = baselineCache.lookupUnchanged(
                    device: dev, inode: ino, mtime: mtime, size: fileSize),
                cached.parentId == parentItemId, cached.name == name {
                seenTracker.markSeen(parentId: parentItemId, name: name)
                scanProgress.incSkipped()
                return
            }

            // Check if local files have been renamed or moved (press dev + ino Find)
            let existingFile:
                ExistingLocalItem? =
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
                            let itemName = stmt.columnText(at: 2) {
                            let rId = stmt.columnText(at: 3)
                            return ExistingLocalItem(
                                itemId: iId, parentId: pId, name: itemName, remoteId: rId)
                        }
                        return nil
                    }

            if let existing = existingFile {
                let parentPath =
                    directoryContext.getRelPath(for: existing.parentId) ?? ""
                let oldPath =
                    parentPath.isEmpty
                    ? existing.name : "\(parentPath)/\(existing.name)"
                if remoteGate.blocks(oldPath) { return }
            }
            if let existing = existingFile,
                existing.name != name || existing.parentId != parentItemId {
                // Local files are renamed or moved
                seenTracker.markSeen(parentId: existing.parentId, name: existing.name)
                do {
                    if let remoteID = existing.remoteId {
                        let oldParentRemoteID = directoryContext.getRemoteId(for: existing.parentId)
                        let newParentRemoteID = directoryContext.getRemoteId(for: parentItemId)
                        let moving = parentItemId != existing.parentId
                        _ = try await engine.client.updateMetadata(
                            remoteId: remoteID, newName: name,
                            addParentId: moving ? newParentRemoteID : nil,
                            removeParentId: moving ? oldParentRemoteID : nil)
                    }
                    try await engine.store.write { conn in
                        let statement = try conn.cachedStatement(
                            "UPDATE items SET name = ?, parent_id = ?, updated_at = ? WHERE item_id = ?;")
                        statement.bindText(name, at: 1)
                        statement.bindInt64(parentItemId, at: 2)
                        statement.bindDouble(now, at: 3)
                        statement.bindInt64(existing.itemId, at: 4)
                        _ = try statement.step()
                        statement.reset()
                    }
                    seenTracker.markSeen(parentId: parentItemId, name: name)
                } catch {
                    if DatabaseFailure.isSQLite(error) { throw error }
                    engine.logger.error(
                        "Failed to rename or move remote file [\(existing.name) -> \(name)]: \(error)")
                    return
                }
                // Path updates do not mean that the text has been verified; retain the old metadata and continue content comparison.
                // A pure name change will still hit the cache below, while a text change will reuse the existing summary and decision process.
            }

            seenTracker.markSeen(parentId: parentItemId, name: name)

            // Quick change comparison (§6.2)
            if !(localChangeScope?.includes(relativePath) ?? false),
                baselineCache.lookupUnchanged(
                    device: dev, inode: ino, mtime: mtime, size: fileSize)
                != nil {
                scanProgress.incSkipped()
                return
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

        @Sendable func copyScanRecords(_ batch: ScanBatch) -> [DiscoveredRecord] {
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
            return itemsInBatch
        }

        @Sendable func processRecords(_ records: [DiscoveredRecord]) async throws {
            var pendingObservations: [IncrementalLocalObservation] = []
            var firstObservationSent = sentFirstObservation.withLock { $0 }
            for record in records {
                try self.actionTracker.throwIfDatabaseFailure()
                let fullPath = record.fullPath
                let relPath: String
                if fullPath.hasPrefix(staticPrefix) {
                    relPath = String(fullPath.dropFirst(staticPrefix.count))
                } else if localChangeScope == nil {
                    relPath = (fullPath as NSString).lastPathComponent
                } else {
                    continue
                }
                guard !relPath.isEmpty,
                    relPath != ".git", !relPath.hasPrefix(".git/"),
                    self.isInLocalScope(relPath)
                else { continue }
                if remoteGate.blocks(relPath) {
                    continue
                }
                let name = (relPath as NSString).lastPathComponent
                let parentRel = (relPath as NSString).deletingLastPathComponent
                let parentRelNormalized = (parentRel == "." || parentRel.isEmpty) ? "" : parentRel
                if !remoteGate.paths.isEmpty,
                    directoryContext.getItemId(byRelPath: parentRelNormalized) == nil,
                    try remoteGate.blocksLocalAncestors(
                        URL(fileURLWithPath: fullPath).deletingLastPathComponent(), root: rootURL) {
                    continue
                }
                if record.type == .directory, !pendingObservations.isEmpty {
                    try self.actionTracker.throwIfDatabaseFailure()
                    try await commitObservations(pendingObservations)
                    pendingObservations.removeAll(keepingCapacity: true)
                    firstObservationSent = true
                }
                if record.type == .directory {
                    try await observeDirectory(record, relPath: relPath,
                        parentRelNormalized: parentRelNormalized, name: name)
                } else if record.type == .file {
                    try await observeFile(record, fullPath: fullPath,
                        parentRelNormalized: parentRelNormalized, name: name,
                        pendingObservations: &pendingObservations,
                        firstObservationSent: &firstObservationSent)
                }
            }
            try self.actionTracker.throwIfDatabaseFailure()
            try await commitObservations(pendingObservations)
            let sent = firstObservationSent || !pendingObservations.isEmpty
            sentFirstObservation.withLock { $0 = sent }
        }

        func validateAncestors(of relativePath: String) throws -> Bool {
            var ancestor = rootURL
            let components = relativePath.split(separator: "/", omittingEmptySubsequences: true)
            for component in components.dropLast() {
                ancestor.appendPathComponent(String(component), isDirectory: true)
                var ancestorStat = stat()
                guard lstat(ancestor.path, &ancestorStat) == 0 else {
                    if errno == ENOENT { return false }
                    throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
                }
                if ancestorStat.st_mode & S_IFMT == S_IFLNK {
                    throw SyncEngineError.general("Refusing to observe a changed path through symbolic link: \(ancestor.path)")
                }
                guard ancestorStat.st_mode & S_IFMT == S_IFDIR else {
                    throw SyncEngineError.general("Changed path has a non-directory ancestor: \(ancestor.path)")
                }
            }
            return true
        }

        func recordAt(relativePath: String) throws -> DiscoveredRecord? {
            guard try validateAncestors(of: relativePath) else { return nil }
            let url = rootURL.appendingPathComponent(relativePath)
            var value = stat()
            guard lstat(url.path, &value) == 0 else {
                if errno == ENOENT { return nil }
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            let type: EntryType
            switch value.st_mode & S_IFMT {
            case S_IFREG: type = .file
            case S_IFDIR: type = .directory
            case S_IFLNK: type = .symbolicLink
            default: type = .other
            }
            return DiscoveredRecord(type: type, fullPath: url.path, dev: Int64(value.st_dev),
                ino: Int64(value.st_ino),
                mtime: Int64(value.st_mtimespec.tv_sec) * 1_000_000_000
                    + Int64(value.st_mtimespec.tv_nsec), fileSize: Int64(value.st_size))
        }

        func planScopedPath(
            _ rawPath: String, scope: LocalChangeScope,
            directRecords: inout [String: DiscoveredRecord],
            directoryRoots: inout [String: DiscoveredRecord]
        ) throws {
            guard let relative = scope.relativePath(rawPath) else { return }
            guard let record = try recordAt(relativePath: relative) else {
                if directoryContext.getItemId(byRelPath: relative) != nil {
                    scope.markMissingSubtree(relative)
                } else {
                    scope.markMissingExact(relative)
                }
                return
            }
            if record.type == .directory {
                scope.markExistingDirectory(relative)
                directoryRoots[relative] = record
            } else {
                scope.markExistingFile(relative)
                directRecords[relative] = record
            }
        }

        func planScopedChanges(
            _ changes: [LocalChange], scope: LocalChangeScope,
            directRecords: inout [String: DiscoveredRecord],
            directoryRoots: inout [String: DiscoveredRecord]
        ) throws {
            for change in changes {
                switch change {
                case .created(let path, _), .modified(let path, _), .deleted(let path, _):
                    try planScopedPath(path, scope: scope, directRecords: &directRecords,
                        directoryRoots: &directoryRoots)
                case .moved(let from, let destination, _):
                    try planScopedPath(from, scope: scope, directRecords: &directRecords,
                        directoryRoots: &directoryRoots)
                    try planScopedPath(destination, scope: scope, directRecords: &directRecords,
                        directoryRoots: &directoryRoots)
                }
            }
        }

        func scanScoped(_ scope: LocalChangeScope) async throws {
            var directRecords: [String: DiscoveredRecord] = [:]
            var directoryRoots: [String: DiscoveredRecord] = [:]
            try planScopedChanges(scope.changes, scope: scope, directRecords: &directRecords,
                directoryRoots: &directoryRoots)

            let subtreeRoots = directoryRoots.keys.filter { candidate in
                !directoryRoots.keys.contains { other in
                    other != candidate && (other.isEmpty || candidate.hasPrefix(other + "/"))
                }
            }
            let roots = Set(subtreeRoots)
            directRecords = directRecords.filter { _, record in
                guard record.fullPath.hasPrefix(staticPrefix) else { return false }
                let relative = String(record.fullPath.dropFirst(staticPrefix.count))
                return !roots.contains { root in root.isEmpty || relative.hasPrefix(root + "/") }
            }

            var ancestorPaths = Set<String>()
            for relative in roots.union(Set(directRecords.values.compactMap { record in
                guard record.fullPath.hasPrefix(staticPrefix) else { return nil }
                return String(record.fullPath.dropFirst(staticPrefix.count))
            })) {
                var current = (relative as NSString).deletingLastPathComponent
                while current != "." && !current.isEmpty {
                    ancestorPaths.insert(current)
                    current = (current as NSString).deletingLastPathComponent
                }
            }
            for relative in ancestorPaths.sorted(by: { $0.split(separator: "/").count < $1.split(separator: "/").count }) {
                guard let record = try recordAt(relativePath: relative), record.type == .directory else {
                    throw SyncEngineError.general("Changed path has no readable directory ancestor: \(relative)")
                }
                scope.markObservedDirectory(relative)
                try await processRecords([record])
            }
            for relative in subtreeRoots.sorted(by: { $0.split(separator: "/").count < $1.split(separator: "/").count }) {
                if let record = directoryRoots[relative] {
                    try await processRecords([record])
                }
            }
            try await processRecords(Array(directRecords.values))

            for relative in subtreeRoots.sorted(by: { $0.split(separator: "/").count < $1.split(separator: "/").count }) {
                let absolute = relative.isEmpty ? localPath : rootURL.appendingPathComponent(relative).path
                let prefix = relative.isEmpty ? staticPrefix : absolute + "/"
                var options = scanOptions
                options.pathPrefix = Array(prefix.utf8)
                let request = ScanRequest(root: absolute, filters: scanFilters, options: options)
                try await engine.directoryScan(request) { batch in
                    try await processRecords(copyScanRecords(batch))
                }
                scope.markScannedSubtree(relative)
            }
        }

        do {
            if let scope = localChangeScope {
                try await scanScoped(scope)
            } else {
                let request = ScanRequest(root: localPath, filters: scanFilters, options: scanOptions)
                try await engine.directoryScan(request) { batch in
                    try await processRecords(copyScanRecords(batch))
                }
            }
        } catch {
            // No transfer may escape a failed/cancelled scan and mutate state after return.
            await self.drainTransfers()
            try self.actionTracker.throwIfDatabaseFailure()
            throw error
        }
        await self.drainTransfers()
        try self.actionTracker.throwIfDatabaseFailure()

    }
}
