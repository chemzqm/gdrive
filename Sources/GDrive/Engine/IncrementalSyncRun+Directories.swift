import Darwin
import Foundation
import os

final class DirectoryContext: @unchecked Sendable {
    private var dirPaths: [Int64: String] = [:]  // item_id -> relPath
    private var dirRemoteIds: [Int64: String] = [:]  // item_id -> remote_file_id
    private var dirIdByRemote: [String: Int64] = [:]  // remote_file_id -> item_id
    private var dirIdByRelPath: [String: Int64] = [:]  // relPath -> item_id
    private var readyDirectories: Set<Int64> = []
    private var lock = os_unfair_lock()

    init(rootItemId: Int64, remoteRootId: String) {
        readyDirectories.insert(rootItemId)
        dirPaths[rootItemId] = ""
        dirRemoteIds[rootItemId] = remoteRootId
        dirIdByRemote[remoteRootId] = rootItemId
        dirIdByRelPath[""] = rootItemId
    }

    func register(
        itemId: Int64, parentItemId: Int64, name: String, remoteId: String,
        remotePresent: Bool = true
    ) {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }
        if remotePresent && readyDirectories.contains(parentItemId) {
            readyDirectories.insert(itemId)
        } else {
            readyDirectories.remove(itemId)
        }
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

extension IncrementalSyncRun {
    static func loadDirectoryContext(
        engine: SyncEngine, rootID: Int64, rootItemID: Int64, remoteRootID: String
    ) async throws -> DirectoryContext {
        struct StoredDirectory: Sendable {
            let id: Int64
            let parentId: Int64
            let name: String
            let remoteId: String
            let present: Bool
        }
        let dirContext = DirectoryContext(rootItemId: rootItemID, remoteRootId: remoteRootID)
        try await engine.store.read { conn in
            let stmt = try conn.cachedStatement(
                """
                SELECT item_id, parent_id, name, remote_file_id, remote_status
                FROM items
                WHERE root_id = ? AND entry_kind = 'directory' AND is_tombstone = 0 AND parent_id IS NOT NULL;
                """)
            stmt.bindInt64(rootID, at: 1)
            var rawDirs: [StoredDirectory] = []
            while try stmt.step() {
                if let id = stmt.columnInt64(at: 0), let parentId = stmt.columnInt64(at: 1),
                    let name = stmt.columnText(at: 2), let rId = stmt.columnText(at: 3) {
                    rawDirs.append(StoredDirectory(id: id, parentId: parentId, name: name, remoteId: rId, present: stmt.columnText(at: 4) == "present"))
                }
            }
            stmt.reset()
            var registered = Set<Int64>([rootItemID])
            var remaining = rawDirs
            while !remaining.isEmpty {
                let countBefore = remaining.count
                remaining.removeAll { dir in
                    if registered.contains(dir.parentId) {
                        dirContext.register(
                            itemId: dir.id, parentItemId: dir.parentId, name: dir.name,
                            remoteId: dir.remoteId,
                            remotePresent: dir.present)
                        registered.insert(dir.id)
                        return true
                    }
                    return false
                }
                if remaining.count == countBefore {
                    for dir in remaining {
                        dirContext.register(
                            itemId: dir.id, parentItemId: rootItemID, name: dir.name,
                            remoteId: dir.remoteId,
                            remotePresent: false)
                    }
                    break
                }
            }
        }
        return dirContext
    }
}

extension IncrementalSyncRun {
    func recoverPendingDirectories(_ dirItems: [DirtyRecord]) async throws {
        // Resumes the last creation of a directory that was persisted before the request but for which a completion receipt has not yet been submitted.
        // createDirectory Use the same pregenerated ID Retry; if the server succeeded last time,DriveClient will be in 409 Then verify the same object.
        let pendingDirectoryCreates = dirItems.compactMap { item -> (DirtyRecord, DurableCreateIntent)? in
            guard let intent = item.pendingCreate else { return nil }
            return (item, intent)
        }.sorted { lhs, rhs in
            let left = directoryContext.getRelPath(for: lhs.0.itemId) ?? lhs.0.name
            let right = directoryContext.getRelPath(for: rhs.0.itemId) ?? rhs.0.name
            return left.split(separator: "/").count < right.split(separator: "/").count
        }

        for (item, intent) in pendingDirectoryCreates {
            do {
                _ = try await engine.client.createDirectory(
                    name: item.name,
                    parentId: intent.targetParentRemoteID,
                    remoteId: intent.targetRemoteID
                )
                try await engine.store.batchWrite { conn in
                    let timestamp = Date().timeIntervalSince1970
                    let stmt = try conn.cachedStatement(
                        """
                        UPDATE items SET
                            remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                        WHERE item_id = ?;
                        """)
                    stmt.bindDouble(timestamp, at: 1)
                    stmt.bindInt64(intent.itemID, at: 2)
                    _ = try stmt.step()
                    stmt.reset()
                    try DurableCreateIntentStore.completeOperation(
                        conn: conn, operationID: intent.operationID, now: timestamp)
                }
            } catch {
                await DurableCreateIntentStore.markUnknownOutcome(
                    store: engine.store,
                    operationID: intent.operationID,
                    error: error
                )
                engine.logger.error(
                    "Failed to restore remote directory creation [\(item.name)]: \(error)")
            }
        }
    }

    func reconcileDirectories(_ dirItems: [DirtyRecord]) async throws {
        // 1. For ordinary directories in a non-deleted state, submit and clear them directly. dirty_generation
        func commitSettledDirectories() async throws {
            if !dirItems.isEmpty {
                try await engine.store.write { conn in
                    for item in dirItems {
                        let isCandidate =
                            (item.local?.status == .absent && item.remote?.status == .present)
                            || (item.remote?.status == .trashed && item.local?.status == .present)
                            || (item.local?.status == .absent && item.remote?.status == .trashed)
                        if !isCandidate {
                            if item.local?.status != .unknown && item.remote?.status != .unknown {
                                do {
                                    let stmt = try conn.cachedStatement(
                                        """
                                        UPDATE items SET phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                                        """)
                                    stmt.bindDouble(self.now, at: 1)
                                    stmt.bindInt64(item.itemId, at: 2)
                                    _ = try stmt.step()
                                    stmt.reset()
                                }
                            }
                        } else if item.local?.status == .absent && item.remote?.status == .trashed {
                            // Both ends have been deleted
                            do {
                                let stmt = try conn.cachedStatement(
                                    """
                                    UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                                    """)
                                stmt.bindDouble(self.now, at: 1)
                                stmt.bindInt64(item.itemId, at: 2)
                                _ = try stmt.step()
                                stmt.reset()
                            }
                            self.actionTracker.counts.withLock { $0.deleted += 1 }
                        }
                    }

                }
            }
        }
        try await commitSettledDirectories()

        // 2. For directories with unilateral deletion intentions, perform barrier verification in reverse order of tree depth (bottom-up, leaf directories first)
        let dirCandidates = dirItems.filter {
            ($0.local?.status == .absent && $0.remote?.status == .present)
                || ($0.remote?.status == .trashed && $0.local?.status == .present)
        }.sorted { firstDirectory, secondDirectory in
            let pathA = directoryContext.getRelPath(for: firstDirectory.itemId) ?? ""
            let pathB = directoryContext.getRelPath(for: secondDirectory.itemId) ?? ""
            let depthA = pathA.isEmpty ? 0 : pathA.split(separator: "/").count
            let depthB = pathB.isEmpty ? 0 : pathB.split(separator: "/").count
            return depthA > depthB
        }

        for dirItem in dirCandidates {
            let parentRel = directoryContext.getRelPath(for: dirItem.parentId) ?? ""
            let relPath = parentRel.isEmpty ? dirItem.name : "\(parentRel)/\(dirItem.name)"
            let localDirURL = rootURL.appendingPathComponent(relPath)

            // Recursively query the status of all descendants of the current directory
            let barrier = try await engine.store.read { conn in
                let stmt = try conn.cachedStatement(
                    """
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
                stmt.bindInt64(rootID, at: 2)
                stmt.bindInt64(rootID, at: 3)
                defer { stmt.reset() }
                if try stmt.step() {
                    let total = Int(stmt.columnInt64(at: 0) ?? 0)
                    let localPresent = Int(stmt.columnInt64(at: 1) ?? 0)
                    let remotePresent = Int(stmt.columnInt64(at: 2) ?? 0)
                    let pending = Int(stmt.columnInt64(at: 3) ?? 0)
                    return (
                        total: total, localPresent: localPresent, remotePresent: remotePresent,
                        pending: pending
                    )
                }
                return (total: 0, localPresent: 0, remotePresent: 0, pending: 0)
            }

            func reconcileRemoteDeletion() async throws {
                // The directory is deleted locally, but the remote directory is still there (original intention: trashRemote)
                // Barrier check: if there are any remote files in the descendants that need to be preserved/Download files locally/conflict/Pending items, deletion of remote directories is absolutely prohibited
                if barrier.remotePresent > 0 || barrier.localPresent > 0 || barrier.pending > 0 {
                    engine.logger.info(
                        "Descendant barrier blocked deletion of remote directory [\(relPath)]: descendants must be retained or added (remotePresent: \(barrier.remotePresent), localPresent: \(barrier.localPresent)); restoring the local directory"
                    )
                    try? FileManager.default.createDirectory(
                        at: localDirURL, withIntermediateDirectories: true)
                    try await engine.store.batchWrite { conn in
                        let stmt = try conn.cachedStatement(
                            """
                            UPDATE items SET local_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                        stmt.bindDouble(self.now, at: 1)
                        stmt.bindInt64(dirItem.itemId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                } else {
                    // All descendants have been safely deleted and sent to the cloud trashRemote
                    do {
                        if let rId = dirItem.remoteFileId {
                            try await engine.client.trash(remoteId: rId)
                        }
                        try await engine.store.batchWrite { conn in
                            let stmt = try conn.cachedStatement(
                                """
                                UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                                """)
                            stmt.bindDouble(self.now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        self.actionTracker.counts.withLock { $0.deleted += 1 }
                    } catch {
                        engine.logger.error(
                            "Failed to delete remote directory [\(relPath)]: \(error)")
                    }
                }
            }

            func reconcileLocalDeletion() async throws {
                // The remote end deleted the directory, but the local directory is still there (original intention: deleteLocal)
                // Barrier check: If there are local new, modified or conflicting files in the descendants, deletion of the local directory is absolutely prohibited
                if barrier.localPresent > 0 || barrier.remotePresent > 0 || barrier.pending > 0 {
                    engine.logger.info(
                        "Descendant barrier blocked deletion of local directory [\(relPath)]: it contains locally added or modified descendants (localPresent: \(barrier.localPresent))"
                    )
                    do {
                        if let rId = dirItem.remoteFileId {
                            try await engine.client.untrash(remoteId: rId)
                        }
                        try await engine.store.batchWrite { conn in
                            let stmt = try conn.cachedStatement(
                                """
                                UPDATE items SET remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                                """)
                            stmt.bindDouble(self.now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                    } catch {
                        engine.logger.error(
                            "Descendant barrier failed to restore remote directory [\(relPath)]: \(error)"
                        )
                    }
                } else {
                    // All descendants have been cleaned up. Verify that the local directory is empty and safely move it to the trash.
                    var trashSucceeded = true
                    if FileManager.default.fileExists(atPath: localDirURL.path) {
                        let contents =
                            (try? FileManager.default.contentsOfDirectory(atPath: localDirURL.path))
                            ?? []
                        let nonHidden = contents.filter { !$0.hasPrefix(".") }
                        if nonHidden.isEmpty {
                            var trashURL: NSURL?
                            do {
                                try FileManager.default.trashItem(
                                    at: localDirURL, resultingItemURL: &trashURL)
                            } catch {
                                trashSucceeded = false
                                engine.logger.warning(
                                    "Unable to move local directory to Trash [\(relPath)]: \(error). Keeping it and marking the operation blocked."
                                )
                            }
                        } else {
                            trashSucceeded = false
                            engine.logger.warning(
                                "Local directory [\(relPath)] is not empty; blocking deletion")
                        }
                    }

                    if trashSucceeded {
                        try await engine.store.batchWrite { conn in
                            let stmt = try conn.cachedStatement(
                                """
                                UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                                """)
                            stmt.bindDouble(self.now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        self.actionTracker.counts.withLock { $0.deleted += 1 }
                    } else {
                        try await engine.store.batchWrite { conn in
                            let stmt = try conn.cachedStatement(
                                """
                                UPDATE items SET phase = 'blocked', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                                """)
                            stmt.bindDouble(self.now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                    }
                }
            }

            if dirItem.local?.status == .absent && dirItem.remote?.status == .present {
                try await reconcileRemoteDeletion()
            } else if dirItem.remote?.status == .trashed && dirItem.local?.status == .present {
                try await reconcileLocalDeletion()
            }
        }
    }
}
