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
        remotePresent: Bool = true, updateDescendantPaths: Bool = false
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
        if updateDescendantPaths, let oldPath = dirPaths[itemId], oldPath != relPath {
            let affected = dirPaths.reduce(into: [Int64: (old: String, new: String)]()) { result, entry in
                let (id, path) = entry
                guard path == oldPath || path.hasPrefix(oldPath + "/") else { return }
                result[id] = (path, relPath + path.dropFirst(oldPath.count))
            }
            for paths in affected.values {
                dirIdByRelPath.removeValue(forKey: paths.old)
            }
            for (id, paths) in affected {
                dirPaths[id] = paths.new
                dirIdByRelPath[paths.new] = id
            }
        }
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

    func markReady(_ itemID: Int64) {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }
        readyDirectories.insert(itemID)
    }

    func removeSubtree(itemID: Int64) {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }
        guard let path = dirPaths[itemID] else { return }
        let removed = dirPaths.compactMap { id, candidate in
            candidate == path || candidate.hasPrefix(path + "/") ? id : nil
        }
        for id in removed {
            if let oldPath = dirPaths.removeValue(forKey: id) {
                dirIdByRelPath.removeValue(forKey: oldPath)
            }
            if let remoteID = dirRemoteIds.removeValue(forKey: id) {
                dirIdByRemote.removeValue(forKey: remoteID)
            }
            readyDirectories.remove(id)
        }
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
                WHERE root_id = ? AND entry_kind = 'directory' AND parent_id IS NOT NULL;
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
            let parent = directoryContext.getRelPath(for: item.parentId) ?? ""
            let path = parent.isEmpty ? item.name : "\(parent)/\(item.name)"
            if failedDirectorySubtrees.blocks(
                path: path, device: item.localDevice ?? -1, inode: item.localInode ?? -1,
                isDirectory: true
            ) {
                try await preserveStoredDirectorySubtree(itemID: item.itemId)
                continue
            }
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
                directoryContext.register(
                    itemId: item.itemId, parentItemId: item.parentId, name: item.name,
                    remoteId: intent.targetRemoteID)
            } catch {
                if shouldAbortRun(for: error) { throw error }
                failedDirectorySubtrees.register(
                    path: path, device: item.localDevice ?? -1, inode: item.localInode ?? -1)
                try await preserveStoredDirectorySubtree(itemID: item.itemId)
                try await DurableCreateIntentStore.markUnknownOutcome(
                    store: engine.store,
                    operationID: intent.operationID,
                    error: error
                )
                await recordIssue(
                    error, stage: .createDirectory, subject: issueSubject(for: item))
                engine.logger.error(
                    "Failed to restore remote directory creation [\(item.name)]: \(error)")
            }
        }
    }

    func reconcileDirectories(_ dirItems: [DirtyRecord]) async throws {
        let deletionCandidates = dirItems.filter {
            ($0.local?.status == .absent && $0.remote?.status == .present)
                || ($0.remote?.status == .trashed && $0.local?.status == .present)
                || ($0.local?.status == .absent && $0.remote?.status == .trashed)
        }
        let deletionIDs = Set(deletionCandidates.map(\.itemId))
        try await engine.store.write { conn in
            let stmt = try conn.cachedStatement(
                "UPDATE items SET phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;")
            defer { stmt.reset() }
            for item in dirItems where !deletionIDs.contains(item.itemId)
                && item.pendingCreate == nil {
                guard item.local?.status != .unknown, item.remote?.status != .unknown else { continue }
                stmt.bindDouble(self.now, at: 1)
                stmt.bindInt64(item.itemId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
        }

        let ordered = deletionCandidates.sorted { first, second in
            let firstPath = directoryContext.getRelPath(for: first.itemId) ?? ""
            let secondPath = directoryContext.getRelPath(for: second.itemId) ?? ""
            return firstPath.split(separator: "/").count
                > secondPath.split(separator: "/").count
        }
        for item in ordered {
            do {
                let expected = ItemCleanupGenerations(
                    local: item.localGeneration, remote: item.remoteGeneration,
                    dirty: item.dirtyGeneration)
                if item.local?.status == .absent && item.remote?.status == .present {
                    try await engine.cleanupLocalDeletionToRemoteUnlocked(
                        itemID: item.itemId, expected: expected, taskRegistry: itemTaskRegistry)
                } else {
                    try await engine.cleanupRemoteDeletionToLocalUnlocked(
                        itemID: item.itemId, expected: expected, taskRegistry: itemTaskRegistry)
                }
                directoryContext.removeSubtree(itemID: item.itemId)
                actionTracker.counts.withLock { $0.deleted += 1 }
            } catch {
                await recordIssue(error, stage: .delete, subject: issueSubject(for: item))
                throw error
            }
        }
    }
}
