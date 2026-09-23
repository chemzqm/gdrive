import Darwin
import Foundation

struct ItemCleanupGenerations: Sendable, Equatable {
    let local: Int64
    let remote: Int64
    let dirty: Int64
}

private struct PreservedLocalFile: Sendable {
    let id: String
    let originalPath: String
    let storedPath: String
}

private struct ItemCleanupIntentPayload: Codable, Sendable, Equatable {
    let localGeneration: Int64
    let remoteGeneration: Int64
    let dirtyGeneration: Int64
    let localDevice: Int64?
    let localInode: Int64?

    var generations: ItemCleanupGenerations {
        ItemCleanupGenerations(
            local: localGeneration, remote: remoteGeneration, dirty: dirtyGeneration)
    }
}

private struct PendingItemCleanup: Sendable {
    let operationID: String
    let itemID: Int64
    let operationType: String
    let payload: ItemCleanupIntentPayload
}

private struct CleanupLocalIdentity: Sendable, Equatable {
    let device: Int64
    let inode: Int64
    let entryKind: String

    static func read(at url: URL) throws -> CleanupLocalIdentity? {
        var value = stat()
        guard lstat(url.path, &value) == 0 else {
            if errno == ENOENT { return nil }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        let kind: String
        switch value.st_mode & S_IFMT {
        case S_IFREG: kind = "file"
        case S_IFDIR: kind = "directory"
        default: kind = "other"
        }
        return CleanupLocalIdentity(
            device: Int64(value.st_dev), inode: Int64(value.st_ino), entryKind: kind)
    }
}

actor ItemTaskRegistry {
    private actor StartGate {
        private var continuation: CheckedContinuation<Void, Never>?
        private var opened = false

        func wait() async {
            if opened { return }
            await withCheckedContinuation { continuation = $0 }
        }

        func open() {
            opened = true
            continuation?.resume()
            continuation = nil
        }
    }

    private struct Entry {
        let itemIDs: Set<Int64>
        let task: Task<Void, Never>
    }

    private var entries: [UUID: Entry] = [:]
    private var blocked = Set<Int64>()
    private var cancelled = false

    func start(
        itemIDs: Set<Int64>, operation: @escaping @Sendable () async -> Void
    ) async throws -> UUID {
        guard !cancelled, blocked.isDisjoint(with: itemIDs) else { throw CancellationError() }
        let id = UUID()
        let gate = StartGate()
        let task = Task {
            await gate.wait()
            // Admitted operations own resources whose defer must run even when cancelled.
            await operation()
            self.finished(id)
        }
        entries[id] = Entry(itemIDs: itemIDs, task: task)
        await gate.open()
        return id
    }

    private func finished(_ id: UUID) {
        entries.removeValue(forKey: id)
    }

    func blockCancelAndDrain(itemIDs: Set<Int64>) async {
        blocked.formUnion(itemIDs)
        let tasks = entries.values.filter { !$0.itemIDs.isDisjoint(with: itemIDs) }.map(\.task)
        tasks.forEach { $0.cancel() }
        for task in tasks { await task.value }
        entries = entries.filter { $0.value.itemIDs.isDisjoint(with: itemIDs) }
    }

    func unblock(itemIDs: Set<Int64>) {
        blocked.subtract(itemIDs)
    }

    func drainAll() async {
        while !entries.isEmpty {
            let tasks = entries.values.map(\.task)
            for task in tasks { await task.value }
        }
    }

    func cancelAll() async {
        cancelled = true
        let tasks = entries.values.map(\.task)
        tasks.forEach { $0.cancel() }
        for task in tasks { await task.value }
    }
}

private struct ItemCleanupPlan: Sendable {
    struct Node: Sendable {
        let id: Int64
        let depth: Int64
        let remoteID: String?
        let relativePath: String
        let entryKind: String
        let baseSHA256: String?
    }

    let rootID: Int64
    let itemID: Int64
    let remoteRootID: String
    let localRootPath: String
    let localURL: URL
    let relativePath: String
    let entryKind: String
    let remoteID: String?
    let remoteStatus: String
    let localDevice: Int64?
    let localInode: Int64?
    let localMtime: Int64?
    let localSize: Int64?
    let localSHA256: String?
    let generations: ItemCleanupGenerations
    let nodes: [Node]
    let conflictPaths: [URL]
}

private enum CleanupPrimaryResult {
    case completed
    case localConflict(StableLocalFileDigest)
    case restoreFailed
}

extension SyncEngine {
    @discardableResult
    func cleanupLocalDeletionToRemote(
        itemID: Int64,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry
    ) async throws -> Bool {
        let initial = try await makeCleanupPlan(itemID: itemID, expected: expected)
        return try await withRootSyncLock(localPath: initial.localRootPath) {
            try await self.cleanupLocalDeletionToRemoteUnlocked(
                initial: initial, expected: expected, taskRegistry: taskRegistry)
        }
    }

    @discardableResult
    func cleanupLocalDeletionToRemoteUnlocked(
        itemID: Int64,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry
    ) async throws -> Bool {
        let initial = try await makeCleanupPlan(itemID: itemID, expected: expected)
        return try await cleanupLocalDeletionToRemoteUnlocked(
            initial: initial, expected: expected, taskRegistry: taskRegistry)
    }

    private func cleanupLocalDeletionToRemoteUnlocked(
        initial: ItemCleanupPlan,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry
    ) async throws -> Bool {
        try await executeCleanup(
            initial: initial, expected: expected, operationType: "trashRemote",
            taskRegistry: taskRegistry,
            removePrimary: { plan, _ in
                if let remoteID = plan.remoteID {
                    try await self.client.trash(remoteId: remoteID)
                }
                return .completed
            })
    }

    @discardableResult
    func cleanupRemoteDeletionToLocal(
        itemID: Int64,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry,
        trash: @escaping @Sendable (URL, AutoreleasingUnsafeMutablePointer<NSURL?>?) throws -> Void = {
            try FileManager.default.trashItem(at: $0, resultingItemURL: $1)
        }
    ) async throws -> Bool {
        let initial = try await makeCleanupPlan(itemID: itemID, expected: expected)
        return try await withRootSyncLock(localPath: initial.localRootPath) {
            try await self.cleanupRemoteDeletionToLocalUnlocked(
                initial: initial, expected: expected, taskRegistry: taskRegistry, trash: trash)
        }
    }

    @discardableResult
    func cleanupRemoteDeletionToLocalUnlocked(
        itemID: Int64,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry,
        trash: @escaping @Sendable (URL, AutoreleasingUnsafeMutablePointer<NSURL?>?) throws -> Void = {
            try FileManager.default.trashItem(at: $0, resultingItemURL: $1)
        }
    ) async throws -> Bool {
        let initial = try await makeCleanupPlan(itemID: itemID, expected: expected)
        return try await cleanupRemoteDeletionToLocalUnlocked(
            initial: initial, expected: expected, taskRegistry: taskRegistry, trash: trash)
    }

    private func cleanupRemoteDeletionToLocalUnlocked(
        initial: ItemCleanupPlan,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry,
        trash: @escaping @Sendable (URL, AutoreleasingUnsafeMutablePointer<NSURL?>?) throws -> Void = {
            try FileManager.default.trashItem(at: $0, resultingItemURL: $1)
        }
    ) async throws -> Bool {
        try await executeCleanup(
            initial: initial, expected: expected, operationType: "deleteLocal",
            taskRegistry: taskRegistry,
            removePrimary: { plan, _ in
                    if plan.entryKind == "directory" {
                        if try CleanupLocalIdentity.read(at: plan.localURL) != nil {
                            try self.validateCleanupDirectoryIdentity(plan)
                            try await self.preserveDirectoryChanges(plan: plan)
                            try self.validateCleanupDirectoryIdentity(plan)
                            try FileManager.default.trashItem(
                                at: plan.localURL, resultingItemURL: nil)
                        }
                        return .completed
                    } else {
                        let result = try LocalDeletionSafety.trashFileIfUnchanged(
                            at: plan.localURL,
                            expectedDevice: plan.localDevice,
                            expectedInode: plan.localInode,
                            expectedSize: plan.localSize,
                            expectedSHA256: plan.localSHA256,
                            trash: trash)
                        switch result {
                        case .missing, .trashed:
                            return .completed
                        case .trashedWithoutURL:
                            self.logger.error(
                                "Trash did not return the moved file path [\(plan.localURL.path)]; SHA-256 cannot be checked")
                            return .completed
                        case .changed(let observed):
                            return .localConflict(observed)
                        case .restoreFailed(let trashURL, let reason):
                            self.logger.error(
                                "Unable to verify or restore trashed local file from \(trashURL.path) to \(plan.localURL.path): \(reason)")
                            return .restoreFailed
                        case .missingEvidence:
                            throw SyncEngineError.general(
                                "The local item lacks deletion evidence: \(plan.localURL.path)")
                        }
                    }
                })
    }

    private func executeCleanup(
        initial: ItemCleanupPlan,
        expected: ItemCleanupGenerations,
        operationType: String,
        taskRegistry: ItemTaskRegistry,
        removePrimary: @Sendable (ItemCleanupPlan, String) async throws -> CleanupPrimaryResult
    ) async throws -> Bool {
        let ids = Set(initial.nodes.map(\.id))
        await taskRegistry.blockCancelAndDrain(itemIDs: ids)
        do {
            let plan = try await makeCleanupPlan(itemID: initial.itemID, expected: expected)
            let intent = try await prepareCleanupIntent(
                plan: plan, operationType: operationType)
            guard try cleanupPreconditionStillHolds(
                plan: plan, operationType: operationType, payload: intent.payload
            ) else {
                try await discardCleanupIntent(operationID: intent.operationID)
                await taskRegistry.unblock(itemIDs: ids)
                return false
            }
            let result = try await removePrimary(plan, intent.operationID)
            if case .localConflict(let observed) = result {
                try await commitChangedLocalConflict(
                    plan: plan, operationID: intent.operationID, observed: observed)
                await taskRegistry.unblock(itemIDs: ids)
                return false
            }
            if case .restoreFailed = result {
                try await blockCleanupAfterFailedRestore(
                    plan: plan, operationID: intent.operationID)
                await taskRegistry.unblock(itemIDs: ids)
                return false
            }
            try removeCleanupArtifacts(plan)
            try await deleteCleanupRows(plan)
            await taskRegistry.unblock(itemIDs: ids)
            return true
        } catch {
            await taskRegistry.unblock(itemIDs: ids)
            throw error
        }
    }

    func recoverPendingItemCleanups(
        rootID: Int64, taskRegistry: ItemTaskRegistry
    ) async throws -> Int {
        let pending = try await store.read { conn -> [PendingItemCleanup] in
            let stmt = try conn.prepare(
                """
                SELECT operation_id, item_id, operation_type, payload FROM operations
                WHERE root_id = ? AND operation_type IN ('trashRemote', 'deleteLocal')
                    AND state IN ('ready', 'inFlight', 'verify', 'unknownOutcome')
                ORDER BY created_at, operation_id;
                """)
            defer { stmt.reset() }
            stmt.bindInt64(rootID, at: 1)
            var result: [PendingItemCleanup] = []
            while try stmt.step(), let operationID = stmt.columnText(at: 0),
                  let itemID = stmt.columnInt64(at: 1),
                  let operationType = stmt.columnText(at: 2),
                  let payload = stmt.columnText(at: 3) {
                let decoded = try JSONDecoder().decode(
                    ItemCleanupIntentPayload.self, from: Data(payload.utf8))
                result.append(PendingItemCleanup(
                    operationID: operationID, itemID: itemID, operationType: operationType,
                    payload: decoded))
            }
            return result
        }
        for cleanup in pending {
            let plan = try await makeCleanupPlan(
                itemID: cleanup.itemID, expected: cleanup.payload.generations)
            do {
                guard try cleanupPreconditionStillHolds(
                    plan: plan, operationType: cleanup.operationType, payload: cleanup.payload
                ) else {
                    try await discardCleanupIntent(operationID: cleanup.operationID)
                    continue
                }
                if cleanup.operationType == "trashRemote" {
                    try await cleanupLocalDeletionToRemoteUnlocked(
                        itemID: cleanup.itemID, expected: cleanup.payload.generations,
                        taskRegistry: taskRegistry)
                } else {
                    try await cleanupRemoteDeletionToLocalUnlocked(
                        itemID: cleanup.itemID, expected: cleanup.payload.generations,
                        taskRegistry: taskRegistry)
                }
            } catch {
                if DatabaseFailure.isSQLite(error) { throw error }
                let root = URL(fileURLWithPath: plan.localRootPath).standardizedFileURL.path
                let path = plan.localURL.standardizedFileURL.path
                let relative = path == root ? "" : String(path.dropFirst(root.count + 1))
                try await SyncIssueStore.record(
                    store: store, rootID: rootID,
                    subject: SyncIssueSubject(
                        itemID: plan.itemID, remoteFileID: plan.remoteID,
                        relativePath: relative),
                    stage: .delete, error: error)
                throw error
            }
        }
        return pending.count
    }

    private func prepareCleanupIntent(
        plan: ItemCleanupPlan, operationType: String
    ) async throws -> (operationID: String, payload: ItemCleanupIntentPayload) {
        let payload = ItemCleanupIntentPayload(
            localGeneration: plan.generations.local,
            remoteGeneration: plan.generations.remote,
            dirtyGeneration: plan.generations.dirty,
            localDevice: operationType == "deleteLocal" ? plan.localDevice : nil,
            localInode: operationType == "deleteLocal" ? plan.localInode : nil)
        guard let payloadText = String(
            bytes: try JSONEncoder().encode(payload), encoding: .utf8) else {
            throw SyncEngineError.general("Unable to encode cleanup intent: \(plan.itemID)")
        }
        return try await store.write { conn in
            let existing = try conn.prepare(
                """
                SELECT operation_id, operation_type, payload FROM operations
                WHERE item_id = ? AND operation_type IN ('trashRemote', 'deleteLocal')
                    AND state IN ('ready', 'inFlight', 'verify', 'unknownOutcome');
                """)
            defer { existing.reset() }
            existing.bindInt64(plan.itemID, at: 1)
            if try existing.step(), let operationID = existing.columnText(at: 0),
               let storedType = existing.columnText(at: 1),
               let storedPayload = existing.columnText(at: 2) {
                let decoded = try JSONDecoder().decode(
                    ItemCleanupIntentPayload.self, from: Data(storedPayload.utf8))
                guard storedType == operationType,
                      decoded == payload else {
                    throw SyncEngineError.general(
                        "Existing cleanup intent does not match item: \(plan.itemID)")
                }
                return (operationID, decoded)
            }
            let operationID = UUID().uuidString
            let now = Date().timeIntervalSince1970
            let insert = try conn.prepare(
                """
                INSERT INTO operations(operation_id, root_id, item_id, operation_type, state,
                    expected_local_generation, target_remote_id, payload, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?);
                """)
            defer { insert.reset() }
            insert.bindText(operationID, at: 1)
            insert.bindInt64(plan.rootID, at: 2)
            insert.bindInt64(plan.itemID, at: 3)
            insert.bindText(operationType, at: 4)
            insert.bindInt64(plan.generations.local, at: 5)
            insert.bindText(plan.remoteID, at: 6)
            insert.bindText(payloadText, at: 7)
            insert.bindDouble(now, at: 8)
            insert.bindDouble(now, at: 9)
            _ = try insert.step()
            return (operationID, payload)
        }
    }

    private func cleanupPreconditionStillHolds(
        plan: ItemCleanupPlan, operationType: String, payload: ItemCleanupIntentPayload
    ) throws -> Bool {
        let current = try CleanupLocalIdentity.read(at: plan.localURL)
        if operationType == "trashRemote" {
            guard let current else { return true }
            return (plan.entryKind == "file" && current.entryKind == "directory")
                || (plan.entryKind == "directory" && current.entryKind == "file")
        }
        guard let current else { return true }
        guard let device = payload.localDevice, let inode = payload.localInode else { return false }
        return current.device == device && current.inode == inode
    }

    private func validateCleanupDirectoryIdentity(_ plan: ItemCleanupPlan) throws {
        guard let current = try CleanupLocalIdentity.read(at: plan.localURL),
              current.entryKind == "directory",
              current.device == plan.localDevice,
              current.inode == plan.localInode else {
            throw SyncEngineError.localFileModified(path: plan.localURL.path)
        }
    }

    private func discardCleanupIntent(operationID: String) async throws {
        try await store.write { conn in
            let stmt = try conn.prepare("DELETE FROM operations WHERE operation_id = ?;")
            defer { stmt.reset() }
            stmt.bindText(operationID, at: 1)
            _ = try stmt.step()
        }
    }

    private func commitChangedLocalConflict(
        plan: ItemCleanupPlan, operationID: String, observed: StableLocalFileDigest
    ) async throws {
        guard let remoteID = plan.remoteID, let oldSHA = plan.localSHA256,
              let oldSize = plan.localSize else {
            throw SyncEngineError.general(
                "The changed file lacks remote deletion evidence: \(plan.localURL.path)")
        }
        try observed.version.validate(at: plan.localURL)
        try await store.batchWrite { conn in
            let now = Date().timeIntervalSince1970
            let update = try conn.cachedStatement("""
                UPDATE items SET local_device = ?, local_inode = ?, local_mtime = ?,
                    local_ctime = ?, local_size = ?, local_sha256 = ?,
                    local_generation = local_generation + 1, local_status = 'present',
                    phase = 'blocked', dirty_generation = 0, updated_at = ?
                WHERE item_id = ? AND root_id = ? AND remote_file_id = ?
                    AND local_generation = ? AND remote_generation = ? AND dirty_generation = ?;
                """)
            update.bindInt64(observed.version.device, at: 1)
            update.bindInt64(observed.version.inode, at: 2)
            update.bindInt64(observed.version.mtime, at: 3)
            update.bindInt64(observed.version.ctime, at: 4)
            update.bindInt64(observed.fileSize, at: 5)
            update.bindText(observed.sha256Hex, at: 6)
            update.bindDouble(now, at: 7)
            update.bindInt64(plan.itemID, at: 8)
            update.bindInt64(plan.rootID, at: 9)
            update.bindText(remoteID, at: 10)
            update.bindInt64(plan.generations.local, at: 11)
            update.bindInt64(plan.generations.remote, at: 12)
            update.bindInt64(plan.generations.dirty, at: 13)
            _ = try update.step()
            update.reset()
            guard conn.changes == 1 else {
                throw SyncEngineError.general(
                    "The changed file cleanup plan is stale: \(plan.relativePath)")
            }

            let conflict = try conn.cachedStatement("""
                INSERT INTO sync_conflicts(conflict_id, root_id, item_id, remote_file_id,
                    relative_path, local_path, conflict_path, remote_sha256, remote_size,
                    remote_version, remote_status, created_at, updated_at)
                SELECT ?, root_id, item_id, remote_file_id, ?, ?, NULL, ?, ?,
                    remote_version, ?, ?, ? FROM items WHERE item_id = ?
                ON CONFLICT(root_id, remote_file_id) DO UPDATE SET
                    relative_path = excluded.relative_path, local_path = excluded.local_path,
                    conflict_path = NULL, remote_sha256 = excluded.remote_sha256,
                    remote_size = excluded.remote_size, remote_version = excluded.remote_version,
                    remote_status = excluded.remote_status,
                    revision = sync_conflicts.revision + 1, updated_at = excluded.updated_at;
                """)
            conflict.bindText("sync-\(plan.rootID)-\(plan.itemID)", at: 1)
            conflict.bindText(plan.relativePath, at: 2)
            conflict.bindText(plan.localURL.path, at: 3)
            conflict.bindText(oldSHA, at: 4)
            conflict.bindInt64(oldSize, at: 5)
            conflict.bindText(plan.remoteStatus == "trashed" ? "trashed" : "removed", at: 6)
            conflict.bindDouble(now, at: 7)
            conflict.bindDouble(now, at: 8)
            conflict.bindInt64(plan.itemID, at: 9)
            _ = try conflict.step()
            conflict.reset()

            let intent = try conn.cachedStatement(
                "DELETE FROM operations WHERE operation_id = ? AND item_id = ?;")
            intent.bindText(operationID, at: 1)
            intent.bindInt64(plan.itemID, at: 2)
            _ = try intent.step()
            intent.reset()
            guard conn.changes == 1 else {
                throw SyncEngineError.general(
                    "The changed file cleanup intent changed: \(plan.relativePath)")
            }
        }
    }

    private func blockCleanupAfterFailedRestore(
        plan: ItemCleanupPlan, operationID: String
    ) async throws {
        try await store.batchWrite { conn in
            let update = try conn.cachedStatement("""
                UPDATE items SET phase = 'blocked', dirty_generation = 0, updated_at = ?
                WHERE item_id = ? AND root_id = ?
                    AND local_generation = ? AND remote_generation = ? AND dirty_generation = ?;
                """)
            update.bindDouble(Date().timeIntervalSince1970, at: 1)
            update.bindInt64(plan.itemID, at: 2)
            update.bindInt64(plan.rootID, at: 3)
            update.bindInt64(plan.generations.local, at: 4)
            update.bindInt64(plan.generations.remote, at: 5)
            update.bindInt64(plan.generations.dirty, at: 6)
            _ = try update.step()
            update.reset()
            guard conn.changes == 1 else {
                throw SyncEngineError.general(
                    "The failed local restore cleanup plan is stale: \(plan.relativePath)")
            }
            let intent = try conn.cachedStatement(
                "DELETE FROM operations WHERE operation_id = ? AND item_id = ?;")
            intent.bindText(operationID, at: 1)
            intent.bindInt64(plan.itemID, at: 2)
            _ = try intent.step()
            intent.reset()
            guard conn.changes == 1 else {
                throw SyncEngineError.general(
                    "The failed local restore cleanup intent changed: \(plan.relativePath)")
            }
        }
    }

    private func makeCleanupPlan(
        itemID: Int64, expected: ItemCleanupGenerations
    ) async throws -> ItemCleanupPlan {
        try await store.read { conn in
            let item = try conn.prepare(
                """
                SELECT i.root_id, r.remote_root_id, r.local_root_path, i.entry_kind,
                    i.remote_file_id, i.local_device, i.local_inode, i.local_mtime,
                    i.local_size, i.local_sha256, i.local_generation,
                    i.remote_generation, i.dirty_generation, i.remote_status
                FROM items i JOIN roots r ON r.root_id = i.root_id
                WHERE i.item_id = ? AND r.is_active = 1;
                """)
            defer { item.reset() }
            item.bindInt64(itemID, at: 1)
            guard try item.step(), let rootID = item.columnInt64(at: 0),
                  let remoteRootID = item.columnText(at: 1),
                  let localRootPath = item.columnText(at: 2),
                  let entryKind = item.columnText(at: 3) else {
                throw SyncEngineError.general("Cleanup item was not found: \(itemID)")
            }
            let actual = ItemCleanupGenerations(
                local: item.columnInt64(at: 10) ?? 0,
                remote: item.columnInt64(at: 11) ?? 0,
                dirty: item.columnInt64(at: 12) ?? 0)
            guard actual == expected else {
                throw SyncEngineError.general("Cleanup plan is stale: \(itemID)")
            }

            guard let relativePath = try conn.itemRelativePath(itemID: itemID) else {
                throw SyncEngineError.general("Unable to resolve cleanup path: \(itemID)")
            }
            let localURL = relativePath.isEmpty ? URL(fileURLWithPath: localRootPath) :
                URL(fileURLWithPath: localRootPath).appendingPathComponent(relativePath)

            let tree = try conn.prepare(
                """
                WITH RECURSIVE subtree(item_id, depth, relative_path) AS (
                    SELECT ?, 0, ''
                    UNION ALL
                    SELECT i.item_id, s.depth + 1,
                        CASE WHEN s.relative_path = '' THEN i.name
                            ELSE s.relative_path || '/' || i.name END
                    FROM items i
                    JOIN subtree s ON i.parent_id = s.item_id
                )
                SELECT s.item_id, s.depth, i.remote_file_id, s.relative_path,
                    i.entry_kind, i.base_sha256
                FROM subtree s
                JOIN items i ON i.item_id = s.item_id ORDER BY s.depth DESC;
                """)
            defer { tree.reset() }
            tree.bindInt64(itemID, at: 1)
            var nodes: [ItemCleanupPlan.Node] = []
            while try tree.step(), let id = tree.columnInt64(at: 0) {
                nodes.append(.init(id: id, depth: tree.columnInt64(at: 1) ?? 0,
                    remoteID: tree.columnText(at: 2), relativePath: tree.columnText(at: 3) ?? "",
                    entryKind: tree.columnText(at: 4) ?? "file",
                    baseSHA256: tree.columnText(at: 5)))
            }
            let conflictPaths = try self.cleanupConflictPaths(conn: conn, nodes: nodes)
            return ItemCleanupPlan(
                rootID: rootID, itemID: itemID, remoteRootID: remoteRootID,
                localRootPath: localRootPath,
                localURL: localURL, relativePath: relativePath,
                entryKind: entryKind, remoteID: item.columnText(at: 4),
                remoteStatus: item.columnText(at: 13) ?? "unknown",
                localDevice: item.columnInt64(at: 5), localInode: item.columnInt64(at: 6),
                localMtime: item.columnInt64(at: 7), localSize: item.columnInt64(at: 8),
                localSHA256: item.columnText(at: 9), generations: actual, nodes: nodes,
                conflictPaths: conflictPaths)
        }
    }

    private func cleanupConflictPaths(
        conn: SQLiteConnection, nodes: [ItemCleanupPlan.Node]
    ) throws -> [URL] {
        try conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS cleanup_item_ids " +
                "(item_id INTEGER PRIMARY KEY NOT NULL) WITHOUT ROWID;")
        try conn.execute("DELETE FROM cleanup_item_ids;")
        defer { try? conn.execute("DELETE FROM cleanup_item_ids;") }
        let insertID = try conn.prepare(
            "INSERT OR IGNORE INTO cleanup_item_ids(item_id) VALUES (?);")
        defer { insertID.reset() }
        for itemID in nodes.map(\.id) {
            insertID.bindInt64(itemID, at: 1)
            _ = try insertID.step()
            insertID.reset()
        }
        let conflicts = try conn.prepare(
            """
            SELECT conflict_path FROM sync_conflicts conflicts
            JOIN cleanup_item_ids ids ON ids.item_id = conflicts.item_id;
            """)
        defer { conflicts.reset() }
        var conflictPaths: [URL] = []
        while try conflicts.step() {
            if let value = conflicts.columnText(at: 0) {
                conflictPaths.append(URL(fileURLWithPath: value))
            }
        }
        return conflictPaths
    }

    private func preserveDirectoryChanges(plan: ItemCleanupPlan) async throws {
        let parentRemoved = conflictDirectory.deletingLastPathComponent()
            .appendingPathComponent("parent_removed", isDirectory: true)
            .appendingPathComponent(String(plan.rootID), isDirectory: true)
        guard !RootSyncCoordinator.contains(parentRemoved.path, in: plan.localRootPath) else {
            throw SyncEngineError.general("Parent-removed storage overlaps the sync root")
        }
        let baselines = Dictionary(uniqueKeysWithValues: plan.nodes.compactMap { node in
            node.entryKind == "file" ? node.baseSHA256.map { (node.relativePath, $0) } : nil
        })
        let candidates = try await DirectoryDeletionInventory.inspect(
            directory: plan.localURL, baselines: baselines)
        guard !candidates.isEmpty else { return }
        try validateCleanupDirectoryIdentity(plan)
        try FileManager.default.createDirectory(at: parentRemoved, withIntermediateDirectories: true)
        let files = candidates.map { candidate in
            let id = SyncConflictStore.newParentRemovedID()
            return PreservedLocalFile(
                id: id, originalPath: candidate.url.path,
                storedPath: parentRemoved.appendingPathComponent(
                    "\(id)-\(candidate.url.lastPathComponent)").path)
        }
        let (moved, failures) = await moveLocalConflicts(files)
        for failure in failures {
            logger.warning("Unable to preserve local file; leaving it for Trash: \(failure)")
        }
        try await insertLocalConflicts(moved, rootID: plan.rootID)
        try Task.checkCancellation()
    }

    private func moveLocalConflicts(
        _ files: [PreservedLocalFile]
    ) async -> (moved: [PreservedLocalFile], failures: [String]) {
        await withTaskGroup(of: (PreservedLocalFile, String?).self) { group in
            var moved: [PreservedLocalFile] = []
            var failures: [String] = []
            for (index, file) in files.enumerated() {
                if index >= 64, let result = await group.next() {
                    if let error = result.1 { failures.append(error) } else { moved.append(result.0) }
                }
                group.addTask {
                    do {
                        try FileManager.default.moveItem(
                            at: URL(fileURLWithPath: file.originalPath),
                            to: URL(fileURLWithPath: file.storedPath))
                        return (file, nil)
                    } catch {
                        return (file, "\(file.originalPath): \(error)")
                    }
                }
            }
            for await result in group {
                if let error = result.1 { failures.append(error) } else { moved.append(result.0) }
            }
            return (moved, failures)
        }
    }

    private func insertLocalConflicts(_ files: [PreservedLocalFile], rootID: Int64) async throws {
        guard !files.isEmpty else { return }
        try await store.write { conn in
            let insert = try conn.prepare("""
                INSERT INTO local_conflicts(conflict_id, root_id, original_path,
                    stored_path, created_at) VALUES (?, ?, ?, ?, ?);
                """)
            defer { insert.reset() }
            let createdAt = Date().timeIntervalSince1970
            for file in files {
                insert.bindText(file.id, at: 1)
                insert.bindInt64(rootID, at: 2)
                insert.bindText(file.originalPath, at: 3)
                insert.bindText(file.storedPath, at: 4)
                insert.bindDouble(createdAt, at: 5)
                _ = try insert.step()
                insert.reset()
            }
        }
    }

    private func removeCleanupArtifacts(_ plan: ItemCleanupPlan) throws {
        let conflictRoot = try SyncConflictStore.directory(
            base: conflictDirectory, remoteRootID: plan.remoteRootID).standardizedFileURL
        let prefix = conflictRoot.path.hasSuffix("/") ? conflictRoot.path : conflictRoot.path + "/"
        for path in plan.conflictPaths {
            let resolved = path.standardizedFileURL
            guard resolved.path.hasPrefix(prefix) else {
                throw SyncEngineError.general("Conflict cleanup path is outside its root: \(path.path)")
            }
            if FileManager.default.fileExists(atPath: resolved.path) {
                try FileManager.default.removeItem(at: resolved)
            }
            var parent = resolved.deletingLastPathComponent()
            while parent.path.hasPrefix(prefix), parent != conflictRoot {
                try DownloadStaging.removeIfEmpty(parent)
                parent.deleteLastPathComponent()
            }
        }
    }

    private func deleteCleanupRows(_ plan: ItemCleanupPlan) async throws {
        try await store.batchWrite { conn in
            for remoteID in Set(plan.nodes.compactMap(\.remoteID)) {
                for table in ["remote_change_inbox", "remote_directory_scans"] {
                    let stmt = try conn.prepare(
                        "DELETE FROM \(table) WHERE root_id = ? AND remote_id = ?;")
                    stmt.bindInt64(plan.rootID, at: 1)
                    stmt.bindText(remoteID, at: 2)
                    _ = try stmt.step()
                    stmt.reset()
                }
            }
            for node in plan.nodes.sorted(by: { $0.depth > $1.depth }) {
                let stmt = try conn.prepare("DELETE FROM items WHERE root_id = ? AND item_id = ?;")
                stmt.bindInt64(plan.rootID, at: 1)
                stmt.bindInt64(node.id, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
        }
    }

}
