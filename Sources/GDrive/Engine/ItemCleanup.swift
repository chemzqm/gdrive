import Darwin
import Foundation

struct ItemCleanupGenerations: Sendable, Equatable {
    let local: Int64
    let remote: Int64
    let dirty: Int64
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

    static func read(at url: URL) throws -> CleanupLocalIdentity? {
        var value = stat()
        guard lstat(url.path, &value) == 0 else {
            if errno == ENOENT { return nil }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        return CleanupLocalIdentity(device: Int64(value.st_dev), inode: Int64(value.st_ino))
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

    func start(
        itemIDs: Set<Int64>, operation: @escaping @Sendable () async -> Void
    ) async throws -> UUID {
        guard blocked.isDisjoint(with: itemIDs) else { throw CancellationError() }
        let id = UUID()
        let gate = StartGate()
        let task = Task {
            await gate.wait()
            if !Task.isCancelled { await operation() }
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
        let tasks = entries.values.map(\.task)
        tasks.forEach { $0.cancel() }
        for task in tasks { await task.value }
        entries.removeAll()
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
    let entryKind: String
    let remoteID: String?
    let localDevice: Int64?
    let localInode: Int64?
    let localMtime: Int64?
    let localSize: Int64?
    let localSHA256: String?
    let generations: ItemCleanupGenerations
    let nodes: [Node]
    let conflictPaths: [URL]
}

private struct PendingTrashedLocalChange: Sendable {
    let id: String
    let relativePath: String
    let trashRelativePath: String
    let originalPath: String
    let baselineSHA256: String
    let observedSHA256: String
}

extension SyncEngine {
    func cleanupLocalDeletionToRemote(
        itemID: Int64,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry
    ) async throws {
        let initial = try await makeCleanupPlan(itemID: itemID, expected: expected)
        try await withRootSyncLock(localPath: initial.localRootPath) {
            try await self.cleanupLocalDeletionToRemoteUnlocked(
                initial: initial, expected: expected, taskRegistry: taskRegistry)
        }
    }

    func cleanupLocalDeletionToRemoteUnlocked(
        itemID: Int64,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry
    ) async throws {
        let initial = try await makeCleanupPlan(itemID: itemID, expected: expected)
        try await cleanupLocalDeletionToRemoteUnlocked(
            initial: initial, expected: expected, taskRegistry: taskRegistry)
    }

    private func cleanupLocalDeletionToRemoteUnlocked(
        initial: ItemCleanupPlan,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry
    ) async throws {
        try await executeCleanup(
            initial: initial, expected: expected, operationType: "trashRemote",
            taskRegistry: taskRegistry,
            removePrimary: { plan, _ in
                if let remoteID = plan.remoteID {
                    try await self.client.trash(remoteId: remoteID)
                }
            })
    }

    func cleanupRemoteDeletionToLocal(
        itemID: Int64,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry
    ) async throws {
        let initial = try await makeCleanupPlan(itemID: itemID, expected: expected)
        try await withRootSyncLock(localPath: initial.localRootPath) {
            try await self.cleanupRemoteDeletionToLocalUnlocked(
                initial: initial, expected: expected, taskRegistry: taskRegistry)
        }
    }

    func cleanupRemoteDeletionToLocalUnlocked(
        itemID: Int64,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry
    ) async throws {
        let initial = try await makeCleanupPlan(itemID: itemID, expected: expected)
        try await cleanupRemoteDeletionToLocalUnlocked(
            initial: initial, expected: expected, taskRegistry: taskRegistry)
    }

    private func cleanupRemoteDeletionToLocalUnlocked(
        initial: ItemCleanupPlan,
        expected: ItemCleanupGenerations,
        taskRegistry: ItemTaskRegistry
    ) async throws {
        try await executeCleanup(
            initial: initial, expected: expected, operationType: "deleteLocal",
            taskRegistry: taskRegistry,
            removePrimary: { plan, operationID in
                    if plan.entryKind == "directory" {
                        if FileManager.default.fileExists(atPath: plan.localURL.path) {
                            let changes = try self.modifiedFilesBeforeDirectoryTrash(plan)
                            let batchID = operationID
                            try await self.insertPendingTrashedLocalChanges(
                                changes, batchID: batchID, plan: plan)
                            var trashURL: NSURL?
                            do {
                                try FileManager.default.trashItem(
                                    at: plan.localURL, resultingItemURL: &trashURL)
                            } catch {
                                try? await self.removePendingTrashedLocalChanges(batchID: batchID)
                                throw error
                            }
                            try await self.commitTrashedLocalChanges(
                                changes, batchID: batchID,
                                trashDirectory: trashURL.map { $0 as URL })
                        }
                    } else {
                        let removed = try LocalDeletionSafety.trashFileIfUnchanged(
                            at: plan.localURL,
                            expectedDevice: plan.localDevice,
                            expectedInode: plan.localInode,
                            expectedMtime: plan.localMtime,
                            expectedSize: plan.localSize,
                            expectedSHA256: plan.localSHA256)
                        guard removed else {
                            throw SyncEngineError.general(
                                "The local item changed before cleanup: \(plan.localURL.path)")
                        }
                    }
                })
    }

    private func executeCleanup(
        initial: ItemCleanupPlan,
        expected: ItemCleanupGenerations,
        operationType: String,
        taskRegistry: ItemTaskRegistry,
        removePrimary: @Sendable (ItemCleanupPlan, String) async throws -> Void
    ) async throws {
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
                return
            }
            try await removePrimary(plan, intent.operationID)
            try removeCleanupArtifacts(plan)
            try await deleteCleanupRows(plan, operationID: intent.operationID)
            await taskRegistry.unblock(itemIDs: ids)
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
                guard storedType == operationType, decoded == payload else {
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
            return current == nil
        }
        guard let current else { return true }
        guard let device = payload.localDevice, let inode = payload.localInode else { return false }
        return current == CleanupLocalIdentity(device: device, inode: inode)
    }

    private func discardCleanupIntent(operationID: String) async throws {
        try await store.write { conn in
            let stmt = try conn.prepare("DELETE FROM operations WHERE operation_id = ?;")
            defer { stmt.reset() }
            stmt.bindText(operationID, at: 1)
            _ = try stmt.step()
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
                    i.remote_generation, i.dirty_generation
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
                localURL: localURL, entryKind: entryKind, remoteID: item.columnText(at: 4),
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

    private func modifiedFilesBeforeDirectoryTrash(
        _ plan: ItemCleanupPlan
    ) throws -> [PendingTrashedLocalChange] {
        let rootURL = URL(fileURLWithPath: plan.localRootPath).standardizedFileURL
        let rootPrefix = rootURL.path.hasSuffix("/") ? rootURL.path : rootURL.path + "/"
        var changes: [PendingTrashedLocalChange] = []
        for node in plan.nodes where node.entryKind == "file" {
            guard let baseline = node.baseSHA256 else { continue }
            let fileURL = node.relativePath.isEmpty ? plan.localURL :
                plan.localURL.appendingPathComponent(node.relativePath)
            guard FileManager.default.fileExists(atPath: fileURL.path) else { continue }
            let observed: String
            do {
                observed = try Self.computeFileSha256(at: fileURL).sha256Hex
            } catch {
                logger.warning(
                    "Unable to inspect file before trashing its directory [\(fileURL.path)]: \(error)")
                continue
            }
            guard observed.caseInsensitiveCompare(baseline) != .orderedSame else { continue }
            let resolved = fileURL.standardizedFileURL.path
            guard resolved.hasPrefix(rootPrefix) else {
                throw SyncEngineError.general(
                    "Trashed local change is outside its sync root: \(fileURL.path)")
            }
            changes.append(PendingTrashedLocalChange(
                id: UUID().uuidString,
                relativePath: String(resolved.dropFirst(rootPrefix.count)),
                trashRelativePath: node.relativePath,
                originalPath: resolved, baselineSHA256: baseline,
                observedSHA256: observed))
        }
        return changes
    }

    private func insertPendingTrashedLocalChanges(
        _ changes: [PendingTrashedLocalChange], batchID: String, plan: ItemCleanupPlan
    ) async throws {
        guard !changes.isEmpty else { return }
        let now = Date().timeIntervalSince1970
        try await store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO trashed_local_changes(change_id, batch_id, local_root_path,
                    relative_path, original_path, baseline_sha256, observed_sha256,
                    state, trashed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?);
                """)
            defer { stmt.reset() }
            for change in changes {
                stmt.bindText(change.id, at: 1)
                stmt.bindText(batchID, at: 2)
                stmt.bindText(plan.localRootPath, at: 3)
                stmt.bindText(change.relativePath, at: 4)
                stmt.bindText(change.originalPath, at: 5)
                stmt.bindText(change.baselineSHA256, at: 6)
                stmt.bindText(change.observedSHA256, at: 7)
                stmt.bindDouble(now, at: 8)
                _ = try stmt.step()
                stmt.reset()
            }
        }
    }

    private func removePendingTrashedLocalChanges(batchID: String) async throws {
        try await store.write { conn in
            let stmt = try conn.prepare(
                "DELETE FROM trashed_local_changes WHERE batch_id = ? AND state = 'pending';")
            defer { stmt.reset() }
            stmt.bindText(batchID, at: 1)
            _ = try stmt.step()
        }
    }

    private func commitTrashedLocalChanges(
        _ changes: [PendingTrashedLocalChange], batchID: String, trashDirectory: URL?
    ) async throws {
        guard !changes.isEmpty else { return }
        try await store.write { conn in
            let stmt = try conn.prepare(
                """
                UPDATE trashed_local_changes SET trash_path = ?, state = 'committed',
                    trashed_at = ? WHERE change_id = ? AND batch_id = ? AND state = 'pending';
                """)
            defer { stmt.reset() }
            for change in changes {
                let trashPath = trashDirectory?
                    .appendingPathComponent(change.trashRelativePath).path
                stmt.bindText(trashPath, at: 1)
                stmt.bindDouble(Date().timeIntervalSince1970, at: 2)
                stmt.bindText(change.id, at: 3)
                stmt.bindText(batchID, at: 4)
                _ = try stmt.step()
                stmt.reset()
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

    private func deleteCleanupRows(_ plan: ItemCleanupPlan, operationID: String) async throws {
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
            let pendingTrash = try conn.prepare(
                """
                UPDATE trashed_local_changes SET state = 'committed', trashed_at = ?
                WHERE batch_id = ? AND state = 'pending';
                """)
            pendingTrash.bindDouble(Date().timeIntervalSince1970, at: 1)
            pendingTrash.bindText(operationID, at: 2)
            _ = try pendingTrash.step()
            pendingTrash.reset()
        }
    }

    public func listTrashedLocalChanges(localPath: String) async throws -> [TrashedLocalChange] {
        let normalized = Self.normalizedPath(localPath)
        return try await store.read { conn in
            let stmt = try conn.prepare(
                """
                SELECT change_id, local_root_path, relative_path, original_path, trash_path,
                    baseline_sha256, observed_sha256, trashed_at
                FROM trashed_local_changes
                WHERE local_root_path = ? AND state = 'committed'
                ORDER BY trashed_at DESC, relative_path;
                """)
            defer { stmt.reset() }
            stmt.bindText(normalized, at: 1)
            var result: [TrashedLocalChange] = []
            while try stmt.step(), let id = stmt.columnText(at: 0),
                  let root = stmt.columnText(at: 1),
                  let relative = stmt.columnText(at: 2),
                  let original = stmt.columnText(at: 3),
                  let baseline = stmt.columnText(at: 5),
                  let observed = stmt.columnText(at: 6),
                  let timestamp = stmt.columnDouble(at: 7) {
                result.append(TrashedLocalChange(
                    id: id, localRootPath: root, relativePath: relative,
                    originalPath: original, trashPath: stmt.columnText(at: 4),
                    baselineSHA256: baseline, observedSHA256: observed,
                    trashedAt: Date(timeIntervalSince1970: timestamp)))
            }
            return result
        }
    }
}
