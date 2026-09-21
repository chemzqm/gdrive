import Foundation

struct DirtyRecord: Sendable {
    let itemId: Int64
    let parentId: Int64
    let name: String
    let remoteFileId: String?
    let entryKind: String
    let baseline: ItemBaseline?
    let local: LocalObservation?
    let remote: RemoteObservation?
    let pendingCreate: DurableCreateIntent?
    let localGeneration: Int64
    let remoteGeneration: Int64
    let dirtyGeneration: Int64
    let localDevice: Int64?
    let localInode: Int64?
    let localMtime: Int64?
}

extension IncrementalSyncRun {
    func loadDirtyItems(_ ids: [Int64]? = nil) async throws -> [DirtyRecord] {
        // CROSS JOIN fixes the driving side to this window. An IN filter can otherwise
        // choose idx_items_dirty and rescan every remaining dirty item for each batch.
        let source =
            ids == nil
            ? "items" : "json_each(?2) selected CROSS JOIN items ON items.item_id = selected.value"
        let encodedIDs = ids.map { "[" + $0.map(String.init).joined(separator: ",") + "]" }
        return try await engine.store.read { conn in
            let stmt = try conn.cachedStatement(
                """
                SELECT items.item_id, items.parent_id, items.name, items.remote_file_id, items.entry_kind,
                       items.base_sha256, items.base_size,
                       items.local_sha256, items.local_size, items.local_status,
                       items.remote_sha256, items.remote_size, items.remote_status,
                       op.operation_id, op.target_remote_id, op.target_parent_remote_id,
                       op.expected_local_generation, op.expected_sha256, op.total_bytes,
                       items.local_generation, items.remote_generation, items.dirty_generation,
                       items.local_device, items.local_inode, items.local_mtime
                FROM \(source)
                LEFT JOIN operations op ON op.operation_id = (
                    SELECT candidate.operation_id
                    FROM operations candidate
                    WHERE candidate.item_id = items.item_id
                      AND candidate.operation_type IN ('createDirectory', 'uploadMultipart', 'createResumableUpload')
                      AND candidate.state IN ('ready', 'inFlight', 'verify', 'unknownOutcome')
                    ORDER BY candidate.created_at DESC
                    LIMIT 1
                )
                WHERE items.root_id = ?1
                  AND items.dirty_generation > 0
                  AND items.phase <> 'blocked';
                """)
            stmt.bindInt64(self.rootID, at: 1)
            if let encodedIDs { stmt.bindText(encodedIDs, at: 2) }
            var records: [DirtyRecord] = []
            while try stmt.step() {
                guard let iId = stmt.columnInt64(at: 0),
                    let pId = stmt.columnInt64(at: 1),
                    let name = stmt.columnText(at: 2)
                else { continue }
                let rFileId = stmt.columnText(at: 3)
                let kind = stmt.columnText(at: 4) ?? "file"

                let bSha = stmt.columnText(at: 5)
                let bSize = stmt.columnInt64(at: 6)
                let baseline: ItemBaseline? =
                    (bSha != nil) ? ItemBaseline(sha256: bSha, size: bSize) : nil

                let lSha = stmt.columnText(at: 7)
                let lSize = stmt.columnInt64(at: 8)
                let lStatusStr = stmt.columnText(at: 9) ?? "unknown"
                let lStatus = LocalObservation.Status(rawValue: lStatusStr) ?? .unknown
                let local = LocalObservation(status: lStatus, sha256: lSha, size: lSize)

                let rSha = stmt.columnText(at: 10)
                let rSize = stmt.columnInt64(at: 11)
                let rStatusStr = stmt.columnText(at: 12) ?? "unknown"
                let rStatus = RemoteObservation.Status(rawValue: rStatusStr) ?? .unknown
                let remote = RemoteObservation(status: rStatus, sha256: rSha, size: rSize)

                let pendingCreate: DurableCreateIntent?
                if let operationID = stmt.columnText(at: 13),
                    let targetRemoteID = stmt.columnText(at: 14),
                    let targetParentRemoteID = stmt.columnText(at: 15) {
                    pendingCreate = DurableCreateIntent(
                        operationID: operationID,
                        itemID: iId,
                        targetRemoteID: targetRemoteID,
                        targetParentRemoteID: targetParentRemoteID,
                        expectedLocalGeneration: stmt.columnInt64(at: 16) ?? 0,
                        expectedSHA256: stmt.columnText(at: 17),
                        totalBytes: stmt.columnInt64(at: 18)
                    )
                } else {
                    pendingCreate = nil
                }

                records.append(
                    DirtyRecord(
                        itemId: iId, parentId: pId, name: name, remoteFileId: rFileId,
                        entryKind: kind, baseline: baseline, local: local, remote: remote,
                        pendingCreate: pendingCreate,
                        localGeneration: stmt.columnInt64(at: 19) ?? 0,
                        remoteGeneration: stmt.columnInt64(at: 20) ?? 0,
                        dirtyGeneration: stmt.columnInt64(at: 21) ?? 0,
                        localDevice: stmt.columnInt64(at: 22),
                        localInode: stmt.columnInt64(at: 23),
                        localMtime: stmt.columnInt64(at: 24)
                    ))
            }
            stmt.reset()
            return records
        }
    }
}

extension IncrementalSyncRun {
    func markMissingAfterSuccessfulScan() async throws {
        // Identify local deleted files and directories (§9.1)
        // Verify again whether the local root directory exists to avoid all misjudgments caused by the local directory being removed during the scan. absent diffuse deletion
        var isStillDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: localPath, isDirectory: &isStillDir),
            isStillDir.boolValue
        else {
            engine.logger.error(
                "[Sync] The local sync root disappeared during scanning: \(localPath). Stopping sync to protect remote files."
            )
            throw SyncEngineError.localRootNotFound(path: localPath)
        }

        let localStatuses = localChangeScope == nil ? "'present'" : "'present', 'unknown'"
        try await engine.store.write { conn in
            // 1. File deletion detection
            let stmt = try conn.cachedStatement(
                """
                SELECT item_id, parent_id, name
                FROM items
                WHERE root_id = ? AND entry_kind = 'file' AND local_status IN (\(localStatuses));
                """)
            stmt.bindInt64(rootID, at: 1)
            var deletedIds: [Int64] = []
            while try stmt.step() {
                if let iId = stmt.columnInt64(at: 0),
                    let pId = stmt.columnInt64(at: 1),
                    let name = stmt.columnText(at: 2) {
                    let parent = directoryContext.getRelPath(for: pId) ?? ""
                    let path = parent.isEmpty ? name : "\(parent)/\(name)"
                    if !remoteGate.blocks(path) && (localChangeScope?.coversMissing(path) ?? true)
                        && !seenTracker.contains(parentId: pId, name: name) {
                        deletedIds.append(iId)
                    }
                }
            }
            stmt.reset()

            // 2. Directory deletion detection (excluding root directory entries)
            let dirStmt = try conn.cachedStatement(
                """
                SELECT item_id, parent_id, name
                FROM items
                WHERE root_id = ? AND entry_kind = 'directory' AND parent_id IS NOT NULL AND local_status = 'present';
                """)
            dirStmt.bindInt64(rootID, at: 1)
            while try dirStmt.step() {
                if let iId = dirStmt.columnInt64(at: 0),
                    let pId = dirStmt.columnInt64(at: 1),
                    let name = dirStmt.columnText(at: 2) {
                    let parent = directoryContext.getRelPath(for: pId) ?? ""
                    let path = parent.isEmpty ? name : "\(parent)/\(name)"
                    if !remoteGate.blocks(path) && (localChangeScope?.coversMissing(path) ?? true)
                        && !seenDirTracker.contains(parentId: pId, name: name) {
                        deletedIds.append(iId)
                    }
                }
            }
            dirStmt.reset()

            for dId in deletedIds {
                let update = try conn.cachedStatement(
                    """
                    UPDATE items SET
                        local_status = 'absent',
                        local_generation = local_generation + 1,
                        dirty_generation = dirty_generation + 1,
                        phase = 'ready',
                        updated_at = ?
                    WHERE item_id = ?;
                    """)
                update.bindDouble(now, at: 1)
                update.bindInt64(dId, at: 2)
                _ = try update.step()
                update.reset()
            }
        }

        // Refresh all uncommitted Changes and scan the batch to disk, ensuring immediate visibility for subsequent queries
        try await engine.store.flush()
    }
}
