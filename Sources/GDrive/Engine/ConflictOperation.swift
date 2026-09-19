import Darwin
import Foundation
import os

/// Both destinations and both content identities are durable before touching files.
/// Pending operations run before discovery so intermediate files are never reinterpreted
/// as a new conflict. A failed recovery leaves the original baseline untouched.
struct ConflictOperation: Codable, Sendable {
    let id: String
    let rootID: Int64
    let itemID: Int64
    let copyItemID: Int64
    let originalPath: String
    let copyPath: String
    let remoteID: String
    let copyRemoteID: String
    let parentRemoteID: String
    let localSHA: String
    let remoteSHA: String
    let localGeneration: Int64
    let remoteGeneration: Int64
    let dirtyGeneration: Int64

    static func pending(store: StateStore, rootID: Int64) async throws -> [Self] {
        try await store.read { conn in
            let q = try conn.cachedStatement("SELECT payload FROM conflict_operations WHERE root_id = ? AND state = 'pending';")
            defer { q.reset() }
            q.bindInt64(rootID, at: 1)
            var result: [Self] = []
            while try q.step() {
                guard let text = q.columnText(at: 0) else { throw SyncEngineError.general("Missing conflict intent") }
                result.append(try JSONDecoder().decode(Self.self, from: Data(text.utf8)))
            }
            return result
        }
    }

    static func prepare(store: StateStore, rootID: Int64, itemID: Int64, parentID: Int64,
                        original: URL, remoteID: String, parentRemoteID: String,
                        copyRemoteID: String, conflictID: String, localSHA: String, remoteSHA: String,
                        localGeneration: Int64, remoteGeneration: Int64, dirtyGeneration: Int64) async throws -> Self {
        let id = "\(itemID)-\(localGeneration)-\(remoteGeneration)-\(conflictID.prefix(16))"
        let ext = original.pathExtension
        let base = original.deletingPathExtension().lastPathComponent
        let name = "\(base) (Conflict \(id))" + (ext.isEmpty ? "" : ".\(ext)")
        let copy = original.deletingLastPathComponent().appendingPathComponent(name)
        let result = OSAllocatedUnfairLock<ConflictOperation?>(initialState: nil)
        try await store.batchWrite { conn in
            let existing = try conn.cachedStatement("SELECT payload FROM conflict_operations WHERE item_id = ? AND state = 'pending';")
            existing.bindInt64(itemID, at: 1)
            defer { existing.reset() }
            if try existing.step(), let payload = existing.columnText(at: 0) {
                let operation = try JSONDecoder().decode(Self.self, from: Data(payload.utf8))
                result.withLock { $0 = operation }
                return
            }
            guard !FileManager.default.fileExists(atPath: copy.path) else {
                throw SyncEngineError.general("冲突副本路径已被占用: \(copy.path)")
            }
            let reserve = try conn.cachedStatement("""
                UPDATE items SET phase = 'conflict', conflict_id = ?, conflict_winner = 'remote'
                WHERE item_id = ? AND local_generation = ? AND remote_generation = ? AND dirty_generation = ? AND is_tombstone = 0;
                """)
            reserve.bindText(id, at: 1)
            reserve.bindInt64(itemID, at: 2)
            reserve.bindInt64(localGeneration, at: 3)
            reserve.bindInt64(remoteGeneration, at: 4)
            reserve.bindInt64(dirtyGeneration, at: 5)
            _ = try reserve.step()
            reserve.reset()
            guard conn.changes == 1 else { throw SyncEngineError.general("冲突计划已过期") }
            let insert = try conn.cachedStatement("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, ?, 'file', ?, 'conflict', 1, ?, ?);
                """)
            insert.bindInt64(rootID, at: 1)
            insert.bindInt64(parentID, at: 2)
            insert.bindText(name, at: 3)
            insert.bindText(copyRemoteID, at: 4)
            insert.bindDouble(Date().timeIntervalSince1970, at: 5)
            insert.bindDouble(Date().timeIntervalSince1970, at: 6)
            _ = try insert.step()
            insert.reset()
            let operation = Self(id: id, rootID: rootID, itemID: itemID, copyItemID: conn.lastInsertRowId,
                originalPath: original.path, copyPath: copy.path, remoteID: remoteID,
                copyRemoteID: copyRemoteID, parentRemoteID: parentRemoteID,
                localSHA: localSHA, remoteSHA: remoteSHA, localGeneration: localGeneration,
                remoteGeneration: remoteGeneration, dirtyGeneration: dirtyGeneration)
            let intent = try conn.cachedStatement("INSERT INTO conflict_operations VALUES (?, ?, ?, ?, 'pending');")
            intent.bindText(id, at: 1)
            intent.bindInt64(rootID, at: 2)
            intent.bindInt64(itemID, at: 3)
            intent.bindText(String(decoding: try JSONEncoder().encode(operation), as: UTF8.self), at: 4)
            _ = try intent.step()
            intent.reset()
            result.withLock { $0 = operation }
        }
        guard let operation = result.withLock({ $0 }) else { throw SyncEngineError.general("Missing conflict receipt") }
        return operation
    }

    func validatePlan(_ conn: SQLiteConnection) throws {
        let q = try conn.cachedStatement("""
            SELECT 1 FROM items WHERE item_id = ? AND local_generation = ? AND remote_generation = ?
                AND dirty_generation = ? AND is_tombstone = 0 AND conflict_id = ?;
            """)
        defer { q.reset() }
        q.bindInt64(itemID, at: 1)
        q.bindInt64(localGeneration, at: 2)
        q.bindInt64(remoteGeneration, at: 3)
        q.bindInt64(dirtyGeneration, at: 4)
        q.bindText(id, at: 5)
        guard try q.step() else { throw SyncEngineError.general("冲突计划已过期，保留 pending 状态") }
        let copy = try conn.cachedStatement("""
            SELECT 1 FROM items WHERE item_id = ? AND remote_file_id = ? AND root_id = ?
                AND local_generation = 0 AND remote_generation = 0 AND dirty_generation = 1
                AND phase = 'conflict' AND is_tombstone = 0;
            """)
        defer { copy.reset() }
        copy.bindInt64(copyItemID, at: 1)
        copy.bindText(copyRemoteID, at: 2)
        copy.bindInt64(rootID, at: 3)
        guard try copy.step() else { throw SyncEngineError.general("冲突副本计划已过期，保留 pending 状态") }
    }
}

enum ConflictCheckpoint: String, Sendable, CaseIterable {
    case intent, copy, upload, publish, beforeCommit
}

extension SyncEngine {
    func resolveConflict(_ op: ConflictOperation,
                         checkpoint: (@Sendable (ConflictCheckpoint) throws -> Void)? = nil) async throws {
        let original = URL(fileURLWithPath: op.originalPath)
        let copy = URL(fileURLWithPath: op.copyPath)
        try await store.read { try op.validatePlan($0) }
        try checkpoint?(.intent)
        if !FileManager.default.fileExists(atPath: copy.path) {
            guard let before = try LocalFileVersion.read(at: original) else { throw CocoaError(.fileNoSuchFile) }
            // APFS clone is exclusive and constant-space; keep the original available until
            // the remote copy is confirmed. Never fall back to a full large-file disk copy.
            guard clonefile(original.path, copy.path, UInt32(CLONE_NOFOLLOW)) == 0 else {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            try before.validate(at: original)
        }
        try checkpoint?(.copy)
        let input = try StableUploadInput.capture(at: copy)
        guard input.sha256 == op.localSHA else {
            throw DriveError.checksumMismatch(expected: op.localSHA, actual: input.sha256)
        }
        // Probe the reserved ID first: includes create-success/response-loss and completed
        // resumable sessions. Only a confirmed 404 permits a create, always with the same ID.
        let uploaded: DriveFile
        do {
            uploaded = try await client.getFile(remoteId: op.copyRemoteID)
        } catch DriveError.notFound {
            if let bytes = input.data {
                uploaded = try await client.uploadMultipart(name: copy.lastPathComponent,
                    parentId: op.parentRemoteID, remoteId: op.copyRemoteID,
                    content: bytes, expectedSha256: op.localSHA)
            } else {
                uploaded = try await performResumableUpload(rootId: op.rootID, itemId: op.copyItemID,
                    fileURL: input.fileURL, fileSize: input.size, expectedSha256: op.localSHA,
                    remoteId: op.copyRemoteID, parentId: op.parentRemoteID,
                    name: copy.lastPathComponent, isUpdate: false)
            }
        }
        guard uploaded.sha256Checksum?.lowercased() == op.localSHA,
              uploaded.sizeBytes == input.size, uploaded.trashed != true,
              uploaded.name == copy.lastPathComponent,
              uploaded.parents?.contains(op.parentRemoteID) == true else {
            throw SyncEngineError.general("冲突远端副本核验失败: \(op.copyRemoteID)")
        }
        try checkpoint?(.upload)
        guard let originalVersion = try LocalFileVersion.read(at: original) else { throw CocoaError(.fileNoSuchFile) }
        let digest = try Self.computeFileSha256(at: original)
        try originalVersion.validate(at: original)
        let published: LocalFileVersion
        if digest.sha256Hex == op.remoteSHA {
            // Publication succeeded before the previous process stopped.
            published = originalVersion
        } else {
            guard digest.sha256Hex == op.localSHA else {
                throw DriveError.fileModifiedDuringUpload(path: original.path)
            }
            published = try await client.downloadFileSafely(remoteId: op.remoteID,
                destinationURL: original, expectedSha256: op.remoteSHA,
                expectedDestination: originalVersion, beforePublish: {
                    try input.version.validate(at: copy)
                    try await self.store.read { try op.validatePlan($0) }
                })
        }
        try checkpoint?(.publish)
        let remote = try await client.getFile(remoteId: op.remoteID)
        guard remote.sha256Checksum?.lowercased() == op.remoteSHA, remote.trashed != true,
              remote.name == original.lastPathComponent,
              remote.parents?.contains(op.parentRemoteID) == true else {
            throw SyncEngineError.general("冲突期间远端原文件再次变化，保留 pending 状态")
        }
        try checkpoint?(.beforeCommit)
        try await store.batchWrite { conn in
            try op.validatePlan(conn)
            try published.validate(at: original)
            try input.version.validate(at: copy)
            // Both baselines and the completion receipt commit in the same transaction.
            for (itemID, sha, version, remoteVersion) in [
                (op.itemID, op.remoteSHA, published, remote.versionNumber),
                (op.copyItemID, op.localSHA, input.version, uploaded.versionNumber)
            ] {
                let update = try conn.cachedStatement("""
                    UPDATE items SET base_sha256 = ?, local_sha256 = ?, remote_sha256 = ?,
                        base_size = ?, local_size = ?, remote_size = ?, local_device = ?, local_inode = ?, local_mtime = ?,
                        base_version = ?, remote_version = ?, base_name = name, base_parent_id = parent_id,
                        local_status = 'present', remote_status = 'present', phase = 'committed', dirty_generation = 0
                    WHERE item_id = ?;
                    """)
                for i: Int32 in 1...3 { update.bindText(sha, at: i) }
                for i: Int32 in 4...6 { update.bindInt64(version.size, at: i) }
                update.bindInt64(version.device, at: 7)
                update.bindInt64(version.inode, at: 8)
                update.bindInt64(version.mtime, at: 9)
                if let remoteVersion {
                    update.bindInt64(remoteVersion, at: 10); update.bindInt64(remoteVersion, at: 11)
                } else { update.bindNull(at: 10); update.bindNull(at: 11) }
                update.bindInt64(itemID, at: 12)
                _ = try update.step()
                update.reset()
            }
            let done = try conn.cachedStatement("UPDATE conflict_operations SET state = 'completed' WHERE operation_id = ?;")
            done.bindText(op.id, at: 1)
            _ = try done.step()
            done.reset()
        }
    }
}
