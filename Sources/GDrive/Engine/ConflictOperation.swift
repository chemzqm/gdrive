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

    private static let activeStates = "'ready', 'inFlight', 'verify', 'unknownOutcome'"

    static func pending(store: StateStore, rootID: Int64) async throws -> [Self] {
        try await store.read { conn in
            let queryStatement = try conn.cachedStatement("SELECT payload FROM operations INDEXED BY idx_operations_conflict_pending_root WHERE root_id = ? AND operation_type = 'resolveConflict' AND state IN (\(activeStates));")
            defer { queryStatement.reset() }
            queryStatement.bindInt64(rootID, at: 1)
            var result: [Self] = []
            while try queryStatement.step() {
                guard let text = queryStatement.columnText(at: 0) else { throw SyncEngineError.general("Missing conflict intent") }
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
            let existing = try conn.cachedStatement("SELECT payload FROM operations INDEXED BY idx_operations_conflict_pending_item WHERE item_id = ? AND operation_type = 'resolveConflict' AND state IN (\(activeStates));")
            existing.bindInt64(itemID, at: 1)
            defer { existing.reset() }
            if try existing.step(), let payload = existing.columnText(at: 0) {
                let operation = try JSONDecoder().decode(Self.self, from: Data(payload.utf8))
                result.withLock { $0 = operation }
                return
            }
            guard !FileManager.default.fileExists(atPath: copy.path) else {
                throw SyncEngineError.general("Conflict copy path already taken: \(copy.path)")
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
            guard conn.changes == 1 else { throw SyncEngineError.general("Conflict plan is stale") }
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
            let intent = try conn.cachedStatement("""
                INSERT INTO operations(operation_id, root_id, item_id, payload, operation_type, state, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'resolveConflict', 'inFlight', ?, ?);
                """)
            intent.bindText(id, at: 1)
            intent.bindInt64(rootID, at: 2)
            intent.bindInt64(itemID, at: 3)
            intent.bindText((String(bytes: try JSONEncoder().encode(operation), encoding: .utf8) ?? "Invalid UTF-8 data"), at: 4)
            let now = Date().timeIntervalSince1970
            intent.bindDouble(now, at: 5)
            intent.bindDouble(now, at: 6)
            _ = try intent.step()
            intent.reset()
            result.withLock { $0 = operation }
        }
        guard let operation = result.withLock({ $0 }) else { throw SyncEngineError.general("Missing conflict receipt") }
        return operation
    }

    func validatePlan(_ conn: SQLiteConnection) throws {
        let queryStatement = try conn.cachedStatement("""
            SELECT 1 FROM items WHERE item_id = ? AND local_generation = ? AND remote_generation = ?
                AND dirty_generation = ? AND is_tombstone = 0 AND conflict_id = ?;
            """)
        defer { queryStatement.reset() }
        queryStatement.bindInt64(itemID, at: 1)
        queryStatement.bindInt64(localGeneration, at: 2)
        queryStatement.bindInt64(remoteGeneration, at: 3)
        queryStatement.bindInt64(dirtyGeneration, at: 4)
        queryStatement.bindText(id, at: 5)
        guard try queryStatement.step() else { throw SyncEngineError.general("Conflict plan is stale; pending state was preserved") }
        let copy = try conn.cachedStatement("""
            SELECT 1 FROM items WHERE item_id = ? AND remote_file_id = ? AND root_id = ?
                AND local_generation = 0 AND remote_generation = 0 AND dirty_generation = 1
                AND phase = 'conflict' AND is_tombstone = 0;
            """)
        defer { copy.reset() }
        copy.bindInt64(copyItemID, at: 1)
        copy.bindText(copyRemoteID, at: 2)
        copy.bindInt64(rootID, at: 3)
        guard try copy.step() else { throw SyncEngineError.general("Conflict copy plan is stale; pending state was preserved") }
    }
}

enum ConflictCheckpoint: String, Sendable, CaseIterable {
    case intent, copy, upload, publish, beforeCommit
}

extension SyncEngine {
    func resolveConflict(_ conflictOperation: ConflictOperation,
                         temporaryDirectory: URL? = nil,
                         checkpoint: (@Sendable (ConflictCheckpoint) throws -> Void)? = nil) async throws {
        let downloadDirectory: URL
        if let temporaryDirectory {
            downloadDirectory = temporaryDirectory
        } else {
            let root = try await store.read { conn -> (String, String) in
                let query = try conn.cachedStatement("SELECT remote_root_id, local_root_path FROM roots WHERE root_id = ?;")
                defer { query.reset() }
                query.bindInt64(conflictOperation.rootID, at: 1)
                guard try query.step(), let remoteID = query.columnText(at: 0),
                      let localPath = query.columnText(at: 1) else {
                    throw SyncEngineError.general("Conflict recovery is missing its sync root")
                }
                return (remoteID, localPath)
            }
            downloadDirectory = try await downloadStagingDirectory(remoteRootID: root.0,
                localRoot: URL(fileURLWithPath: (root.1 as NSString).expandingTildeInPath))
        }
        defer {
            if temporaryDirectory == nil { cleanupDownloadStagingDirectory(downloadDirectory) }
        }
        let original = URL(fileURLWithPath: conflictOperation.originalPath)
        let copy = URL(fileURLWithPath: conflictOperation.copyPath)
        try await store.read { try conflictOperation.validatePlan($0) }
        try checkpoint?(.intent)
        func ensureConflictCopy() throws {
            if !FileManager.default.fileExists(atPath: copy.path) {
                guard let before = try LocalFileVersion.read(at: original) else { throw CocoaError(.fileNoSuchFile) }
                // APFS clone is exclusive and constant-space; keep the original available until
                // the remote copy is confirmed. Never fall back to a full large-file disk copy.
                guard clonefile(original.path, copy.path, UInt32(CLONE_NOFOLLOW)) == 0 else {
                    throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
                }
                try before.validate(at: original)
            }
        }
        try ensureConflictCopy()
        try checkpoint?(.copy)
        let input = try StableUploadInput.capture(at: copy)
        guard input.sha256 == conflictOperation.localSHA else {
            throw DriveError.checksumMismatch(expected: conflictOperation.localSHA, actual: input.sha256)
        }
        // Probe the reserved ID first: includes create-success/response-loss and completed
        // resumable sessions. Only a confirmed 404 permits a create, always with the same ID.
        func uploadConflictCopy() async throws -> DriveFile {
            do {
                return try await client.getFile(remoteId: conflictOperation.copyRemoteID)
            } catch DriveError.notFound {
                if let bytes = input.data {
                    return try await client.uploadMultipart(name: copy.lastPathComponent,
                        parentId: conflictOperation.parentRemoteID, remoteId: conflictOperation.copyRemoteID,
                        content: bytes, expectedSha256: conflictOperation.localSHA)
                } else {
                    return try await performResumableUpload(rootId: conflictOperation.rootID, itemId: conflictOperation.copyItemID,
                        fileURL: input.fileURL, fileSize: input.size, expectedSha256: conflictOperation.localSHA,
                        remoteId: conflictOperation.copyRemoteID, parentId: conflictOperation.parentRemoteID,
                        name: copy.lastPathComponent, isUpdate: false)
                }
            }
        }
        let uploaded = try await uploadConflictCopy()
        guard uploaded.sha256Checksum?.lowercased() == conflictOperation.localSHA,
              uploaded.sizeBytes == input.size, uploaded.trashed != true,
              uploaded.name == copy.lastPathComponent,
              uploaded.parents?.contains(conflictOperation.parentRemoteID) == true else {
            throw SyncEngineError.general("Conflicting remote copy verification failed: \(conflictOperation.copyRemoteID)")
        }
        try checkpoint?(.upload)
        guard let originalVersion = try LocalFileVersion.read(at: original) else { throw CocoaError(.fileNoSuchFile) }
        let digest = try Self.computeFileSha256(at: original)
        try originalVersion.validate(at: original)
        let published: LocalFileVersion
        if digest.sha256Hex == conflictOperation.remoteSHA {
            // Publication succeeded before the previous process stopped.
            published = originalVersion
        } else {
            guard digest.sha256Hex == conflictOperation.localSHA else {
                throw DriveError.fileModifiedDuringUpload(path: original.path)
            }
            published = try await client.downloadFileSafely(remoteId: conflictOperation.remoteID,
                destinationURL: original, expectedSha256: conflictOperation.remoteSHA,
                expectedDestination: originalVersion, temporaryDirectory: downloadDirectory, beforePublish: {
                    try input.version.validate(at: copy)
                    try await self.store.read { try conflictOperation.validatePlan($0) }
                })
        }
        try checkpoint?(.publish)
        let remote = try await client.getFile(remoteId: conflictOperation.remoteID)
        guard remote.sha256Checksum?.lowercased() == conflictOperation.remoteSHA, remote.trashed != true,
              remote.name == original.lastPathComponent,
              remote.parents?.contains(conflictOperation.parentRemoteID) == true else {
            throw SyncEngineError.general("During the conflict, the remote original file changes again and is retained pending Status")
        }
        try checkpoint?(.beforeCommit)
        @Sendable func commitConflict(_ conn: SQLiteConnection) throws {
            try conflictOperation.validatePlan(conn)
            try published.validate(at: original)
            try input.version.validate(at: copy)
            // Both baselines and the completion receipt commit in the same transaction.
            for (itemID, sha, version, remoteVersion) in [
                (conflictOperation.itemID, conflictOperation.remoteSHA, published, remote.versionNumber),
                (conflictOperation.copyItemID, conflictOperation.localSHA, input.version, uploaded.versionNumber)
            ] {
                let update = try conn.cachedStatement("""
                    UPDATE items SET base_sha256 = ?, local_sha256 = ?, remote_sha256 = ?,
                        base_size = ?, local_size = ?, remote_size = ?, local_device = ?, local_inode = ?, local_mtime = ?,
                        base_version = ?, remote_version = ?, base_name = name, base_parent_id = parent_id,
                        local_status = 'present', remote_status = 'present', phase = 'committed', dirty_generation = 0
                    WHERE item_id = ?;
                    """)
                for bindingIndex: Int32 in 1...3 { update.bindText(sha, at: bindingIndex) }
                for bindingIndex: Int32 in 4...6 { update.bindInt64(version.size, at: bindingIndex) }
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
            let done = try conn.cachedStatement("UPDATE operations SET state = 'completed', updated_at = strftime('%s','now') WHERE operation_id = ? AND operation_type = 'resolveConflict';")
            done.bindText(conflictOperation.id, at: 1)
            _ = try done.step()
            done.reset()
        }
        try await store.batchWrite(commitConflict)
    }
}
