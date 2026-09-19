import Foundation
import os

/// A create request whose identity and input were committed before the request
/// is allowed to reach Google Drive.
struct DurableCreateIntent: Sendable, Equatable {
    let operationID: String
    let itemID: Int64
    let targetRemoteID: String
    let targetParentRemoteID: String
    let expectedLocalGeneration: Int64
    let expectedSHA256: String?
    let totalBytes: Int64?
}

private final class IntentResultBox: @unchecked Sendable {
    private var lock = os_unfair_lock()
    private var value: Result<DurableCreateIntent, Error>?

    func store(_ value: Result<DurableCreateIntent, Error>) {
        os_unfair_lock_lock(&lock)
        self.value = value
        os_unfair_lock_unlock(&lock)
    }

    func load() throws -> DurableCreateIntent {
        os_unfair_lock_lock(&lock)
        let result = value
        os_unfair_lock_unlock(&lock)
        guard let result else {
            throw SyncEngineError.general("Persisting the creation intent returned no result")
        }
        return try result.get()
    }
}

enum DurableCreateIntentStore {
    private struct ItemIdentity: Sendable {
        let itemID: Int64
        let remoteID: String
        let localGeneration: Int64
    }

    private static let recoverableStates = "'ready', 'inFlight', 'verify', 'unknownOutcome'"

    enum FileUploadTransport: Sendable {
        case multipart
        case resumable

        var operationType: String {
            switch self {
            case .multipart: "uploadMultipart"
            case .resumable: "createResumableUpload"
            }
        }
    }

    static func prepareDirectory(
        store: StateStore,
        rootID: Int64,
        parentItemID: Int64,
        name: String,
        targetParentRemoteID: String,
        candidateRemoteID: String,
        device: Int64,
        inode: Int64
    ) async throws -> DurableCreateIntent {
        let box = IntentResultBox()
        try await store.batchWrite { conn in
            do {
                let now = Date().timeIntervalSince1970
                let item = try upsertDirectoryItem(
                    conn: conn,
                    rootID: rootID,
                    parentItemID: parentItemID,
                    name: name,
                    candidateRemoteID: candidateRemoteID,
                    device: device,
                    inode: inode,
                    now: now
                )
                let intent = try upsertOperation(
                    conn: conn,
                    rootID: rootID,
                    itemID: item.itemID,
                    operationType: "createDirectory",
                    expectedLocalGeneration: item.localGeneration,
                    expectedSHA256: nil,
                    totalBytes: nil,
                    candidateRemoteID: item.remoteID,
                    targetParentRemoteID: targetParentRemoteID,
                    now: now
                )
                box.store(.success(intent))
            } catch {
                box.store(.failure(error))
                throw error
            }
        }
        return try box.load()
    }

    static func prepareFileUpload(
        store: StateStore,
        rootID: Int64,
        itemID: Int64? = nil,
        parentItemID: Int64,
        name: String,
        targetParentRemoteID: String,
        candidateRemoteID: String,
        device: Int64,
        inode: Int64,
        mtime: Int64,
        size: Int64,
        sha256: String,
        transport: FileUploadTransport
    ) async throws -> DurableCreateIntent {
        let operationType = transport.operationType
        let box = IntentResultBox()
        try await store.batchWrite { conn in
            do {
                let now = Date().timeIntervalSince1970
                let item = try upsertFileItem(
                    conn: conn,
                    rootID: rootID,
                    requestedItemID: itemID,
                    parentItemID: parentItemID,
                    name: name,
                    candidateRemoteID: candidateRemoteID,
                    device: device,
                    inode: inode,
                    mtime: mtime,
                    size: size,
                    sha256: sha256,
                    operationType: operationType,
                    now: now
                )
                let intent = try upsertOperation(
                    conn: conn,
                    rootID: rootID,
                    itemID: item.itemID,
                    operationType: operationType,
                    expectedLocalGeneration: item.localGeneration,
                    expectedSHA256: sha256,
                    totalBytes: size,
                    candidateRemoteID: item.remoteID,
                    targetParentRemoteID: targetParentRemoteID,
                    now: now
                )
                box.store(.success(intent))
            } catch {
                box.store(.failure(error))
                throw error
            }
        }
        return try box.load()
    }

    static func prepareMultipartUpload(
        store: StateStore,
        rootID: Int64,
        itemID: Int64? = nil,
        parentItemID: Int64,
        name: String,
        targetParentRemoteID: String,
        candidateRemoteID: String,
        device: Int64,
        inode: Int64,
        mtime: Int64,
        size: Int64,
        sha256: String
    ) async throws -> DurableCreateIntent {
        try await prepareFileUpload(
            store: store,
            rootID: rootID,
            itemID: itemID,
            parentItemID: parentItemID,
            name: name,
            targetParentRemoteID: targetParentRemoteID,
            candidateRemoteID: candidateRemoteID,
            device: device,
            inode: inode,
            mtime: mtime,
            size: size,
            sha256: sha256,
            transport: .multipart
        )
    }

    static func markUnknownOutcome(
        store: StateStore,
        operationID: String,
        error: Error
    ) async {
        try? await store.batchWrite { conn in
            let stmt = try conn.cachedStatement("""
            UPDATE operations
            SET state = 'unknownOutcome', last_error_message = ?, updated_at = ?
            WHERE operation_id = ? AND state != 'completed';
            """)
            stmt.bindText(String(describing: error), at: 1)
            stmt.bindDouble(Date().timeIntervalSince1970, at: 2)
            stmt.bindText(operationID, at: 3)
            _ = try stmt.step()
            stmt.reset()
        }
    }

    static func completeOperation(
        conn: SQLiteConnection,
        operationID: String,
        now: Double
    ) throws {
        let stmt = try conn.cachedStatement("""
        UPDATE operations
        SET state = 'completed', last_error_code = NULL, last_error_message = NULL, updated_at = ?
        WHERE operation_id = ?;
        """)
        stmt.bindDouble(now, at: 1)
        stmt.bindText(operationID, at: 2)
        _ = try stmt.step()
        stmt.reset()
    }

    private static func upsertDirectoryItem(
        conn: SQLiteConnection,
        rootID: Int64,
        parentItemID: Int64,
        name: String,
        candidateRemoteID: String,
        device: Int64,
        inode: Int64,
        now: Double
    ) throws -> ItemIdentity {
        let stmt = try conn.cachedStatement("""
        INSERT INTO items (
            root_id, parent_id, name, entry_kind, remote_file_id,
            local_device, local_inode, local_status, remote_status,
            local_generation, phase, dirty_generation, created_at, updated_at
        ) VALUES (?, ?, ?, 'directory', ?, ?, ?, 'present', 'unknown', 1, 'inFlight', 1, ?, ?)
        ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
        DO UPDATE SET
            remote_file_id = COALESCE(items.remote_file_id, excluded.remote_file_id),
            local_device = excluded.local_device,
            local_inode = excluded.local_inode,
            local_status = 'present',
            phase = CASE WHEN items.phase = 'committed' AND items.remote_status = 'present' THEN items.phase ELSE 'inFlight' END,
            dirty_generation = CASE WHEN items.phase = 'committed' AND items.remote_status = 'present' THEN items.dirty_generation ELSE MAX(items.dirty_generation, 1) END,
            updated_at = excluded.updated_at;
        """)
        stmt.bindInt64(rootID, at: 1)
        stmt.bindInt64(parentItemID, at: 2)
        stmt.bindText(name, at: 3)
        stmt.bindText(candidateRemoteID, at: 4)
        stmt.bindInt64(device, at: 5)
        stmt.bindInt64(inode, at: 6)
        stmt.bindDouble(now, at: 7)
        stmt.bindDouble(now, at: 8)
        _ = try stmt.step()
        stmt.reset()
        return try loadItemIdentity(conn: conn, rootID: rootID, parentItemID: parentItemID, name: name)
    }

    private static func upsertFileItem(
        conn: SQLiteConnection,
        rootID: Int64,
        requestedItemID: Int64?,
        parentItemID: Int64,
        name: String,
        candidateRemoteID: String,
        device: Int64,
        inode: Int64,
        mtime: Int64,
        size: Int64,
        sha256: String,
        operationType: String,
        now: Double
    ) throws -> ItemIdentity {
        if let requestedItemID {
            let update = try conn.cachedStatement("""
            UPDATE items SET
                remote_file_id = COALESCE(remote_file_id, ?),
                local_device = ?, local_inode = ?, local_mtime = ?, local_size = ?, local_sha256 = ?,
                local_status = 'present', phase = 'inFlight', dirty_generation = MAX(dirty_generation, 1), updated_at = ?
            WHERE item_id = ? AND root_id = ? AND is_tombstone = 0;
            """)
            update.bindText(candidateRemoteID, at: 1)
            update.bindInt64(device, at: 2)
            update.bindInt64(inode, at: 3)
            update.bindInt64(mtime, at: 4)
            update.bindInt64(size, at: 5)
            update.bindText(sha256, at: 6)
            update.bindDouble(now, at: 7)
            update.bindInt64(requestedItemID, at: 8)
            update.bindInt64(rootID, at: 9)
            _ = try update.step()
            update.reset()
        } else {
            let insert = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, local_mtime, local_size, local_sha256,
                local_generation, local_status, remote_status, phase, dirty_generation,
                created_at, updated_at
            ) VALUES (?, ?, ?, 'file', ?, ?, ?, ?, ?, ?, 1, 'present', 'unknown', 'inFlight', 1, ?, ?)
            ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
            DO UPDATE SET
                remote_file_id = COALESCE(items.remote_file_id, excluded.remote_file_id),
                local_device = excluded.local_device,
                local_inode = excluded.local_inode,
                local_mtime = excluded.local_mtime,
                local_size = excluded.local_size,
                local_sha256 = excluded.local_sha256,
                local_status = 'present',
                phase = 'inFlight',
                dirty_generation = MAX(items.dirty_generation, 1),
                updated_at = excluded.updated_at;
            """)
            insert.bindInt64(rootID, at: 1)
            insert.bindInt64(parentItemID, at: 2)
            insert.bindText(name, at: 3)
            insert.bindText(candidateRemoteID, at: 4)
            insert.bindInt64(device, at: 5)
            insert.bindInt64(inode, at: 6)
            insert.bindInt64(mtime, at: 7)
            insert.bindInt64(size, at: 8)
            insert.bindText(sha256, at: 9)
            insert.bindDouble(now, at: 10)
            insert.bindDouble(now, at: 11)
            _ = try insert.step()
            insert.reset()
        }

        let item = try loadItemIdentity(conn: conn, rootID: rootID, parentItemID: parentItemID, name: name)
        let pending = try loadActiveOperation(conn: conn, itemID: item.itemID, operationType: operationType)
        if let pending, let persistedSHA = pending.expectedSHA256,
           persistedSHA.caseInsensitiveCompare(sha256) != .orderedSame {
            throw SyncEngineError.general("Incomplete file creation intent does not match the current content digest: \(name)")
        }
        return item
    }

    private static func loadItemIdentity(
        conn: SQLiteConnection,
        rootID: Int64,
        parentItemID: Int64,
        name: String
    ) throws -> ItemIdentity {
        let query = try conn.cachedStatement("""
        SELECT item_id, remote_file_id, local_generation
        FROM items
        WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;
        """)
        query.bindInt64(rootID, at: 1)
        query.bindInt64(parentItemID, at: 2)
        query.bindText(name, at: 3)
        defer { query.reset() }
        guard try query.step(),
              let itemID = query.columnInt64(at: 0),
              let remoteID = query.columnText(at: 1) else {
            throw SyncEngineError.general("Unable to read the corresponding creation intent item: \(name)")
        }
        return ItemIdentity(itemID: itemID, remoteID: remoteID, localGeneration: query.columnInt64(at: 2) ?? 0)
    }

    private static func upsertOperation(
        conn: SQLiteConnection,
        rootID: Int64,
        itemID: Int64,
        operationType: String,
        expectedLocalGeneration: Int64,
        expectedSHA256: String?,
        totalBytes: Int64?,
        candidateRemoteID: String,
        targetParentRemoteID: String,
        now: Double
    ) throws -> DurableCreateIntent {
        if let existing = try loadActiveOperation(conn: conn, itemID: itemID, operationType: operationType) {
            return existing
        }

        let operationID = UUID().uuidString
        let stmt = try conn.cachedStatement("""
        INSERT INTO operations (
            operation_id, root_id, item_id, operation_type, state,
            expected_local_generation, expected_sha256,
            target_remote_id, target_parent_remote_id, total_bytes,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'inFlight', ?, ?, ?, ?, ?, ?, ?);
        """)
        stmt.bindText(operationID, at: 1)
        stmt.bindInt64(rootID, at: 2)
        stmt.bindInt64(itemID, at: 3)
        stmt.bindText(operationType, at: 4)
        stmt.bindInt64(expectedLocalGeneration, at: 5)
        if let expectedSHA256 { stmt.bindText(expectedSHA256, at: 6) } else { stmt.bindNull(at: 6) }
        stmt.bindText(candidateRemoteID, at: 7)
        stmt.bindText(targetParentRemoteID, at: 8)
        if let totalBytes { stmt.bindInt64(totalBytes, at: 9) } else { stmt.bindNull(at: 9) }
        stmt.bindDouble(now, at: 10)
        stmt.bindDouble(now, at: 11)
        _ = try stmt.step()
        stmt.reset()
        return DurableCreateIntent(
            operationID: operationID,
            itemID: itemID,
            targetRemoteID: candidateRemoteID,
            targetParentRemoteID: targetParentRemoteID,
            expectedLocalGeneration: expectedLocalGeneration,
            expectedSHA256: expectedSHA256,
            totalBytes: totalBytes
        )
    }

    private static func loadActiveOperation(
        conn: SQLiteConnection,
        itemID: Int64,
        operationType: String
    ) throws -> DurableCreateIntent? {
        let query = try conn.cachedStatement("""
        SELECT operation_id, root_id, target_remote_id, target_parent_remote_id,
               expected_local_generation, expected_sha256, total_bytes
        FROM operations
        WHERE item_id = ? AND operation_type = ? AND state IN (\(recoverableStates))
        ORDER BY created_at DESC
        LIMIT 1;
        """)
        query.bindInt64(itemID, at: 1)
        query.bindText(operationType, at: 2)
        defer { query.reset() }
        guard try query.step(),
              let operationID = query.columnText(at: 0),
              query.columnInt64(at: 1) != nil,
              let targetRemoteID = query.columnText(at: 2),
              let targetParentRemoteID = query.columnText(at: 3) else {
            return nil
        }
        return DurableCreateIntent(
            operationID: operationID,
            itemID: itemID,
            targetRemoteID: targetRemoteID,
            targetParentRemoteID: targetParentRemoteID,
            expectedLocalGeneration: query.columnInt64(at: 4) ?? 0,
            expectedSHA256: query.columnText(at: 5),
            totalBytes: query.columnInt64(at: 6)
        )
    }
}
