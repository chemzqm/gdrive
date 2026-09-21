import Foundation

struct RemotePathReceiptExpectation: Sendable {
    let itemID: Int64
    let parentItemID: Int64
    let name: String
}

enum RemotePathReceiptPayload: Sendable {
    case file
    case directory(device: Int64, inode: Int64)
}

enum LocalPathOperation: Sendable, Equatable {
    case move(source: URL, destination: URL, device: Int64?, inode: Int64?)
    case createDirectory(URL)

    private static func samePath(_ lhs: URL, _ rhs: URL) -> Bool {
        lhs.standardizedFileURL.path == rhs.standardizedFileURL.path
    }

    static func == (lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case let (.move(lhsSource, lhsDestination, lhsDevice, lhsInode),
                  .move(rhsSource, rhsDestination, rhsDevice, rhsInode)):
            return samePath(lhsSource, rhsSource)
                && samePath(lhsDestination, rhsDestination)
                && lhsDevice == rhsDevice
                && lhsInode == rhsInode
        case let (.createDirectory(lhsURL), .createDirectory(rhsURL)):
            return samePath(lhsURL, rhsURL)
        default:
            return false
        }
    }

    func execute() throws -> Bool {
        switch self {
        case .move(let source, let destination, let device, let inode):
            if FileManager.default.fileExists(atPath: source.path) {
                try FileManager.default.createDirectory(
                    at: destination.deletingLastPathComponent(),
                    withIntermediateDirectories: true)
                try FileManager.default.moveItem(at: source, to: destination)
            } else if FileManager.default.fileExists(atPath: destination.path) {
                // A previous process may have moved it before its receipt committed.
                let attrs = try FileManager.default.attributesOfItem(atPath: destination.path)
                guard (attrs[.systemNumber] as? NSNumber)?.int64Value == device,
                      (attrs[.systemFileNumber] as? NSNumber)?.int64Value == inode else {
                    return false
                }
            }
            return true
        case .createDirectory(let destination):
            try FileManager.default.createDirectory(
                at: destination, withIntermediateDirectories: true)
            return true
        }
    }
}

extension SyncEngine {
    func executeRemotePathOperation(
        remoteID: String?,
        oldParentRemoteID: String?,
        newParentRemoteID: String?,
        expectation: RemotePathReceiptExpectation,
        newParentItemID: Int64,
        newName: String,
        payload: RemotePathReceiptPayload,
        now: Double
    ) async throws -> SyncReceiptResult {
        if let remoteID {
            let moving = expectation.parentItemID != newParentItemID
            _ = try await client.updateMetadata(
                remoteId: remoteID,
                newName: newName,
                addParentId: moving ? newParentRemoteID : nil,
                removeParentId: moving ? oldParentRemoteID : nil
            )
        }
        return try await store.commitRemotePathReceipt(
            expectation: expectation,
            newParentItemID: newParentItemID,
            newName: newName,
            payload: payload,
            now: now
        )
    }
}

extension StateStore {
    func commitRemotePathReceipt(
        expectation: RemotePathReceiptExpectation,
        newParentItemID: Int64,
        newName: String,
        payload: RemotePathReceiptPayload,
        now: Double = Date().timeIntervalSince1970
    ) async throws -> SyncReceiptResult {
        try await write { conn in
            let statement: SQLiteStatement
            switch payload {
            case .file:
                statement = try conn.cachedStatement(
                    """
                    UPDATE items SET name = ?, parent_id = ?, updated_at = ?
                    WHERE item_id = ? AND parent_id = ? AND name = ?;
                    """)
                statement.bindText(newName, at: 1)
                statement.bindInt64(newParentItemID, at: 2)
                statement.bindDouble(now, at: 3)
                statement.bindInt64(expectation.itemID, at: 4)
                statement.bindInt64(expectation.parentItemID, at: 5)
                statement.bindText(expectation.name, at: 6)
            case .directory(let device, let inode):
                statement = try conn.cachedStatement(
                    """
                    UPDATE items SET
                        name = ?, parent_id = ?,
                        local_status = 'present', local_device = ?, local_inode = ?,
                        phase = CASE WHEN remote_status = 'trashed' THEN phase ELSE 'committed' END,
                        dirty_generation = CASE WHEN remote_status = 'trashed' THEN dirty_generation ELSE 0 END,
                        updated_at = ?
                    WHERE item_id = ? AND parent_id = ? AND name = ?;
                    """)
                statement.bindText(newName, at: 1)
                statement.bindInt64(newParentItemID, at: 2)
                statement.bindInt64(device, at: 3)
                statement.bindInt64(inode, at: 4)
                statement.bindDouble(now, at: 5)
                statement.bindInt64(expectation.itemID, at: 6)
                statement.bindInt64(expectation.parentItemID, at: 7)
                statement.bindText(expectation.name, at: 8)
            }
            _ = try statement.step()
            statement.reset()
            return conn.changes == 1 ? .applied : .stale
        }
    }
}
