import Foundation
@testable import GDrive

func setStoredRootIdentity(store: StateStore, rootID: Int64, localURL: URL) async throws {
    guard let identity = try LocalDirectoryIdentity.read(at: localURL) else {
        throw SyncEngineError.localRootNotFound(path: localURL.path)
    }
    try await store.write { conn in
        let statement = try conn.prepare(
            "UPDATE roots SET local_root_device = ?, local_root_inode = ? WHERE root_id = ?;")
        statement.bindInt64(identity.device, at: 1)
        statement.bindInt64(identity.inode, at: 2)
        statement.bindInt64(rootID, at: 3)
        _ = try statement.step()
    }
}
