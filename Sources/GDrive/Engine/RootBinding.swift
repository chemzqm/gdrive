import Foundation

extension SyncEngine {
    func validateRootBinding(localPath: String, remoteRootID: String) async throws {
        try await store.read { conn in
            try Self.validateRootBinding(
                conn: conn, localPath: localPath, remoteRootID: remoteRootID)
        }
    }

    static func validateRootBinding(
        conn: SQLiteConnection,
        localPath: String,
        remoteRootID: String
    ) throws {
        let statement = try conn.cachedStatement("""
            SELECT local_root_path, remote_root_id
            FROM roots
            WHERE account_id = 'default'
                AND (local_root_path = ? OR remote_root_id = ?);
            """)
        defer { statement.reset() }
        statement.bindText(localPath, at: 1)
        statement.bindText(remoteRootID, at: 2)

        while try statement.step() {
            guard let existingLocalPath = statement.columnText(at: 0),
                  let existingRemoteRootID = statement.columnText(at: 1) else {
                throw SyncEngineError.general("Stored root binding is incomplete")
            }
            guard existingLocalPath == localPath, existingRemoteRootID == remoteRootID else {
                throw SyncEngineError.rootBindingConflict(
                    localPath: localPath,
                    remoteRootId: remoteRootID,
                    existingLocalPath: existingLocalPath,
                    existingRemoteRootId: existingRemoteRootID)
            }
        }
    }
}
