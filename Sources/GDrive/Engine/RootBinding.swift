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
            WHERE account_id = 'default';
            """)
        defer { statement.reset() }
        let normalizedLocalPath = RootSyncCoordinator.normalizedPath(localPath)

        while try statement.step() {
            guard let existingLocalPath = statement.columnText(at: 0),
                  let existingRemoteRootID = statement.columnText(at: 1) else {
                throw SyncEngineError.general("Stored root binding is incomplete")
            }
            let normalizedExistingPath = RootSyncCoordinator.normalizedPath(existingLocalPath)
            let sameBinding = normalizedExistingPath == normalizedLocalPath
                && existingRemoteRootID == remoteRootID
            let conflicts = existingRemoteRootID == remoteRootID
                || RootSyncCoordinator.overlaps(normalizedExistingPath, normalizedLocalPath)
            guard sameBinding || !conflicts else {
                throw SyncEngineError.rootBindingConflict(
                    localPath: normalizedLocalPath,
                    remoteRootId: remoteRootID,
                    existingLocalPath: existingLocalPath,
                    existingRemoteRootId: existingRemoteRootID)
            }
        }
    }
}
