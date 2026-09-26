import Foundation

extension SyncEngine {
    func registerSyncStorageDirectories(
        rootID: Int64,
        remoteRootID: String,
        downloadDirectory: URL? = nil
    ) async throws {
        let conflict = try SyncConflictStore.directory(
            base: conflictDirectory, remoteRootID: remoteRootID)
        var paths = [RootSyncCoordinator.normalizedPath(conflict.path)]
        if let downloadDirectory {
            guard downloadDirectory.isFileURL else {
                throw SyncEngineError.general("Download temporary directory must be a local file path")
            }
            paths.append(RootSyncCoordinator.normalizedPath(downloadDirectory.path))
        }
        try await claimSyncStorageDirectories(rootID: rootID, paths: Set(paths))
    }

    func claimSyncStorageDirectories(rootID: Int64, paths: Set<String>) async throws {
        try await store.write { conn in
            let existing = try conn.prepare("""
                SELECT path FROM root_storage_directories WHERE root_id != ?;
                """)
            defer { existing.reset() }
            existing.bindInt64(rootID, at: 1)
            var otherPaths = [String]()
            while try existing.step() {
                if let path = existing.columnText(at: 0) { otherPaths.append(path) }
            }
            for path in paths where otherPaths.contains(where: {
                Self.storagePathsOverlap(path, $0)
            }) {
                throw SyncEngineError.general(
                    "Storage directory overlaps another sync root: \(path)")
            }
            let insert = try conn.prepare("""
                INSERT INTO root_storage_directories(root_id, path) VALUES (?, ?)
                ON CONFLICT(root_id, path) DO NOTHING;
                """)
            defer { insert.reset() }
            for path in paths {
                insert.bindInt64(rootID, at: 1)
                insert.bindText(path, at: 2)
                _ = try insert.step()
                insert.reset()
            }
        }
    }

    static func storagePathsOverlap(_ first: String, _ second: String) -> Bool {
        storagePathContains(first, in: second) || storagePathContains(second, in: first)
    }

    static func storagePathContains(_ path: String, in directory: String) -> Bool {
        guard path != directory else { return true }
        let prefix = directory.hasSuffix("/") ? directory : directory + "/"
        return path.hasPrefix(prefix)
    }
}
