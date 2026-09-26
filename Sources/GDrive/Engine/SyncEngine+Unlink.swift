import Darwin
import Foundation

private struct UnlinkRoot: Sendable {
    let id: Int64
    let remoteRootID: String
}

private struct UnlinkSnapshot: Sendable {
    let root: UnlinkRoot?
    let rootPaths: Set<String>
    let storagePaths: Set<String>
    let conflictPaths: Set<String>
    let otherRootStoragePaths: Set<String>
    let otherConflictPaths: Set<String>
    let localConflictPaths: Set<String>
}

extension SyncEngine {
    /// Removes the local binding and its owned temporary/conflict artifacts.
    /// It deliberately leaves ordinary local files, Drive content, credentials,
    /// and parent-removed file bytes alone.
    public func unlink(localPath: String) async throws {
        let localRootPath = Self.normalizedPath(localPath)
        let token = try await RootSyncCoordinator.shared.acquireForUnlink(
            localRootPath: localRootPath)
        let result: Result<Void, any Error>
        do {
            try await store.flush()
            let root = try await store.read { conn in
                try Self.unlinkRoot(conn: conn, localRootPath: localRootPath)
            }
            if let root {
                let conflict = try SyncConflictStore.directory(
                    base: conflictDirectory, remoteRootID: root.remoteRootID)
                let conflictPath = Self.unlinkCurrentStoragePath(
                    base: conflictDirectory, leaf: conflict.lastPathComponent)
                _ = try DownloadStaging.directory(
                    base: downloadTemporaryDirectory,
                    remoteRootID: root.remoteRootID,
                    excluding: [URL(fileURLWithPath: localRootPath)])
                let downloadPath = Self.unlinkCurrentStoragePath(
                    base: downloadTemporaryDirectory, leaf: root.remoteRootID)
                try await claimSyncStorageDirectories(
                    rootID: root.id, paths: Set([conflictPath, downloadPath]))
            }
            let snapshot = try await unlinkSnapshot(localRootPath: localRootPath)
            let deletionPaths = try unlinkDeletionPaths(snapshot: snapshot)
            try unlinkArtifacts(at: deletionPaths)
            try await deleteUnlinkedRows(snapshot: snapshot, localRootPath: localRootPath)
            result = .success(())
        } catch {
            result = .failure(error)
        }
        await RootSyncCoordinator.shared.release(token)
        try result.get()
    }

    private func unlinkSnapshot(localRootPath: String) async throws -> UnlinkSnapshot {
        try await store.read { conn in
            let root = try Self.unlinkRoot(conn: conn, localRootPath: localRootPath)
            let allRoots = try conn.cachedStatement("SELECT local_root_path FROM roots;")
            defer { allRoots.reset() }
            var rootPaths = Set<String>()
            while try allRoots.step() {
                if let path = allRoots.columnText(at: 0) {
                    rootPaths.insert(Self.normalizedPath(path))
                }
            }
            let storageQuery = try conn.cachedStatement("""
                SELECT root_id, path FROM root_storage_directories;
                """)
            defer { storageQuery.reset() }
            var storagePaths = Set<String>()
            var otherRootStoragePaths = Set<String>()
            while try storageQuery.step() {
                guard let rootID = storageQuery.columnInt64(at: 0),
                      let path = storageQuery.columnText(at: 1) else { continue }
                if rootID == root?.id {
                    storagePaths.insert(Self.unlinkLexicalPath(path))
                } else {
                    otherRootStoragePaths.insert(Self.normalizedPath(path))
                }
            }
            let conflictQuery = try conn.cachedStatement("""
                SELECT root_id, conflict_path FROM sync_conflicts
                WHERE conflict_path IS NOT NULL;
                """)
            defer { conflictQuery.reset() }
            var conflictPaths = Set<String>()
            var otherConflictPaths = Set<String>()
            while try conflictQuery.step() {
                guard let rootID = conflictQuery.columnInt64(at: 0),
                      let path = conflictQuery.columnText(at: 1) else { continue }
                if rootID == root?.id {
                    conflictPaths.insert(Self.unlinkLexicalPath(path))
                } else {
                    otherConflictPaths.insert(Self.normalizedPath(path))
                }
            }
            let localQuery = try conn.cachedStatement(
                "SELECT stored_path FROM local_conflicts;")
            defer { localQuery.reset() }
            var localConflictPaths = Set<String>()
            while try localQuery.step() {
                if let path = localQuery.columnText(at: 0) {
                    localConflictPaths.insert(Self.normalizedPath(path))
                }
            }
            return UnlinkSnapshot(
                root: root, rootPaths: rootPaths, storagePaths: storagePaths, conflictPaths: conflictPaths,
                otherRootStoragePaths: otherRootStoragePaths,
                otherConflictPaths: otherConflictPaths,
                localConflictPaths: localConflictPaths)
        }
    }

    private func unlinkDeletionPaths(snapshot: UnlinkSnapshot) throws -> [String] {
        var paths = snapshot.storagePaths
        if let root = snapshot.root {
            let configuredConflict = try SyncConflictStore.directory(
                base: conflictDirectory, remoteRootID: root.remoteRootID)
            let configuredConflictPath = Self.unlinkLexicalPath(configuredConflict.path)
            for conflictPath in snapshot.conflictPaths where !paths.contains(where: {
                Self.storagePathContains(conflictPath, in: $0)
            }) && !Self.storagePathContains(conflictPath, in: configuredConflictPath) {
                paths.insert(conflictPath)
            }
        } else {
            paths.formUnion(snapshot.conflictPaths)
        }
        guard !paths.isEmpty else { return [] }

        let protectedFiles = [auth.fileURL.path, store.path].map(Self.normalizedPath)
        let parentRemoved = conflictDirectory.deletingLastPathComponent()
            .appendingPathComponent("parent_removed", isDirectory: true).path
        let protectedPaths = snapshot.localConflictPaths
            .union(snapshot.otherRootStoragePaths)
            .union(snapshot.otherConflictPaths)
            .union([Self.normalizedPath(parentRemoved)])
        for path in paths {
            let parent = URL(fileURLWithPath: path).deletingLastPathComponent().path
            guard Self.normalizedPath(parent) == parent else {
                throw SyncEngineError.general("Unlink storage path no longer resolves to its registered location: \(path)")
            }
            for rootPath in snapshot.rootPaths where Self.storagePathsOverlap(path, rootPath) {
                throw SyncEngineError.general("Unlink storage path overlaps a sync root: \(path)")
            }
            for protected in protectedFiles where Self.storagePathsOverlap(path, protected) {
                throw SyncEngineError.general("Unlink storage path overlaps operational state: \(path)")
            }
            for protected in protectedPaths where Self.storagePathsOverlap(path, protected) {
                throw SyncEngineError.general("Unlink storage path overlaps protected artifacts: \(path)")
            }
        }
        return paths.sorted { $0.count > $1.count }
    }

    private func unlinkArtifacts(at paths: [String]) throws {
        for path in paths {
            var info = stat()
            guard lstat(path, &info) == 0 else {
                if errno == ENOENT { continue }
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            try FileManager.default.removeItem(at: URL(fileURLWithPath: path))
        }
    }

    private func deleteUnlinkedRows(
        snapshot: UnlinkSnapshot, localRootPath: String
    ) async throws {
        let prefix = localRootPath.hasSuffix("/") ? localRootPath : localRootPath + "/"
        try await store.write { conn in
            let deleteLocal = try conn.prepare("""
                DELETE FROM local_conflicts
                WHERE root_id = ? OR (original_path >= ? AND original_path < ?);
                """)
            defer { deleteLocal.reset() }
            deleteLocal.bindInt64(snapshot.root?.id, at: 1)
            deleteLocal.bindText(prefix, at: 2)
            deleteLocal.bindText(prefix + "\u{10FFFF}", at: 3)
            _ = try deleteLocal.step()
            if let root = snapshot.root {
                let rootID = root.id
                let items = try conn.prepare("""
                    WITH RECURSIVE tree(item_id, depth) AS (
                        SELECT item_id, 0 FROM items WHERE root_id = ? AND parent_id IS NULL
                        UNION ALL
                        SELECT i.item_id, tree.depth + 1 FROM items i
                        JOIN tree ON i.parent_id = tree.item_id
                    ) SELECT item_id FROM tree ORDER BY depth DESC, item_id DESC;
                    """)
                defer { items.reset() }
                items.bindInt64(rootID, at: 1)
                var itemIDs: [Int64] = []
                while try items.step() {
                    if let itemID = items.columnInt64(at: 0) { itemIDs.append(itemID) }
                }
                let deleteItem = try conn.prepare("DELETE FROM items WHERE item_id = ?;")
                defer { deleteItem.reset() }
                for itemID in itemIDs {
                    deleteItem.bindInt64(itemID, at: 1)
                    _ = try deleteItem.step()
                    deleteItem.reset()
                }
                let deleteRoot = try conn.prepare("DELETE FROM roots WHERE root_id = ?;")
                defer { deleteRoot.reset() }
                deleteRoot.bindInt64(rootID, at: 1)
                _ = try deleteRoot.step()
            }
        }
    }

    private static func unlinkLexicalPath(_ path: String) -> String {
        URL(fileURLWithPath: (path as NSString).expandingTildeInPath).standardizedFileURL.path
    }

    private static func unlinkCurrentStoragePath(base: URL, leaf: String) -> String {
        let parent = RootSyncCoordinator.normalizedPath(base.path)
        return URL(fileURLWithPath: parent).appendingPathComponent(leaf, isDirectory: true).path
    }

    private static func unlinkRoot(
        conn: SQLiteConnection, localRootPath: String
    ) throws -> UnlinkRoot? {
        let query = try conn.cachedStatement("""
            SELECT root_id, remote_root_id FROM roots
            WHERE account_id = 'default' AND local_root_path = ?;
            """)
        defer { query.reset() }
        query.bindText(localRootPath, at: 1)
        guard try query.step() else { return nil }
        guard let id = query.columnInt64(at: 0), let remoteRootID = query.columnText(at: 1) else {
            throw SyncEngineError.general("Stored root binding is incomplete")
        }
        return UnlinkRoot(id: id, remoteRootID: remoteRootID)
    }

}
