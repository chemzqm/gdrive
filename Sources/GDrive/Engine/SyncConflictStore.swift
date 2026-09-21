import Foundation

enum SyncConflictStore {
    struct Record: Sendable {
        let conflict: SyncConflict
        let rootID: Int64
        let itemID: Int64
    }

    static func directory(base: URL, remoteRootID: String) throws -> URL {
        guard base.isFileURL, !remoteRootID.isEmpty,
              remoteRootID != ".", remoteRootID != "..",
              !remoteRootID.contains("/"), !remoteRootID.contains("\0") else {
            throw SyncEngineError.general("Invalid conflict directory or remote root ID")
        }
        return base.appendingPathComponent(remoteRootID, isDirectory: true)
    }

    static func list(store: StateStore, localPath: String? = nil, rootID: Int64? = nil) async throws -> [SyncConflict] {
        try await store.read { conn in
            let sql: String
            if localPath != nil {
                sql = """
                SELECT c.conflict_id, c.remote_file_id, c.relative_path, c.local_path,
                    c.conflict_path, c.remote_sha256, c.remote_size, c.remote_version, c.remote_status
                FROM sync_conflicts c JOIN roots r ON r.root_id = c.root_id
                WHERE r.local_root_path = ? AND r.is_active = 1 ORDER BY c.relative_path;
                """
            } else {
                sql = """
                SELECT conflict_id, remote_file_id, relative_path, local_path,
                    conflict_path, remote_sha256, remote_size, remote_version, remote_status
                FROM sync_conflicts WHERE root_id = ? ORDER BY relative_path;
                """
            }
            let query = try conn.cachedStatement(sql)
            defer { query.reset() }
            if let localPath { query.bindText(localPath, at: 1) } else { query.bindInt64(rootID ?? -1, at: 1) }
            var result: [SyncConflict] = []
            while try query.step() {
                guard let id = query.columnText(at: 0), let remoteID = query.columnText(at: 1),
                      let relative = query.columnText(at: 2), let local = query.columnText(at: 3),
                      let sha = query.columnText(at: 5),
                      let size = query.columnInt64(at: 6),
                      let statusText = query.columnText(at: 8),
                      let status = SyncConflict.RemoteStatus(rawValue: statusText) else { continue }
                result.append(SyncConflict(
                    id: id, remoteFileId: remoteID, relativePath: relative,
                    localPath: local, conflictPath: query.columnText(at: 4), remoteSHA256: sha,
                    remoteSize: size, remoteVersion: query.columnInt64(at: 7), remoteStatus: status))
            }
            return result
        }
    }

    static func record(store: StateStore, id: String) async throws -> Record? {
        try await store.read { conn in
            let query = try conn.cachedStatement("""
            SELECT conflict_id, remote_file_id, relative_path, local_path, conflict_path,
                remote_sha256, remote_size, remote_version, remote_status, root_id, item_id
            FROM sync_conflicts WHERE conflict_id = ?;
            """)
            defer { query.reset() }
            query.bindText(id, at: 1)
            guard try query.step(), let remoteID = query.columnText(at: 1),
                  let relative = query.columnText(at: 2), let local = query.columnText(at: 3),
                  let sha = query.columnText(at: 5),
                  let size = query.columnInt64(at: 6), let statusText = query.columnText(at: 8),
                  let status = SyncConflict.RemoteStatus(rawValue: statusText),
                  let rootID = query.columnInt64(at: 9), let itemID = query.columnInt64(at: 10) else { return nil }
            return Record(conflict: SyncConflict(
                id: id, remoteFileId: remoteID, relativePath: relative, localPath: local,
                conflictPath: query.columnText(at: 4), remoteSHA256: sha, remoteSize: size,
                remoteVersion: query.columnInt64(at: 7), remoteStatus: status),
                rootID: rootID, itemID: itemID)
        }
    }

    static func commitBootstrap(
        store: StateStore, rootID: Int64, parentItemID: Int64, file: DriveFile,
        relativePath: String, localURL: URL, conflictURL: URL
    ) async throws {
        guard let remoteSHA = file.sha256Checksum?.lowercased(), let remoteSize = file.sizeBytes,
              let localVersion = try LocalFileVersion.read(at: localURL) else {
            throw SyncEngineError.general("Incomplete sync conflict evidence: \(relativePath)")
        }
        let localSHA = try SyncEngine.computeFileSha256(at: localURL).sha256Hex
        let conflictID = "initial-\(rootID)-\(file.id)"
        try await store.batchWrite { conn in
            let now = Date().timeIntervalSince1970
            let item = try conn.cachedStatement("""
            INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, local_mtime, local_size, local_sha256,
                base_sha256, base_size, remote_sha256, remote_size, remote_version,
                remote_parent_file_id, remote_name, local_generation, remote_generation,
                local_status, remote_status, phase, dirty_generation, created_at, updated_at)
            VALUES (?, ?, ?, 'file', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1,
                'present', 'present', 'blocked', 0, ?, ?)
            ON CONFLICT(root_id, parent_id, name) WHERE parent_id IS NOT NULL
            DO UPDATE SET remote_file_id = excluded.remote_file_id,
                local_device = excluded.local_device, local_inode = excluded.local_inode,
                local_mtime = excluded.local_mtime, local_size = excluded.local_size,
                local_sha256 = excluded.local_sha256, base_sha256 = excluded.base_sha256,
                base_size = excluded.base_size, remote_sha256 = excluded.remote_sha256,
                remote_size = excluded.remote_size, remote_version = excluded.remote_version,
                remote_parent_file_id = excluded.remote_parent_file_id, remote_name = excluded.remote_name,
                local_status = 'present', remote_status = 'present', phase = 'blocked',
                dirty_generation = 0, updated_at = excluded.updated_at
            WHERE items.remote_file_id IS NULL OR items.remote_file_id = excluded.remote_file_id
            RETURNING item_id;
            """)
            item.bindInt64(rootID, at: 1)
            item.bindInt64(parentItemID, at: 2)
            item.bindText(file.name, at: 3)
            item.bindText(file.id, at: 4)
            item.bindInt64(localVersion.device, at: 5)
            item.bindInt64(localVersion.inode, at: 6)
            item.bindInt64(localVersion.mtime, at: 7)
            item.bindInt64(localVersion.size, at: 8)
            item.bindText(localSHA, at: 9)
            item.bindText(remoteSHA, at: 10)
            item.bindInt64(remoteSize, at: 11)
            item.bindText(remoteSHA, at: 12)
            item.bindInt64(remoteSize, at: 13)
            item.bindInt64(file.versionNumber, at: 14)
            item.bindText(file.parents?.first, at: 15)
            item.bindText(file.name, at: 16)
            item.bindDouble(now, at: 17)
            item.bindDouble(now, at: 18)
            guard try item.step(), let itemID = item.columnInt64(at: 0) else {
                item.reset()
                throw SyncEngineError.general("Unable to reserve sync conflict: \(relativePath)")
            }
            item.reset()
            let conflict = try conn.cachedStatement("""
            INSERT INTO sync_conflicts(conflict_id, root_id, item_id, remote_file_id,
                relative_path, local_path, conflict_path, remote_sha256, remote_size,
                remote_version, remote_status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'present', ?, ?)
            ON CONFLICT(root_id, remote_file_id) DO UPDATE SET item_id = excluded.item_id,
                relative_path = excluded.relative_path, local_path = excluded.local_path,
                conflict_path = excluded.conflict_path, remote_sha256 = excluded.remote_sha256,
                remote_size = excluded.remote_size, remote_version = excluded.remote_version,
                remote_status = 'present', updated_at = excluded.updated_at;
            """)
            conflict.bindText(conflictID, at: 1)
            conflict.bindInt64(rootID, at: 2)
            conflict.bindInt64(itemID, at: 3)
            conflict.bindText(file.id, at: 4)
            conflict.bindText(relativePath, at: 5)
            conflict.bindText(localURL.path, at: 6)
            conflict.bindText(conflictURL.path, at: 7)
            conflict.bindText(remoteSHA, at: 8)
            conflict.bindInt64(remoteSize, at: 9)
            conflict.bindInt64(file.versionNumber, at: 10)
            conflict.bindDouble(now, at: 11)
            conflict.bindDouble(now, at: 12)
            _ = try conflict.step()
            conflict.reset()
            let inbox = try conn.cachedStatement("DELETE FROM remote_change_inbox WHERE root_id = ? AND remote_id = ?;")
            inbox.bindInt64(rootID, at: 1)
            inbox.bindText(file.id, at: 2)
            _ = try inbox.step()
            inbox.reset()
        }
    }

    static func commitIncremental(
        store: StateStore, rootID: Int64, itemID: Int64, remoteFileID: String,
        relativePath: String, localURL: URL, conflictURL: URL?,
        remoteSHA: String, remoteSize: Int64, remoteStatus: SyncConflict.RemoteStatus = .present,
        expectedLocalGeneration: Int64, expectedRemoteGeneration: Int64,
        expectedDirtyGeneration: Int64
    ) async throws {
        let conflictID = "sync-\(rootID)-\(itemID)"
        try await store.batchWrite { conn in
            let now = Date().timeIntervalSince1970
            let insert = try conn.cachedStatement("""
            INSERT INTO sync_conflicts(conflict_id, root_id, item_id, remote_file_id,
                relative_path, local_path, conflict_path, remote_sha256, remote_size,
                remote_version, remote_status, created_at, updated_at)
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, remote_version, ?, ?, ?
            FROM items WHERE item_id = ? AND root_id = ? AND remote_file_id = ?
                AND local_generation = ? AND remote_generation = ? AND dirty_generation = ?
            ON CONFLICT(root_id, remote_file_id) DO UPDATE SET
                relative_path = excluded.relative_path, local_path = excluded.local_path,
                conflict_path = excluded.conflict_path, remote_sha256 = excluded.remote_sha256,
                remote_size = excluded.remote_size, remote_version = excluded.remote_version,
                remote_status = excluded.remote_status, updated_at = excluded.updated_at;
            """)
            insert.bindText(conflictID, at: 1)
            insert.bindInt64(rootID, at: 2)
            insert.bindInt64(itemID, at: 3)
            insert.bindText(remoteFileID, at: 4)
            insert.bindText(relativePath, at: 5)
            insert.bindText(localURL.path, at: 6)
            insert.bindText(conflictURL?.path, at: 7)
            insert.bindText(remoteSHA, at: 8)
            insert.bindInt64(remoteSize, at: 9)
            insert.bindText(remoteStatus.rawValue, at: 10)
            insert.bindDouble(now, at: 11)
            insert.bindDouble(now, at: 12)
            insert.bindInt64(itemID, at: 13)
            insert.bindInt64(rootID, at: 14)
            insert.bindText(remoteFileID, at: 15)
            insert.bindInt64(expectedLocalGeneration, at: 16)
            insert.bindInt64(expectedRemoteGeneration, at: 17)
            insert.bindInt64(expectedDirtyGeneration, at: 18)
            _ = try insert.step()
            insert.reset()
            guard conn.changes == 1 else {
                throw SyncEngineError.general("The incremental conflict plan is stale: \(relativePath)")
            }
            let block = try conn.cachedStatement("""
            UPDATE items SET phase = 'blocked', dirty_generation = 0, updated_at = ?
            WHERE item_id = ? AND root_id = ? AND remote_file_id = ?
                AND local_generation = ? AND remote_generation = ? AND dirty_generation = ?;
            """)
            block.bindDouble(now, at: 1)
            block.bindInt64(itemID, at: 2)
            block.bindInt64(rootID, at: 3)
            block.bindText(remoteFileID, at: 4)
            block.bindInt64(expectedLocalGeneration, at: 5)
            block.bindInt64(expectedRemoteGeneration, at: 6)
            block.bindInt64(expectedDirtyGeneration, at: 7)
            _ = try block.step()
            block.reset()
            guard conn.changes == 1 else {
                throw SyncEngineError.general("The incremental conflict plan is stale: \(relativePath)")
            }
        }
    }
}

extension SyncEngine {
    func refreshSyncConflicts(rootID: Int64) async throws {
        struct Pending: Sendable {
            let conflict: SyncConflict
            let change: DriveChange
            let itemID: Int64
        }
        let remoteRootID: String = try await store.read { conn in
            let query = try conn.cachedStatement("SELECT remote_root_id FROM roots WHERE root_id = ?;")
            defer { query.reset() }
            query.bindInt64(rootID, at: 1)
            guard try query.step(), let id = query.columnText(at: 0) else {
                throw SyncEngineError.general("Sync conflict root is unavailable: \(rootID)")
            }
            return id
        }
        let pending: [Pending] = try await store.read { conn in
            let query = try conn.cachedStatement("""
            SELECT c.conflict_id, c.remote_file_id, c.relative_path, c.local_path,
                c.conflict_path, c.remote_sha256, c.remote_size, c.remote_version,
                c.remote_status, c.item_id, i.payload
            FROM sync_conflicts c JOIN remote_change_inbox i
                ON i.root_id = c.root_id AND i.remote_id = c.remote_file_id
            WHERE c.root_id = ? ORDER BY c.relative_path;
            """)
            defer { query.reset() }
            query.bindInt64(rootID, at: 1)
            var result: [Pending] = []
            while try query.step() {
                guard let id = query.columnText(at: 0), let remoteID = query.columnText(at: 1),
                      let relative = query.columnText(at: 2), let local = query.columnText(at: 3),
                      let sha = query.columnText(at: 5),
                      let size = query.columnInt64(at: 6), let statusText = query.columnText(at: 8),
                      let status = SyncConflict.RemoteStatus(rawValue: statusText),
                      let itemID = query.columnInt64(at: 9), let payload = query.columnText(at: 10) else { continue }
                let conflict = SyncConflict(
                    id: id, remoteFileId: remoteID, relativePath: relative,
                    localPath: local, conflictPath: query.columnText(at: 4), remoteSHA256: sha,
                    remoteSize: size, remoteVersion: query.columnInt64(at: 7), remoteStatus: status)
                result.append(Pending(conflict: conflict,
                    change: try JSONDecoder().decode(DriveChange.self, from: Data(payload.utf8)),
                    itemID: itemID))
            }
            return result
        }
        for entry in pending {
            do {
                if entry.change.removed == true || entry.change.file?.trashed == true {
                    let status = entry.change.file?.trashed == true ? "trashed" : "removed"
                    let itemStatus = status == "trashed" ? "trashed" : "absent"
                    try await store.batchWrite { conn in
                        let update = try conn.cachedStatement("""
                        UPDATE sync_conflicts SET remote_status = ?, updated_at = ?
                        WHERE conflict_id = ?;
                        """)
                        update.bindText(status, at: 1)
                        update.bindDouble(Date().timeIntervalSince1970, at: 2)
                        update.bindText(entry.conflict.id, at: 3)
                        _ = try update.step()
                        update.reset()
                        let item = try conn.cachedStatement("""
                        UPDATE items SET remote_status = ?, phase = 'blocked', dirty_generation = 0,
                            updated_at = ? WHERE item_id = ?;
                        """)
                        item.bindText(itemStatus, at: 1)
                        item.bindDouble(Date().timeIntervalSince1970, at: 2)
                        item.bindInt64(entry.itemID, at: 3)
                        _ = try item.step()
                        item.reset()
                        let remove = try conn.cachedStatement(
                            "DELETE FROM remote_change_inbox WHERE root_id = ? AND remote_id = ?;")
                        remove.bindInt64(rootID, at: 1)
                        remove.bindText(entry.conflict.remoteFileId, at: 2)
                        _ = try remove.step()
                        remove.reset()
                    }
                    continue
                }
                guard let file = entry.change.file, !file.isDirectory,
                      let sha = file.sha256Checksum?.lowercased(), let size = file.sizeBytes else {
                    throw SyncEngineError.general(
                        "Incomplete remote conflict update: \(entry.conflict.remoteFileId)")
                }
                let conflictURL: URL
                if let path = entry.conflict.conflictPath {
                    conflictURL = URL(fileURLWithPath: path)
                } else {
                    conflictURL = try SyncConflictStore.directory(
                        base: conflictDirectory, remoteRootID: remoteRootID)
                        .appendingPathComponent(entry.conflict.relativePath)
                }
                try FileManager.default.createDirectory(
                    at: conflictURL.deletingLastPathComponent(), withIntermediateDirectories: true)
                let alreadyCurrent = entry.conflict.remoteSHA256.caseInsensitiveCompare(sha) == .orderedSame
                    && entry.conflict.remoteSize == size
                    && (try? Self.computeFileSha256(at: conflictURL).sha256Hex) == sha
                if !alreadyCurrent {
                    let expected = try LocalFileVersion.read(at: conflictURL)
                    _ = try await client.downloadFileSafely(
                        remoteId: file.id, destinationURL: conflictURL, expectedSha256: sha,
                        expectedDestination: expected,
                        temporaryDirectory: conflictURL.deletingLastPathComponent())
                }
                try await store.batchWrite { conn in
                    let now = Date().timeIntervalSince1970
                    let update = try conn.cachedStatement("""
                    UPDATE sync_conflicts SET remote_sha256 = ?, remote_size = ?,
                        remote_version = ?, remote_status = 'present', conflict_path = ?, updated_at = ?
                    WHERE conflict_id = ?;
                    """)
                    update.bindText(sha, at: 1)
                    update.bindInt64(size, at: 2)
                    update.bindInt64(file.versionNumber, at: 3)
                    update.bindText(conflictURL.path, at: 4)
                    update.bindDouble(now, at: 5)
                    update.bindText(entry.conflict.id, at: 6)
                    _ = try update.step()
                    update.reset()
                    let item = try conn.cachedStatement("""
                    UPDATE items SET remote_sha256 = ?, remote_size = ?,
                        remote_version = ?, remote_status = 'present',
                        phase = 'blocked', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                    """)
                    item.bindText(sha, at: 1)
                    item.bindInt64(size, at: 2)
                    item.bindInt64(file.versionNumber, at: 3)
                    item.bindDouble(now, at: 4)
                    item.bindInt64(entry.itemID, at: 5)
                    _ = try item.step()
                    item.reset()
                    let remove = try conn.cachedStatement(
                        "DELETE FROM remote_change_inbox WHERE root_id = ? AND remote_id = ?;")
                    remove.bindInt64(rootID, at: 1)
                    remove.bindText(file.id, at: 2)
                    _ = try remove.step()
                    remove.reset()
                }
            } catch {
                if DatabaseFailure.isSQLite(error) { throw error }
                logger.error("Failed to refresh sync conflict [\(entry.conflict.relativePath)]: \(error)")
            }
        }
    }

    public func listConflicts(localPath: String) async throws -> [SyncConflict] {
        try await SyncConflictStore.list(store: store, localPath: Self.normalizedPath(localPath))
    }

    public func resolveConflict(id: String, resolution: SyncConflictResolution) async throws {
        guard let initial = try await SyncConflictStore.record(store: store, id: id) else {
            throw SyncEngineError.general("Sync conflict was not found: \(id)")
        }
        let localRoot = try await store.read { conn -> String in
            let query = try conn.cachedStatement("SELECT local_root_path FROM roots WHERE root_id = ? AND is_active = 1;")
            defer { query.reset() }
            query.bindInt64(initial.rootID, at: 1)
            guard try query.step(), let path = query.columnText(at: 0) else {
                throw SyncEngineError.general("Sync conflict root is unavailable: \(id)")
            }
            return path
        }
        try await withRootSyncLock(localPath: localRoot) {
            try await self.resolveInitialConflict(initial, resolution: resolution)
        }
    }

    private func commitConflictDeletion(
        _ record: SyncConflictStore.Record, storedURL: URL?
    ) async throws {
        try await store.batchWrite { conn in
            let item = try conn.cachedStatement("DELETE FROM items WHERE item_id = ?;")
            item.bindInt64(record.itemID, at: 1)
            _ = try item.step()
            item.reset()
        }
        if let storedURL { try? FileManager.default.removeItem(at: storedURL) }
    }

    private func resolveRemoteDeletionIfNeeded(
        _ record: SyncConflictStore.Record, localURL: URL, storedURL: URL?
    ) async throws -> Bool {
        guard record.conflict.remoteStatus != .present else { return false }
        if FileManager.default.fileExists(atPath: localURL.path) {
            var trashURL: NSURL?
            try FileManager.default.trashItem(at: localURL, resultingItemURL: &trashURL)
        }
        try await commitConflictDeletion(record, storedURL: storedURL)
        return true
    }

    private func resolveLocalDeletion(
        _ record: SyncConflictStore.Record, storedURL: URL?
    ) async throws {
        if record.conflict.remoteStatus == .present {
            try await client.trash(remoteId: record.conflict.remoteFileId)
        }
        try await commitConflictDeletion(record, storedURL: storedURL)
    }

    private func resolveRemoteConflict(
        _ record: SyncConflictStore.Record, localURL: URL, storedURL: URL?
    ) async throws {
        let conflict = record.conflict
        if try await resolveRemoteDeletionIfNeeded(
            record, localURL: localURL, storedURL: storedURL) { return }
        guard let storedURL else {
            throw SyncEngineError.general(
                "The remote conflict copy is unavailable: \(conflict.relativePath)")
        }
        let digest = try Self.computeFileSha256(at: storedURL)
        guard digest.sha256Hex.caseInsensitiveCompare(conflict.remoteSHA256) == .orderedSame,
              digest.fileSize == conflict.remoteSize else {
            throw SyncEngineError.general(
                "The stored remote conflict file changed: \(storedURL.path)")
        }
        let expected = try LocalFileVersion.read(at: localURL)
        let result = try LocalFilePublication.publish(
            storedURL, to: localURL, expected: expected,
            expectedSHA256: conflict.remoteSHA256)
        guard case .published(let published) = result else {
            throw SyncEngineError.localFileModified(path: localURL.path)
        }
        try await commitRemoteConflictResolution(
            record, published: published,
            remotePresent: conflict.remoteStatus == .present)
        try FileManager.default.removeItem(at: storedURL)
    }

    private func commitRemoteConflictResolution(
        _ record: SyncConflictStore.Record, published: LocalFileVersion, remotePresent: Bool
    ) async throws {
        let conflict = record.conflict
        try await store.batchWrite { conn in
            let update = try conn.cachedStatement("""
            UPDATE items SET local_device = ?, local_inode = ?, local_mtime = ?, local_size = ?,
                local_sha256 = ?, base_sha256 = ?, base_size = ?, local_status = 'present',
                phase = ?, dirty_generation = ?, updated_at = ? WHERE item_id = ?;
            """)
            update.bindInt64(published.device, at: 1)
            update.bindInt64(published.inode, at: 2)
            update.bindInt64(published.mtime, at: 3)
            update.bindInt64(published.size, at: 4)
            update.bindText(conflict.remoteSHA256, at: 5)
            update.bindText(remotePresent ? conflict.remoteSHA256 : nil, at: 6)
            if remotePresent { update.bindInt64(conflict.remoteSize, at: 7) }
            update.bindText(remotePresent ? "committed" : "ready", at: 8)
            update.bindInt64(remotePresent ? 0 : 1, at: 9)
            update.bindDouble(Date().timeIntervalSince1970, at: 10)
            update.bindInt64(record.itemID, at: 11)
            _ = try update.step()
            update.reset()
            let remove = try conn.cachedStatement(
                "DELETE FROM sync_conflicts WHERE conflict_id = ?;")
            remove.bindText(conflict.id, at: 1)
            _ = try remove.step()
            remove.reset()
        }
    }

    private func resolveInitialConflict(_ record: SyncConflictStore.Record,
                                        resolution: SyncConflictResolution) async throws {
        let conflict = record.conflict
        let localURL = URL(fileURLWithPath: conflict.localPath)
        let storedURL = conflict.conflictPath.map(URL.init(fileURLWithPath:))
        switch resolution {
        case .remote:
            try await resolveRemoteConflict(
                record, localURL: localURL, storedURL: storedURL)
        case .local:
            guard let version = try LocalFileVersion.read(at: localURL) else {
                try await resolveLocalDeletion(record, storedURL: storedURL)
                return
            }
            // Existing remote bodies cannot be overwritten safely yet. Keep the conflict evidence
            // intact so this path remains gated and can be resolved after safe overwrite is available.
            guard conflict.remoteStatus != .present else {
                return
            }
            try await client.untrash(remoteId: conflict.remoteFileId)
            let digest = try Self.computeFileSha256(at: localURL)
            try await store.batchWrite { conn in
                let update = try conn.cachedStatement("""
                UPDATE items SET local_device = ?, local_inode = ?, local_mtime = ?, local_size = ?,
                    local_sha256 = ?, base_sha256 = ?, base_size = ?, local_status = 'present',
                    remote_status = 'present', local_generation = local_generation + 1,
                    phase = 'ready', dirty_generation = dirty_generation + 1, updated_at = ? WHERE item_id = ?;
                """)
                update.bindInt64(version.device, at: 1)
                update.bindInt64(version.inode, at: 2)
                update.bindInt64(version.mtime, at: 3)
                update.bindInt64(version.size, at: 4)
                update.bindText(digest.sha256Hex, at: 5)
                update.bindText(conflict.remoteSHA256, at: 6)
                update.bindInt64(conflict.remoteSize, at: 7)
                update.bindDouble(Date().timeIntervalSince1970, at: 8)
                update.bindInt64(record.itemID, at: 9)
                _ = try update.step()
                update.reset()
                let remove = try conn.cachedStatement("DELETE FROM sync_conflicts WHERE conflict_id = ?;")
                remove.bindText(conflict.id, at: 1)
                _ = try remove.step()
                remove.reset()
            }
            if let storedURL { try? FileManager.default.removeItem(at: storedURL) }
        }
    }
}
