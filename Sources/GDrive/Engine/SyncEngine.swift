import Darwin
import Foundation
import CommonCrypto
import Logging
import DirectoryScanner
import os

// Core synchronization logic. Public entry points and root locking live in SyncEngineAPI.swift.
extension SyncEngine {
    private struct ExistingSyncRoot: Sendable {
        let rootId: Int64
        let rootItemId: Int64
        let bootstrapState: String
        let initialDir: String
    }

    // MARK: - Unified synchronization portal (Automatic status detection and direction diversion)

    func syncUnlocked(
        localPath: String,
        remoteFolderId: String,
        concurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?
    ) async throws -> SyncStats {
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath

        // 1. Check SQLite Whether there is already an active synchronization root
        let existingRoot: ExistingSyncRoot? = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT roots.root_id, items.item_id, roots.bootstrap_state, roots.initial_sync_direction
            FROM roots
            JOIN items ON items.root_id = roots.root_id
                AND items.parent_id IS NULL
            WHERE roots.local_root_path = ? AND roots.remote_root_id = ? AND roots.is_active = 1;
            """)
            stmt.bindText(resolvedLocalPath, at: 1)
            stmt.bindText(remoteFolderId, at: 2)
            defer { stmt.reset() }
            if try stmt.step() {
                return ExistingSyncRoot(
                    rootId: stmt.columnInt64(at: 0) ?? 0,
                    rootItemId: stmt.columnInt64(at: 1) ?? 0,
                    bootstrapState: stmt.columnText(at: 2) ?? "freshCreated",
                    initialDir: stmt.columnText(at: 3) ?? "localToRemoteEmpty"
                )
            }
            return nil
        }

        if let existing = existingRoot {
            if existing.bootstrapState == "existingKnown" {
                logger.info("[Sync] Found a shared baseline; starting incremental bidirectional sync: \(resolvedLocalPath) <-> \(remoteFolderId)")
                return try await syncIncrementalUnlocked(
                    rootId: existing.rootId,
                    rootItemId: existing.rootItemId,
                    localPath: resolvedLocalPath,
                    remoteRootId: remoteFolderId,
                    maxConcurrency: concurrency,
                    onProgress: onProgress
                )
            } else {
                logger.info("[Sync] Found an unfinished initialization baseline (bootstrapState: \(existing.bootstrapState)); resuming initialization...")
                if existing.initialDir == "remoteToLocalEmpty" {
                    return try await initializeRemoteToLocalEmpty(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxDownloadConcurrency: concurrency, onProgress: onProgress, initialCursor: nil)
                } else {
                    return try await syncLocalToRemoteEmptyUnlocked(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxUploadConcurrency: concurrency, onProgress: onProgress)
                }
            }
        }

        // 2. First synchronization: automatically detect the true status of local and remote directories
        logger.info("[Sync] No baseline exists; checking local and remote directory state...")

        // Detect remote directories: verify existence, whether it is a directory, and whether it contains non-recycle bin subkeys
        let remoteFile = try await client.getFile(remoteId: remoteFolderId)
        guard remoteFile.trashed != true else {
            throw SyncEngineError.remoteRootLost(remoteId: remoteFolderId, reason: "trashed")
        }
        guard remoteFile.isDirectory else {
            throw SyncEngineError.general(
                "The remote target is not a valid directory: \(remoteFolderId)")
        }
        // Probe only the top level; hidden entries are included, only .git directories are pruned.
        let isLocalEmpty = try Self.isLocalRootEmpty(resolvedLocalPath)
        // Capture before listing so a concurrent remote creation cannot fall before the cursor.
        let emptyRootCursor = isLocalEmpty ? try await client.getStartPageToken() : nil
        let remoteChildren = try await client.listChildren(parentId: remoteFolderId)
        let isRemoteEmpty = remoteChildren.isEmpty

        // 3. Safe diversion based on detection results
        if !isLocalEmpty && isRemoteEmpty {
            logger.info("[Sync] Local content and an empty remote directory detected; starting localToRemoteEmpty initialization")
            return try await syncLocalToRemoteEmptyUnlocked(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxUploadConcurrency: concurrency, onProgress: onProgress)
        } else if isLocalEmpty && !isRemoteEmpty {
            logger.info("[Sync] Remote content and an empty local directory detected; starting remoteToLocalEmpty initialization")
            return try await initializeRemoteToLocalEmpty(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxDownloadConcurrency: concurrency, onProgress: onProgress, initialCursor: emptyRootCursor)
        } else if isLocalEmpty && isRemoteEmpty {
            logger.info("[Sync] Both directories are empty; creating an empty baseline")
            try FileManager.default.createDirectory(atPath: resolvedLocalPath, withIntermediateDirectories: true)
            let rootURL = URL(fileURLWithPath: resolvedLocalPath)
            guard let rootIdentity = try LocalDirectoryIdentity.read(at: rootURL) else {
                throw SyncEngineError.localRootNotFound(path: resolvedLocalPath)
            }
            let rootName = URL(fileURLWithPath: resolvedLocalPath).lastPathComponent
            let now = Date().timeIntervalSince1970
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                INSERT INTO roots (
                    account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
                ) VALUES ('default', ?, ?, ?, ?, 'localToRemoteEmpty', 'existingKnown', ?, ?);
                """)
                stmt.bindText(resolvedLocalPath, at: 1)
                stmt.bindInt64(rootIdentity.device, at: 2)
                stmt.bindInt64(rootIdentity.inode, at: 3)
                stmt.bindText(remoteFolderId, at: 4)
                stmt.bindDouble(now, at: 5)
                stmt.bindDouble(now, at: 6)
                _ = try stmt.step()
                stmt.reset()
                let rootID = conn.lastInsertRowId
                let item = try conn.cachedStatement("""
                INSERT INTO items(root_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status, phase, created_at, updated_at)
                VALUES (?, ?, 'directory', ?, ?, ?, 'present', 'present', 'committed', ?, ?);
                """)
                item.bindInt64(rootID, at: 1)
                item.bindText(rootName, at: 2)
                item.bindText(remoteFolderId, at: 3)
                item.bindInt64(rootIdentity.device, at: 4)
                item.bindInt64(rootIdentity.inode, at: 5)
                item.bindDouble(now, at: 6)
                item.bindDouble(now, at: 7)
                _ = try item.step()
                item.reset()
                let cursor = try conn.cachedStatement("""
                INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at)
                VALUES (?, 'default', 'drive_changes', ?, ?);
                """)
                cursor.bindInt64(rootID, at: 1)
                cursor.bindText(emptyRootCursor!, at: 2)
                cursor.bindDouble(now, at: 3)
                _ = try cursor.step()
                cursor.reset()
            }
            return SyncStats()
        } else {
            // Both ends are not empty
            throw SyncEngineError.general(
                "Initial bidirectional sync requires one side to be empty. Both the local path "
                    + "(\(resolvedLocalPath)) and remote folder (\(remoteFolderId)) contain files. "
                    + "Use an empty directory for initialization to avoid overwrites or widespread conflicts.")
        }
    }

    /// O(1) memory, no recursion or content reads; matches includeHidden + excludeDirectory(".git").
    static func isLocalRootEmpty(_ path: String) throws -> Bool {
        guard let directory = opendir(path) else {
            if errno == ENOENT { return true }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        defer { closedir(directory) }
        while true {
            errno = 0
            guard let entry = readdir(directory) else {
                if errno != 0 { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
                return true
            }
            let name = withUnsafePointer(to: &entry.pointee.d_name) {
                $0.withMemoryRebound(to: CChar.self, capacity: Int(entry.pointee.d_namlen) + 1) { String(cString: $0) }
            }
            if name == "." || name == ".." { continue }
            if name == ".git" {
                if try isDirectoryEntry(entry, named: name, in: directory) { continue }
            }
            return false
        }
    }

    private static func isDirectoryEntry(
        _ entry: UnsafeMutablePointer<dirent>, named name: String, in directory: UnsafeMutablePointer<DIR>
    ) throws -> Bool {
        var type = entry.pointee.d_type
        if type == UInt8(DT_UNKNOWN) {
            var metadata = stat()
            guard fstatat(dirfd(directory), name, &metadata, AT_SYMLINK_NOFOLLOW) == 0 else {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            if metadata.st_mode & S_IFMT == S_IFDIR { type = UInt8(DT_DIR) }
        }
        return type == UInt8(DT_DIR)
    }

}
