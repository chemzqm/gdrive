import Darwin
import Foundation
import CommonCrypto
import Logging
import DirectoryScanner
import os

// Internal SyncEngine implementation split by synchronization phase.
extension SyncEngine {
    // MARK: - mode 2:remote directory -> Fast download of local empty directory (remoteToLocalEmpty)

    func initializeRemoteToLocalEmpty(
        localPath: String,
        remoteRootId: String,
        maxDownloadConcurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?,
        initialCursor: String?
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)

        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)

        // Verify that the remote root directory exists
        let remoteRoot = try await client.getFile(remoteId: remoteRootId)
        guard remoteRoot.isDirectory else {
            throw NSError(domain: "SyncEngine", code: 10, userInfo: [NSLocalizedDescriptionKey: "The remote target is not a valid directory: \(remoteRootId)"])
        }

        let downloadDirectory = try await downloadStagingDirectory(remoteRootID: remoteRootId, localRoot: rootURL)
        defer { cleanupDownloadStagingDirectory(downloadDirectory) }

        let rootExists = try await store.read { conn in
            let queryStatement = try conn.cachedStatement("""
                SELECT 1 FROM roots
                WHERE account_id = 'default' AND local_root_path = ? AND remote_root_id = ?
                    AND initial_sync_direction = 'remoteToLocalEmpty' AND is_active = 1;
                """)
            defer { queryStatement.reset() }
            queryStatement.bindText(resolvedLocalPath, at: 1)
            queryStatement.bindText(remoteRootId, at: 2)
            return try queryStatement.step()
        }

        // A new bootstrap requires an empty target. A retry owns the partial
        // files recorded under the existing root and resumes in place.
        var isDir: ObjCBool = false
        if FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir) {
            guard isDir.boolValue else {
                throw NSError(domain: "SyncEngine", code: 11, userInfo: [NSLocalizedDescriptionKey: "The local path already exists and is not a directory: \(resolvedLocalPath)"])
            }
            let contents = try FileManager.default.contentsOfDirectory(atPath: resolvedLocalPath)
            guard rootExists || contents.isEmpty else {
                throw NSError(domain: "SyncEngine", code: 12, userInfo: [NSLocalizedDescriptionKey: "The local target directory must be empty: \(resolvedLocalPath)"])
            }
        } else {
            try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        }

        let now = Date().timeIntervalSince1970

        // 1. Register or get Root Records and root entries
        let (rootId, rootItemId): (Int64, Int64) = try await store.write { conn in
            let rootStmt = try conn.cachedStatement("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', ?, 1, 1, ?, 'remoteToLocalEmpty', 'freshCreated', ?, ?)
            ON CONFLICT (account_id, remote_root_id) DO UPDATE SET updated_at = excluded.updated_at;
            """)
            rootStmt.bindText(resolvedLocalPath, at: 1)
            rootStmt.bindText(remoteRootId, at: 2)
            rootStmt.bindDouble(now, at: 3)
            rootStmt.bindDouble(now, at: 4)
            _ = try rootStmt.step()
            rootStmt.reset()

            let rootQuery = try conn.cachedStatement("SELECT root_id FROM roots WHERE account_id = 'default' AND remote_root_id = ?;")
            rootQuery.bindText(remoteRootId, at: 1)
            guard try rootQuery.step(), let rId = rootQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 13, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root_id"])
            }
            rootQuery.reset()

            let itemStmt = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (?, NULL, ?, 'directory', ?, ?, ?)
            ON CONFLICT (root_id) WHERE parent_id IS NULL AND is_tombstone = 0 DO UPDATE SET updated_at = excluded.updated_at;
            """)
            itemStmt.bindInt64(rId, at: 1)
            itemStmt.bindText(rootURL.lastPathComponent, at: 2)
            itemStmt.bindText(remoteRootId, at: 3)
            itemStmt.bindDouble(now, at: 4)
            itemStmt.bindDouble(now, at: 5)
            _ = try itemStmt.step()
            itemStmt.reset()

            let itemQuery = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL AND is_tombstone = 0;")
            itemQuery.bindInt64(rId, at: 1)
            guard try itemQuery.step(), let rItemId = itemQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 14, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root item_id"])
            }
            itemQuery.reset()

            return (rId, rItemId)
        }

        try await RemoteChanges.saveInitialCursor(store: store, client: client, rootID: rootId, requireExisting: rootExists, initialToken: initialCursor)

        let effectiveDownloadConcurrency = max(1, min(64, maxDownloadConcurrency))
        let downloadSemaphore = AsyncSemaphore(count: effectiveDownloadConcurrency)
        let downloadGroup = DispatchGroup()
        let databaseError = OSAllocatedUnfairLock<(any Error)?>(initialState: nil)

        @Sendable func checkDatabaseFailure() throws {
            if let error = databaseError.withLock({ $0 }) { throw error }
        }

        @Sendable func recordDatabaseFailure(_ error: Error) {
            guard DatabaseFailure.isSQLite(error) else { return }
            databaseError.withLock { if $0 == nil { $0 = error } }
        }
        final class DownloadTracker: @unchecked Sendable {
            var filesDownloaded = 0
            var bytesDownloaded: Int64 = 0
            var dirsCreated = 0
            let failures = OSAllocatedUnfairLock(initialState: 0)
        }
        let progress = DownloadTracker()

        func commitDownloadedFile(
            _ item: DriveFile, parentItemId: Int64, localURL: URL, published: LocalFileVersion
        ) async throws -> Bool {
            let receiptApplied = OSAllocatedUnfairLock(initialState: false)
            try await self.store.batchWrite { conn in
                guard (try? LocalFileVersion.read(at: localURL)) == published else { return }
                let stmt = try conn.cachedStatement("""
                    INSERT INTO items (
                        root_id, parent_id, name, entry_kind, remote_file_id,
                        local_mtime, local_size, local_sha256,
                        base_sha256, base_size,
                        remote_sha256, remote_size, remote_status,
                        local_generation, local_status, phase,
                        local_device, local_inode,
                        created_at, updated_at
                    ) VALUES (
                        ?, ?, ?, 'file', ?,
                        ?, ?, ?,
                        ?, ?,
                        ?, ?, 'present',
                        1, 'present', 'committed',
                        ?, ?,
                        ?, ?
                    )
                    ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                    DO UPDATE SET
                        remote_file_id = excluded.remote_file_id,
                        local_mtime = excluded.local_mtime, local_size = excluded.local_size,
                        local_sha256 = excluded.local_sha256,
                        base_sha256 = excluded.base_sha256, base_size = excluded.base_size,
                        remote_sha256 = excluded.remote_sha256, remote_size = excluded.remote_size,
                        remote_status = 'present', local_status = 'present', phase = 'committed',
                        local_device = excluded.local_device, local_inode = excluded.local_inode,
                        updated_at = excluded.updated_at
                    WHERE items.remote_file_id = excluded.remote_file_id;
                    """)
                stmt.bindInt64(rootId, at: 1)
                stmt.bindInt64(parentItemId, at: 2)
                stmt.bindText(item.name, at: 3)
                stmt.bindText(item.id, at: 4)
                stmt.bindInt64(Int64(published.mtime), at: 5)
                stmt.bindInt64(published.size, at: 6)
                stmt.bindText(item.sha256Checksum, at: 7)
                stmt.bindText(item.sha256Checksum, at: 8)
                stmt.bindInt64(published.size, at: 9)
                stmt.bindText(item.sha256Checksum, at: 10)
                stmt.bindInt64(published.size, at: 11)
                stmt.bindInt64(published.device, at: 12)
                stmt.bindInt64(published.inode, at: 13)
                let timestamp = Date().timeIntervalSince1970
                stmt.bindDouble(timestamp, at: 14)
                stmt.bindDouble(timestamp, at: 15)
                _ = try stmt.step()
                stmt.reset()
                guard conn.changes == 1 else { return }
                let inbox = try conn.cachedStatement(
                    "DELETE FROM remote_change_inbox WHERE root_id = ? AND remote_id = ?;")
                inbox.bindInt64(rootId, at: 1)
                inbox.bindText(item.id, at: 2)
                _ = try inbox.step()
                inbox.reset()
                receiptApplied.withLock { $0 = true }
            }
            return receiptApplied.withLock { $0 }
        }

        func recoverPublishedFile(
            _ item: DriveFile, parentItemId: Int64, localURL: URL
        ) async throws -> Bool {
            guard FileManager.default.fileExists(atPath: localURL.path),
                  (try? SyncEngine.computeFileSha256(at: localURL).sha256Hex) == item.sha256Checksum,
                  let published = try LocalFileVersion.read(at: localURL),
                  published.size == item.sizeBytes else { return false }
            return try await commitDownloadedFile(
                item, parentItemId: parentItemId, localURL: localURL, published: published)
        }

        // 2. Recursive enumeration of remote files and streaming download
        func traverseRemote(parentRemoteId: String, currentLocalURL: URL, parentItemId: Int64) async throws {
            let children = try await self.client.listChildren(parentId: parentRemoteId)
            try RemoteNameMapping.validateSiblings(children)

            for item in children {
                try checkDatabaseFailure()
                let itemLocalURL = currentLocalURL.appendingPathComponent(item.name)
                try RemoteNameMapping.validateDestination(itemLocalURL, root: rootURL)

                if item.isDirectory {
                    // Create local directory
                    try FileManager.default.createDirectory(at: itemLocalURL, withIntermediateDirectories: true)
                    progress.dirsCreated += 1

                    // write SQLite
                    let dirItemId: Int64 = try await self.store.write { conn in
                        let stmt = try conn.cachedStatement("""
                        INSERT INTO items (
                            root_id, parent_id, name, entry_kind, remote_file_id,
                            phase, created_at, updated_at
                        ) VALUES (?, ?, ?, 'directory', ?, 'committed', ?, ?)
                        ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                        DO UPDATE SET updated_at = excluded.updated_at WHERE items.remote_file_id = excluded.remote_file_id;
                        """)
                        stmt.bindInt64(rootId, at: 1)
                        stmt.bindInt64(parentItemId, at: 2)
                        stmt.bindText(item.name, at: 3)
                        stmt.bindText(item.id, at: 4)
                        let timestamp = Date().timeIntervalSince1970
                        stmt.bindDouble(timestamp, at: 5)
                        stmt.bindDouble(timestamp, at: 6)
                        _ = try stmt.step()
                        stmt.reset()
                        guard conn.changes == 1 else {
                            throw SyncEngineError.general("The remote directory name belongs to another file ID: \(item.name)")
                        }

                        let qStmt = try conn.cachedStatement("""
                        SELECT item_id FROM items WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;
                        """)
                        qStmt.bindInt64(rootId, at: 1)
                        qStmt.bindInt64(parentItemId, at: 2)
                        qStmt.bindText(item.name, at: 3)
                        guard try qStmt.step(), let dId = qStmt.columnInt64(at: 0) else {
                            throw NSError(domain: "SyncEngine", code: 15, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain dir item_id"])
                        }
                        qStmt.reset()
                        return dId
                    }

                    // Recurse to the next level
                    try await traverseRemote(parentRemoteId: item.id, currentLocalURL: itemLocalURL, parentItemId: dirItemId)
                } else {
                    try await RemoteChanges.retainBootstrapObservation(
                        store: self.store, rootID: rootId, file: item)
                    if try await recoverPublishedFile(
                        item, parentItemId: parentItemId, localURL: itemLocalURL) {
                        continue
                    }
                    // File: join concurrent download queue
                    let downloadBytes = item.sizeBytes ?? 0
                    notifier.addDiscovered(files: 1, bytes: downloadBytes)
                    self.monitor.enqueueDownload(id: item.id, name: item.name, totalBytes: downloadBytes)
                    downloadGroup.enter()
                    Task {
                        await downloadSemaphore.wait()
                        defer {
                            self.monitor.finishDownload(id: item.id)
                            downloadSemaphore.signal()
                            downloadGroup.leave()
                            notifier.addCompleted(files: 1, bytes: downloadBytes)
                        }

                        do {
                            try checkDatabaseFailure()
                            self.monitor.startDownload(id: item.id, name: item.name, totalBytes: item.sizeBytes ?? 0)
                            // Streaming download and verification SHA-256
                            let published = try await self.client.downloadFileSafely(
                                remoteId: item.id,
                                destinationURL: itemLocalURL,
                                expectedSha256: item.sha256Checksum,
                                expectedDestination: nil,
                                temporaryDirectory: downloadDirectory,
                                onProgress: { delta in
                                    self.monitor.reportDownloadProgress(id: item.id, additionalBytes: delta)
                                }
                            )

                            // Get metadata after local placement
                            let fileSize = published.size

                            guard try await commitDownloadedFile(
                                item, parentItemId: parentItemId,
                                localURL: itemLocalURL, published: published) else {
                                throw SyncEngineError.general("The initial download receipt has expired, retain the existing status: \(item.name)")
                            }

                            progress.filesDownloaded += 1
                            progress.bytesDownloaded += fileSize
                        } catch {
                            recordDatabaseFailure(error)
                            progress.failures.withLock { $0 += 1 }
                            self.logger.error("Failed to download file [\(item.name)]: \(error)")
                        }
                    }
                }
            }
        }

        // Start recursive enumeration and downloading
        var traversalError: Error?
        do {
            try await traverseRemote(parentRemoteId: remoteRootId, currentLocalURL: rootURL, parentItemId: rootItemId)
        } catch { traversalError = error }

        // Wait for all in-flight download tasks to complete
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            downloadGroup.notify(queue: .global(qos: .userInitiated)) {
                cont.resume()
            }
        }

        try checkDatabaseFailure()
        try await store.flush()
        if let traversalError {
            // Preserve a durable recovery route for a partially downloaded bootstrap.
            try await store.write { conn in
                let queryStatement = try conn.cachedStatement("INSERT INTO remote_directory_scans(root_id, remote_id, scan_id, state) VALUES (?, ?, ?, 'pending') ON CONFLICT(root_id, remote_id) DO UPDATE SET state = 'pending', page_token = NULL;")
                queryStatement.bindInt64(rootId, at: 1)
                queryStatement.bindText(remoteRootId, at: 2)
                queryStatement.bindText(UUID().uuidString, at: 3)
                _ = try queryStatement.step()
                queryStatement.reset()
            }
            throw traversalError
        }
        try await store.checkpoint()

        let filesFailed = progress.failures.withLock { $0 }
        if filesFailed == 0 {
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                UPDATE roots SET bootstrap_state = 'existingKnown', updated_at = ? WHERE root_id = ?;
                """)
                stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                stmt.bindInt64(rootId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
        }

        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        stats.directoriesCreated = progress.dirsCreated
        stats.filesDownloaded = progress.filesDownloaded
        stats.bytesDownloaded = progress.bytesDownloaded
        stats.filesFailed = filesFailed
        stats.remoteWorkPending = filesFailed
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }

}
