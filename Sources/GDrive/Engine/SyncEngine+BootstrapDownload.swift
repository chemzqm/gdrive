import Darwin
import Foundation
import CommonCrypto
import Logging
import DirectoryScanner
import os

private final class BootstrapDownloadTracker: @unchecked Sendable {
    struct State: Sendable {
        var filesDownloaded = 0
        var bytesDownloaded: Int64 = 0
        var dirsCreated = 0
        var failures = 0
        var conflicts = 0
    }
    private let state = OSAllocatedUnfairLock(initialState: State())

    func recordDirectory() { state.withLock { $0.dirsCreated += 1 } }
    func recordDownload(bytes: Int64, conflict: Bool = false) {
        state.withLock {
            $0.filesDownloaded += 1
            $0.bytesDownloaded += bytes
            if conflict { $0.conflicts += 1 }
        }
    }
    func recordFailure() { state.withLock { $0.failures += 1 } }
    func snapshot() -> State { state.withLock { $0 } }
}

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
        try await validateRootBinding(
            localPath: resolvedLocalPath, remoteRootID: remoteRootId)

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

        // Existing local entries are preserved. Equal content is adopted and
        // divergent same-path content is recorded as a durable conflict.
        var isDir: ObjCBool = false
        if FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir) {
            guard isDir.boolValue else {
                throw NSError(domain: "SyncEngine", code: 11, userInfo: [NSLocalizedDescriptionKey: "The local path already exists and is not a directory: \(resolvedLocalPath)"])
            }
        } else {
            try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        }
        let rootIdentity = try LocalDirectoryIdentity.require(
            at: rootURL,
            or: NSError(domain: "SyncEngine", code: 11, userInfo: [
                NSLocalizedDescriptionKey: "The local path is not a directory: \(resolvedLocalPath)"
            ]))

        let now = Date().timeIntervalSince1970

        // 1. Register or get Root Records and root entries
        let (rootId, rootItemId): (Int64, Int64) = try await store.write { conn in
            try Self.validateRootBinding(
                conn: conn, localPath: resolvedLocalPath, remoteRootID: remoteRootId)
            let rootStmt = try conn.cachedStatement("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', ?, ?, ?, ?, 'remoteToLocalEmpty', 'freshCreated', ?, ?)
            ON CONFLICT (account_id, remote_root_id) DO UPDATE SET updated_at = excluded.updated_at;
            """)
            rootStmt.bindText(resolvedLocalPath, at: 1)
            rootStmt.bindInt64(rootIdentity.device, at: 2)
            rootStmt.bindInt64(rootIdentity.inode, at: 3)
            rootStmt.bindText(remoteRootId, at: 4)
            rootStmt.bindDouble(now, at: 5)
            rootStmt.bindDouble(now, at: 6)
            _ = try rootStmt.step()
            rootStmt.reset()

            let rootQuery = try conn.cachedStatement(
                "SELECT root_id, local_root_device, local_root_inode FROM roots WHERE account_id = 'default' AND remote_root_id = ?;")
            rootQuery.bindText(remoteRootId, at: 1)
            guard try rootQuery.step(), let rId = rootQuery.columnInt64(at: 0),
                  let storedDevice = rootQuery.columnInt64(at: 1),
                  let storedInode = rootQuery.columnInt64(at: 2) else {
                throw NSError(domain: "SyncEngine", code: 13, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root_id"])
            }
            guard storedDevice == rootIdentity.device, storedInode == rootIdentity.inode else {
                throw SyncEngineError.localRootChanged(path: resolvedLocalPath)
            }
            rootQuery.reset()

            let itemStmt = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, local_status, remote_status,
                phase, created_at, updated_at
            ) VALUES (?, NULL, ?, 'directory', ?, ?, ?, 'present', 'present', 'committed', ?, ?)
            ON CONFLICT (root_id) WHERE parent_id IS NULL DO UPDATE SET updated_at = excluded.updated_at;
            """)
            itemStmt.bindInt64(rId, at: 1)
            itemStmt.bindText(rootURL.lastPathComponent, at: 2)
            itemStmt.bindText(remoteRootId, at: 3)
            itemStmt.bindInt64(rootIdentity.device, at: 4)
            itemStmt.bindInt64(rootIdentity.inode, at: 5)
            itemStmt.bindDouble(now, at: 6)
            itemStmt.bindDouble(now, at: 7)
            _ = try itemStmt.step()
            itemStmt.reset()

            let itemQuery = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL;")
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
        let downloadTasks = TaskLifecycle()
        let databaseError = OSAllocatedUnfairLock<(any Error)?>(initialState: nil)

        @Sendable func checkDatabaseFailure() throws {
            if let error = databaseError.withLock({ $0 }) { throw error }
        }

        @Sendable func recordDatabaseFailure(_ error: Error) {
            guard DatabaseFailure.isSQLite(error) else { return }
            databaseError.withLock { if $0 == nil { $0 = error } }
        }
        let progress = BootstrapDownloadTracker()

        func recoverPublishedFile(
            _ item: DriveFile, parentItemId: Int64, localURL: URL
        ) async throws -> Bool {
            guard let snapshot = try? self.stableFileDigestCapture(localURL),
                  snapshot.sha256Hex == item.sha256Checksum,
                  snapshot.fileSize == item.sizeBytes else { return false }
            let receipt = try await self.store.commitFileDownloadReceipt(
                expectation: .bootstrap(
                    rootID: rootId, parentItemID: parentItemId, file: item),
                localURL: localURL,
                published: snapshot.version
            )
            return receipt == .applied
        }

        let conflictRoot = try SyncConflictStore.directory(
            base: conflictDirectory, remoteRootID: remoteRootId)

        @Sendable func stageConflict(
            _ item: DriveFile, parentItemId: Int64, localURL: URL, relativePath: String
        ) async throws -> Int64 {
            let conflictURL = conflictRoot.appendingPathComponent(relativePath)
            try FileManager.default.createDirectory(
                at: conflictURL.deletingLastPathComponent(), withIntermediateDirectories: true)
            let expected = try LocalFileVersion.read(at: conflictURL)
            let stored = try await self.client.downloadFileSafely(
                remoteId: item.id, destinationURL: conflictURL,
                expectedSha256: item.sha256Checksum, expectedDestination: expected,
                temporaryDirectory: conflictRoot)
            try await SyncConflictStore.commitBootstrap(
                store: self.store, rootID: rootId, parentItemID: parentItemId, file: item,
                relativePath: relativePath, localURL: localURL, conflictURL: conflictURL)
            return stored.size
        }

        func enqueueFileDownload(
            _ item: DriveFile, parentItemId: Int64, localURL: URL
        ) async throws {
            try await RemoteChanges.retainBootstrapObservation(
                store: self.store, rootID: rootId, file: item)
            if try await recoverPublishedFile(
                item, parentItemId: parentItemId, localURL: localURL) {
                return
            }
            let relativePath = String(localURL.path.dropFirst(rootURL.path.count + 1))
            let downloadBytes = item.sizeBytes ?? 0
            notifier.addDiscovered(files: 1, bytes: downloadBytes)
            self.monitor.enqueueDownload(id: item.id, name: item.name, totalBytes: downloadBytes)
            await downloadSemaphore.wait()
            if Task.isCancelled {
                downloadSemaphore.signal()
                throw CancellationError()
            }
            downloadTasks.start {
                defer {
                    self.monitor.finishDownload(id: item.id)
                    downloadSemaphore.signal()
                    notifier.addCompleted(files: 1, bytes: downloadBytes)
                }

                do {
                    try Task.checkCancellation()
                    try checkDatabaseFailure()
                    self.monitor.startDownload(
                        id: item.id, name: item.name, totalBytes: item.sizeBytes ?? 0)
                    if FileManager.default.fileExists(atPath: localURL.path) {
                        let fileSize = try await stageConflict(
                            item, parentItemId: parentItemId,
                            localURL: localURL, relativePath: relativePath)
                        progress.recordDownload(bytes: fileSize, conflict: true)
                        return
                    }
                    let execution = try await self.executeFileDownload(
                        remoteID: item.id,
                        expectedSHA256: item.sha256Checksum,
                        destination: localURL,
                        expectedDestination: nil,
                        temporaryDirectory: downloadDirectory,
                        onProgress: { delta in
                            self.monitor.reportDownloadProgress(
                                id: item.id, additionalBytes: delta)
                        }
                    )

                    let published: LocalFileVersion
                    switch execution {
                    case .published(let version):
                        published = version
                    case .destinationChanged(let download):
                        try? FileManager.default.removeItem(at: download.url)
                        let fileSize = try await stageConflict(
                            item, parentItemId: parentItemId,
                            localURL: localURL, relativePath: relativePath)
                        progress.recordDownload(bytes: fileSize, conflict: true)
                        return
                    }

                    let receipt = try await self.store.commitFileDownloadReceipt(
                        expectation: .bootstrap(
                            rootID: rootId, parentItemID: parentItemId, file: item),
                        localURL: localURL,
                        published: published
                    )
                    guard receipt == .applied else {
                        throw SyncEngineError.general(
                            "The initial download receipt has expired, retain the existing status: \(item.name)"
                        )
                    }
                    progress.recordDownload(bytes: published.size)
                } catch {
                    recordDatabaseFailure(error)
                    progress.recordFailure()
                    do {
                        try await SyncIssueStore.record(
                            store: self.store, rootID: rootId,
                            subject: SyncIssueSubject(
                                itemID: nil, remoteFileID: item.id,
                                relativePath: relativePath),
                            stage: .download, error: error)
                    } catch {
                        recordDatabaseFailure(error)
                    }
                    self.logger.error("Failed to download file [\(item.name)]: \(error)")
                }
            }
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
                    var directoryStat = stat()
                    guard lstat(itemLocalURL.path, &directoryStat) == 0 else {
                        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
                    }
                    guard directoryStat.st_mode & S_IFMT == S_IFDIR else {
                        throw SyncEngineError.general(
                            "The downloaded directory path is not a directory: \(itemLocalURL.path)"
                        )
                    }
                    let directoryDevice = Int64(directoryStat.st_dev)
                    let directoryInode = Int64(directoryStat.st_ino)
                    progress.recordDirectory()

                    // write SQLite
                    let dirItemId: Int64 = try await self.store.write { conn in
                        let stmt = try conn.cachedStatement("""
                        INSERT INTO items (
                            root_id, parent_id, name, entry_kind, remote_file_id,
                            local_device, local_inode, local_status,
                            remote_parent_file_id, remote_name, remote_version, remote_status,
                            phase, dirty_generation, created_at, updated_at
                        ) VALUES (?, ?, ?, 'directory', ?, ?, ?, 'present', ?, ?, ?, 'present',
                            'committed', 0, ?, ?)
                        ON CONFLICT (root_id, parent_id, name) WHERE parent_id IS NOT NULL
                        DO UPDATE SET
                            local_device = excluded.local_device,
                            local_inode = excluded.local_inode,
                            local_status = 'present',
                            remote_parent_file_id = excluded.remote_parent_file_id,
                            remote_name = excluded.remote_name,
                            remote_version = COALESCE(excluded.remote_version, items.remote_version),
                            remote_status = 'present',
                            phase = 'committed',
                            dirty_generation = 0,
                            updated_at = excluded.updated_at
                        WHERE items.remote_file_id = excluded.remote_file_id;
                        """)
                        stmt.bindInt64(rootId, at: 1)
                        stmt.bindInt64(parentItemId, at: 2)
                        stmt.bindText(item.name, at: 3)
                        stmt.bindText(item.id, at: 4)
                        stmt.bindInt64(directoryDevice, at: 5)
                        stmt.bindInt64(directoryInode, at: 6)
                        stmt.bindText(parentRemoteId, at: 7)
                        stmt.bindText(item.name, at: 8)
                        stmt.bindInt64(item.versionNumber, at: 9)
                        let timestamp = Date().timeIntervalSince1970
                        stmt.bindDouble(timestamp, at: 10)
                        stmt.bindDouble(timestamp, at: 11)
                        _ = try stmt.step()
                        stmt.reset()
                        guard conn.changes == 1 else {
                            throw SyncEngineError.general("The remote directory name belongs to another file ID: \(item.name)")
                        }

                        let qStmt = try conn.cachedStatement("""
                        SELECT item_id FROM items WHERE root_id = ? AND parent_id = ? AND name = ?;
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
                    try await enqueueFileDownload(
                        item, parentItemId: parentItemId, localURL: itemLocalURL)
                }
            }
        }

        // Start recursive enumeration and downloading
        var traversalError: Error?
        do {
            try await traverseRemote(parentRemoteId: remoteRootId, currentLocalURL: rootURL, parentItemId: rootItemId)
        } catch { traversalError = error }

        // Wait for all admitted downloads. Cancellation reaches every active task.
        await withTaskCancellationHandler {
            await downloadTasks.waitForAll()
        } onCancel: {
            downloadTasks.cancelAll()
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

        let progressSnapshot = progress.snapshot()
        let filesFailed = progressSnapshot.failures
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
        stats.directoriesCreated = progressSnapshot.dirsCreated
        stats.filesDownloaded = progressSnapshot.filesDownloaded
        stats.bytesDownloaded = progressSnapshot.bytesDownloaded
        stats.filesFailed = filesFailed
        stats.conflicts = try await SyncConflictStore.list(store: store, rootID: rootId)
        stats.remoteWorkPending = filesFailed + stats.conflicts.count
        if filesFailed == 0 && stats.remoteWorkPending == 0 && stats.conflicts.isEmpty {
            try await SyncIssueStore.clear(store: store, rootID: rootId)
        }
        stats.issueCount = try await SyncIssueStore.count(store: store, rootID: rootId)
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }

}
