import Darwin
import Foundation
import CommonCrypto
import Logging
import DirectoryScanner
import os

// Internal SyncEngine implementation split by synchronization phase.
extension SyncEngine {
    // MARK: - mode 1:local directory -> Extremely fast upload of remote empty directories (localToRemoteEmpty)

    func syncLocalToRemoteEmptyUnlocked(
        localPath: String,
        remoteRootId: String,
        maxUploadConcurrency: Int,
        onProgress: (@Sendable (SyncProgress) -> Void)?
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)

        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)

        // Verify local root directory
        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir), isDir.boolValue else {
            throw NSError(domain: "SyncEngine", code: 1, userInfo: [NSLocalizedDescriptionKey: "The local path does not exist or is not a directory: \(resolvedLocalPath)"])
        }

        // Verify that the remote root directory exists
        let remoteRoot = try await client.getFile(remoteId: remoteRootId)
        guard remoteRoot.isDirectory else {
            throw NSError(domain: "SyncEngine", code: 2, userInfo: [NSLocalizedDescriptionKey: "The remote target is not a directory: \(remoteRootId)"])
        }

        // Only verify whether the remote end is an empty directory when the root baseline is not established for the first time.
        let rootExists: Bool = try await store.read { conn in
            let stmt = try conn.cachedStatement("SELECT 1 FROM roots WHERE remote_root_id = ? AND is_active = 1;")
            stmt.bindText(remoteRootId, at: 1)
            defer { stmt.reset() }
            return try stmt.step()
        }
        if !rootExists {
            let existingChildren = try await client.listChildren(parentId: remoteRootId)
            guard existingChildren.isEmpty else {
                throw NSError(domain: "SyncEngine", code: 20, userInfo: [NSLocalizedDescriptionKey: "The remote target is not empty; localToRemoteEmpty cannot run: \(remoteRootId)"])
            }
        }

        let now = Date().timeIntervalSince1970

        // 1. Register or get Root Records and root entries
        let (rootId, rootItemId): (Int64, Int64) = try await store.write { conn in
            let rootStmt = try conn.cachedStatement("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'freshCreated', ?, ?)
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
                throw NSError(domain: "SyncEngine", code: 3, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root_id"])
            }
            rootQuery.reset()

            let itemStmt = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (?, NULL, ?, 'directory', ?, ?, ?)
            ON CONFLICT (root_id) WHERE parent_id IS NULL DO UPDATE SET updated_at = excluded.updated_at;
            """)
            itemStmt.bindInt64(rId, at: 1)
            itemStmt.bindText(rootURL.lastPathComponent, at: 2)
            itemStmt.bindText(remoteRootId, at: 3)
            itemStmt.bindDouble(now, at: 4)
            itemStmt.bindDouble(now, at: 5)
            _ = try itemStmt.step()
            itemStmt.reset()

            let itemQuery = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL;")
            itemQuery.bindInt64(rId, at: 1)
            guard try itemQuery.step(), let rItemId = itemQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 4, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root item_id"])
            }
            itemQuery.reset()

            return (rId, rItemId)
        }

        try await RemoteChanges.saveInitialCursor(store: store, client: client, rootID: rootId, requireExisting: rootExists)

        // A directory is registered in this table before its children can be emitted.
        // Its value becomes available only after remote creation and the local commit succeed.
        let taskRegistry = BootstrapTaskRegistry(root: BootstrapDirectoryTarget(
            remoteID: remoteRootId,
            itemID: rootItemId
        ))
        let databaseError = OSAllocatedUnfairLock<Error?>(initialState: nil)

        @Sendable func recordDatabaseError(_ error: Error) {
            guard DatabaseFailure.isSQLite(error) else { return }
            databaseError.withLock { if $0 == nil { $0 = error } }
        }

        @Sendable func recordUnknownOutcome(
            _ error: Error, createIntent: DurableCreateIntent?
        ) async {
            recordDatabaseError(error)
            guard let createIntent else { return }
            do {
                try await DurableCreateIntentStore.markUnknownOutcome(
                    store: self.store, operationID: createIntent.operationID, error: error)
            } catch {
                recordDatabaseError(error)
            }
        }
        func restoreKnownDirectories() async throws {
            // in advance from SQLite Load all known subdirectory mappings, restore bootstrap Or reuse it resolutely when re-running to avoid blindly re-creating
            struct ExistingDirectory: Sendable {
                let id: Int64
                let parentId: Int64
                let name: String
                let remoteId: String
            }
            let existingDirs: [ExistingDirectory] = try await store.read { conn in
                let stmt = try conn.cachedStatement("""
                SELECT item_id, parent_id, name, remote_file_id
                FROM items
                WHERE root_id = ? AND entry_kind = 'directory'
                    AND parent_id IS NOT NULL AND phase = 'committed' AND remote_status = 'present';
                """)
                stmt.bindInt64(rootId, at: 1)
                defer { stmt.reset() }
                var list: [ExistingDirectory] = []
                while try stmt.step() {
                    if let id = stmt.columnInt64(at: 0),
                       let pId = stmt.columnInt64(at: 1),
                       let name = stmt.columnText(at: 2),
                       let rId = stmt.columnText(at: 3) {
                        list.append(ExistingDirectory(id: id, parentId: pId, name: name, remoteId: rId))
                    }
                }
                return list
            }

            var dirPathsById: [Int64: String] = [rootItemId: ""]
            var registered = Set<Int64>([rootItemId])
            var remainingDirs = existingDirs
            var topoProgress = true
            while !remainingDirs.isEmpty && topoProgress {
                topoProgress = false
                remainingDirs.removeAll { dir in
                    if registered.contains(dir.parentId), let parentPath = dirPathsById[dir.parentId] {
                        let relPath = parentPath.isEmpty ? dir.name : "\(parentPath)/\(dir.name)"
                        dirPathsById[dir.id] = relPath
                        taskRegistry.registerReady(
                            BootstrapDirectoryTarget(remoteID: dir.remoteId, itemID: dir.id),
                            for: relPath
                        )
                        registered.insert(dir.id)
                        topoProgress = true
                        return true
                    }
                    return false
                }
            }
        }
        try await restoreKnownDirectories()

        // 3. Set up a bounded concurrent upload pipeline (strict upper limit 64 Concurrency)
        let effectiveConcurrency = max(1, min(64, maxUploadConcurrency))
        let uploadSemaphore = AsyncSemaphore(count: effectiveConcurrency)
        // Directory metadata requests do not consume file transfer slots, but
        // they still need their own network concurrency bound.
        let directorySemaphore = AsyncSemaphore(count: max(1, min(8, effectiveConcurrency)))

        // Load fast change comparison baseline cache (§6.2)
        let baselineCache = try await LocalBaselineCache.load(store: store, rootId: rootId)

        final class ProgressTracker: @unchecked Sendable {
            private var _filesUploaded = 0
            private var _filesSkipped = 0
            private var _filesFailed = 0
            private var _bytesUploaded: Int64 = 0
            private var _dirsCreated = 0
            private var _directoryFailures = 0
            private var lock = os_unfair_lock()

            var filesUploaded: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _filesUploaded
            }

            var filesSkipped: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _filesSkipped
            }

            var filesFailed: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _filesFailed
            }

            var bytesUploaded: Int64 {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _bytesUploaded
            }

            var dirsCreated: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _dirsCreated
            }

            var hasDirectoryFailures: Bool {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _directoryFailures != 0
            }

            func recordSuccess(bytes: Int64) {
                os_unfair_lock_lock(&lock)
                _filesUploaded += 1
                _bytesUploaded += bytes
                os_unfair_lock_unlock(&lock)
            }

            func recordFailure() {
                os_unfair_lock_lock(&lock)
                _filesFailed += 1
                os_unfair_lock_unlock(&lock)
            }

            func recordSkipped() {
                os_unfair_lock_lock(&lock)
                _filesSkipped += 1
                os_unfair_lock_unlock(&lock)
            }

            func recordDirCreated() {
                os_unfair_lock_lock(&lock)
                _dirsCreated += 1
                os_unfair_lock_unlock(&lock)
            }

            func recordDirectoryFailure() {
                os_unfair_lock_lock(&lock)
                _directoryFailures += 1
                os_unfair_lock_unlock(&lock)
            }
        }
        let progress = ProgressTracker()

        // 4. start DirectoryScanner concurrent scan
        let staticPrefix = resolvedLocalPath.hasSuffix("/") ? resolvedLocalPath : resolvedLocalPath + "/"
        let prefixBytes = Array(staticPrefix.utf8)

        let scanOptions = ScanOptions(
            mode: .basic,
            emission: .all,
            includeHidden: true,
            workers: min(6, ProcessInfo.processInfo.activeProcessorCount),
            batchCapacity: 512,
            delimiter: 0x00,
            pathPrefix: prefixBytes
        )
        let scanFilters: [FilterRule] = [.excludeDirectory(".git")]
        let request = ScanRequest(root: resolvedLocalPath, filters: scanFilters, options: scanOptions)
        @Sendable func uploadFile(fullPath: String, relPath: String, parent: BootstrapDirectoryDependency, name: String, fileSize: Int64) async {
            var createIntent: DurableCreateIntent?
            var didAcquireSemaphore = false
            var parentItemID: Int64?

            defer {
                if didAcquireSemaphore {
                    uploadSemaphore.signal()
                }
                self.monitor.finishUpload(id: fullPath)
                notifier.addCompleted(files: 1, bytes: Int64(fileSize))
            }

            do {
                try Task.checkCancellation()
                // Resolving the parent happens before a transfer slot is acquired.
                let parentTarget = try await parent.value()
                try Task.checkCancellation()
                let remoteParentId = parentTarget.remoteID
                parentItemID = parentTarget.itemID

                await uploadSemaphore.wait()
                didAcquireSemaphore = true
                try Task.checkCancellation()
                if let error = databaseError.withLock({ $0 }) { throw error }

                let fileURL = URL(fileURLWithPath: fullPath)
                let limit8MB: Int64 = 8 * 1024 * 1024

                let input = try StableUploadInput.capture(at: fileURL)
                let sha256Hex = input.sha256
                let fileSize = input.size
                let smallContent = input.data

                self.monitor.startUpload(id: fullPath, name: name, totalBytes: fileSize)

                let dev = input.version.device
                let ino = input.version.inode
                let mtime = input.version.mtime

                let parentDirItemId = parentTarget.itemID

                // Check whether the file is already in SQLite recorded (whether it has been committed Still unfinished inFlight)
                struct ExistingFileTarget {
                    let itemID: Int64
                    let remoteID: String?
                    let phase: String
                    let remoteStatus: String
                    let hasBaseline: Bool
                    let localGeneration: Int64
                    let remoteGeneration: Int64
                    let dirtyGeneration: Int64
                }

                let existingTarget: ExistingFileTarget? = try await self.store.read { conn in
                    let stmt = try conn.cachedStatement("""
                    SELECT item_id, remote_file_id, phase, remote_status, base_sha256,
                           local_generation, remote_generation, dirty_generation
                    FROM items
                    WHERE root_id = ? AND parent_id = ? AND name = ?;
                    """)
                    stmt.bindInt64(rootId, at: 1)
                    stmt.bindInt64(parentDirItemId, at: 2)
                    stmt.bindText(name, at: 3)
                    defer { stmt.reset() }
                    if try stmt.step(), let iId = stmt.columnInt64(at: 0) {
                        return ExistingFileTarget(
                            itemID: iId,
                            remoteID: stmt.columnText(at: 1),
                            phase: stmt.columnText(at: 2) ?? "discovered",
                            remoteStatus: stmt.columnText(at: 3) ?? "unknown",
                            hasBaseline: stmt.columnText(at: 4) != nil,
                            localGeneration: stmt.columnInt64(at: 5) ?? 0,
                            remoteGeneration: stmt.columnInt64(at: 6) ?? 0,
                            dirtyGeneration: stmt.columnInt64(at: 7) ?? 0
                        )
                    }
                    return nil
                }

                // Perform upload based on file size:≤ 8MB go Multipart,> 8MB go Resumable
                if let existingTarget, existingTarget.hasBaseline, let remoteID = existingTarget.remoteID {
                    throw DriveError.unsafeOverwrite(fileId: remoteID)
                }
                let receiptApplied = OSAllocatedUnfairLock(initialState: false)
                let expectedRemoteGeneration = existingTarget?.remoteGeneration ?? 0
                let expectedDirtyGeneration = max(existingTarget?.dirtyGeneration ?? 0, 1)
                func uploadSmallFile(_ content: Data) async throws {
                    let uploadedFile: DriveFile
                    let committedItemID: Int64
                    if let existing = existingTarget, let existingRemoteId = existing.remoteID,
                       existing.phase == "committed" && existing.remoteStatus == "present" {
                        // The content of the file submitted in the cloud has changed: call directly updateMultipart Update existing remote objects, never create new ones Drive Object!
                        uploadedFile = try await self.client.updateMultipart(
                            remoteId: existingRemoteId,
                            content: content,
                            expectedSha256: sha256Hex
                        )
                        committedItemID = existing.itemID
                    } else {
                        // Create new files or rerun unfinished files: Prioritize reusing existing files remote_file_id,avoid regeneration
                        let targetRemoteId: String
                        if let existingRemoteId = existingTarget?.remoteID {
                            targetRemoteId = existingRemoteId
                        } else {
                            targetRemoteId = try await self.idPool.nextId()
                        }
                        let intent = try await DurableCreateIntentStore.prepareFileUpload(
                            store: self.store,
                            rootID: rootId,
                            itemID: existingTarget?.itemID,
                            parentItemID: parentDirItemId,
                            name: name,
                            targetParentRemoteID: remoteParentId,
                            candidateRemoteID: targetRemoteId,
                            device: dev,
                            inode: ino,
                            mtime: mtime,
                            size: fileSize,
                            sha256: sha256Hex,
                            transport: .multipart
                        )
                        createIntent = intent

                        do {
                            uploadedFile = try await self.client.uploadMultipart(
                                name: name,
                                parentId: intent.targetParentRemoteID,
                                remoteId: intent.targetRemoteID,
                                content: content,
                                expectedSha256: sha256Hex
                            )
                        } catch let error as DriveError {
                            switch error {
                            case .conflict:
                                // The remote end may have successfully written the ID metadata, converted to update text
                                uploadedFile = try await self.client.updateMultipart(
                                    remoteId: intent.targetRemoteID,
                                    content: content,
                                    expectedSha256: sha256Hex
                                )
                            case .serverError(let code, _) where code == 400 || code == 409:
                                uploadedFile = try await self.client.updateMultipart(
                                    remoteId: intent.targetRemoteID,
                                    content: content,
                                    expectedSha256: sha256Hex
                                )
                            default:
                                throw error
                            }
                        }
                        committedItemID = intent.itemID
                    }
                    self.monitor.reportUploadProgress(id: fullPath, additionalBytes: fileSize)

                    // After the upload is successful, pass batchWrite(Group Commit,merge 256 item or 5ms Flush the disk) and write the baseline
                    try input.version.validate(at: fileURL)
                    let completedCreateIntent = createIntent
                    let expectedLocalGeneration = completedCreateIntent?.expectedLocalGeneration ?? existingTarget?.localGeneration ?? 1
                    try await self.store.batchWrite { conn in
                        guard (try? LocalFileVersion.read(at: fileURL)) == input.version else { return }
                        let itemStmt = try conn.cachedStatement("""
                        UPDATE items SET
                            remote_file_id = ?,
                            local_device = ?, local_inode = ?, local_mtime = ?,
                            local_size = ?, local_sha256 = ?,
                            base_sha256 = ?, base_size = ?,
                            remote_sha256 = ?, remote_size = ?, remote_status = 'present',
                            local_status = 'present', phase = 'committed', dirty_generation = 0,
                            updated_at = ?
                        WHERE item_id = ? AND local_generation = ? AND remote_generation = ?
                            AND dirty_generation = ?;
                        """)
                        itemStmt.bindText(uploadedFile.id, at: 1)
                        itemStmt.bindInt64(dev, at: 2)
                        itemStmt.bindInt64(ino, at: 3)
                        itemStmt.bindInt64(mtime, at: 4)
                        itemStmt.bindInt64(fileSize, at: 5)
                        itemStmt.bindText(sha256Hex, at: 6)
                        itemStmt.bindText(sha256Hex, at: 7)
                        itemStmt.bindInt64(fileSize, at: 8)
                        itemStmt.bindText(sha256Hex, at: 9)
                        itemStmt.bindInt64(fileSize, at: 10)
                        let timestamp = Date().timeIntervalSince1970
                        itemStmt.bindDouble(timestamp, at: 11)
                        itemStmt.bindInt64(committedItemID, at: 12)
                        itemStmt.bindInt64(expectedLocalGeneration, at: 13)
                        itemStmt.bindInt64(expectedRemoteGeneration, at: 14)
                        itemStmt.bindInt64(expectedDirtyGeneration, at: 15)
                        _ = try itemStmt.step()
                        itemStmt.reset()
                        guard conn.changes == 1 else { return }
                        receiptApplied.withLock { $0 = true }
                        if let createIntent = completedCreateIntent {
                            try DurableCreateIntentStore.completeOperation(
                                conn: conn,
                                operationID: createIntent.operationID,
                                now: timestamp
                            )
                        }
                    }
                }

                func uploadLargeFile() async throws {
                    // large files (> 8MB):Reuse existing remote_file_id with breakpoints, no blind replacement ID!
                    let remoteFileId: String
                    if let existingRemoteId = existingTarget?.remoteID {
                        remoteFileId = existingRemoteId
                    } else {
                        remoteFileId = try await self.idPool.nextId()
                    }
                    let isUpdate = existingTarget?.hasBaseline == true || (existingTarget?.phase == "committed" && existingTarget?.remoteStatus == "present")
                    if isUpdate { throw DriveError.unsafeOverwrite(fileId: remoteFileId) }

                    let currentItemId: Int64 = try await self.store.write { conn in
                        let itemStmt = try conn.cachedStatement("""
                        INSERT INTO items (
                            root_id, parent_id, name, entry_kind, remote_file_id,
                            local_device, local_inode, local_mtime, local_size, local_sha256,
                            local_generation, local_status, phase, dirty_generation,
                            created_at, updated_at
                        ) VALUES (
                            ?, ?, ?, 'file', ?,
                            ?, ?, ?, ?, ?,
                            1, 'present', 'inFlight', 1,
                            ?, ?
                        )
                        ON CONFLICT (root_id, parent_id, name) WHERE parent_id IS NOT NULL
                        DO UPDATE SET
                            remote_file_id = COALESCE(items.remote_file_id, excluded.remote_file_id),
                            local_device = excluded.local_device,
                            local_inode = excluded.local_inode,
                            local_mtime = excluded.local_mtime,
                            local_size = excluded.local_size,
                            local_sha256 = excluded.local_sha256,
                            phase = 'inFlight',
                            dirty_generation = MAX(items.dirty_generation, 1),
                            updated_at = excluded.updated_at;
                        """)
                        itemStmt.bindInt64(rootId, at: 1)
                        itemStmt.bindInt64(parentDirItemId, at: 2)
                        itemStmt.bindText(name, at: 3)
                        itemStmt.bindText(remoteFileId, at: 4)
                        itemStmt.bindInt64(dev, at: 5)
                        itemStmt.bindInt64(ino, at: 6)
                        itemStmt.bindInt64(mtime, at: 7)
                        itemStmt.bindInt64(fileSize, at: 8)
                        itemStmt.bindText(sha256Hex, at: 9)
                        let timestamp = Date().timeIntervalSince1970
                        itemStmt.bindDouble(timestamp, at: 10)
                        itemStmt.bindDouble(timestamp, at: 11)
                        _ = try itemStmt.step()
                        itemStmt.reset()

                        let qStmt = try conn.cachedStatement("""
                        SELECT item_id FROM items
                        WHERE root_id = ? AND parent_id = ? AND name = ?;
                        """)
                        qStmt.bindInt64(rootId, at: 1)
                        qStmt.bindInt64(parentDirItemId, at: 2)
                        qStmt.bindText(name, at: 3)
                        defer { qStmt.reset() }
                        if try qStmt.step(), let id = qStmt.columnInt64(at: 0) {
                            return id
                        }
                        return 0
                    }

                    // large files (> 8MB) Resumable 8MB Streaming block-based breakpoint resumption (each block is placed on the disk) offset)
                    _ = try await self.performResumableUpload(
                        rootId: rootId,
                        itemId: currentItemId,
                        fileURL: input.fileURL,
                        fileSize: fileSize,
                        expectedSha256: sha256Hex,
                        remoteId: remoteFileId,
                        parentId: remoteParentId,
                        name: name,
                        isUpdate: isUpdate
                    )

                    // Submit a common baseline B
                    try input.version.validate(at: fileURL)
                    let expectedLocalGeneration = existingTarget?.localGeneration ?? 1
                    try await self.store.batchWrite { conn in
                        guard (try? LocalFileVersion.read(at: fileURL)) == input.version else { return }
                        let updateStmt = try conn.cachedStatement("""
                        UPDATE items SET
                            base_sha256 = ?,
                            base_size = ?,
                            remote_sha256 = ?,
                            remote_size = ?,
                            remote_status = 'present',
                            phase = 'committed',
                            dirty_generation = 0,
                            updated_at = ?
                        WHERE root_id = ? AND remote_file_id = ? AND local_generation = ?
                            AND remote_generation = ? AND dirty_generation = ?;
                        """)
                        updateStmt.bindText(sha256Hex, at: 1)
                        updateStmt.bindInt64(fileSize, at: 2)
                        updateStmt.bindText(sha256Hex, at: 3)
                        updateStmt.bindInt64(fileSize, at: 4)
                        updateStmt.bindDouble(Date().timeIntervalSince1970, at: 5)
                        updateStmt.bindInt64(rootId, at: 6)
                        updateStmt.bindText(remoteFileId, at: 7)
                        updateStmt.bindInt64(expectedLocalGeneration, at: 8)
                        updateStmt.bindInt64(expectedRemoteGeneration, at: 9)
                        updateStmt.bindInt64(expectedDirtyGeneration, at: 10)
                        _ = try updateStmt.step()
                        updateStmt.reset()
                        if conn.changes == 1 { receiptApplied.withLock { $0 = true } }
                    }
                }

                if fileSize <= limit8MB, let content = smallContent {
                    try await uploadSmallFile(content)
                } else {
                    try await uploadLargeFile()
                }

                guard receiptApplied.withLock({ $0 }) else {
                    throw SyncEngineError.general("The upload receipt is stale; the newer generation remains pending: \(name)")
                }
                progress.recordSuccess(bytes: fileSize)
            } catch {
                await recordUnknownOutcome(error, createIntent: createIntent)
                progress.recordFailure()
                if case DriveError.unsafeOverwrite = error, let parentItemID {
                    do {
                        try await self.store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET phase = 'blocked', dirty_generation = MAX(dirty_generation, 1)
                            WHERE root_id = ? AND parent_id = ? AND name = ?;
                            """)
                            stmt.bindInt64(rootId, at: 1)
                            stmt.bindInt64(parentItemID, at: 2)
                            stmt.bindText(name, at: 3)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                    } catch {
                        recordDatabaseError(error)
                    }
                }
                self.logger.error("Failed to upload file [\(relPath)]: \(error)")
            }
        }

        @Sendable func createDirectory(relPath: String, parent: BootstrapDirectoryDependency, name: String, metadata: FileMetadata?) async throws -> BootstrapDirectoryTarget {
            var createIntent: DurableCreateIntent?
            do {
                try Task.checkCancellation()
                let parentTarget = try await parent.value()
                try Task.checkCancellation()
                await directorySemaphore.wait()
                defer { directorySemaphore.signal() }
                try Task.checkCancellation()
                if let error = databaseError.withLock({ $0 }) { throw error }
                let localAttributes = try FileManager.default.attributesOfItem(
                    atPath: rootURL.appendingPathComponent(relPath).path)
                let localDevice = (localAttributes[.systemNumber] as? NSNumber)?.int64Value
                let localInode = (localAttributes[.systemFileNumber] as? NSNumber)?.int64Value
                let candidateRemoteId = try await self.idPool.nextId()
                let intent = try await DurableCreateIntentStore.prepareDirectory(
                    store: self.store,
                    rootID: rootId,
                    parentItemID: parentTarget.itemID,
                    name: name,
                    targetParentRemoteID: parentTarget.remoteID,
                    candidateRemoteID: candidateRemoteId,
                    device: metadata.map { Int64($0.identity.device) }
                        ?? localDevice ?? 1,
                    inode: metadata.map { Int64($0.identity.inode) }
                        ?? localInode ?? 0
                )
                createIntent = intent
                try Task.checkCancellation()

                // Intent of group-commit Only after confirmation can the remote creation request be issued.
                _ = try await self.client.createDirectory(
                    name: name,
                    parentId: intent.targetParentRemoteID,
                    remoteId: intent.targetRemoteID
                )

                try await self.store.batchWrite { conn in
                    let timestamp = Date().timeIntervalSince1970
                    let stmt = try conn.cachedStatement("""
                    UPDATE items SET
                        remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                    WHERE item_id = ?;
                    """)
                    stmt.bindDouble(timestamp, at: 1)
                    stmt.bindInt64(intent.itemID, at: 2)
                    _ = try stmt.step()
                    stmt.reset()
                    try DurableCreateIntentStore.completeOperation(conn: conn, operationID: intent.operationID, now: timestamp)
                }
                progress.recordDirCreated()
                return BootstrapDirectoryTarget(remoteID: intent.targetRemoteID, itemID: intent.itemID)
            } catch {
                recordDatabaseError(error)
                if let createIntent {
                    do {
                        try await DurableCreateIntentStore.markUnknownOutcome(
                            store: self.store, operationID: createIntent.operationID, error: error)
                    } catch {
                        recordDatabaseError(error)
                    }
                }
                progress.recordDirectoryFailure()
                self.logger.error("Failed to create remote directory [\(relPath)]: \(error)")
                throw error
            }
        }

        @Sendable func processBatch(_ batch: ScanBatch) async throws {
            try Task.checkCancellation()
            if let error = databaseError.withLock({ $0 }) { throw error }
            try batch.withRawData { rawBuf in
                guard let basePtr = rawBuf.baseAddress else { return }

                for idx in 0..<batch.count {
                    try Task.checkCancellation()
                    if let error = databaseError.withLock({ $0 }) { throw error }
                    let record = batch.records[idx]
                    let rawPtr = UnsafeRawPointer(basePtr + Int(record.offset))
                    let cPath = rawPtr.assumingMemoryBound(to: CChar.self)
                    // Copy values needed by the asynchronous task while the batch's raw buffer is valid.
                    let type = record.type
                    let metadata = record.metadata
                    let fullPath = String(cString: cPath)

                    let relPath: String
                    if fullPath.hasPrefix(staticPrefix) {
                        relPath = String(fullPath.dropFirst(staticPrefix.count))
                    } else {
                        relPath = (fullPath as NSString).lastPathComponent
                    }

                    if relPath.isEmpty || relPath == ".git" || relPath.hasPrefix(".git/") { continue }

                    let parentRel = (relPath as NSString).deletingLastPathComponent
                    let name = (relPath as NSString).lastPathComponent
                    let parent = try taskRegistry.dependency(for: parentRel)

                    if type == .directory {
                        if (try? taskRegistry.dependency(for: relPath)) != nil {
                            continue
                        }
                        let task = Task {
                            try await createDirectory(relPath: relPath, parent: parent, name: name, metadata: metadata)
                        }
                        taskRegistry.registerDirectoryTask(task, for: relPath)
                    } else if type == .file {
                        let dev = Int64(metadata?.identity.device ?? 1)
                        let ino = Int64(metadata?.identity.inode ?? 0)
                        let mtime = (metadata?.modificationTime.seconds ?? 0) * 1_000_000_000 + Int64(metadata?.modificationTime.nanoseconds ?? 0)
                        let fileSize = metadata?.fileSize ?? 0

                        // Rapid change detection (§6.2):dev + inode + mtime + size Matching skips content reading and hash calculations
                        if baselineCache.lookupUnchanged(device: dev, inode: ino, mtime: mtime, size: fileSize) != nil {
                            progress.recordSkipped()
                            continue
                        }

                        // File handling: wait for its immediate parent directory to be ready and upload immediately
                        notifier.addDiscovered(files: 1, bytes: Int64(fileSize))
                        self.monitor.enqueueUpload(id: fullPath, name: name, totalBytes: Int64(fileSize))
                        let task = Task {
                            await uploadFile(fullPath: fullPath, relPath: relPath, parent: parent, name: name, fileSize: Int64(fileSize))
                        }
                        taskRegistry.registerFileTask(task)
                    }
                }
            }
        }
        do {
            try await withTaskCancellationHandler {
                try await directoryScan(request, processBatch)
            } onCancel: {
                taskRegistry.cancelAll()
            }
        } catch {
            taskRegistry.cancelAll()
            await taskRegistry.waitForAll()
            if let databaseFailure = databaseError.withLock({ $0 }) { throw databaseFailure }
            throw error
        }

        // 5. Wait for all scheduled work. Cancellation stops and drains both
        // directory and file tasks, including work registered during scanning.
        await withTaskCancellationHandler {
            await taskRegistry.waitForAll()
        } onCancel: {
            taskRegistry.cancelAll()
        }
        if let error = databaseError.withLock({ $0 }) { throw error }
        try Task.checkCancellation()

        // 6. Force the buffer to be written to disk and execute WAL checkpoint
        try await store.flush()
        try await store.checkpoint()

        if progress.filesFailed == 0 && !progress.hasDirectoryFailures {
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
        stats.filesUploaded = progress.filesUploaded
        stats.filesSkipped = progress.filesSkipped
        stats.filesFailed = progress.filesFailed
        stats.bytesUploaded = progress.bytesUploaded
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }

}
