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
        defer { notifier.stop() }

        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)
        try await validateRootBinding(
            localPath: resolvedLocalPath, remoteRootID: remoteRootId)

        // Verify local root directory
        let rootIdentity = try LocalDirectoryIdentity.require(
            at: rootURL,
            or: NSError(domain: "SyncEngine", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "The local path does not exist or is not a directory: \(resolvedLocalPath)"
            ]))

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
        let initialCursor = try await RemoteChanges.initialBootstrapCursor(
            client: client, rootExists: rootExists, initialToken: nil)
        if !rootExists {
            let existingChildren = try await client.listChildren(parentId: remoteRootId)
            guard existingChildren.isEmpty else {
                throw NSError(domain: "SyncEngine", code: 20, userInfo: [NSLocalizedDescriptionKey: "The remote target is not empty; localToRemoteEmpty cannot run: \(remoteRootId)"])
            }
        }

        let now = Date().timeIntervalSince1970

        // 1. Register or get Root Records and root entries
        let (rootId, rootItemId): (Int64, Int64) = try await store.write { conn in
            try Self.validateRootBinding(
                conn: conn, localPath: resolvedLocalPath, remoteRootID: remoteRootId)
            let rootStmt = try conn.cachedStatement("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', ?, ?, ?, ?, 'localToRemoteEmpty', 'freshCreated', ?, ?)
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
                throw NSError(domain: "SyncEngine", code: 3, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root_id"])
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
                throw NSError(domain: "SyncEngine", code: 4, userInfo: [NSLocalizedDescriptionKey: "Unable to obtain root item_id"])
            }
            itemQuery.reset()

            try RemoteChanges.insertInitialCursor(
                conn: conn, rootID: rId, token: initialCursor, now: now)

            return (rId, rItemId)
        }

        try await RemoteChanges(
            store: store, client: client, rootID: rootId, remoteRootID: remoteRootId,
            rootURL: rootURL
        ).recoverBootstrapCursorIfNeeded(maxConcurrency: maxUploadConcurrency)

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
        @Sendable func recordUploadFailure(
            _ error: Error, intent: DurableCreateIntent?, parentItemID: Int64?,
            relPath: String, name: String
        ) async {
            do {
                try await SyncIssueStore.record(
                    store: self.store, rootID: rootId,
                    subject: SyncIssueSubject(
                        itemID: intent?.itemID, remoteFileID: intent?.targetRemoteID,
                        relativePath: relPath),
                    stage: .upload, error: error)
            } catch {
                recordDatabaseError(error)
            }
            guard case DriveError.unsafeOverwrite = error, let parentItemID else { return }
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

                try await uploadSemaphore.wait()
                didAcquireSemaphore = true
                try Task.checkCancellation()
                if let error = databaseError.withLock({ $0 }) { throw error }

                let fileURL = URL(fileURLWithPath: fullPath)
                let input = try StableUploadInput.capture(at: fileURL)
                let sha256Hex = input.sha256
                let fileSize = input.size
                let smallContent = input.data

                self.monitor.startUpload(id: fullPath, name: name, totalBytes: fileSize)

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
                let expectedRemoteGeneration = existingTarget?.remoteGeneration ?? 0
                let expectedDirtyGeneration = max(existingTarget?.dirtyGeneration ?? 0, 1)
                if let existing = existingTarget,
                   let existingRemoteID = existing.remoteID,
                   existing.phase == "committed",
                   existing.remoteStatus == "present",
                   let content = smallContent {
                    let uploadedFile = try await self.client.updateMultipart(
                        remoteId: existingRemoteID,
                        content: content,
                        expectedSha256: sha256Hex
                    )
                    let expectation = FileUploadReceiptExpectation(
                        itemID: existing.itemID,
                        localGeneration: existing.localGeneration,
                        remoteGeneration: expectedRemoteGeneration,
                        dirtyGeneration: expectedDirtyGeneration
                    )
                    let receipt = try await DurableCreateIntentStore.commitFileUploadReceipt(
                        store: self.store,
                        expectation: expectation,
                        uploadedFile: uploadedFile,
                        input: input
                    )
                    guard case .applied = receipt else {
                        throw SyncEngineError.general(
                            "The upload receipt is stale; the newer generation remains pending: \(name)"
                        )
                    }
                } else {
                    let intent = try await self.prepareNewFileUpload(
                        rootID: rootId,
                        itemID: existingTarget?.itemID,
                        parentItemID: parentDirItemId,
                        name: name,
                        parentRemoteID: remoteParentId,
                        candidateRemoteID: existingTarget?.remoteID,
                        input: input,
                        pendingIntent: nil
                    )
                    createIntent = intent
                    let uploadedFile = try await self.performNewFileUpload(
                        rootID: rootId,
                        intent: intent,
                        input: input,
                        name: name,
                        multipartRecovery: .updatePersistedRemoteID
                    )
                    let receipt = try await DurableCreateIntentStore.commitFileUploadReceipt(
                        store: self.store,
                        expectation: FileUploadReceiptExpectation(
                            intent: intent,
                            localGeneration: existingTarget?.localGeneration
                                ?? intent.expectedLocalGeneration,
                            remoteGeneration: expectedRemoteGeneration,
                            dirtyGeneration: expectedDirtyGeneration
                        ),
                        uploadedFile: uploadedFile,
                        input: input
                    )
                    guard case .applied = receipt else {
                        throw SyncEngineError.general(
                            "The upload receipt is stale; the newer generation remains pending: \(name)"
                        )
                    }
                }
                self.monitor.reportUploadProgress(id: fullPath, additionalBytes: fileSize)
                progress.recordSuccess(bytes: fileSize)
            } catch {
                await recordUnknownOutcome(error, createIntent: createIntent)
                progress.recordFailure()
                await recordUploadFailure(
                    error, intent: createIntent, parentItemID: parentItemID,
                    relPath: relPath, name: name)
                self.logger.error("Failed to upload file [\(relPath)]: \(error)")
            }
        }

        @Sendable func createDirectory(relPath: String, parent: BootstrapDirectoryDependency, name: String, metadata: FileMetadata?) async throws -> BootstrapDirectoryTarget {
            var createIntent: DurableCreateIntent?
            do {
                try Task.checkCancellation()
                let parentTarget = try await parent.value()
                try Task.checkCancellation()
                try await directorySemaphore.wait()
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
                do {
                    try await SyncIssueStore.record(
                        store: self.store, rootID: rootId,
                        subject: SyncIssueSubject(
                            itemID: createIntent?.itemID,
                            remoteFileID: createIntent?.targetRemoteID,
                            relativePath: relPath),
                        stage: .createDirectory, error: error)
                } catch {
                    recordDatabaseError(error)
                }
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
                        taskRegistry.startDirectoryTask(for: relPath) {
                            try await createDirectory(relPath: relPath, parent: parent, name: name, metadata: metadata)
                        }
                    } else if type == .file {
                        let dev = Int64(metadata?.identity.device ?? 1)
                        let ino = Int64(metadata?.identity.inode ?? 0)
                        let mtime = (metadata?.modificationTime.seconds ?? 0) * 1_000_000_000 + Int64(metadata?.modificationTime.nanoseconds ?? 0)
                        let ctime = (metadata?.changeTime.seconds ?? 0) * 1_000_000_000 + Int64(metadata?.changeTime.nanoseconds ?? 0)
                        let fileSize = metadata?.fileSize ?? 0

                        // A shared inode does not prove this path has been uploaded.
                        // A ready parent supplies the path identity without waiting in the scanner.
                        if case .ready(let parentTarget) = parent,
                           baselineCache.lookupUnchanged(
                            device: dev, inode: ino, mtime: mtime, ctime: ctime,
                            size: fileSize, parentId: parentTarget.itemID, name: name) != nil {
                            progress.recordSkipped()
                            continue
                        }

                        // File handling: wait for its immediate parent directory to be ready and upload immediately
                        notifier.addDiscovered(files: 1, bytes: Int64(fileSize))
                        self.monitor.enqueueUpload(id: fullPath, name: name, totalBytes: Int64(fileSize))
                        taskRegistry.startFileTask {
                            await uploadFile(fullPath: fullPath, relPath: relPath, parent: parent, name: name, fileSize: Int64(fileSize))
                        }
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

        let converged = progress.filesFailed == 0 && !progress.hasDirectoryFailures
        if converged {
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                UPDATE roots SET bootstrap_state = 'existingKnown', updated_at = ? WHERE root_id = ?;
                """)
                stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                stmt.bindInt64(rootId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
            try await SyncIssueStore.clear(store: store, rootID: rootId)
        }

        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        stats.directoriesCreated = progress.dirsCreated
        stats.filesUploaded = progress.filesUploaded
        stats.filesSkipped = progress.filesSkipped
        stats.filesFailed = progress.filesFailed
        stats.issueCount = try await SyncIssueStore.count(store: store, rootID: rootId)
        stats.bytesUploaded = progress.bytesUploaded
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }

}
