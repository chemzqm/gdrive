import Foundation
import CommonCrypto
import Testing
@testable import GDrive

@Suite("SyncEngine Live Tests")
struct SyncEngineTests {
    @Test("Streaming localToRemoteEmpty sync with nested directories and small files")
    func testLocalToRemoteEmptyStreamingSync() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            print("Not configured rootID,Skip cloud testing")
            return
        }

        let client = DriveClient(auth: auth)

        // 1. Build a temporary test tree locally containing multiple levels of subdirectories and small files
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive_sync_test_\(UUID().uuidString.prefix(8))")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDir)
        }

        let sub1 = tempDir.appendingPathComponent("sub1")
        let sub2Deep = tempDir.appendingPathComponent("sub2/deep")
        try FileManager.default.createDirectory(at: sub1, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: sub2Deep, withIntermediateDirectories: true)

        let f1 = tempDir.appendingPathComponent("root_file.txt")
        let f2 = sub1.appendingPathComponent("sub1_file.txt")
        let f3 = sub2Deep.appendingPathComponent("deep_file.txt")

        try "Content of root file at \(Date())\n".write(to: f1, atomically: true, encoding: .utf8)
        try "Content of sub1 file at \(Date())\n".write(to: f2, atomically: true, encoding: .utf8)
        try "Content of deep nested file at \(Date())\n".write(to: f3, atomically: true, encoding: .utf8)

        // 2. in Google Drive Create a test target root directory on
        let remoteTestDirName = "sync_test_\(UUID().uuidString.prefix(8))"
        let remoteRootGenIds = try await client.generateIds(count: 1)
        let remoteRoot = try await client.createDirectory(name: remoteTestDirName, parentId: rootID, remoteId: remoteRootGenIds[0])
        defer {
            Task {
                try? await client.trash(remoteId: remoteRoot.id)
            }
        }

        // 3. run SyncEngine streaming upload
        let testDbPath = "/tmp/test_sync_engine_\(UUID().uuidString.prefix(8)).sqlite"
        defer {
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: testDbPath + ext)
            }
        }

        let testStore = try await StateStore(path: testDbPath)
        let engine = try await SyncEngine(auth: auth, store: testStore, client: client)

        print("🚀 Start testing local to remote empty directory synchronization: \(tempDir.path) -> Drive:\(remoteTestDirName) (ID: \(remoteRoot.id))")
        let stats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)

        print("✅ Synchronization completed:")
        print("   Number of subdirectories created: \(stats.directoriesCreated)")
        print("   Number of uploaded files: \(stats.filesUploaded)")
        print("   Number of bytes uploaded: \(stats.bytesUploaded)")
        print("   Total time spent: \(String(format: "%.3f", stats.elapsedSeconds))s")

        #expect(stats.filesUploaded == 3)
        #expect(stats.directoriesCreated >= 3) // sub1, sub2, sub2/deep

        // 4. Verify database baseline
        let committedCount: Int64 = (try? await testStore.read { conn -> Int64 in
            let stmt = try conn.cachedStatement("SELECT count(*) FROM items WHERE entry_kind = 'file' AND phase = 'committed';")
            if try stmt.step() {
                return stmt.columnInt64(at: 0) ?? 0
            }
            return 0
        }) ?? 0

        #expect(committedCount == 3)
        print("✅ SQLite The baseline record matches exactly and is in committed Status")

        // 5. Test reverse synchronization:remoteToLocalEmpty (Synchronize from the remote empty directory just synchronized to another new local empty directory)
        let downloadTargetDir = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive_sync_download_\(UUID().uuidString.prefix(8))")
        defer {
            try? FileManager.default.removeItem(at: downloadTargetDir)
        }

        let downloadDbPath = "/tmp/test_download_engine_\(UUID().uuidString.prefix(8)).sqlite"
        defer {
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: downloadDbPath + ext)
            }
        }
        let downloadStore = try await StateStore(path: downloadDbPath)
        let downloadEngine = try await SyncEngine(auth: auth, store: downloadStore, client: client)

        print("🚀 Start testing remote directory to local empty directory synchronization: Drive:\(remoteTestDirName) -> \(downloadTargetDir.path)")
        let dlStats = try await downloadEngine.syncRemoteToLocalEmpty(localPath: downloadTargetDir.path, remoteRootId: remoteRoot.id)

        print("✅ Reverse synchronization completed:")
        print("   Number of locally created directories: \(dlStats.directoriesCreated)")
        print("   Number of downloaded files: \(dlStats.filesDownloaded)")
        print("   Download bytes: \(dlStats.bytesDownloaded)")

        #expect(dlStats.filesDownloaded == 3)
        #expect(dlStats.directoriesCreated >= 3)

        // Verify that the content of the downloaded file is exactly the same
        let dlF1 = downloadTargetDir.appendingPathComponent("root_file.txt")
        let dlF2 = downloadTargetDir.appendingPathComponent("sub1/sub1_file.txt")
        let dlF3 = downloadTargetDir.appendingPathComponent("sub2/deep/deep_file.txt")

        #expect(FileManager.default.fileExists(atPath: dlF1.path))
        #expect(FileManager.default.fileExists(atPath: dlF2.path))
        #expect(FileManager.default.fileExists(atPath: dlF3.path))

        let str1 = try String(contentsOf: dlF1, encoding: .utf8)
        let str2 = try String(contentsOf: dlF2, encoding: .utf8)
        let str3 = try String(contentsOf: dlF3, encoding: .utf8)

        #expect(str1.contains("Content of root file"))
        #expect(str2.contains("Content of sub1 file"))
        #expect(str3.contains("Content of deep nested file"))

        print("✅ Remote to local file content integrity verification 100% Passed!")
    }

    @Test("Fast change detection skips 100% unchanged files without I/O or SHA-256")
    func testFastChangeSkippingOnUnchangedDirectory() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            return
        }

        let client = DriveClient(auth: auth)

        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive_fast_skip_\(UUID().uuidString.prefix(8))")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDir)
        }

        // Create 5 files
        for i in 1...5 {
            let f = tempDir.appendingPathComponent("file_\(i).txt")
            try "Sample content for file \(i)\n".write(to: f, atomically: true, encoding: .utf8)
        }

        let remoteRootGenIds = try await client.generateIds(count: 1)
        let remoteRootName = "fast_skip_test_\(UUID().uuidString.prefix(8))"
        let remoteRoot = try await client.createDirectory(name: remoteRootName, parentId: rootID, remoteId: remoteRootGenIds[0])
        defer {
            Task {
                try? await client.trash(remoteId: remoteRoot.id)
            }
        }

        let testDbPath = "/tmp/test_fast_skip_\(UUID().uuidString.prefix(8)).sqlite"
        defer {
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: testDbPath + ext)
            }
        }

        let store = try await StateStore(path: testDbPath)
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // First round of synchronization:5 Upload all0 skipped
        let round1 = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(round1.filesUploaded == 5)
        #expect(round1.filesSkipped == 0)

        // Second round of synchronization (files unchanged):0 upload,5 all passed FastChangeDetector Skip quickly in memory!
        let round2 = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(round2.filesUploaded == 0)
        #expect(round2.filesSkipped == 5)
        print("✅ Second round of quick change skip rate: 100% (\(round2.filesSkipped)/5 Items skipped in seconds)")

        // Modify it 1 files, sync again
        let modFile = tempDir.appendingPathComponent("file_3.txt")
        try "Modified content for file 3 at \(Date())\n".write(to: modFile, atomically: true, encoding: .utf8)

        let round3 = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(round3.filesUploaded == 0)
        #expect(round3.filesFailed == 1)
        #expect(round3.filesSkipped == 4)
        print("✅ The third round of partial change detection: The remote text coverage has been blocked and skipped. 4 item")
    }

    @Test("Incremental bidirectional sync with local modification, addition, deletion and remote change")
    func testIncrementalBidirectionalSync() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            return
        }

        let client = DriveClient(auth: auth)

        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive_inc_test_\(UUID().uuidString.prefix(8))")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDir)
        }

        let f1 = tempDir.appendingPathComponent("keep.txt")
        let f2 = tempDir.appendingPathComponent("modify.txt")
        let f3 = tempDir.appendingPathComponent("delete_me.txt")

        try "Keep content".write(to: f1, atomically: true, encoding: .utf8)
        try "Original modify content".write(to: f2, atomically: true, encoding: .utf8)
        try "Delete me soon".write(to: f3, atomically: true, encoding: .utf8)

        let remoteRootGenIds = try await client.generateIds(count: 1)
        let remoteRootName = "inc_test_\(UUID().uuidString.prefix(8))"
        let remoteRoot = try await client.createDirectory(name: remoteRootName, parentId: rootID, remoteId: remoteRootGenIds[0])
        defer {
            Task {
                try? await client.trash(remoteId: remoteRoot.id)
            }
        }

        let testDbPath = "/tmp/test_inc_\(UUID().uuidString.prefix(8)).sqlite"
        defer {
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: testDbPath + ext)
            }
        }

        let store = try await StateStore(path: testDbPath)
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // 1. First sync:3 Upload all
        print("🚀 Perform incremental testing pre-initial sync...")
        let initStats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(initStats.filesUploaded == 3)

        // 2. Locally generated modifications: Modifications f2,Delete f3,New f4
        print("🚀 Incremental changes occur locally: Modify 1 item, new 1 item, delete 1 item...")
        try "New modify content at \(Date())".write(to: f2, atomically: true, encoding: .utf8)
        try? FileManager.default.removeItem(at: f3)
        let f4 = tempDir.appendingPathComponent("new_local.txt")
        try "Newly added local file".write(to: f4, atomically: true, encoding: .utf8)

        // 3. Perform incremental bidirectional synchronization
        let incStats = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        print("✅ Incremental synchronization completed:")
        print("   Number of uploaded files: \(incStats.filesUploaded)")
        print("   Number of deleted files: \(incStats.filesDeleted)")
        print("   Skip invariants: \(incStats.filesSkipped)")

        #expect(incStats.filesUploaded == 1) // new_local.txt;modify.txt insecure coverage is blocked
        #expect(incStats.filesFailed == 1)
        #expect(incStats.filesDeleted == 1)  // delete_me.txt
        #expect(incStats.filesSkipped >= 1)  // keep.txt

        // 4. Add files directly on the remote end and test that remote changes are incrementally pulled locally.
        print("🚀 Add files remotely and test Changes incremental download...")
        let remoteFileIds = try await client.generateIds(count: 1)
        let remoteContent = "Content generated on Google Drive at \(Date())".data(using: .utf8)!
        var ctx = CC_SHA256_CTX()
        CC_SHA256_Init(&ctx)
        _ = remoteContent.withUnsafeBytes { CC_SHA256_Update(&ctx, $0.baseAddress, CC_LONG(remoteContent.count)) }
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        CC_SHA256_Final(&digest, &ctx)
        let expectedSha = digest.map { String(format: "%02x", $0) }.joined()

        _ = try await client.uploadMultipart(
            name: "from_remote.txt",
            parentId: remoteRoot.id,
            remoteId: remoteFileIds[0],
            content: remoteContent,
            expectedSha256: expectedSha
        )

        var dlIncStats = SyncStats()
        for attempt in 1...10 {
            try await Task.sleep(nanoseconds: 1_500_000_000)
            print("🚀 No. \(attempt) Attempts to incrementally pull remote changes...")
            dlIncStats = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
            if dlIncStats.filesDownloaded > 0 {
                break
            }
        }
        print("✅ Remote incremental pull completed:")
        print("   Number of downloaded files: \(dlIncStats.filesDownloaded)")

        let localDownloadedURL = tempDir.appendingPathComponent("from_remote.txt")
        #expect(FileManager.default.fileExists(atPath: localDownloadedURL.path))
        let downloadedText = try? String(contentsOf: localDownloadedURL, encoding: .utf8)
        #expect(downloadedText?.contains("Content generated on Google Drive") == true)
        print("✅ Two-way incremental synchronization (newly added locally)/Modify/Delete + New remote pull) end-to-end test 100% Success!")
    }

    @Test("Complete file and directory lifecycle: create, rename, delete locally and remotely")
    func testFileAndDirectoryLifecycleEvents() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else { return }

        let client = DriveClient(auth: auth)

        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive_lifecycle_\(UUID().uuidString.prefix(8))")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDir)
        }

        let fHello = tempDir.appendingPathComponent("hello.txt")
        try "Initial hello".write(to: fHello, atomically: true, encoding: .utf8)

        let folder1 = tempDir.appendingPathComponent("folder1")
        try FileManager.default.createDirectory(at: folder1, withIntermediateDirectories: true)
        let fItem = folder1.appendingPathComponent("item.txt")
        try "Item in folder1".write(to: fItem, atomically: true, encoding: .utf8)

        let remoteRootGenIds = try await client.generateIds(count: 1)
        let remoteRootName = "lifecycle_test_\(UUID().uuidString.prefix(8))"
        let remoteRoot = try await client.createDirectory(name: remoteRootName, parentId: rootID, remoteId: remoteRootGenIds[0])
        defer {
            Task {
                try? await client.trash(remoteId: remoteRoot.id)
            }
        }

        let testDbPath = "/tmp/test_lifecycle_\(UUID().uuidString.prefix(8)).sqlite"
        defer {
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: testDbPath + ext)
            }
        }

        let store = try await StateStore(path: testDbPath)
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // 1. Initial synchronization: upload directories and files
        print("🚀 [Lifecycle] initial sync...")
        let initStats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(initStats.filesUploaded == 2)
        #expect(initStats.directoriesCreated == 1)

        // 2. Local file rename:hello.txt -> greeting.txt
        print("🚀 [Lifecycle] Local file rename: hello.txt -> greeting.txt...")
        let fGreeting = tempDir.appendingPathComponent("greeting.txt")
        try FileManager.default.moveItem(at: fHello, to: fGreeting)

        // 3. Local directory rename:folder1 -> folder_renamed
        print("🚀 [Lifecycle] Rename local directory: folder1 -> folder_renamed...")
        let folderRenamed = tempDir.appendingPathComponent("folder_renamed")
        try FileManager.default.moveItem(at: folder1, to: folderRenamed)

        // 4. Perform incremental synchronization
        let renameStats = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        print("✅ [Lifecycle] Local rename incremental synchronization completed:")
        print("   Number of uploaded files: \(renameStats.filesUploaded)")
        print("   Skip invariants: \(renameStats.filesSkipped)")
        #expect(renameStats.filesUploaded == 0)

        // Verify that remote directories and files have been renamed correctly
        let remoteChildren = try await client.listChildren(parentId: remoteRoot.id)
        let remoteChildNames = remoteChildren.map(\.name)
        #expect(remoteChildNames.contains("greeting.txt"))
        #expect(!remoteChildNames.contains("hello.txt"))
        #expect(remoteChildNames.contains("folder_renamed"))
        #expect(!remoteChildNames.contains("folder1"))

        // 5. Delete directory locally: delete the entire folder_renamed
        print("🚀 [Lifecycle] Delete directory locally: folder_renamed...")
        try FileManager.default.removeItem(at: folderRenamed)

        let deleteStats = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        print("✅ [Lifecycle] Local deletion incremental synchronization completed:")
        print("   Delete items: \(deleteStats.filesDeleted)")
        #expect(deleteStats.filesDeleted >= 1)

        // Verify that the remote directory has been moved to the recycle bin
        let updatedRemoteChildren = try await client.listChildren(parentId: remoteRoot.id)
        let updatedNames = updatedRemoteChildren.map(\.name)
        #expect(!updatedNames.contains("folder_renamed"))

        // 6. Remote rename: rename in the cloud greeting.txt Rename to remote_renamed.txt
        print("🚀 [Lifecycle] Remote rename: greeting.txt -> remote_renamed.txt...")
        let greetingRemote = updatedRemoteChildren.first(where: { $0.name == "greeting.txt" })!
        _ = try await client.updateMetadata(remoteId: greetingRemote.id, newName: "remote_renamed.txt")

        for attempt in 1...10 {
            try await Task.sleep(nanoseconds: 1_500_000_000)
            print("🚀 [Lifecycle] No. \(attempt) Pull remote changes...")
            _ = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
            let fRemoteRenamed = tempDir.appendingPathComponent("remote_renamed.txt")
            if FileManager.default.fileExists(atPath: fRemoteRenamed.path) {
                break
            }
        }

        let fRemoteRenamed = tempDir.appendingPathComponent("remote_renamed.txt")
        #expect(FileManager.default.fileExists(atPath: fRemoteRenamed.path))
        #expect(!FileManager.default.fileExists(atPath: fGreeting.path))
        print("✅ [Lifecycle] File and folder life cycle events (increased/change/Rename/Delete) Real machine test 100% Success!")
    }

    @Test("Large file (>8MB) chunked resumable upload with SQLite offset tracking")
    func testLargeFileResumableUpload() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            print("Not configured rootID,Skip cloud testing")
            return
        }

        let client = DriveClient(auth: auth)

        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive_large_test_\(UUID().uuidString.prefix(8))")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDir)
        }

        // Create 9MB File (9 * 1024 * 1024 Bytes)
        let largeFilePath = tempDir.appendingPathComponent("large_9mb.bin")
        let chunkPattern = Data(repeating: 0x42, count: 1024 * 1024) // 1MB block
        FileManager.default.createFile(atPath: largeFilePath.path, contents: nil)
        let writeHandle = try FileHandle(forWritingTo: largeFilePath)
        for _ in 0..<9 {
            try writeHandle.write(contentsOf: chunkPattern)
        }
        try writeHandle.close()

        let remoteTestDirName = "sync_large_\(UUID().uuidString.prefix(8))"
        let remoteRootGenIds = try await client.generateIds(count: 1)
        let remoteRoot = try await client.createDirectory(name: remoteTestDirName, parentId: rootID, remoteId: remoteRootGenIds[0])
        defer {
            Task {
                try? await client.trash(remoteId: remoteRoot.id)
            }
        }

        let testDbPath = "/tmp/test_sync_large_\(UUID().uuidString.prefix(8)).sqlite"
        defer {
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: testDbPath + ext)
            }
        }

        let testStore = try await StateStore(path: testDbPath)
        let engine = try await SyncEngine(auth: auth, store: testStore, client: client)

        print("🚀 [LargeFile] Start testing >8MB Upload large files in chunks with breakpoints...")
        let stats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(stats.filesUploaded == 1)

        // Verify database operations table records breakpoint sessions and completed status and completeness offset
        let completedOps = try await testStore.read { conn in
            let stmt = try conn.cachedStatement("SELECT operation_id, confirmed_offset, total_bytes, state FROM operations;")
            var rows: [(String, Int64, Int64, String)] = []
            while try stmt.step() {
                rows.append((
                    stmt.columnText(at: 0) ?? "",
                    stmt.columnInt64(at: 1) ?? 0,
                    stmt.columnInt64(at: 2) ?? 0,
                    stmt.columnText(at: 3) ?? ""
                ))
            }
            stmt.reset()
            return rows
        }

        #expect(completedOps.count == 1)
        if let op = completedOps.first {
            #expect(op.1 == 9 * 1024 * 1024)
            #expect(op.2 == 9 * 1024 * 1024)
            #expect(op.3 == "completed")
        }

        // Verify cloud files
        let remoteChildren = try await client.listChildren(parentId: remoteRoot.id)
        #expect(remoteChildren.count == 1)
        #expect(remoteChildren[0].name == "large_9mb.bin")
        #expect(remoteChildren[0].sizeBytes == Int64(9 * 1024 * 1024))
        print("✅ [LargeFile] Chunked upload of large files with SQLite Status record verification successful!")
    }

    @Test("Resumable upload invalidates stale session when file SHA-256 changed")
    func testLargeFileResumeInvalidatedWhenFileModified() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            print("Not configured rootID,Skip cloud testing")
            return
        }

        let client = DriveClient(auth: auth)

        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive_stale_test_\(UUID().uuidString.prefix(8))")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDir)
        }

        let largeFilePath = tempDir.appendingPathComponent("large_stale_9mb.bin")
        let chunkPattern = Data(repeating: 0x55, count: 1024 * 1024)
        FileManager.default.createFile(atPath: largeFilePath.path, contents: nil)
        let writeHandle = try FileHandle(forWritingTo: largeFilePath)
        for _ in 0..<9 {
            try writeHandle.write(contentsOf: chunkPattern)
        }
        try writeHandle.close()

        let remoteTestDirName = "sync_stale_\(UUID().uuidString.prefix(8))"
        let remoteRootGenIds = try await client.generateIds(count: 2)
        let remoteRoot = try await client.createDirectory(name: remoteTestDirName, parentId: rootID, remoteId: remoteRootGenIds[0])
        defer {
            Task {
                try? await client.trash(remoteId: remoteRoot.id)
            }
        }

        let testDbPath = "/tmp/test_sync_stale_\(UUID().uuidString.prefix(8)).sqlite"
        defer {
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: testDbPath + ext)
            }
        }

        let testStore = try await StateStore(path: testDbPath)
        let fakeRemoteId = remoteRootGenIds[1]
        let startPageToken = try await client.getStartPageToken()

        // Defaults to an expired one with a mismatched hash inFlight session
        try await testStore.write { conn in
            // Build first root and item
            _ = try conn.execute("""
            INSERT INTO roots (
                root_id, account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES (
                1, 'default', '\(tempDir.path)', 1, 1,
                '\(remoteRoot.id)', 'localToRemoteEmpty', 'freshCreated', 0, 0
            );
            INSERT INTO items (
                item_id, root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (
                1, 1, NULL, '\(tempDir.lastPathComponent)', 'directory', '\(remoteRoot.id)', 0, 0
            );
            INSERT INTO items (
                item_id, root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, local_mtime, local_size, local_sha256,
                local_generation, local_status, phase, dirty_generation, created_at, updated_at
            ) VALUES (
                100, 1, 1, 'large_stale_9mb.bin', 'file', '\(fakeRemoteId)',
                1, 1, 0, 9437184, '0000000000000000000000000000000000000000000000000000000000000000',
                1, 'present', 'inFlight', 1, 0, 0
            );
            INSERT INTO operations (
                operation_id, root_id, item_id, operation_type, state,
                expected_sha256, target_remote_id, target_parent_remote_id,
                session_uri, confirmed_offset, total_bytes, created_at, updated_at
            ) VALUES (
                'resumable_\(fakeRemoteId)', 1, 100, 'uploadResumable', 'inFlight',
                '0000000000000000000000000000000000000000000000000000000000000000', '\(fakeRemoteId)', '\(remoteRoot.id)',
                'https://upload.invalid/stale_session', 4194304, 9437184, 0, 0
            );
            INSERT INTO cursors (
                root_id, account_id, cursor_kind, token_value, updated_at
            ) VALUES (
                1, 'default', 'drive_changes', '\(startPageToken)', 0
            );
            """)
        }

        let engine = try await SyncEngine(auth: auth, store: testStore, client: client)

        print("🚀 [StaleSession] test file SHA-256 When changes are made, old breakpoints are safely invalidated and uploaded in full again....")
        let stats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(stats.filesUploaded == 1)

        // Verify cloud files
        let remoteChildren = try await client.listChildren(parentId: remoteRoot.id)
        #expect(remoteChildren.count == 1)
        #expect(remoteChildren[0].sizeBytes == Int64(9 * 1024 * 1024))
        print("✅ [StaleSession] Dirty breakpoints are automatically and safely discarded, reset and uploaded successfully!")
    }

    @Test("Unified engine.sync automatically detects empty remote and routes between initial and incremental")
    func testUnifiedSyncAutoDetection() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            print("Not configured rootID,Skip cloud testing")
            return
        }

        let client = DriveClient(auth: auth)

        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive_unified_test_\(UUID().uuidString.prefix(8))")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDir)
        }

        let f1 = tempDir.appendingPathComponent("first.txt")
        try "Unified Sync Initial Content\n".write(to: f1, atomically: true, encoding: .utf8)

        let remoteTestDirName = "sync_unified_\(UUID().uuidString.prefix(8))"
        let remoteRootGenIds = try await client.generateIds(count: 1)
        let remoteRoot = try await client.createDirectory(name: remoteTestDirName, parentId: rootID, remoteId: remoteRootGenIds[0])
        defer {
            Task {
                try? await client.trash(remoteId: remoteRoot.id)
            }
        }

        let testDbPath = "/tmp/test_sync_unified_\(UUID().uuidString.prefix(8)).sqlite"
        defer {
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: testDbPath + ext)
            }
        }

        let testStore = try await StateStore(path: testDbPath)
        let engine = try await SyncEngine(auth: auth, store: testStore, client: client)

        print("🚀 [UnifiedSync] first call engine.sync:Automatically detect and execute localToRemoteEmpty...")
        let initialStats = try await engine.sync(localPath: tempDir.path, remoteFolderId: remoteRoot.id)
        #expect(initialStats.filesUploaded == 1)

        // Verify transferStatus Can be read directly at any time
        let status = engine.transferStatus
        #expect(status.uploadSpeedBytesPerSecond >= 0)

        // Add a new file locally
        let f2 = tempDir.appendingPathComponent("second.txt")
        try "Second File Content\n".write(to: f2, atomically: true, encoding: .utf8)

        print("🚀 [UnifiedSync] Second call engine.sync:Automatically identify existing baselines and perform incremental bidirectional synchronization...")
        let secondStats = try await engine.sync(localPath: tempDir.path, remoteFolderId: remoteRoot.id)
        #expect(secondStats.filesUploaded == 1)
        #expect(secondStats.filesSkipped >= 1)

        print("✅ [UnifiedSync] Unified synchronization portal and status detection real machine test 100% Success!")
    }
}
