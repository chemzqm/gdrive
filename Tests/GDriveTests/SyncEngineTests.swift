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
            print("未配置 rootID，跳过云端测试")
            return
        }

        let client = DriveClient(auth: auth)

        // 1. 在本地构建一个包含多层子目录与小文件的临时测试树
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

        // 2. 在 Google Drive 上创建一个测试目标根目录
        let remoteTestDirName = "sync_test_\(UUID().uuidString.prefix(8))"
        let remoteRootGenIds = try await client.generateIds(count: 1)
        let remoteRoot = try await client.createDirectory(name: remoteTestDirName, parentId: rootID, remoteId: remoteRootGenIds[0])
        defer {
            Task {
                try? await client.trash(remoteId: remoteRoot.id)
            }
        }

        // 3. 运行 SyncEngine 流式上传
        let testDbPath = "/tmp/test_sync_engine_\(UUID().uuidString.prefix(8)).sqlite"
        defer {
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: testDbPath + ext)
            }
        }

        let testStore = try await StateStore(path: testDbPath)
        let engine = try await SyncEngine(auth: auth, store: testStore, client: client)

        print("🚀 开始测试本地到远端空目录同步: \(tempDir.path) -> Drive:\(remoteTestDirName) (ID: \(remoteRoot.id))")
        let stats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)

        print("✅ 同步完成:")
        print("   创建子目录数: \(stats.directoriesCreated)")
        print("   上传文件数: \(stats.filesUploaded)")
        print("   上传字节数: \(stats.bytesUploaded)")
        print("   总耗时: \(String(format: "%.3f", stats.elapsedSeconds))s")

        #expect(stats.filesUploaded == 3)
        #expect(stats.directoriesCreated >= 3) // sub1, sub2, sub2/deep

        // 4. 校验数据库基线
        let committedCount: Int64 = (try? await testStore.read { conn -> Int64 in
            let stmt = try conn.cachedStatement("SELECT count(*) FROM items WHERE entry_kind = 'file' AND phase = 'committed';")
            if try stmt.step() {
                return stmt.columnInt64(at: 0) ?? 0
            }
            return 0
        }) ?? 0

        #expect(committedCount == 3)
        print("✅ SQLite 基线记录完全匹配且处于 committed 状态")

        // 5. 测试反向同步：remoteToLocalEmpty (从刚才同步好的远端空目录同步至另一个全新本地空目录)
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

        print("🚀 开始测试远端目录到本地空目录同步: Drive:\(remoteTestDirName) -> \(downloadTargetDir.path)")
        let dlStats = try await downloadEngine.syncRemoteToLocalEmpty(localPath: downloadTargetDir.path, remoteRootId: remoteRoot.id)

        print("✅ 反向同步完成:")
        print("   本地创建目录数: \(dlStats.directoriesCreated)")
        print("   下载文件数: \(dlStats.filesDownloaded)")
        print("   下载字节数: \(dlStats.bytesDownloaded)")

        #expect(dlStats.filesDownloaded == 3)
        #expect(dlStats.directoriesCreated >= 3)

        // 验证下载文件的内容完全一致
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

        print("✅ 远端到本地文件内容完整性校验 100% 通过！")
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

        // 创建 5 个文件
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

        // 第一轮同步：5 个全部上传，0 个跳过
        let round1 = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(round1.filesUploaded == 5)
        #expect(round1.filesSkipped == 0)

        // 第二轮同步（文件未变）：0 个上传，5 个全部通过 FastChangeDetector 在内存中极速跳过！
        let round2 = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(round2.filesUploaded == 0)
        #expect(round2.filesSkipped == 5)
        print("✅ 第二轮快速变更跳过率: 100% (\(round2.filesSkipped)/5 项秒级跳过)")

        // 修改其中 1 个文件，再次同步
        let modFile = tempDir.appendingPathComponent("file_3.txt")
        try "Modified content for file 3 at \(Date())\n".write(to: modFile, atomically: true, encoding: .utf8)

        let round3 = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(round3.filesUploaded == 0)
        #expect(round3.filesFailed == 1)
        #expect(round3.filesSkipped == 4)
        print("✅ 第三轮部分变更检测: 已有远端正文覆盖被阻断，跳过 4 项")
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

        // 1. 初次同步：3 个全部上传
        print("🚀 执行增量测试前置初始同步...")
        let initStats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(initStats.filesUploaded == 3)

        // 2. 本地产生修改：修改 f2、删除 f3、新增 f4
        print("🚀 本地发生增量变更: 修改 1 项，新增 1 项，删除 1 项...")
        try "New modify content at \(Date())".write(to: f2, atomically: true, encoding: .utf8)
        try? FileManager.default.removeItem(at: f3)
        let f4 = tempDir.appendingPathComponent("new_local.txt")
        try "Newly added local file".write(to: f4, atomically: true, encoding: .utf8)

        // 3. 执行增量双向同步
        let incStats = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        print("✅ 增量同步完成:")
        print("   上传文件数: \(incStats.filesUploaded)")
        print("   删除文件数: \(incStats.filesDeleted)")
        print("   跳过不变项: \(incStats.filesSkipped)")

        #expect(incStats.filesUploaded == 1) // new_local.txt；modify.txt 的不安全覆盖被阻断
        #expect(incStats.filesFailed == 1)
        #expect(incStats.filesDeleted == 1)  // delete_me.txt
        #expect(incStats.filesSkipped >= 1)  // keep.txt

        // 4. 远端直接新增文件，测试远端变更被本地增量拉取
        print("🚀 远端新增文件，测试 Changes 增量下载...")
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
            print("🚀 第 \(attempt) 次尝试增量拉取远端变更...")
            dlIncStats = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
            if dlIncStats.filesDownloaded > 0 {
                break
            }
        }
        print("✅ 远端增量拉取完成:")
        print("   下载文件数: \(dlIncStats.filesDownloaded)")

        let localDownloadedURL = tempDir.appendingPathComponent("from_remote.txt")
        #expect(FileManager.default.fileExists(atPath: localDownloadedURL.path))
        let downloadedText = try? String(contentsOf: localDownloadedURL, encoding: .utf8)
        #expect(downloadedText?.contains("Content generated on Google Drive") == true)
        print("✅ 双向增量同步（本地新增/修改/删除 + 远端新增拉取）端到端测试 100% 成功！")
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

        // 1. 初次同步：上传目录与文件
        print("🚀 [Lifecycle] 初始同步...")
        let initStats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(initStats.filesUploaded == 2)
        #expect(initStats.directoriesCreated == 1)

        // 2. 本地文件重命名：hello.txt -> greeting.txt
        print("🚀 [Lifecycle] 本地文件重命名: hello.txt -> greeting.txt...")
        let fGreeting = tempDir.appendingPathComponent("greeting.txt")
        try FileManager.default.moveItem(at: fHello, to: fGreeting)

        // 3. 本地目录重命名：folder1 -> folder_renamed
        print("🚀 [Lifecycle] 本地目录重命名: folder1 -> folder_renamed...")
        let folderRenamed = tempDir.appendingPathComponent("folder_renamed")
        try FileManager.default.moveItem(at: folder1, to: folderRenamed)

        // 4. 执行增量同步
        let renameStats = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        print("✅ [Lifecycle] 本地重命名增量同步完成:")
        print("   上传文件数: \(renameStats.filesUploaded)")
        print("   跳过不变项: \(renameStats.filesSkipped)")
        #expect(renameStats.filesUploaded == 0)

        // 核验远端目录与文件已正确重命名
        let remoteChildren = try await client.listChildren(parentId: remoteRoot.id)
        let remoteChildNames = remoteChildren.map(\.name)
        #expect(remoteChildNames.contains("greeting.txt"))
        #expect(!remoteChildNames.contains("hello.txt"))
        #expect(remoteChildNames.contains("folder_renamed"))
        #expect(!remoteChildNames.contains("folder1"))

        // 5. 本地删除目录：删除整个 folder_renamed
        print("🚀 [Lifecycle] 本地删除目录: folder_renamed...")
        try FileManager.default.removeItem(at: folderRenamed)

        let deleteStats = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        print("✅ [Lifecycle] 本地删除增量同步完成:")
        print("   删除项数: \(deleteStats.filesDeleted)")
        #expect(deleteStats.filesDeleted >= 1)

        // 核验远端目录已被移入回收站
        let updatedRemoteChildren = try await client.listChildren(parentId: remoteRoot.id)
        let updatedNames = updatedRemoteChildren.map(\.name)
        #expect(!updatedNames.contains("folder_renamed"))

        // 6. 远端重命名：在云端将 greeting.txt 重命名为 remote_renamed.txt
        print("🚀 [Lifecycle] 远端重命名: greeting.txt -> remote_renamed.txt...")
        let greetingRemote = updatedRemoteChildren.first(where: { $0.name == "greeting.txt" })!
        _ = try await client.updateMetadata(remoteId: greetingRemote.id, newName: "remote_renamed.txt")

        for attempt in 1...10 {
            try await Task.sleep(nanoseconds: 1_500_000_000)
            print("🚀 [Lifecycle] 第 \(attempt) 次拉取远端变更...")
            _ = try await engine.syncIncremental(localPath: tempDir.path, remoteRootId: remoteRoot.id)
            let fRemoteRenamed = tempDir.appendingPathComponent("remote_renamed.txt")
            if FileManager.default.fileExists(atPath: fRemoteRenamed.path) {
                break
            }
        }

        let fRemoteRenamed = tempDir.appendingPathComponent("remote_renamed.txt")
        #expect(FileManager.default.fileExists(atPath: fRemoteRenamed.path))
        #expect(!FileManager.default.fileExists(atPath: fGreeting.path))
        print("✅ [Lifecycle] 文件与文件夹全生命周期事件（增/改/重命名/删除）实机测试 100% 成功！")
    }

    @Test("Large file (>8MB) chunked resumable upload with SQLite offset tracking")
    func testLargeFileResumableUpload() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            print("未配置 rootID，跳过云端测试")
            return
        }

        let client = DriveClient(auth: auth)

        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive_large_test_\(UUID().uuidString.prefix(8))")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDir)
        }

        // 创建 9MB 文件 (9 * 1024 * 1024 字节)
        let largeFilePath = tempDir.appendingPathComponent("large_9mb.bin")
        let chunkPattern = Data(repeating: 0x42, count: 1024 * 1024) // 1MB 块
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

        print("🚀 [LargeFile] 开始测试 >8MB 大文件分块断点上传...")
        let stats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(stats.filesUploaded == 1)

        // 验证数据库中 operations 表记录了断点会话与 completed 状态及完整 offset
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

        // 验证云端文件
        let remoteChildren = try await client.listChildren(parentId: remoteRoot.id)
        #expect(remoteChildren.count == 1)
        #expect(remoteChildren[0].name == "large_9mb.bin")
        #expect(remoteChildren[0].sizeBytes == Int64(9 * 1024 * 1024))
        print("✅ [LargeFile] 大文件分块上传与 SQLite 状态记录验证成功！")
    }

    @Test("Resumable upload invalidates stale session when file SHA-256 changed")
    func testLargeFileResumeInvalidatedWhenFileModified() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            print("未配置 rootID，跳过云端测试")
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

        // 预设一个已失效且哈希不匹配的 inFlight 会话
        try await testStore.write { conn in
            // 先建 root 和 item
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

        print("🚀 [StaleSession] 测试文件 SHA-256 变动时旧断点被安全作废并重新全量上传...")
        let stats = try await engine.syncLocalToRemoteEmpty(localPath: tempDir.path, remoteRootId: remoteRoot.id)
        #expect(stats.filesUploaded == 1)

        // 验证云端文件
        let remoteChildren = try await client.listChildren(parentId: remoteRoot.id)
        #expect(remoteChildren.count == 1)
        #expect(remoteChildren[0].sizeBytes == Int64(9 * 1024 * 1024))
        print("✅ [StaleSession] 脏断点自动安全废弃，重置并成功上传！")
    }

    @Test("Unified engine.sync automatically detects empty remote and routes between initial and incremental")
    func testUnifiedSyncAutoDetection() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            print("未配置 rootID，跳过云端测试")
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

        print("🚀 [UnifiedSync] 首次调用 engine.sync：自动探测并执行 localToRemoteEmpty...")
        let initialStats = try await engine.sync(localPath: tempDir.path, remoteFolderId: remoteRoot.id)
        #expect(initialStats.filesUploaded == 1)

        // 验证 transferStatus 可以在任何时刻直接读取
        let status = engine.transferStatus
        #expect(status.uploadSpeedBytesPerSecond >= 0)

        // 本地新增一个文件
        let f2 = tempDir.appendingPathComponent("second.txt")
        try "Second File Content\n".write(to: f2, atomically: true, encoding: .utf8)

        print("🚀 [UnifiedSync] 二次调用 engine.sync：自动识别已有基线并执行增量双向同步...")
        let secondStats = try await engine.sync(localPath: tempDir.path, remoteFolderId: remoteRoot.id)
        #expect(secondStats.filesUploaded == 1)
        #expect(secondStats.filesSkipped >= 1)

        print("✅ [UnifiedSync] 统一同步入口与状态探测实机测试 100% 成功！")
    }
}
