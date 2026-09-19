import Testing
import Foundation
@testable import GDrive

final class MockBootstrapSafetyURLProtocol: URLProtocol, @unchecked Sendable {
    nonisolated(unsafe) static var requestHandler: (@Sendable (URLRequest) throws -> (HTTPURLResponse, Data))?
    private static let lock = NSLock()

    static func setHandler(_ handler: (@Sendable (URLRequest) throws -> (HTTPURLResponse, Data))?) {
        lock.lock()
        defer { lock.unlock() }
        requestHandler = handler
    }

    static func getHandler() -> (@Sendable (URLRequest) throws -> (HTTPURLResponse, Data))? {
        lock.lock()
        defer { lock.unlock() }
        return requestHandler
    }

    override class func canInit(with request: URLRequest) -> Bool {
        return true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        return request
    }

    override func startLoading() {
        guard let handler = Self.getHandler() else {
            client?.urlProtocol(self, didFailWithError: URLError(.badURL))
            return
        }

        do {
            let (response, data) = try handler(request)
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}

final class SafeCounter: @unchecked Sendable {
    private var val: Int
    private let lock = NSLock()

    init(_ val: Int) { self.val = val }

    func next(count: Int = 1) -> Int {
        lock.lock()
        defer { lock.unlock() }
        let cur = val
        val += count
        return cur
    }
}

final class RequestEventRecorder: @unchecked Sendable {
    private var _dirCreateCount = 0
    private var _fileCreateCount = 0
    private var _filePatchCount = 0
    private var _resumedRangeStarts: [Int64] = []
    private var _createdRemoteIds: [String] = []
    private let lock = NSLock()

    var dirCreateCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return _dirCreateCount
    }

    var fileCreateCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return _fileCreateCount
    }

    var filePatchCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return _filePatchCount
    }

    var resumedRangeStarts: [Int64] {
        lock.lock()
        defer { lock.unlock() }
        return _resumedRangeStarts
    }

    var createdRemoteIds: [String] {
        lock.lock()
        defer { lock.unlock() }
        return _createdRemoteIds
    }

    func recordDirCreate(id: String) {
        lock.lock()
        _dirCreateCount += 1
        _createdRemoteIds.append(id)
        lock.unlock()
    }

    func recordFileCreate(id: String) {
        lock.lock()
        _fileCreateCount += 1
        _createdRemoteIds.append(id)
        lock.unlock()
    }

    func recordFilePatch() {
        lock.lock()
        _filePatchCount += 1
        lock.unlock()
    }

    func recordResumedRange(start: Int64) {
        lock.lock()
        _resumedRangeStarts.append(start)
        lock.unlock()
    }
}

@Suite("Bootstrap and Resume Safety Tests (A08)", .serialized)
struct BootstrapResumeSafetyTests {

    private func createMockAuth(tempDir: URL) throws -> Auth {
        let authPath = tempDir.appendingPathComponent("auth.json").path
        let authData = AuthData(
            clientId: "mock_client",
            rootID: "mock_root",
            accessToken: "mock_access_token",
            expiresAt: Date().addingTimeInterval(3600)
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(authData)
        try data.write(to: URL(fileURLWithPath: authPath))
        return try Auth(path: authPath)
    }

    private func createMockClient(auth: Auth) -> DriveClient {
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [MockBootstrapSafetyURLProtocol.self]
        let session = URLSession(configuration: config)
        return DriveClient(auth: auth, session: session)
    }

    // MARK: - Test 1: LocalBaselineCache Excludes inFlight, Dirty, and Uncommitted Files

    @Test("LocalBaselineCache excludes inFlight, dirty, or uncommitted files")
    func testLocalBaselineCacheFilter() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a08_cache_test_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootId: Int64 = try await store.write { conn in
            let stmt = try conn.cachedStatement("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', '/tmp/mock_root', 1, 1, 'mock_root_remote', 'localToRemoteEmpty', 'freshCreated', 1, 1);
            """)
            _ = try stmt.step()
            stmt.reset()

            let q = try conn.cachedStatement("SELECT root_id FROM roots WHERE account_id = 'default' AND remote_root_id = 'mock_root_remote';")
            guard try q.step(), let rId = q.columnInt64(at: 0) else { throw NSError(domain: "test", code: 1) }
            q.reset()
            return rId
        }

        let sha256_64 = String(repeating: "a", count: 64)

        let rootItemId: Int64 = try await store.write { conn in
            let s = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (?, NULL, 'mock_root', 'directory', 'mock_root_remote', 1, 1);
            """)
            s.bindInt64(rootId, at: 1)
            _ = try s.step()
            s.reset()
            let q = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL;")
            q.bindInt64(rootId, at: 1)
            defer { q.reset() }
            if try q.step(), let id = q.columnInt64(at: 0) { return id }
            return 1
        }

        // 插入三种状态的文件：
        // 1. inFlight 状态的大文件（未提交基线，dirty_generation = 1）
        // 2. committed 但 dirty_generation = 1 的文件（本地被修改）
        // 3. committed、remote_status = 'present'、dirty_generation = 0 的已确认基线文件
        try await store.write { conn in
            // File 1: inFlight 未竟上传
            let s1 = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, local_mtime, local_size, local_sha256,
                phase, dirty_generation, created_at, updated_at
            ) VALUES (?, ?, 'inflight.bin', 'file', 'r_inflight', 1, 1001, 100, 500, ?, 'inFlight', 1, 1, 1);
            """)
            s1.bindInt64(rootId, at: 1)
            s1.bindInt64(rootItemId, at: 2)
            s1.bindText(sha256_64, at: 3)
            _ = try s1.step()
            s1.reset()

            // File 2: committed 但本地又有变更 (dirty_generation = 1)
            let s2 = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, local_mtime, local_size, local_sha256,
                base_sha256, base_size, remote_status, phase, dirty_generation, created_at, updated_at
            ) VALUES (?, ?, 'dirty.txt', 'file', 'r_dirty', 1, 1002, 200, 600, ?, ?, 600, 'present', 'committed', 1, 1, 1);
            """)
            s2.bindInt64(rootId, at: 1)
            s2.bindInt64(rootItemId, at: 2)
            s2.bindText(sha256_64, at: 3)
            s2.bindText(sha256_64, at: 4)
            _ = try s2.step()
            s2.reset()

            // File 3: 完全已同步基线 (phase = 'committed', dirty_generation = 0, base_sha256 != nil)
            let s3 = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, local_mtime, local_size, local_sha256,
                base_sha256, base_size, remote_status, phase, dirty_generation, created_at, updated_at
            ) VALUES (?, ?, 'synced.txt', 'file', 'r_synced', 1, 1003, 300, 700, ?, ?, 700, 'present', 'committed', 0, 1, 1);
            """)
            s3.bindInt64(rootId, at: 1)
            s3.bindInt64(rootItemId, at: 2)
            s3.bindText(sha256_64, at: 3)
            s3.bindText(sha256_64, at: 4)
            _ = try s3.step()
            s3.reset()
        }

        let cache = try await LocalBaselineCache.load(store: store, rootId: rootId)
        #expect(cache.count == 1)

        // 验证 lookupUnchanged
        #expect(cache.lookupUnchanged(device: 1, inode: 1001, mtime: 100, size: 500) == nil)
        #expect(cache.lookupUnchanged(device: 1, inode: 1002, mtime: 200, size: 600) == nil)
        let hit = cache.lookupUnchanged(device: 1, inode: 1003, mtime: 300, size: 700)
        #expect(hit != nil)
        #expect(hit?.name == "synced.txt")
        #expect(hit?.remoteFileId == "r_synced")
    }

    // MARK: - Test 2: Repeated Initialization Calls Do Not Duplicate Directories or Files

    @Test("Repeated initialization calls do not duplicate directories or files, remote IDs and counts remain stable")
    func testRepeatedInitializationStable() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a08_repeat_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        let subDir = localRootDir.appendingPathComponent("folderA/subFolder")
        try FileManager.default.createDirectory(at: subDir, withIntermediateDirectories: true)

        let file1 = subDir.appendingPathComponent("test1.txt")
        let file2 = localRootDir.appendingPathComponent("test2.txt")
        try "Content of file 1\n".write(to: file1, atomically: true, encoding: .utf8)
        try "Content of file 2\n".write(to: file2, atomically: true, encoding: .utf8)

        let dbPath = tempDir.appendingPathComponent("state.sqlite").path
        let store = try await StateStore(path: dbPath)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)

        let recorder = RequestEventRecorder()
        let idCounter = SafeCounter(100)

        MockBootstrapSafetyURLProtocol.setHandler { request in
            guard let url = request.url else {
                return (HTTPURLResponse(url: URL(string: "https://invalid")!, statusCode: 400, httpVersion: nil, headerFields: nil)!, Data())
            }
            let path = url.path

            if path.hasSuffix("/files/generateIds") {
                let count = 10
                let start = idCounter.next(count: count)
                let ids = (start..<(start + count)).map { "drive_id_\($0)" }
                let json = try! JSONSerialization.data(withJSONObject: ["ids": ids])
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if path.hasSuffix("/files/mock_remote_root") {
                let json = """
                {"id": "mock_remote_root", "name": "mock_remote_root", "mimeType": "application/vnd.google-apps.folder"}
                """.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if path.hasSuffix("/files") && request.httpMethod == "GET" {
                // listChildren of remote root
                let json = #"{"files": []}"#.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if path.hasSuffix("/changes/startPageToken") {
                let json = #"{"startPageToken": "token_1"}"#.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            // Multipart file upload (POST to /upload/drive/v3/files)
            if request.httpMethod == "POST" && url.absoluteString.contains("/upload/drive/v3/files") {
                let reqId = "uploaded_file_\(idCounter.next())"
                recorder.recordFileCreate(id: reqId)
                let body = request.extractBodyData ?? Data()
                let bodyStr = String(decoding: body, as: UTF8.self)
                let isFile1 = bodyStr.contains("file 1")
                let contentData = (isFile1 ? "Content of file 1\n" : "Content of file 2\n").data(using: .utf8)!
                let sha = SyncEngine.computeSha256(of: contentData)
                let name = isFile1 ? "test1.txt" : "test2.txt"
                let json = """
                {"id": "\(reqId)", "name": "\(name)", "size": "\(contentData.count)", "sha256Checksum": "\(sha)"}
                """.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            // Directory creation (POST to /drive/v3/files without /upload/)
            if request.httpMethod == "POST" && path.hasSuffix("/drive/v3/files") && !url.absoluteString.contains("/upload/") {
                var reqId = "unknown_dir_id"
                if let body = request.extractBodyData,
                   let dict = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                   let id = dict["id"] as? String {
                    reqId = id
                }
                recorder.recordDirCreate(id: reqId)
                let json = """
                {"id": "\(reqId)", "name": "dir", "mimeType": "application/vnd.google-apps.folder"}
                """.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, "{}".data(using: .utf8)!)
        }

        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // Round 1
        let round1 = try await engine.syncLocalToRemoteEmpty(localPath: localRootDir.path, remoteRootId: "mock_remote_root")
        #expect(round1.directoriesCreated == 2)
        #expect(round1.filesUploaded == 2)
        #expect(round1.filesSkipped == 0)
        #expect(round1.filesFailed == 0)

        // 验证 SQLite 中 bootstrap_state 已置为 existingKnown
        let bootstrapStateRound1: String? = try await store.read { conn in
            let s = try conn.cachedStatement("SELECT bootstrap_state FROM roots WHERE remote_root_id = 'mock_remote_root';")
            defer { s.reset() }
            if try s.step() { return s.columnText(at: 0) }
            return nil
        }
        #expect(bootstrapStateRound1 == "existingKnown")

        // 记录 Round 1 产生的所有 remote_file_id
        let itemRemoteIdsRound1: [String] = try await store.read { conn in
            let s = try conn.cachedStatement("SELECT remote_file_id FROM items WHERE is_tombstone = 0 AND parent_id IS NOT NULL ORDER BY item_id;")
            defer { s.reset() }
            var res: [String] = []
            while try s.step() {
                if let r = s.columnText(at: 0) { res.append(r) }
            }
            return res
        }
        #expect(itemRemoteIdsRound1.count == 4) // 2 dirs + 2 files

        // Round 2 (无任何变更再次运行初始化调用)
        let round2 = try await engine.syncLocalToRemoteEmpty(localPath: localRootDir.path, remoteRootId: "mock_remote_root")
        #expect(round2.directoriesCreated == 0)
        #expect(round2.filesUploaded == 0)
        #expect(round2.filesSkipped == 2)
        #expect(round2.filesFailed == 0)

        // Round 3
        let round3 = try await engine.syncLocalToRemoteEmpty(localPath: localRootDir.path, remoteRootId: "mock_remote_root")
        #expect(round3.directoriesCreated == 0)
        #expect(round3.filesUploaded == 0)
        #expect(round3.filesSkipped == 2)
        #expect(round3.filesFailed == 0)

        // 断言：服务端记录的目录创建请求总数严格等于 2，文件创建请求总数严格等于 2！
        #expect(recorder.dirCreateCount == 2)
        #expect(recorder.fileCreateCount == 2)

        // 断言：SQLite 中的 remote_file_id 序列完全未变，绝不重新生成 ID！
        let itemRemoteIdsRound3: [String] = try await store.read { conn in
            let s = try conn.cachedStatement("SELECT remote_file_id FROM items WHERE is_tombstone = 0 AND parent_id IS NOT NULL ORDER BY item_id;")
            defer { s.reset() }
            var res: [String] = []
            while try s.step() {
                if let r = s.columnText(at: 0) { res.append(r) }
            }
            return res
        }
        #expect(itemRemoteIdsRound3 == itemRemoteIdsRound1)
    }

    // MARK: - Test 3: Changed File Updates In-Place via updateMultipart Without Creating Duplicate Object

    @Test("Changed existing file remains pending without an unsafe remote overwrite (A11)")
    func testChangedFileUpdatesInPlace() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a08_change_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)

        let targetFile = localRootDir.appendingPathComponent("document.txt")
        let v1Content = "Version 1 Content\n"
        try v1Content.write(to: targetFile, atomically: true, encoding: .utf8)

        let dbPath = tempDir.appendingPathComponent("state.sqlite").path
        let store = try await StateStore(path: dbPath)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)

        let recorder = RequestEventRecorder()
        let fixedFileId = "remote_file_doc_100"

        MockBootstrapSafetyURLProtocol.setHandler { request in
            guard let url = request.url else {
                return (HTTPURLResponse(url: URL(string: "https://invalid")!, statusCode: 400, httpVersion: nil, headerFields: nil)!, Data())
            }
            let path = url.path

            if path.hasSuffix("/files/generateIds") {
                let json = #"{"ids": ["id_gen_1", "id_gen_2"]}"#.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }

            if path.hasSuffix("/files/mock_remote_root") {
                let json = """
                {"id": "mock_remote_root", "name": "mock_remote_root", "mimeType": "application/vnd.google-apps.folder"}
                """.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }

            if path.hasSuffix("/files") && request.httpMethod == "GET" {
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, #"{"files": []}"#.data(using: .utf8)!)
            }

            if path.hasSuffix("/changes/startPageToken") {
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, #"{"startPageToken": "token_1"}"#.data(using: .utf8)!)
            }

            // POST /upload/drive/v3/files (Multipart 创建新文件)
            if request.httpMethod == "POST" && url.absoluteString.contains("/upload/drive/v3/files") {
                recorder.recordFileCreate(id: fixedFileId)
                let v1Sha = SyncEngine.computeSha256(of: v1Content.data(using: .utf8)!)
                let json = """
                {"id": "\(fixedFileId)", "name": "document.txt", "size": "\(v1Content.utf8.count)", "sha256Checksum": "\(v1Sha)"}
                """.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            // PATCH /upload/drive/v3/files/{remoteId} (updateMultipart 更新已有文件)
            if request.httpMethod == "PATCH" && url.absoluteString.contains("/upload/drive/v3/files/\(fixedFileId)") {
                recorder.recordFilePatch()
                let body = request.extractBodyData ?? Data()
                let v2Sha = SyncEngine.computeSha256(of: body)
                let json = """
                {"id": "\(fixedFileId)", "name": "document.txt", "size": "\(body.count)", "sha256Checksum": "\(v2Sha)"}
                """.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, "{}".data(using: .utf8)!)
        }

        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // Round 1: 首次上传
        let round1 = try await engine.syncLocalToRemoteEmpty(localPath: localRootDir.path, remoteRootId: "mock_remote_root")
        #expect(round1.filesUploaded == 1)
        #expect(recorder.fileCreateCount == 1)
        #expect(recorder.filePatchCount == 0)

        // 修改文件内容
        let v2Content = "Version 2 Modified Content with different size and hash\n"
        try v2Content.write(to: targetFile, atomically: true, encoding: .utf8)

        // Round 2: 变更后重跑同步
        let round2 = try await engine.syncLocalToRemoteEmpty(localPath: localRootDir.path, remoteRootId: "mock_remote_root")
        #expect(round2.filesUploaded == 0)
        #expect(round2.filesFailed == 1)
        #expect(round2.filesSkipped == 0)

        // A11: only the original POST is allowed; no overwrite or duplicate creation.
        #expect(recorder.fileCreateCount == 1)
        #expect(recorder.filePatchCount == 0)

        // 断言 SQLite 中 items 表仅有 1 个文件项，且其 remote_file_id 保持为 fixedFileId
        let (itemCount, currentRemoteId, currentBaseSha): (Int, String?, String?) = try await store.read { conn in
            let s = try conn.cachedStatement("SELECT COUNT(*), remote_file_id, base_sha256 FROM items WHERE entry_kind = 'file' AND is_tombstone = 0;")
            defer { s.reset() }
            if try s.step() {
                return (Int(s.columnInt64(at: 0) ?? 0), s.columnText(at: 1), s.columnText(at: 2))
            }
            return (0, nil, nil)
        }
        #expect(itemCount == 1)
        #expect(currentRemoteId == fixedFileId)
        let expectedV1Sha = SyncEngine.computeSha256(of: v1Content.data(using: .utf8)!)
        #expect(currentBaseSha == expectedV1Sha)
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT phase, dirty_generation FROM items WHERE entry_kind = 'file';")
            #expect(try stmt.step())
            #expect(stmt.columnText(at: 0) == "blocked")
            #expect((stmt.columnInt64(at: 1) ?? 0) > 0)
        }
    }

    // MARK: - Test 4: Interrupted Resumable Upload Resumes Session and Reuses Remote ID

    @Test("Interrupted resumable upload resumes session and reuses remote ID")
    func testInterruptedResumableUploadResumes() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a08_resumable_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)

        // 创建一个 9MB 本地大文件
        let largeFilePath = localRootDir.appendingPathComponent("large_9mb.bin")
        let chunk1MB = Data(repeating: 0x42, count: 1024 * 1024)
        FileManager.default.createFile(atPath: largeFilePath.path, contents: nil)
        let handle = try FileHandle(forWritingTo: largeFilePath)
        for _ in 0..<9 {
            try handle.write(contentsOf: chunk1MB)
        }
        try handle.close()

        let (expectedSha256, totalSize) = try SyncEngine.computeFileSha256(at: largeFilePath)
        #expect(totalSize == 9 * 1024 * 1024)

        let dbPath = tempDir.appendingPathComponent("state.sqlite").path
        let store = try await StateStore(path: dbPath)

        let persistentRemoteId = "large_file_remote_42"
        let sessionURIString = "https://upload.invalid/resumable_session_42"

        // 预设未完成的断点与 inFlight 记录（模拟上次运行在 4MB 处中断）
        try await store.write { conn in
            _ = try conn.execute("""
            INSERT INTO roots (
                root_id, account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES (
                1, 'default', '\(localRootDir.path)', 1, 1,
                'mock_remote_root', 'localToRemoteEmpty', 'freshCreated', 1, 1
            );
            INSERT INTO items (
                item_id, root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (
                1, 1, NULL, 'local_root', 'directory', 'mock_remote_root', 1, 1
            );
            INSERT INTO items (
                item_id, root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, local_mtime, local_size, local_sha256,
                local_generation, local_status, phase, dirty_generation, created_at, updated_at
            ) VALUES (
                100, 1, 1, 'large_9mb.bin', 'file', '\(persistentRemoteId)',
                1, 1, 0, \(totalSize), '\(expectedSha256)',
                1, 'present', 'inFlight', 1, 1, 1
            );
            INSERT INTO operations (
                operation_id, root_id, item_id, operation_type, state,
                expected_sha256, target_remote_id, target_parent_remote_id,
                session_uri, confirmed_offset, total_bytes, created_at, updated_at
            ) VALUES (
                'resumable_\(persistentRemoteId)', 1, 100, 'uploadResumable', 'inFlight',
                '\(expectedSha256)', '\(persistentRemoteId)', 'mock_remote_root',
                '\(sessionURIString)', 4194304, \(totalSize), 1, 1
            );
            """)
        }

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let recorder = RequestEventRecorder()
        let resumedChunkAttempt = SafeCounter(0)

        MockBootstrapSafetyURLProtocol.setHandler { request in
            guard let url = request.url else {
                return (HTTPURLResponse(url: URL(string: "https://invalid")!, statusCode: 400, httpVersion: nil, headerFields: nil)!, Data())
            }
            let path = url.path

            if path.hasSuffix("/files/mock_remote_root") {
                let json = """
                {"id": "mock_remote_root", "name": "mock_remote_root", "mimeType": "application/vnd.google-apps.folder"}
                """.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }

            if path.hasSuffix("/files") && request.httpMethod == "GET" {
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, #"{"files": []}"#.data(using: .utf8)!)
            }

            if path.hasSuffix("/changes/startPageToken") {
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, #"{"startPageToken": "token_1"}"#.data(using: .utf8)!)
            }

            // queryResumableOffset (PUT 到 sessionURIString，带 Content-Range: bytes */total)
            if url.absoluteString == sessionURIString {
                if let contentRange = request.value(forHTTPHeaderField: "Content-Range") {
                    if contentRange.starts(with: "bytes */") {
                        // 探测请求，服务端确认已收到 0-4194303 (4MB)
                        let headers = ["Range": "bytes=0-4194303"]
                        return (HTTPURLResponse(url: url, statusCode: 308, httpVersion: nil, headerFields: headers)!, Data())
                    } else if contentRange.starts(with: "bytes 4194304-") && resumedChunkAttempt.next() == 0 {
                        // 服务端只确认到 6MB；引擎必须按 Range 而非发送长度推进。
                        recorder.recordResumedRange(start: 4194304)
                        return (HTTPURLResponse(url: url, statusCode: 308, httpVersion: nil, headerFields: ["Range": "bytes=0-6291455"])!, Data())
                    } else if contentRange.starts(with: "bytes 6291456-") {
                        recorder.recordResumedRange(start: 6291456)
                        let json = """
                        {"id": "\(persistentRemoteId)", "name": "large_9mb.bin", "size": "\(totalSize)", "sha256Checksum": "\(expectedSha256)"}
                        """.data(using: .utf8)!
                        return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
                    }
                }
            }

            // 严禁发起重新生成 ID 或全新创建 session 的请求
            if path.hasSuffix("/files/generateIds") {
                return (HTTPURLResponse(url: url, statusCode: 500, httpVersion: nil, headerFields: nil)!, #"{"error": "Should not generate new ID"}"#.data(using: .utf8)!)
            }
            if request.httpMethod == "POST" && url.absoluteString.contains("uploadType=resumable") {
                return (HTTPURLResponse(url: url, statusCode: 500, httpVersion: nil, headerFields: nil)!, #"{"error": "Should not initiate new session"}"#.data(using: .utf8)!)
            }

            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, "{}".data(using: .utf8)!)
        }

        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // 执行同步
        let stats = try await engine.syncLocalToRemoteEmpty(localPath: localRootDir.path, remoteRootId: "mock_remote_root")
        #expect(stats.filesUploaded == 1)
        #expect(stats.filesFailed == 0)

        // 断言：确实复用了既有 session，从 4194304 (4MB) 处续传！
        #expect(recorder.resumedRangeStarts.contains(4194304))
        #expect(recorder.resumedRangeStarts.contains(6291456))

        // 断言：SQLite 中的文件已转为 committed 且 remote_file_id 严格保持为 persistentRemoteId
        let (phase, dirtyGen, baseSha, finalRemoteId): (String?, Int64?, String?, String?) = try await store.read { conn in
            let s = try conn.cachedStatement("SELECT phase, dirty_generation, base_sha256, remote_file_id FROM items WHERE name = 'large_9mb.bin';")
            defer { s.reset() }
            if try s.step() {
                return (s.columnText(at: 0), s.columnInt64(at: 1), s.columnText(at: 2), s.columnText(at: 3))
            }
            return (nil, nil, nil, nil)
        }
        #expect(phase == "committed")
        #expect(dirtyGen == 0)
        #expect(baseSha == expectedSha256)
        #expect(finalRemoteId == persistentRemoteId)

        let operation: (Int64, String)? = try await store.read { conn in
            let s = try conn.cachedStatement("SELECT confirmed_offset, state FROM operations WHERE operation_id = ?;")
            s.bindText("resumable_\(persistentRemoteId)", at: 1)
            defer { s.reset() }
            guard try s.step() else { return nil }
            return (s.columnInt64(at: 0) ?? -1, s.columnText(at: 1) ?? "")
        }
        #expect(operation?.0 == totalSize)
        #expect(operation?.1 == "completed")
    }

    @Test("Resumable client exposes server-confirmed offsets and terminal session states")
    func testResumableServerStates() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a09_client_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let total: Int64 = 1024
        let finalJSON = #"{"id":"file","name":"file.bin","size":"1024","sha256Checksum":"abc"}"#.data(using: .utf8)!
        let transientAttempts = SafeCounter(0)

        MockBootstrapSafetyURLProtocol.setHandler { request in
            let url = request.url!
            switch url.lastPathComponent {
            case "no-range":
                return (HTTPURLResponse(url: url, statusCode: 308, httpVersion: nil, headerFields: nil)!, Data())
            case "partial":
                return (HTTPURLResponse(url: url, statusCode: 308, httpVersion: nil, headerFields: ["Range": "bytes=0-511"])!, Data())
            case "complete-200":
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, finalJSON)
            case "complete-201":
                return (HTTPURLResponse(url: url, statusCode: 201, httpVersion: nil, headerFields: nil)!, finalJSON)
            case "expired":
                return (HTTPURLResponse(url: url, statusCode: 404, httpVersion: nil, headerFields: nil)!, Data())
            case "transient":
                if transientAttempts.next() == 0 {
                    return (HTTPURLResponse(url: url, statusCode: 503, httpVersion: nil, headerFields: ["Retry-After": "0"])!, Data())
                }
                return (HTTPURLResponse(url: url, statusCode: 308, httpVersion: nil, headerFields: ["Range": "bytes=0-255"])!, Data())
            default:
                return (HTTPURLResponse(url: url, statusCode: 500, httpVersion: nil, headerFields: nil)!, Data())
            }
        }

        if case .incomplete(let offset) = try await client.queryResumableOffset(sessionURL: URL(string: "https://upload.invalid/no-range")!, totalBytes: total) {
            #expect(offset == 0)
        } else { Issue.record("308 without Range must report offset zero") }

        if case .incomplete(let offset) = try await client.uploadResumableChunk(sessionURL: URL(string: "https://upload.invalid/partial")!, chunkData: Data(repeating: 1, count: 1024), offset: 0, totalBytes: total) {
            #expect(offset == 512)
        } else { Issue.record("308 must expose the server-confirmed Range") }

        if case .complete(let file) = try await client.queryResumableOffset(sessionURL: URL(string: "https://upload.invalid/complete-200")!, totalBytes: total) {
            #expect(file.id == "file")
        } else { Issue.record("200 status query must be complete") }

        if case .complete(let file) = try await client.queryResumableOffset(sessionURL: URL(string: "https://upload.invalid/complete-201")!, totalBytes: total) {
            #expect(file.id == "file")
        } else { Issue.record("201 status query must be complete") }

        if case .expired = try await client.queryResumableOffset(sessionURL: URL(string: "https://upload.invalid/expired")!, totalBytes: total) {
            // expected
        } else { Issue.record("404 status query must expire the session") }

        if case .incomplete(let offset) = try await client.queryResumableOffset(sessionURL: URL(string: "https://upload.invalid/transient")!, totalBytes: total) {
            #expect(offset == 256)
            #expect(transientAttempts.next(count: 0) == 2)
        } else { Issue.record("503 must retry the existing session instead of expiring it") }
    }

    // MARK: - Test 5: Unfinished File is NOT Falsely Skipped by Cache on Re-run

    @Test("Unfinished file is NOT falsely skipped by cache on rerun")
    func testUnfinishedFileNotSkippedOnRerun() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a08_unfinished_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)

        let targetFile = localRootDir.appendingPathComponent("unfinished.txt")
        let content = "Incomplete file content\n"
        try content.write(to: targetFile, atomically: true, encoding: .utf8)

        let attrs = try FileManager.default.attributesOfItem(atPath: targetFile.path)
        let size = (attrs[.size] as? NSNumber)?.int64Value ?? Int64(content.utf8.count)
        let mtime = Int64(((attrs[.modificationDate] as? Date)?.timeIntervalSince1970 ?? 0) * 1_000_000_000)

        let dbPath = tempDir.appendingPathComponent("state.sqlite").path
        let store = try await StateStore(path: dbPath)

        // 预设：在 SQLite 中存有该文件的元数据（mtime、size完全匹配），但状态为 inFlight 且 base_sha256 为 NULL
        try await store.write { conn in
            _ = try conn.execute("""
            INSERT INTO roots (
                root_id, account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES (
                1, 'default', '\(localRootDir.path)', 1, 1,
                'mock_remote_root', 'localToRemoteEmpty', 'freshCreated', 1, 1
            );
            INSERT INTO items (
                item_id, root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (
                1, 1, NULL, 'local_root', 'directory', 'mock_remote_root', 1, 1
            );
            INSERT INTO items (
                item_id, root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, local_mtime, local_size, local_sha256,
                base_sha256, phase, dirty_generation, created_at, updated_at
            ) VALUES (
                50, 1, 1, 'unfinished.txt', 'file', 'r_unfin_50',
                1, 1, \(mtime), \(size), NULL,
                NULL, 'inFlight', 1, 1, 1
            );
            """)
        }

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let recorder = RequestEventRecorder()

        MockBootstrapSafetyURLProtocol.setHandler { request in
            guard let url = request.url else {
                return (HTTPURLResponse(url: URL(string: "https://invalid")!, statusCode: 400, httpVersion: nil, headerFields: nil)!, Data())
            }
            let path = url.path

            if path.hasSuffix("/files/mock_remote_root") {
                let json = """
                {"id": "mock_remote_root", "name": "mock_remote_root", "mimeType": "application/vnd.google-apps.folder"}
                """.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }

            if path.hasSuffix("/files") && request.httpMethod == "GET" {
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, #"{"files": []}"#.data(using: .utf8)!)
            }

            if path.hasSuffix("/changes/startPageToken") {
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, #"{"startPageToken": "token_1"}"#.data(using: .utf8)!)
            }

            if request.httpMethod == "POST" && url.absoluteString.contains("/upload/drive/v3/files") {
                recorder.recordFileCreate(id: "r_unfin_50")
                let sha = SyncEngine.computeSha256(of: content.data(using: .utf8)!)
                let json = """
                {"id": "r_unfin_50", "name": "unfinished.txt", "size": "\(content.utf8.count)", "sha256Checksum": "\(sha)"}
                """.data(using: .utf8)!
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, "{}".data(using: .utf8)!)
        }

        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        let stats = try await engine.syncLocalToRemoteEmpty(localPath: localRootDir.path, remoteRootId: "mock_remote_root")

        // 关键断言：即使 mtime 和 size 在 items 中存在，由于此前为 inFlight，绝不能被缓存跳过！
        #expect(stats.filesSkipped == 0)
        #expect(stats.filesUploaded == 1)
        #expect(recorder.fileCreateCount == 1)

        // 验证 SQLite 最终处于 committed
        let (phase, dirtyGen, baseSha): (String?, Int64?, String?) = try await store.read { conn in
            let s = try conn.cachedStatement("SELECT phase, dirty_generation, base_sha256 FROM items WHERE name = 'unfinished.txt';")
            defer { s.reset() }
            if try s.step() {
                return (s.columnText(at: 0), s.columnInt64(at: 1), s.columnText(at: 2))
            }
            return (nil, nil, nil)
        }
        #expect(phase == "committed")
        #expect(dirtyGen == 0)
        #expect(baseSha != nil)
    }
}
