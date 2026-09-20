import Foundation
import CryptoKit
import Testing
@testable import GDrive

final class MockFailureSafetyURLProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = TestHTTPContext<TestRequestHandler>.value(for: request)?.requestHandler else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
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

final class FailureControlState: @unchecked Sendable {
    private var flag = true
    private let lock = NSLock()

    var shouldFail: Bool {
        get {
            lock.lock()
            defer { lock.unlock() }
            return flag
        }
        set {
            lock.lock()
            defer { lock.unlock() }
            flag = newValue
        }
    }
}

@Suite("API Failure and State Safety Tests (A07)")
struct FailureStateSafetyTests {
    private let context = TestHTTPContext(TestRequestHandler())

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
        context.configure(config)
        config.protocolClasses = [MockFailureSafetyURLProtocol.self]
        let session = URLSession(configuration: config)
        return DriveClient(
            auth: auth,
            session: session,
            requestsPerSecond: nil,
            maxRetries: 1,
            retrySleep: { _ in try Task.checkCancellation() }
        )
    }

    @Test("Directory rename failure does not commit new name to SQLite; retrying succeeds")
    func testDirectoryRenameFailurePreservesBaselineAndRetries() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a07_dir_rename_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        let dirA = localRootDir.appendingPathComponent("dir_A")
        try FileManager.default.createDirectory(at: dirA, withIntermediateDirectories: true)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_dir"
        let dirARemoteId = "dir_A_remote_id"

        // Set up root and dir_A in SQLite baseline
        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'existingKnown', 100, 100);
            """)
            stmt.bindText(localRootDir.path, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let rootDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'local_root', 'directory', ?, 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let dirAAttrs = try FileManager.default.attributesOfItem(atPath: dirA.path)
        let dirADev = (dirAAttrs[.systemNumber] as? NSNumber)?.int64Value ?? 1
        let dirAIno = (dirAAttrs[.systemFileNumber] as? NSNumber)?.int64Value ?? 0

        let dirAItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'dir_A', 'directory', ?,
                    ?, ?, 'present', 'present', 'committed', 0, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            stmt.bindText(dirARemoteId, at: 3)
            stmt.bindInt64(dirADev, at: 4)
            stmt.bindInt64(dirAIno, at: 5)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Rename dir_A -> dir_B on disk
        let dirB = localRootDir.appendingPathComponent("dir_B")
        try FileManager.default.moveItem(at: dirA, to: dirB)

        let control = FailureControlState()

        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let path = url.path

            if path.contains("/files/\(rootRemoteId)") && request.httpMethod == "GET" {
                let json = Data("""
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes/startPageToken") {
                let json = Data(#"{"startPageToken": "token_1"}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes") {
                let json = Data(#"{"newStartPageToken": "token_2", "changes": []}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/files/generateIds") {
                let json = Data(#"{"ids": ["mock_id_gen"]}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if request.httpMethod == "PATCH" && path.contains("/files/\(dirARemoteId)") {
                if control.shouldFail {
                    // Inject 500 internal server error
                    let errJson = Data(#"{"error": {"code": 500, "message": "Simulated Drive failure"}}"#.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 500, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, errJson)
                } else {
                    let json = Data("""
                    {"id": "\(dirARemoteId)", "name": "dir_B", "mimeType": "application/vnd.google-apps.folder"}
                    """.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
                }
            }

            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, Data("{}".utf8))
        }

        // Established-baseline fixtures include their durable Changes boundary.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_1', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // 1. Run sync while updateMetadata fails
        try await engine.syncIncremental(localPath: localRootDir.path)

        // SQLite should NOT be updated to 'dir_B' because remote update failed!
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT name FROM items WHERE item_id = ?;")
            stmt.bindInt64(dirAItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnText(at: 0) == "dir_A", "Directory name in DB must remain dir_A after failed rename")
        }

        // 2. Allow updateMetadata to succeed on retry
        control.shouldFail = false
        try await engine.syncIncremental(localPath: localRootDir.path)

        // SQLite should now be updated to 'dir_B'
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT name FROM items WHERE item_id = ?;")
            stmt.bindInt64(dirAItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnText(at: 0) == "dir_B", "Directory name in DB must be updated to dir_B after successful retry")
        }
    }

    @Test("File rename preserves content detection and retries safely (A10)", arguments: [false, true], [false, true])
    func testFileRenameFailurePreservesBaselineAndRetries(contentChanged: Bool, failRename: Bool) async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a07_file_rename_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)
        let fileA = localRootDir.appendingPathComponent("file_A.txt")
        try "file content".write(to: fileA, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.modificationDate: Date(timeIntervalSince1970: 1_700_000_000)], ofItemAtPath: fileA.path)
        let originalSHA = SyncEngine.computeSha256(of: Data("file content".utf8))
        let expectedContent = Data((contentChanged ? "new contents" : "file content").utf8)
        let expectedSHA = SyncEngine.computeSha256(of: expectedContent)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_file"
        let fileARemoteId = "file_A_remote_id"

        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'existingKnown', 100, 100);
            """)
            stmt.bindText(localRootDir.path, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let rootDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'local_root', 'directory', ?, 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let fileAAttrs = try FileManager.default.attributesOfItem(atPath: fileA.path)
        let fileADev = (fileAAttrs[.systemNumber] as? NSNumber)?.int64Value ?? 1
        let fileAIno = (fileAAttrs[.systemFileNumber] as? NSNumber)?.int64Value ?? 0

        let fileAItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'file_A.txt', 'file', ?,
                    ?, ?, 'present', 'present', 'committed', 0, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            stmt.bindText(fileARemoteId, at: 3)
            stmt.bindInt64(fileADev, at: 4)
            stmt.bindInt64(fileAIno, at: 5)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        try await store.write { conn in
            let stmt = try conn.prepare("""
                UPDATE items SET local_mtime = 1700000000000000000, local_size = 12,
                    base_size = 12, remote_size = 12,
                    local_sha256 = ?, base_sha256 = ?, remote_sha256 = ? WHERE item_id = ?;
                """)
            for index in 1...3 { stmt.bindText(originalSHA, at: Int32(index)) }
            stmt.bindInt64(fileAItemId, at: 4)
            _ = try stmt.step()
        }

        // Rename file_A.txt -> file_B.txt on disk
        let fileB = localRootDir.appendingPathComponent("file_B.txt")
        try FileManager.default.moveItem(at: fileA, to: fileB)
        if contentChanged {
            let handle = try FileHandle(forWritingTo: fileB)
            try handle.write(contentsOf: expectedContent)
            try handle.close()
            try FileManager.default.setAttributes([.modificationDate: Date(timeIntervalSince1970: 1_700_000_010)], ofItemAtPath: fileB.path)
        }
        let renamedAttrs = try FileManager.default.attributesOfItem(atPath: fileB.path)
        #expect((renamedAttrs[.systemFileNumber] as? NSNumber)?.int64Value == fileAIno)

        let control = FailureControlState()
        control.shouldFail = failRename
        let uploads = RequestEventRecorder()
        let renames = RequestEventRecorder()

        @Sendable func handleRequest(_ request: URLRequest) throws -> (HTTPURLResponse, Data) {
            let url = try #require(request.url)
            let path = url.path

            if path.contains("/files/\(rootRemoteId)") && request.httpMethod == "GET" {
                let json = Data("""
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes/startPageToken") {
                let json = Data(#"{"startPageToken": "token_1"}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes") {
                let json = Data(#"{"newStartPageToken": "token_2", "changes": []}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/files/generateIds") {
                let json = Data(#"{"ids": ["mock_id_gen"]}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if request.httpMethod == "PATCH" && path.contains("/files/\(fileARemoteId)") {
                if path.contains("/upload/") {
                    #expect(!control.shouldFail)
                    let body = request.extractBodyData ?? Data()
                    #expect(body == expectedContent)
                    uploads.recordFilePatch()
                    let json = Data("""
                    {"id":"\(fileARemoteId)","name":"file_B.txt","mimeType":"text/plain","size":"12","sha256Checksum":"\(expectedSHA)"}
                    """.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
                }
                renames.recordFilePatch()
                #expect(!(url.query ?? "").contains("removeParents"))
                #expect(!(url.query ?? "").contains("addParents"))
                if control.shouldFail {
                    let errJson = Data(#"{"error": {"code": 400, "message": "Bad Request"}}"#.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 400, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, errJson)
                } else {
                    let json = Data("""
                    {"id": "\(fileARemoteId)", "name": "file_B.txt", "mimeType": "text/plain"}
                    """.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
                }
            }
            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, Data("{}".utf8))
        }
        context.value.requestHandler = handleRequest

        // Established-baseline fixtures include their durable Changes boundary.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_1', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // A failed rename must preserve both the mapping and verified content metadata.
        if failRename {
            try await engine.syncIncremental(localPath: localRootDir.path)

            try await store.read { conn in
                let stmt = try conn.prepare("SELECT name, local_mtime, local_sha256 FROM items WHERE item_id = ?;")
                stmt.bindInt64(fileAItemId, at: 1)
                #expect(try stmt.step())
                #expect(stmt.columnText(at: 0) == "file_A.txt", "File name in DB must remain file_A.txt after failed rename")
                #expect(stmt.columnInt64(at: 1) == 1_700_000_000_000_000_000)
                #expect(stmt.columnText(at: 2) == originalSHA)
            }
            #expect(uploads.filePatchCount == 0)
        }

        // 2. Allow update to succeed on retry
        control.shouldFail = false
        let stats = try await engine.syncIncremental(localPath: localRootDir.path)
        #expect(stats.filesUploaded == 0) // A11 blocks unconditioned remote overwrites.
        #expect(stats.filesFailed == (contentChanged ? 1 : 0))
        #expect(stats.filesSkipped == (contentChanged ? 0 : 1))

        try await store.read { conn in
            let stmt = try conn.prepare("SELECT name, base_sha256, local_sha256, dirty_generation FROM items WHERE item_id = ?;")
            stmt.bindInt64(fileAItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnText(at: 0) == "file_B.txt", "File name in DB must be updated to file_B.txt after successful retry")
            #expect(stmt.columnText(at: 1) == originalSHA)
            #expect(stmt.columnText(at: 2) == expectedSHA)
            #expect((stmt.columnInt64(at: 3) ?? 0) == (contentChanged ? 1 : 0))
        }
        let second = try await engine.syncIncremental(localPath: localRootDir.path)
        #expect(second.filesUploaded == 0)
        #expect(second.filesSkipped == (contentChanged ? 0 : 1))
        #expect(second.filesFailed == (contentChanged ? 1 : 0))
        #expect(uploads.filePatchCount == 0)
        #expect(renames.filePatchCount == (failRename ? 2 : 1))
    }

    @Test("Remote trash remains pending without a verified conditional metadata update")
    func testRemoteTrashFailurePreservesItemAndRetries() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a07_trash_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)
        let fileToDelete = localRootDir.appendingPathComponent("file_to_delete.txt")
        try "content".write(to: fileToDelete, atomically: true, encoding: .utf8)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_trash"
        let fileRemoteId = "file_remote_trash_id"

        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'existingKnown', 100, 100);
            """)
            stmt.bindText(localRootDir.path, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let rootDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'local_root', 'directory', ?, 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let sha256Hex = String(repeating: "a", count: 64)
        let fileItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    base_sha256, base_size,
                    remote_sha256, remote_size, remote_status,
                    local_generation, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'file_to_delete.txt', 'file', ?,
                    1, 2, 100, 7, ?,
                    ?, 7,
                    ?, 7, 'present',
                    1, 'present', 'committed', 0, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            stmt.bindText(fileRemoteId, at: 3)
            stmt.bindText(sha256Hex, at: 4)
            stmt.bindText(sha256Hex, at: 5)
            stmt.bindText(sha256Hex, at: 6)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Delete file locally
        try FileManager.default.removeItem(at: fileToDelete)

        let control = FailureControlState()

        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let path = url.path

            if path.contains("/files/\(rootRemoteId)") && request.httpMethod == "GET" {
                let json = Data("""
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes/startPageToken") {
                let json = Data(#"{"startPageToken": "token_1"}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes") {
                let json = Data(#"{"newStartPageToken": "token_2", "changes": []}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/files/generateIds") {
                let json = Data(#"{"ids": ["mock_id_gen"]}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if request.httpMethod == "PATCH" && path.contains("/files/\(fileRemoteId)") {
                if control.shouldFail {
                    let errJson = Data(#"{"error": {"code": 403, "message": "Cannot trash file"}}"#.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 403, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, errJson)
                } else {
                    let json = Data("""
                    {"id": "\(fileRemoteId)", "trashed": true}
                    """.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
                }
            }

            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, Data("{}".utf8))
        }

        // Established-baseline fixtures include their durable Changes boundary.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_1', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // 1. First sync with trash failing
        let stats1 = try await engine.syncIncremental(localPath: localRootDir.path)
        #expect(stats1.filesDeleted == 0, "Failed trash must not be counted in filesDeleted")

        // In SQLite: is_tombstone MUST still be 0!
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT is_tombstone FROM items WHERE item_id = ?;")
            stmt.bindInt64(fileItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnInt64(at: 0) == 0, "Item must NOT be marked tombstone when trash fails")
        }

        // A retry still must not issue an unconditional PATCH.
        control.shouldFail = false
        let stats2 = try await engine.syncIncremental(localPath: localRootDir.path)
        #expect(stats2.filesDeleted == 0)

        try await store.read { conn in
            let stmt = try conn.prepare(
                "SELECT is_tombstone, phase, dirty_generation FROM items WHERE item_id = ?;")
            stmt.bindInt64(fileItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnInt64(at: 0) == 0)
            #expect(stmt.columnText(at: 1) == "blocked")
            #expect((stmt.columnInt64(at: 2) ?? 0) > 0)
        }
    }

    @Test("Directory creation failure during incremental scan does not mark committed in SQLite; retrying succeeds")
    func testDirectoryCreationFailureDoesNotCommitCommittedStateAndRetries() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a07_dir_create_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        let newFolder = localRootDir.appendingPathComponent("new_folder")
        try FileManager.default.createDirectory(at: newFolder, withIntermediateDirectories: true)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_create_dir"

        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'existingKnown', 100, 100);
            """)
            stmt.bindText(localRootDir.path, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        _ = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'local_root', 'directory', ?, 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let control = FailureControlState()

        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let path = url.path

            if path.contains("/files/\(rootRemoteId)") && request.httpMethod == "GET" {
                let json = Data("""
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes/startPageToken") {
                let json = Data(#"{"startPageToken": "token_1"}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes") {
                let json = Data(#"{"newStartPageToken": "token_2", "changes": []}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/files/generateIds") {
                let json = Data(#"{"ids": ["valid_server_id_1"]}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if request.httpMethod == "POST" && path.hasSuffix("/drive/v3/files") {
                if control.shouldFail {
                    let errJson = Data(#"{"error": {"code": 500, "message": "Simulated Drive folder create error"}}"#.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 500, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, errJson)
                } else {
                    let json = Data("""
                    {"id": "valid_server_id_1", "name": "new_folder", "mimeType": "application/vnd.google-apps.folder"}
                    """.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
                }
            }

            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, Data("{}".utf8))
        }

        // Established-baseline fixtures include their durable Changes boundary.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_1', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // 1. Run sync while createDirectory fails
        try await engine.syncIncremental(localPath: localRootDir.path)

        // SQLite: new_folder MUST NOT be committed!
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT phase, remote_status FROM items WHERE root_id = ? AND name = 'new_folder';")
            stmt.bindInt64(rootId, at: 1)
            if try stmt.step() {
                let phase = stmt.columnText(at: 0)
                let remoteStatus = stmt.columnText(at: 1)
                #expect(phase != "committed", "Directory must NOT be committed when remote create failed")
                #expect(remoteStatus != "present", "remote_status must NOT be present when remote create failed")
            }
        }

        // 2. Retry with createDirectory succeeding
        control.shouldFail = false
        try await engine.syncIncremental(localPath: localRootDir.path)

        // SQLite: new_folder must now be committed
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT phase, remote_status FROM items WHERE root_id = ? AND name = 'new_folder';")
            stmt.bindInt64(rootId, at: 1)
            #expect(try stmt.step(), "new_folder must exist in DB")
            #expect(stmt.columnText(at: 0) == "committed", "Directory must be committed after successful creation")
            #expect(stmt.columnText(at: 1) == "present", "remote_status must be present after successful creation")
        }
    }

    @Test("IDPool fetch failure does not generate UUID fallback creation request")
    func testIdPoolFailureDoesNotProduceUUIDRequest() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a07_idpool_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        let subFolder = localRootDir.appendingPathComponent("sub_folder")
        try FileManager.default.createDirectory(at: subFolder, withIntermediateDirectories: true)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_idpool"

        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'existingKnown', 100, 100);
            """)
            stmt.bindText(localRootDir.path, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        _ = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'local_root', 'directory', ?, 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindText(rootRemoteId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let control = FailureControlState()
        final class CreatedIdsTracker: @unchecked Sendable {
            var requestedCreationIds: [String] = []
            private let lock = NSLock()
            func add(_ id: String) {
                lock.lock()
                defer { lock.unlock() }
                requestedCreationIds.append(id)
            }
        }
        let tracker = CreatedIdsTracker()

        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let path = url.path

            if path.contains("/files/\(rootRemoteId)") && request.httpMethod == "GET" {
                let json = Data("""
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes/startPageToken") {
                let json = Data(#"{"startPageToken": "token_1"}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/changes") {
                let json = Data(#"{"newStartPageToken": "token_2", "changes": []}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/files/generateIds") {
                if control.shouldFail {
                    let errJson = Data(#"{"error": {"code": 500, "message": "IDPool API failure"}}"#.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 500, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, errJson)
                } else {
                    let json = Data(#"{"ids": ["valid_remote_id_42"]}"#.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
                }
            }
            if request.httpMethod == "POST" && path.hasSuffix("/drive/v3/files") {
                if let body = request.extractBodyData,
                    let dict = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                    let requestedId = dict["id"] as? String {
                    tracker.add(requestedId)
                }
                let json = Data("""
                {"id": "valid_remote_id_42", "name": "sub_folder", "mimeType": "application/vnd.google-apps.folder"}
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }

            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, Data("{}".utf8))
        }

        // Established-baseline fixtures include their durable Changes boundary.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_1', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // 1. Run sync when ID generation fails
        try await engine.syncIncremental(localPath: localRootDir.path)

        // Assert: NO creation request was made with a generated UUID!
        #expect(tracker.requestedCreationIds.isEmpty, "No createDirectory request should be issued when IDPool fails")

        // 2. Allow ID generation to succeed on retry
        control.shouldFail = false
        try await engine.syncIncremental(localPath: localRootDir.path)

        // Assert: Creation was issued with the valid server ID
        #expect(tracker.requestedCreationIds == ["valid_remote_id_42"], "Creation request must use valid pre-generated server ID")
    }
}
