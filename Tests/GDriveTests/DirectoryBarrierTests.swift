import Foundation
import CryptoKit
import Testing
@testable import GDrive

final class MockDirectoryBarrierURLProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool {
        return true
    }

    override static func canonicalRequest(for request: URLRequest) -> URLRequest {
        return request
    }

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

@Suite("Directory Barrier Tests (A04)")
struct DirectoryBarrierTests {
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
        config.protocolClasses = [MockDirectoryBarrierURLProtocol.self]
        let session = URLSession(configuration: config)
        return DriveClient(auth: auth, session: session, requestsPerSecond: nil)
    }

    @Test("Scenario 1: Local directory deleted but remote child modified -> child downloaded, parent directory recreated, remote parent NOT trashed")
    func testLocalDirDeletedRemoteChildModified() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("barrier_test1_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_1"
        defer { removeTestDownloadDirectory(remoteRootID: rootRemoteId) }
        let parentDirRemoteId = "remote_folder_sub"
        let childFileRemoteId = "remote_file_child"

        // 1. Set root records and directory baselines
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

        let subDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    remote_status, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'sub', 'directory', ?,
                    'present', 'absent', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            stmt.bindText(parentDirRemoteId, at: 3)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let newRemoteContent = "New Remote File Data"
        let remoteData = newRemoteContent.data(using: .utf8)!
        let remoteSha256 = SHA256.hash(data: remoteData).map { String(format: "%02x", $0) }.joined()
        let oldSha256 = String(repeating: "a", count: 64)

        _ = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    base_sha256, base_size,
                    remote_sha256, remote_size, remote_status,
                    local_generation, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'child.txt', 'file', ?,
                    1, 2, 100, 12, ?,
                    ?, 12,
                    ?, 20, 'present',
                    1, 'absent', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(subDirItemId, at: 2)
            stmt.bindText(childFileRemoteId, at: 3)
            stmt.bindText(oldSha256, at: 4)
            stmt.bindText(oldSha256, at: 5)
            stmt.bindText(remoteSha256, at: 6)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Mock HTTP requests
        nonisolated(unsafe) var trashedRemoteFolders: [String] = []

        context.value.requestHandler = { request in
            let url = request.url?.absoluteString ?? ""
            let method = request.httpMethod ?? "GET"

            if url.contains("/drive/v3/files/\(rootRemoteId)") {
                let json = """
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/changes/startPageToken") {
                let json = "{\"startPageToken\": \"token_123\"}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/changes") {
                let json = "{\"newStartPageToken\": \"token_456\", \"changes\": []}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/files/\(childFileRemoteId)") && url.contains("alt=media") {
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, newRemoteContent.data(using: .utf8)!)
            }

            if method == "PATCH" && url.contains("/drive/v3/files/\(parentDirRemoteId)") {
                trashedRemoteFolders.append(parentDirRemoteId)
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, Data("{}".utf8))
            }

            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, Data("{}".utf8))
        }

        // These cases exercise an established baseline; a missing cursor now correctly
        // requests index reconstruction, covered separately by ChangesRecoveryTests.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_123', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)
        try await engine.syncIncremental(localPath: localRootDir.path, remoteRootId: rootRemoteId)

        // Verification:
        // 1. The remote parent directory must not be trash
        #expect(!trashedRemoteFolders.contains(parentDirRemoteId), "Remote parent folder must NOT be trashed due to descendant barrier")

        // 2. The local subdirectory is re-created and the sub-file is downloaded
        let downloadedSubDir = localRootDir.appendingPathComponent("sub")
        let downloadedFile = downloadedSubDir.appendingPathComponent("child.txt")
        #expect(FileManager.default.fileExists(atPath: downloadedSubDir.path), "Local parent directory must be recreated on disk")
        #expect(FileManager.default.fileExists(atPath: downloadedFile.path), "Local child file must be downloaded and present")

        let contentOnDisk = try? String(contentsOf: downloadedFile, encoding: .utf8)
        #expect(contentOnDisk == newRemoteContent)

        // 3. Database status:sub of local_status Revert to present,child.txt also for present
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT local_status, remote_status, phase, is_tombstone FROM items WHERE item_id = ?;")
            stmt.bindInt64(subDirItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnText(at: 0) == "present", "sub local_status must be present")
            #expect(stmt.columnText(at: 1) == "present", "sub remote_status must be present")
            #expect(stmt.columnText(at: 2) == "committed")
            #expect(stmt.columnInt64(at: 3) == 0, "sub must not be tombstone")
        }
    }

    @Test("Scenario 2: Remote directory trashed but local child newly created -> child uploaded, remote folder untrashed, local parent directory NOT deleted")
    func testRemoteDirTrashedLocalChildNewlyAdded() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("barrier_test2_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        let subDir = localRootDir.appendingPathComponent("sub")
        try FileManager.default.createDirectory(at: subDir, withIntermediateDirectories: true)

        let localNewFile = subDir.appendingPathComponent("local_new.txt")
        let fileContent = "Local newly created file content"
        try fileContent.write(to: localNewFile, atomically: true, encoding: .utf8)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_2"
        defer { removeTestDownloadDirectory(remoteRootID: rootRemoteId) }
        let parentDirRemoteId = "remote_folder_sub_2"

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

        // sub The directory on the remote end is set to trashed,But still locally present
        let subDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    remote_status, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'sub', 'directory', ?,
                    'trashed', 'present', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            stmt.bindText(parentDirRemoteId, at: 3)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        nonisolated(unsafe) var untrashedRemoteFolders: [String] = []
        nonisolated(unsafe) var uploadedFiles: [String] = []

        context.value.requestHandler = { request in
            let url = request.url?.absoluteString ?? ""
            let method = request.httpMethod ?? "GET"

            if url.contains("/drive/v3/files/\(rootRemoteId)") {
                let json = """
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/changes/startPageToken") {
                let json = "{\"startPageToken\": \"token_123\"}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/files/generateIds") {
                let json = "{\"ids\":[\"generated-child-file-id\"]}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/changes") {
                let json = "{\"newStartPageToken\": \"token_456\", \"changes\": []}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            // Untrash folder PATCH
            if method == "PATCH" && url.contains("/drive/v3/files/\(parentDirRemoteId)") {
                untrashedRemoteFolders.append(parentDirRemoteId)
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, Data("{}".utf8))
            }

            // Multipart upload
            if url.contains("/upload/drive/v3/files") {
                uploadedFiles.append("local_new.txt")
                let json = """
                {"id": "remote_new_file_id", "name": "local_new.txt", "mimeType": "text/plain", "size": "\(fileContent.count)", "trashed": false}
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, Data("{}".utf8))
        }

        // These cases exercise an established baseline; a missing cursor now correctly
        // requests index reconstruction, covered separately by ChangesRecoveryTests.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_123', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)
        try await engine.syncIncremental(localPath: localRootDir.path, remoteRootId: rootRemoteId)

        // Verification:
        // 1. The local directory and new files are still intact on the disk and have never been deleted.
        #expect(FileManager.default.fileExists(atPath: subDir.path), "Local sub directory must NOT be deleted")
        #expect(FileManager.default.fileExists(atPath: localNewFile.path), "Local child file must remain intact")

        // 2. The remote directory is executed untrash
        #expect(untrashedRemoteFolders.contains(parentDirRemoteId), "Remote parent folder must be untrashed to protect child upload")
        #expect(uploadedFiles.contains("local_new.txt"), "Local child must be uploaded after its durable ID is allocated")

        // 3. Database status:sub of remote_status Revert to present,is_tombstone for 0
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT local_status, remote_status, phase, is_tombstone FROM items WHERE item_id = ?;")
            stmt.bindInt64(subDirItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnText(at: 0) == "present")
            #expect(stmt.columnText(at: 1) == "present", "sub remote_status must be restored to present")
            #expect(stmt.columnText(at: 2) == "committed")
            #expect(stmt.columnInt64(at: 3) == 0, "sub must not be tombstoned")
        }
    }

    @Test("Scenario 3: Bottom-up safe directory clean when all children deleted locally")
    func testBottomUpDirectorySafeCleanup() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("barrier_test3_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_3"
        defer { removeTestDownloadDirectory(remoteRootID: rootRemoteId) }
        let parentDirRemoteId = "remote_folder_level1"
        let childDirRemoteId = "remote_folder_level2"
        let fileRemoteId = "remote_file_level3"

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

        // Build hierarchy: local_root / level1 / level2 / file.txt
        let level1ItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    remote_status, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'level1', 'directory', ?,
                    'present', 'absent', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            stmt.bindText(parentDirRemoteId, at: 3)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let level2ItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    remote_status, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'level2', 'directory', ?,
                    'present', 'absent', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(level1ItemId, at: 2)
            stmt.bindText(childDirRemoteId, at: 3)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let validSha256 = String(repeating: "b", count: 64)
        let fileItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    base_sha256, base_size,
                    remote_sha256, remote_size, remote_status,
                    local_generation, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'file.txt', 'file', ?,
                    1, 2, 100, 10, ?,
                    ?, 10,
                    ?, 10, 'present',
                    1, 'absent', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(level2ItemId, at: 2)
            stmt.bindText(fileRemoteId, at: 3)
            stmt.bindText(validSha256, at: 4)
            stmt.bindText(validSha256, at: 5)
            stmt.bindText(validSha256, at: 6)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        nonisolated(unsafe) var trashedRemoteOrder: [String] = []

        context.value.requestHandler = { request in
            let url = request.url?.absoluteString ?? ""
            let method = request.httpMethod ?? "GET"

            if url.contains("/drive/v3/files/\(rootRemoteId)") {
                let json = """
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/changes/startPageToken") {
                let json = "{\"startPageToken\": \"token_123\"}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/changes") {
                let json = "{\"newStartPageToken\": \"token_456\", \"changes\": []}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if method == "PATCH" {
                if url.contains(fileRemoteId) {
                    trashedRemoteOrder.append(fileRemoteId)
                } else if url.contains(childDirRemoteId) {
                    trashedRemoteOrder.append(childDirRemoteId)
                } else if url.contains(parentDirRemoteId) {
                    trashedRemoteOrder.append(parentDirRemoteId)
                }
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, Data("{}".utf8))
            }

            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, Data("{}".utf8))
        }

        // These cases exercise an established baseline; a missing cursor now correctly
        // requests index reconstruction, covered separately by ChangesRecoveryTests.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_123', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)
        try await engine.syncIncremental(localPath: localRootDir.path, remoteRootId: rootRemoteId)

        // Verification:
        // 1. Delete the file first, then press bottom-up (level2 depth greater than level1)Delete remote directories sequentially
        #expect(trashedRemoteOrder.contains(fileRemoteId))
        #expect(trashedRemoteOrder.contains(childDirRemoteId))
        #expect(trashedRemoteOrder.contains(parentDirRemoteId))

        let fileIdx = trashedRemoteOrder.firstIndex(of: fileRemoteId)!
        let level2Idx = trashedRemoteOrder.firstIndex(of: childDirRemoteId)!
        let level1Idx = trashedRemoteOrder.firstIndex(of: parentDirRemoteId)!

        #expect(fileIdx < level2Idx, "File must be deleted before its parent directory")
        #expect(level2Idx < level1Idx, "Deeper directory level2 must be deleted before shallower level1")

        // 2. All levels in the database are converted to tombstone
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT is_tombstone, phase FROM items WHERE item_id IN (?, ?, ?);")
            // level1
            stmt.bindInt64(level1ItemId, at: 1)
            // level2
            stmt.bindInt64(level2ItemId, at: 2)
            // file
            stmt.bindInt64(fileItemId, at: 3)
            var count = 0
            while try stmt.step() {
                #expect(stmt.columnInt64(at: 0) == 1, "Must be marked tombstone")
                #expect(stmt.columnText(at: 1) == "committed")
                count += 1
            }
            #expect(count == 3)
        }
    }

    @Test("Scenario 4: remote child download honors generation and directory barrier (A04/A11)", arguments: ["none", "local_generation", "remote_generation", "dirty_generation"])
    func testLocalDirDeletedRemoteChildNewlyAdded(invalidation: String) async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("barrier_test4_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_4-\(UUID().uuidString)"
        defer { removeTestDownloadDirectory(remoteRootID: rootRemoteId) }
        let parentDirRemoteId = "remote_folder_sub_4"
        let newChildRemoteId = "remote_new_child_file_4"

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

        // Deleted locally sub Directory (local_status = 'absent', remote_status = 'present')
        let subDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    remote_status, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'sub', 'directory', ?,
                    'present', 'absent', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            stmt.bindText(parentDirRemoteId, at: 3)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let newRemoteContent = "New file added on Google Drive"
        let newFileData = newRemoteContent.data(using: .utf8)!
        let newFileSha256 = SHA256.hash(data: newFileData).map { String(format: "%02x", $0) }.joined()

        nonisolated(unsafe) var trashedRemoteFolders: [String] = []

        context.value.requestHandler = { request in
            let url = request.url?.absoluteString ?? ""
            let method = request.httpMethod ?? "GET"

            if url.contains("/drive/v3/files/\(rootRemoteId)") {
                let json = """
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/changes/startPageToken") {
                let json = "{\"startPageToken\": \"token_123\"}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/changes") {
                let json = """
                {
                    "newStartPageToken": "token_456",
                    "changes": [
                        {
                            "fileId": "\(newChildRemoteId)",
                            "removed": false,
                            "file": {
                                "id": "\(newChildRemoteId)",
                                "name": "remote_added.txt",
                                "mimeType": "text/plain",
                                "size": "\(newFileData.count)",
                                "sha256Checksum": "\(newFileSha256)",
                                "parents": ["\(parentDirRemoteId)"],
                                "trashed": false
                            }
                        }
                    ]
                }
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }

            if url.contains("/drive/v3/files/\(newChildRemoteId)") && url.contains("alt=media") {
                if invalidation != "none" {
                    let writer = try SQLiteConnection(path: dbPath)
                    try writer.execute("UPDATE items SET \(invalidation) = \(invalidation) + 1 WHERE entry_kind = 'file';")
                }
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, newFileData)
            }

            if method == "PATCH" && url.contains("/drive/v3/files/\(parentDirRemoteId)") {
                trashedRemoteFolders.append(parentDirRemoteId)
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, Data("{}".utf8))
            }

            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, Data("{}".utf8))
        }

        // These cases exercise an established baseline; a missing cursor now correctly
        // requests index reconstruction, covered separately by ChangesRecoveryTests.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_123', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)
        try await engine.syncIncremental(localPath: localRootDir.path, remoteRootId: rootRemoteId)

        // Verification:
        // 1. The remote parent directory is never trash
        #expect(!trashedRemoteFolders.contains(parentDirRemoteId), "Remote parent folder must NOT be trashed due to descendant barrier")

        // 2. local sub The directory is rebuilt,remote_added.txt The file was downloaded correctly
        let subDirURL = localRootDir.appendingPathComponent("sub")
        let downloadedFile = subDirURL.appendingPathComponent("remote_added.txt")
        #expect(FileManager.default.fileExists(atPath: subDirURL.path), "Local sub directory must be recreated")
        #expect(FileManager.default.fileExists(atPath: downloadedFile.path) == (invalidation == "none"))

        let fileContentOnDisk = try? String(contentsOf: downloadedFile, encoding: .utf8)
        #expect(fileContentOnDisk == (invalidation == "none" ? newRemoteContent : nil))
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT dirty_generation, base_sha256 FROM items WHERE remote_file_id = ?;")
            stmt.bindText(newChildRemoteId, at: 1)
            #expect(try stmt.step())
            if invalidation == "none" {
                #expect(stmt.columnInt64(at: 0) == 0)
                #expect(stmt.columnText(at: 1) == newFileSha256)
            } else {
                #expect((stmt.columnInt64(at: 0) ?? 0) > 0)
                #expect(stmt.columnText(at: 1) == nil)
            }
        }

        // 3. in database sub The directory is restored to present / committed
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT local_status, remote_status, phase, is_tombstone FROM items WHERE item_id = ?;")
            stmt.bindInt64(subDirItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnText(at: 0) == "present")
            #expect(stmt.columnText(at: 1) == "present")
            #expect(stmt.columnText(at: 2) == "committed")
            #expect(stmt.columnInt64(at: 3) == 0)
        }
    }

    @Test("Scenario 5: Remote directory and child deleted -> child deleted first, empty local parent directory moved to trash safely")
    func testRemoteDirAndChildDeletedLocalCleanedBottomUp() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("barrier_test5_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        let subDir = localRootDir.appendingPathComponent("sub")
        try FileManager.default.createDirectory(at: subDir, withIntermediateDirectories: true)

        let childFile = subDir.appendingPathComponent("child.txt")
        let contentData = Data("Content to delete".utf8)
        try contentData.write(to: childFile)
        let validSha256 = SHA256.hash(data: contentData).map { String(format: "%02x", $0) }.joined()

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootRemoteId = "remote_root_5"
        defer { removeTestDownloadDirectory(remoteRootID: rootRemoteId) }
        let parentDirRemoteId = "remote_folder_sub_5"
        let childFileRemoteId = "remote_file_child_5"

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

        let subDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    remote_status, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'sub', 'directory', ?,
                    'trashed', 'present', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            stmt.bindText(parentDirRemoteId, at: 3)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let childFileItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    base_sha256, base_size,
                    remote_sha256, remote_size, remote_status,
                    local_generation, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'child.txt', 'file', ?,
                    1, 2, 100, ?, ?,
                    ?, ?,
                    ?, ?, 'trashed',
                    1, 'present', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(subDirItemId, at: 2)
            stmt.bindText(childFileRemoteId, at: 3)
            stmt.bindInt64(Int64(contentData.count), at: 4)
            stmt.bindText(validSha256, at: 5)
            stmt.bindText(validSha256, at: 6)
            stmt.bindInt64(Int64(contentData.count), at: 7)
            stmt.bindText(validSha256, at: 8)
            stmt.bindInt64(Int64(contentData.count), at: 9)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        context.value.requestHandler = { request in
            let url = request.url?.absoluteString ?? ""
            if url.contains("/drive/v3/files/\(rootRemoteId)") {
                let json = """
                {"id": "\(rootRemoteId)", "name": "local_root", "mimeType": "application/vnd.google-apps.folder", "trashed": false}
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }
            if url.contains("/drive/v3/changes/startPageToken") {
                let json = "{\"startPageToken\": \"token_123\"}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }
            if url.contains("/drive/v3/changes") {
                let json = "{\"newStartPageToken\": \"token_456\", \"changes\": []}"
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, Data("{}".utf8))
        }

        // These cases exercise an established baseline; a missing cursor now correctly
        // requests index reconstruction, covered separately by ChangesRecoveryTests.
        try await store.write { conn in
            try conn.execute("INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) SELECT root_id, 'default', 'drive_changes', 'token_123', 100 FROM roots;")
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client)
        try await engine.syncIncremental(localPath: localRootDir.path, remoteRootId: rootRemoteId)

        // Verification:
        // 1. Local file removed (moved to Trash)
        #expect(!FileManager.default.fileExists(atPath: childFile.path))
        // 2. Local subdirectories that become empty are also safely moved to the Trash
        #expect(!FileManager.default.fileExists(atPath: subDir.path))

        // 3. Both are in the database tombstone
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT is_tombstone FROM items WHERE item_id IN (?, ?);")
            stmt.bindInt64(subDirItemId, at: 1)
            stmt.bindInt64(childFileItemId, at: 2)
            var count = 0
            while try stmt.step() {
                #expect(stmt.columnInt64(at: 0) == 1)
                count += 1
            }
            #expect(count == 2)
        }
    }
}
