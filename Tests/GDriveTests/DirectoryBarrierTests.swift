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

    @Test("Renaming a directory updates every descendant path index")
    func directoryContextRenameUpdatesDescendants() {
        let directories = DirectoryContext(rootItemId: 1, remoteRootId: "root")
        directories.register(itemId: 2, parentItemId: 1, name: "old", remoteId: "parent")
        directories.register(itemId: 3, parentItemId: 2, name: "child", remoteId: "child")
        directories.register(itemId: 4, parentItemId: 3, name: "deep", remoteId: "deep")

        directories.register(
            itemId: 2, parentItemId: 1, name: "new", remoteId: "parent",
            updateDescendantPaths: true)

        #expect(directories.getRelPath(for: 2) == "new")
        #expect(directories.getRelPath(for: 3) == "new/child")
        #expect(directories.getRelPath(for: 4) == "new/child/deep")
        #expect(directories.getItemId(byRelPath: "old") == nil)
        #expect(directories.getItemId(byRelPath: "old/child") == nil)
        #expect(directories.getItemId(byRelPath: "old/child/deep") == nil)
        #expect(directories.getItemId(byRelPath: "new") == 2)
        #expect(directories.getItemId(byRelPath: "new/child") == 3)
        #expect(directories.getItemId(byRelPath: "new/child/deep") == 4)
    }

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

    @Test("Scenario 1: Local directory deletion trashes the remote subtree")
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
        try await setStoredRootIdentity(store: store, rootID: rootId, localURL: localRootDir)

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
        let conflictDirectory = tempDir.appendingPathComponent("conflicts")
        let engine = try await SyncEngine(
            auth: auth, store: store, client: client, conflictDirectory: conflictDirectory)
        try await engine.syncIncremental(localPath: localRootDir.path)

        #expect(trashedRemoteFolders.contains(parentDirRemoteId))
        let downloadedSubDir = localRootDir.appendingPathComponent("sub")
        let downloadedFile = downloadedSubDir.appendingPathComponent("child.txt")
        #expect(!FileManager.default.fileExists(atPath: downloadedSubDir.path))
        #expect(!FileManager.default.fileExists(atPath: downloadedFile.path))
        #expect(try await engine.listConflicts(localPath: localRootDir.path).isEmpty)
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT COUNT(*) FROM items WHERE item_id = ?;")
            stmt.bindInt64(subDirItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnInt64(at: 0) == 0)
        }
    }

    @Test("Scenario 2: Remote directory deletion trashes the local subtree")
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
        try await setStoredRootIdentity(store: store, rootID: rootId, localURL: localRootDir)

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
        try await engine.syncIncremental(localPath: localRootDir.path)

        #expect(!FileManager.default.fileExists(atPath: subDir.path))
        #expect(!FileManager.default.fileExists(atPath: localNewFile.path))
        #expect(!untrashedRemoteFolders.contains(parentDirRemoteId))
        #expect(!uploadedFiles.contains("local_new.txt"))
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT COUNT(*) FROM items WHERE item_id = ?;")
            stmt.bindInt64(subDirItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnInt64(at: 0) == 0)
        }
    }

    @Test("Scenario 3: Nested local directory deletions trash remote directories bottom up")
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
        try await setStoredRootIdentity(store: store, rootID: rootId, localURL: localRootDir)

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
        try await engine.syncIncremental(localPath: localRootDir.path)

        #expect(trashedRemoteOrder == [childDirRemoteId, parentDirRemoteId])
        try await store.read { conn in
            let all = try conn.prepare("SELECT COUNT(*) FROM items WHERE item_id IN (?, ?, ?);")
            all.bindInt64(level1ItemId, at: 1)
            all.bindInt64(level2ItemId, at: 2)
            all.bindInt64(fileItemId, at: 3)
            #expect(try all.step())
            #expect(all.columnInt64(at: 0) == 0)
        }
    }

    @Test("Scenario 4: Local directory deletion wins over pending remote descendants", arguments: ["none", "local_generation", "remote_generation", "dirty_generation"])
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
        try await setStoredRootIdentity(store: store, rootID: rootId, localURL: localRootDir)

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
        try await engine.syncIncremental(localPath: localRootDir.path)

        #expect(trashedRemoteFolders.contains(parentDirRemoteId))
        let subDirURL = localRootDir.appendingPathComponent("sub")
        let downloadedFile = subDirURL.appendingPathComponent("remote_added.txt")
        #expect(!FileManager.default.fileExists(atPath: subDirURL.path))
        #expect(!FileManager.default.fileExists(atPath: downloadedFile.path))
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT COUNT(*) FROM items WHERE remote_file_id = ?;")
            stmt.bindText(newChildRemoteId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnInt64(at: 0) == 0)
        }
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT COUNT(*) FROM items WHERE item_id = ?;")
            stmt.bindInt64(subDirItemId, at: 1)
            #expect(try stmt.step())
            #expect(stmt.columnInt64(at: 0) == 0)
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
        let observedChildVersion = try LocalFileVersion.read(at: childFile)
        let childVersion = try #require(observedChildVersion)

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
        try await setStoredRootIdentity(store: store, rootID: rootId, localURL: localRootDir)

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
                    ?, ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, 'trashed',
                    1, 'present', 'ready', 1, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(subDirItemId, at: 2)
            stmt.bindText(childFileRemoteId, at: 3)
            stmt.bindInt64(childVersion.device, at: 4)
            stmt.bindInt64(childVersion.inode, at: 5)
            stmt.bindInt64(childVersion.mtime, at: 6)
            stmt.bindInt64(Int64(contentData.count), at: 7)
            stmt.bindText(validSha256, at: 8)
            stmt.bindText(validSha256, at: 9)
            stmt.bindInt64(Int64(contentData.count), at: 10)
            stmt.bindText(validSha256, at: 11)
            stmt.bindInt64(Int64(contentData.count), at: 12)
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
        try await engine.syncIncremental(localPath: localRootDir.path)

        // Verification:
        // 1. Local file removed (moved to Trash)
        #expect(!FileManager.default.fileExists(atPath: childFile.path))
        // 2. Local subdirectories that become empty are also safely moved to the Trash
        #expect(!FileManager.default.fileExists(atPath: subDir.path))

        // 3. Both rows are physically deleted
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT COUNT(*) FROM items WHERE item_id IN (?, ?);")
            stmt.bindInt64(subDirItemId, at: 1)
            stmt.bindInt64(childFileItemId, at: 2)
            #expect(try stmt.step())
            #expect(stmt.columnInt64(at: 0) == 0)
        }
    }
}
