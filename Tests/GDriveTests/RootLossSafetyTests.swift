import Foundation
import Testing
@testable import GDrive

final class MockRootURLProtocol: URLProtocol, @unchecked Sendable {
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

@Suite("Root Loss Safety Tests")
struct RootLossSafetyTests {
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
        config.protocolClasses = [MockRootURLProtocol.self]
        let session = URLSession(configuration: config)
        return DriveClient(auth: auth, session: session, requestsPerSecond: nil)
    }

    @Test("Same-path root replacement stops before remote mutation")
    func samePathReplacementStopsBeforeRemoteMutation() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "root-replacement-\(UUID().uuidString)")
        let localRoot = directory.appendingPathComponent("local")
        let displacedRoot = directory.appendingPathComponent("displaced")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let identity = try #require(try LocalDirectoryIdentity.read(at: localRoot))
        let auth = try createMockAuth(tempDir: directory)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        try await store.write { conn in
            let root = try conn.prepare("""
                INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, ?, ?, 'root', 'localToRemoteEmpty', 'existingKnown', 1, 1);
                """)
            root.bindText(localRoot.path, at: 1)
            root.bindInt64(identity.device, at: 2)
            root.bindInt64(identity.inode, at: 3)
            _ = try root.step()
            let rootID = conn.lastInsertRowId
            let rootItem = try conn.prepare("""
                INSERT INTO items(root_id, name, entry_kind, remote_file_id, local_status,
                    remote_status, phase, created_at, updated_at)
                VALUES (?, 'local', 'directory', 'root', 'present', 'present', 'committed', 1, 1);
                """)
            rootItem.bindInt64(rootID, at: 1)
            _ = try rootItem.step()
            let itemID = conn.lastInsertRowId
            let file = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'keep.txt', 'file', 'remote-keep',
                    'present', 'present', 'committed', 0, 1, 1);
                """)
            file.bindInt64(rootID, at: 1)
            file.bindInt64(itemID, at: 2)
            _ = try file.step()
        }

        try FileManager.default.moveItem(at: localRoot, to: displacedRoot)
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        context.value.requestHandler = { _ in
            Issue.record("Root identity validation must happen before remote requests")
            throw URLError(.badURL)
        }
        defer { context.value.requestHandler = nil }
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"))

        let error = await #expect(throws: SyncEngineError.self) {
            try await engine.syncIncremental(localPath: localRoot.path)
        }
        #expect(error == .localRootChanged(path: localRoot.path))
        let state = try await store.read { conn in
            let query = try conn.prepare(
                "SELECT local_status, remote_status, phase, dirty_generation FROM items WHERE remote_file_id = 'remote-keep';")
            defer { query.reset() }
            #expect(try query.step())
            return (query.columnText(at: 0), query.columnText(at: 1),
                query.columnText(at: 2), query.columnInt64(at: 3))
        }
        #expect(state.0 == "present")
        #expect(state.1 == "present")
        #expect(state.2 == "committed")
        #expect(state.3 == 0)
    }

    @Test("Root replacement after scanning stops before deletion propagation")
    func scanReplacementStopsBeforeRemoteMutation() async throws {
        final class Requests: @unchecked Sendable {
            private let lock = NSLock()
            private var mutationCount = 0
            func recordMutation() { lock.withLock { mutationCount += 1 } }
            func count() -> Int { lock.withLock { mutationCount } }
        }

        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "root-scan-replacement-\(UUID().uuidString)")
        let localRoot = directory.appendingPathComponent("local")
        let displacedRoot = directory.appendingPathComponent("displaced")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let localFile = localRoot.appendingPathComponent("keep.txt")
        let contents = Data("keep".utf8)
        try contents.write(to: localFile)
        let identity = try #require(try LocalDirectoryIdentity.read(at: localRoot))
        let version = try #require(try LocalFileVersion.read(at: localFile))
        let sha256 = SyncEngine.computeSha256(of: contents)
        let auth = try createMockAuth(tempDir: directory)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        try await store.write { conn in
            let root = try conn.prepare("""
                INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, ?, ?, 'root', 'localToRemoteEmpty', 'existingKnown', 1, 1);
                """)
            root.bindText(localRoot.path, at: 1)
            root.bindInt64(identity.device, at: 2)
            root.bindInt64(identity.inode, at: 3)
            _ = try root.step()
            let rootID = conn.lastInsertRowId
            let rootItem = try conn.prepare("""
                INSERT INTO items(root_id, name, entry_kind, remote_file_id, local_status,
                    remote_status, phase, created_at, updated_at)
                VALUES (?, 'local', 'directory', 'root', 'present', 'present', 'committed', 1, 1);
                """)
            rootItem.bindInt64(rootID, at: 1)
            _ = try rootItem.step()
            let rootItemID = conn.lastInsertRowId
            let file = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    base_sha256, base_size, remote_sha256, remote_size,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'keep.txt', 'file', 'remote-keep', ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    'present', 'present', 'committed', 0, 1, 1);
                """)
            file.bindInt64(rootID, at: 1)
            file.bindInt64(rootItemID, at: 2)
            file.bindInt64(version.device, at: 3)
            file.bindInt64(version.inode, at: 4)
            file.bindInt64(version.mtime, at: 5)
            file.bindInt64(version.size, at: 6)
            file.bindText(sha256, at: 7)
            file.bindText(sha256, at: 8)
            file.bindInt64(version.size, at: 9)
            file.bindText(sha256, at: 10)
            file.bindInt64(version.size, at: 11)
            _ = try file.step()
            let cursor = try conn.prepare("""
                INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at)
                VALUES (?, 'default', 'drive_changes', 'C0', 1);
                """)
            cursor.bindInt64(rootID, at: 1)
            _ = try cursor.step()
        }

        let requests = Requests()
        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let response = try #require(HTTPURLResponse(
                url: url, statusCode: 200, httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]))
            if request.httpMethod != "GET" { requests.recordMutation() }
            if url.path.hasSuffix("/files/root") {
                return (response, Data(
                    #"{"id":"root","name":"root","mimeType":"application/vnd.google-apps.folder"}"#.utf8))
            }
            if url.path.hasSuffix("/changes") {
                return (response, Data(#"{"changes":[],"newStartPageToken":"C1"}"#.utf8))
            }
            throw URLError(.badURL)
        }
        defer { context.value.requestHandler = nil }
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            incrementalScan: { request, consume in
                try await SyncEngine.defaultDirectoryScan(request, consume)
                try FileManager.default.moveItem(at: localRoot, to: displacedRoot)
                try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
            })

        let error = await #expect(throws: SyncEngineError.self) {
            try await engine.syncIncremental(localPath: localRoot.path)
        }
        #expect(error == .localRootChanged(path: localRoot.path))
        #expect(requests.count() == 0)
        let state = try await store.read { conn in
            let query = try conn.prepare(
                "SELECT local_status, remote_status, phase, dirty_generation FROM items WHERE remote_file_id = 'remote-keep';")
            defer { query.reset() }
            #expect(try query.step())
            return (query.columnText(at: 0), query.columnText(at: 1),
                query.columnText(at: 2), query.columnInt64(at: 3))
        }
        #expect(state.0 == "present")
        #expect(state.1 == "present")
        #expect(state.2 == "committed")
        #expect(state.3 == 0)
    }

    @Test(
        "Incremental sync requires a complete active binding for the local path",
        arguments: ["missing", "inactive", "missingRootItem"]
    )
    func incrementalSyncRequiresLocalBinding(_ scenario: String) async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(
            "incremental-binding-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }
        let localRoot = tempDir.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        let auth = try createMockAuth(tempDir: tempDir)
        let store = try await StateStore(path: tempDir.appendingPathComponent("state.sqlite").path)

        if scenario != "missing" {
            let rootID = try await store.write { conn in
                let statement = try conn.prepare("""
                    INSERT INTO roots (
                        account_id, local_root_path, local_root_device, local_root_inode,
                        remote_root_id, initial_sync_direction, bootstrap_state, is_active,
                        created_at, updated_at
                    ) VALUES ('default', ?, 1, 1, 'remote-root', 'localToRemoteEmpty',
                        'existingKnown', ?, 1, 1);
                    """)
                statement.bindText(localRoot.path, at: 1)
                statement.bindInt64(scenario == "inactive" ? 0 : 1, at: 2)
                _ = try statement.step()
                return conn.lastInsertRowId
            }
            if scenario == "inactive" {
                try await store.write { conn in
                    let statement = try conn.prepare("""
                        INSERT INTO items (
                            root_id, name, entry_kind, remote_file_id, phase, created_at, updated_at
                        ) VALUES (?, 'local', 'directory', 'remote-root', 'committed', 1, 1);
                        """)
                    statement.bindInt64(rootID, at: 1)
                    _ = try statement.step()
                }
            }
        }

        context.value.requestHandler = { _ in
            Issue.record("Incremental binding failure must happen before any remote request")
            throw URLError(.badURL)
        }
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth))
        let error = await #expect(throws: SyncEngineError.self) {
            _ = try await engine.syncIncremental(localPath: localRoot.path)
        }
        #expect(error?.localizedDescription ==
            "Directory has no available remote root ID: \(localRoot.path)")
    }

    @Test("Local root missing throws localRootNotFound error without trashing remote files or clearing DB")
    func testLocalRootMissingThrowsError() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("root_test_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let nonExistentPath = tempDir.appendingPathComponent("missing_local_root").path

        // Setup existing baseline
        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, 'remote_root_123', 'localToRemoteEmpty', 'existingKnown', 100, 100);
            """)
            stmt.bindText(nonExistentPath, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }
        let rootDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'missing_local_root', 'directory', 'remote_root_123', 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        _ = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    base_sha256, base_size, remote_sha256, remote_size, remote_status,
                    local_generation, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'file.txt', 'file', 'remote_file_abc',
                    1, 2, 100, 10, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
                    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 10,
                    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 10, 'present',
                    1, 'present', 'committed', 0, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        // Attempt syncIncremental: should throw SyncEngineError.localRootNotFound
        var threwExpectedError = false
        do {
            try await engine.syncIncremental(localPath: nonExistentPath)
        } catch let error as SyncEngineError {
            if case .localRootNotFound = error {
                threwExpectedError = true
            }
        } catch {}

        #expect(threwExpectedError, "Expected SyncEngineError.localRootNotFound when local root does not exist")

        // Baseline in SQLite must be preserved (not deleted)
        let rootCount = try await store.read { conn in
            let stmt = try conn.prepare("SELECT count(*) FROM roots WHERE root_id = ?;")
            stmt.bindInt64(rootId, at: 1)
            if try stmt.step() { return stmt.columnInt64(at: 0) ?? 0 }
            return 0
        }
        #expect(rootCount == 1, "Root record must be preserved in DB")

        let itemCount = try await store.read { conn in
            let stmt = try conn.prepare("SELECT count(*) FROM items WHERE root_id = ?;")
            stmt.bindInt64(rootId, at: 1)
            if try stmt.step() { return stmt.columnInt64(at: 0) ?? 0 }
            return 0
        }
        #expect(itemCount == 2, "Items must be preserved in DB")
    }

    @Test("Remote root trashed throws remoteRootLost error and preserves local files and DB")
    func testRemoteRootTrashedThrowsError() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("root_test_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)
        let localFile = localRootDir.appendingPathComponent("important_doc.pdf")
        try "important content".write(to: localFile, atomically: true, encoding: .utf8)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, 'remote_root_trashed', 'localToRemoteEmpty', 'existingKnown', 100, 100);
            """)
            stmt.bindText(localRootDir.path, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let rootDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'local_root', 'directory', 'remote_root_trashed', 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }
        try await setStoredRootIdentity(store: store, rootID: rootId, localURL: localRootDir)

        _ = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    base_sha256, base_size, remote_sha256, remote_size, remote_status,
                    local_generation, local_status, phase, dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, 'important_doc.pdf', 'file', 'remote_doc_xyz',
                    1, 2, 100, 17, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
                    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 17,
                    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 17, 'present',
                    1, 'present', 'committed', 0, 100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Mock remote root folder as trashed
        context.value.requestHandler = { request in
            let url = request.url?.absoluteString ?? ""
            if url.contains("/drive/v3/files/remote_root_trashed") {
                let json = """
                {
                    "id": "remote_root_trashed",
                    "name": "remote_root",
                    "mimeType": "application/vnd.google-apps.folder",
                    "trashed": true
                }
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }
            let response = HTTPURLResponse(url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!
            return (response, Data())
        }

        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        var threwExpectedError = false
        do {
            try await engine.syncIncremental(localPath: localRootDir.path)
        } catch let error as SyncEngineError {
            if case .remoteRootLost(let id, let reason) = error {
                #expect(id == "remote_root_trashed")
                #expect(reason == "trashed")
                threwExpectedError = true
            }
        } catch {}

        #expect(threwExpectedError, "Expected SyncEngineError.remoteRootLost when remote root is trashed")

        // Local file MUST NOT be deleted!
        #expect(FileManager.default.fileExists(atPath: localFile.path), "Local file must be protected and intact")

        // DB record must be preserved
        let rootCount = try await store.read { conn in
            let stmt = try conn.prepare("SELECT count(*) FROM roots WHERE root_id = ?;")
            stmt.bindInt64(rootId, at: 1)
            if try stmt.step() { return stmt.columnInt64(at: 0) ?? 0 }
            return 0
        }
        #expect(rootCount == 1, "Root record must be preserved in DB")
    }

    @Test("Remote root 404 (not found) throws remoteRootLost error and preserves local files and DB")
    func testRemoteRoot404ThrowsError() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("root_test_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)
        let localFile = localRootDir.appendingPathComponent("keep_me.txt")
        try "keep this".write(to: localFile, atomically: true, encoding: .utf8)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, 'remote_root_404', 'localToRemoteEmpty', 'existingKnown', 100, 100);
            """)
            stmt.bindText(localRootDir.path, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }
        try await setStoredRootIdentity(store: store, rootID: rootId, localURL: localRootDir)

        _ = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'local_root', 'directory', 'remote_root_404', 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Mock remote root folder as 404 Not Found
        context.value.requestHandler = { request in
            let response = HTTPURLResponse(url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!
            return (response, Data())
        }

        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        var threwExpectedError = false
        do {
            try await engine.syncIncremental(localPath: localRootDir.path)
        } catch let error as SyncEngineError {
            if case .remoteRootLost(let id, let reason) = error {
                #expect(id == "remote_root_404")
                #expect(reason == "notFound")
                threwExpectedError = true
            }
        } catch {}

        #expect(threwExpectedError, "Expected SyncEngineError.remoteRootLost when remote root is 404")
        #expect(FileManager.default.fileExists(atPath: localFile.path), "Local file must remain intact")
    }

    @Test("Remote root trashed in Changes feed throws remoteRootLost error and aborts immediately")
    func testRemoteRootTrashedInChangesStream() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("root_test_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        try FileManager.default.createDirectory(at: localRootDir, withIntermediateDirectories: true)
        let localFile = localRootDir.appendingPathComponent("local_doc.txt")
        try "data".write(to: localFile, atomically: true, encoding: .utf8)

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)

        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, 'remote_root_change_test', 'localToRemoteEmpty', 'existingKnown', 100, 100);
            """)
            stmt.bindText(localRootDir.path, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }
        try await setStoredRootIdentity(store: store, rootID: rootId, localURL: localRootDir)

        _ = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'local_root', 'directory', 'remote_root_change_test', 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Insert change cursor token
        try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO cursors (root_id, account_id, cursor_kind, token_value, updated_at)
                VALUES (?, 'default', 'drive_changes', 'token_page_1', 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
        }

        context.value.requestHandler = { request in
            let url = request.url?.absoluteString ?? ""
            if url.contains("/drive/v3/files/remote_root_change_test") {
                // Pre-flight check passes (returns active directory)
                let json = """
                {
                    "id": "remote_root_change_test",
                    "name": "remote_root",
                    "mimeType": "application/vnd.google-apps.folder",
                    "trashed": false
                }
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            } else if url.contains("/drive/v3/changes") {
                // Changes feed reports remote root was trashed
                let json = """
                {
                    "changes": [
                        {
                            "fileId": "remote_root_change_test",
                            "file": {
                                "id": "remote_root_change_test",
                                "name": "remote_root",
                                "mimeType": "application/vnd.google-apps.folder",
                                "trashed": true
                            }
                        }
                    ],
                    "newStartPageToken": "token_page_2"
                }
                """
                let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
                return (response, json.data(using: .utf8)!)
            }
            let response = HTTPURLResponse(url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!
            return (response, Data())
        }

        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        var threwExpectedError = false
        do {
            try await engine.syncIncremental(localPath: localRootDir.path)
        } catch let error as SyncEngineError {
            if case .remoteRootLost(let id, let reason) = error {
                #expect(id == "remote_root_change_test")
                #expect(reason == "trashed")
                threwExpectedError = true
            }
        } catch {}

        #expect(threwExpectedError, "Expected SyncEngineError.remoteRootLost when root is trashed in change stream")
        #expect(FileManager.default.fileExists(atPath: localFile.path), "Local file must remain intact")
    }
}
