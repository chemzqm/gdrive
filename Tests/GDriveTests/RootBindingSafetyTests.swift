import Foundation
import Testing
@testable import GDrive

final class MockRootBindingURLProtocol: URLProtocol, @unchecked Sendable {
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

@Suite("Root binding safety")
struct RootBindingSafetyTests {
    private enum Entry: String, CaseIterable, Sendable {
        case unified
        case upload
        case download
    }

    private let context = TestHTTPContext(TestRequestHandler())

    private enum OperationalState: String, CaseIterable, Sendable {
        case credentials
        case database
        case downloads
        case conflicts
    }

    private func createMockAuth(tempDir: URL) throws -> Auth {
        let authURL = tempDir.appendingPathComponent("auth.json")
        let authData = AuthData(
            clientId: "mock_client",
            rootID: "root",
            accessToken: "mock_access_token",
            expiresAt: Date().addingTimeInterval(3600))
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(authData).write(to: authURL)
        return try Auth(path: authURL.path)
    }

    private func createMockClient(auth: Auth) -> DriveClient {
        let configuration = URLSessionConfiguration.ephemeral
        context.configure(configuration)
        configuration.protocolClasses = [MockRootBindingURLProtocol.self]
        return DriveClient(
            auth: auth, session: URLSession(configuration: configuration), requestsPerSecond: nil)
    }

    @Test("Operational state cannot overlap a sync root", arguments: OperationalState.allCases)
    private func operationalStateCannotOverlapSyncRoot(_ state: OperationalState) async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "state-overlap-\(UUID().uuidString)")
        let localRoot = directory.appendingPathComponent("local")
        let external = directory.appendingPathComponent("external")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: external, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let credentialsDirectory = state == .credentials ? localRoot : external
        let auth = try createMockAuth(tempDir: credentialsDirectory)
        let databaseDirectory = state == .database ? localRoot : external
        let store = try await StateStore(
            path: databaseDirectory.appendingPathComponent("state.sqlite").path)
        let downloads = (state == .downloads ? localRoot : external)
            .appendingPathComponent("downloads")
        let conflicts = (state == .conflicts ? localRoot : external)
            .appendingPathComponent("conflicts")

        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth),
            downloadTemporaryDirectory: downloads, conflictDirectory: conflicts)

        let error = await #expect(throws: SyncEngineError.self) {
            try await engine.syncLocalToRemoteEmpty(
                localPath: localRoot.path, remoteRootId: "root")
        }
        guard case .general(let message) = error else {
            Issue.record("Expected an operational-state overlap error")
            return
        }
        let expectedDescription = switch state {
        case .credentials: "credential"
        case .database: "database"
        case .downloads: "download temporary"
        case .conflicts: "conflict"
        }
        #expect(message.contains(expectedDescription))
        let rootCount = try await store.read { conn in
            let query = try conn.prepare("SELECT COUNT(*) FROM roots;")
            _ = try #require(try query.step())
            return query.columnInt64(at: 0)
        }
        #expect(rootCount == 0)
    }

    @discardableResult
    private func seedBinding(
        store: StateStore,
        localRoot: URL,
        remoteRootID: String,
        bootstrapState: String = "existingKnown"
    ) async throws -> Int64 {
        let identity = try #require(try LocalDirectoryIdentity.read(at: localRoot))
        return try await store.write { conn in
            let root = try conn.prepare("""
                INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, ?, ?, ?, 'localToRemoteEmpty', ?, 1, 1);
                """)
            root.bindText(localRoot.path, at: 1)
            root.bindInt64(identity.device, at: 2)
            root.bindInt64(identity.inode, at: 3)
            root.bindText(remoteRootID, at: 4)
            root.bindText(bootstrapState, at: 5)
            _ = try root.step()
            let rootID = conn.lastInsertRowId

            let rootItem = try conn.prepare("""
                INSERT INTO items(root_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status, phase,
                    created_at, updated_at)
                VALUES (?, ?, 'directory', ?, ?, ?, 'present', 'present', 'committed', 1, 1);
                """)
            rootItem.bindInt64(rootID, at: 1)
            rootItem.bindText(localRoot.lastPathComponent, at: 2)
            rootItem.bindText(remoteRootID, at: 3)
            rootItem.bindInt64(identity.device, at: 4)
            rootItem.bindInt64(identity.inode, at: 5)
            _ = try rootItem.step()

            let cursor = try conn.prepare("""
                INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at)
                VALUES (?, 'default', 'drive_changes', 'C0', 1);
                """)
            cursor.bindInt64(rootID, at: 1)
            _ = try cursor.step()
            return rootID
        }
    }

    private func seedConflict(
        store: StateStore,
        rootID: Int64,
        localRoot: URL,
        conflictPath: URL
    ) async throws {
        try await store.write { conn in
            let rootItem = try conn.prepare(
                "SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL;")
            rootItem.bindInt64(rootID, at: 1)
            _ = try #require(try rootItem.step())
            let rootItemID = try #require(rootItem.columnInt64(at: 0))

            let item = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    remote_sha256, remote_size, local_status, remote_status, phase,
                    created_at, updated_at)
                VALUES (?, ?, 'file.txt', 'file', 'remote-file', ?, 1,
                    'absent', 'present', 'blocked', 1, 1);
                """)
            item.bindInt64(rootID, at: 1)
            item.bindInt64(rootItemID, at: 2)
            item.bindText(String(repeating: "a", count: 64), at: 3)
            _ = try item.step()
            let itemID = conn.lastInsertRowId

            let conflict = try conn.prepare("""
                INSERT INTO sync_conflicts(
                    conflict_id, root_id, item_id, remote_file_id, relative_path,
                    local_path, conflict_path, remote_sha256, remote_size,
                    remote_status, created_at, updated_at)
                VALUES ('conflict', ?, ?, 'remote-file', 'file.txt', ?, ?, ?, 1,
                    'present', 1, 1);
                """)
            conflict.bindInt64(rootID, at: 1)
            conflict.bindInt64(itemID, at: 2)
            conflict.bindText(localRoot.appendingPathComponent("file.txt").path, at: 3)
            conflict.bindText(conflictPath.path, at: 4)
            conflict.bindText(String(repeating: "a", count: 64), at: 5)
            _ = try conflict.step()
        }
    }

    @Test("Operational directories cannot overlap another bound sync root")
    private func operationalDirectoryCannotOverlapAnotherBoundRoot() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "cross-root-state-overlap-\(UUID().uuidString)")
        let localA = directory.appendingPathComponent("local-a")
        let localB = directory.appendingPathComponent("local-b")
        let external = directory.appendingPathComponent("external")
        try FileManager.default.createDirectory(at: localA, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: localB, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: external, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await StateStore(path: external.appendingPathComponent("state.sqlite").path)
        _ = try await seedBinding(store: store, localRoot: localA, remoteRootID: "root-a")
        context.value.requestHandler = { _ in
            Issue.record("Cross-root rejection must happen before any remote request")
            throw URLError(.badURL)
        }
        defer { context.value.requestHandler = nil }
        let auth = try createMockAuth(tempDir: external)
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth),
            downloadTemporaryDirectory: external.appendingPathComponent("downloads"),
            conflictDirectory: localA.appendingPathComponent("conflicts"))

        let error = await #expect(throws: SyncEngineError.self) {
            try await engine.syncLocalToRemoteEmpty(
                localPath: localB.path, remoteRootId: "root-b")
        }
        guard case .general(let message) = error else {
            Issue.record("Expected an operational-state overlap error")
            return
        }
        #expect(message.contains("conflict directory"))
        #expect(message.contains(localA.path))
    }

    @Test("Persisted conflict copies cannot be inside a bound sync root")
    private func persistedConflictCannotBeInsideBoundRoot() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "persisted-conflict-overlap-\(UUID().uuidString)")
        let localRoot = directory.appendingPathComponent("local")
        let external = directory.appendingPathComponent("external")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: external, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await StateStore(path: external.appendingPathComponent("state.sqlite").path)
        let rootID = try await seedBinding(
            store: store, localRoot: localRoot, remoteRootID: "root")
        try await seedConflict(
            store: store, rootID: rootID, localRoot: localRoot,
            conflictPath: localRoot.appendingPathComponent("saved-conflict"))
        context.value.requestHandler = { _ in
            Issue.record("Persisted-conflict rejection must happen before any remote request")
            throw URLError(.badURL)
        }
        defer { context.value.requestHandler = nil }
        let auth = try createMockAuth(tempDir: external)
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth),
            downloadTemporaryDirectory: external.appendingPathComponent("downloads"),
            conflictDirectory: external.appendingPathComponent("conflicts"))

        let error = await #expect(throws: SyncEngineError.self) {
            try await engine.syncIncremental(localPath: localRoot.path)
        }
        guard case .general(let message) = error else {
            Issue.record("Expected a persisted-conflict overlap error")
            return
        }
        #expect(message.contains("persisted conflict"))
        #expect(message.contains(localRoot.path))
    }

    @Test("A remote root cannot be rebound to another local root", arguments: Entry.allCases)
    private func cannotBindRemoteRootToDifferentLocalRoot(entry: Entry) async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "root-binding-\(UUID().uuidString)")
        let localA = directory.appendingPathComponent("local-a")
        let localB = directory.appendingPathComponent("local-b")
        try FileManager.default.createDirectory(at: localA, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: localB, withIntermediateDirectories: true)
        let keptFile = localA.appendingPathComponent("keep.txt")
        try Data("keep".utf8).write(to: keptFile)
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let rootID = try await seedBinding(store: store, localRoot: localA, remoteRootID: "root")
        try await store.write { conn in
            let query = try conn.prepare(
                "SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL;")
            query.bindInt64(rootID, at: 1)
            _ = try #require(try query.step())
            let rootItemID = try #require(query.columnInt64(at: 0))
            let item = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, created_at, updated_at)
                VALUES (?, ?, 'keep.txt', 'file', 'remote-keep',
                    'present', 'present', 'committed', 1, 1);
                """)
            item.bindInt64(rootID, at: 1)
            item.bindInt64(rootItemID, at: 2)
            _ = try item.step()
        }

        context.value.requestHandler = { _ in
            Issue.record("Binding rejection must happen before any remote request")
            throw URLError(.badURL)
        }
        defer { context.value.requestHandler = nil }
        let auth = try createMockAuth(tempDir: directory)
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"))

        let error = await #expect(throws: SyncEngineError.self) {
            switch entry {
            case .unified:
                try await engine.sync(localPath: localB.path, remoteFolderId: "root")
            case .upload:
                try await engine.syncLocalToRemoteEmpty(localPath: localB.path, remoteRootId: "root")
            case .download:
                try await engine.syncRemoteToLocalEmpty(localPath: localB.path, remoteRootId: "root")
            }
        }
        #expect(error == .rootBindingConflict(
            localPath: localB.path,
            remoteRootId: "root",
            existingLocalPath: localA.path,
            existingRemoteRootId: "root"))
        #expect(try Data(contentsOf: keptFile) == Data("keep".utf8))
        #expect(try FileManager.default.contentsOfDirectory(atPath: localB.path).isEmpty)

        let persisted = try await store.read { conn in
            let root = try conn.prepare("""
                SELECT local_root_path, remote_root_id, bootstrap_state
                FROM roots WHERE root_id = ?;
                """)
            root.bindInt64(rootID, at: 1)
            _ = try #require(try root.step())
            let cursor = try conn.prepare(
                "SELECT token_value FROM cursors WHERE root_id = ? AND cursor_kind = 'drive_changes';")
            cursor.bindInt64(rootID, at: 1)
            _ = try #require(try cursor.step())
            let count = try conn.prepare("SELECT COUNT(*) FROM items WHERE root_id = ?;")
            count.bindInt64(rootID, at: 1)
            _ = try #require(try count.step())
            return (root.columnText(at: 0), root.columnText(at: 1), root.columnText(at: 2),
                cursor.columnText(at: 0), count.columnInt64(at: 0))
        }
        #expect(persisted.0 == localA.path)
        #expect(persisted.1 == "root")
        #expect(persisted.2 == "existingKnown")
        #expect(persisted.3 == "C0")
        #expect(persisted.4 == 2)
    }

    @Test("A local root cannot be rebound to another remote root")
    private func cannotBindLocalRootToDifferentRemoteRoot() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "local-binding-\(UUID().uuidString)")
        let localRoot = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        _ = try await seedBinding(store: store, localRoot: localRoot, remoteRootID: "root-a")
        context.value.requestHandler = { _ in
            Issue.record("Binding rejection must happen before any remote request")
            throw URLError(.badURL)
        }
        defer { context.value.requestHandler = nil }
        let auth = try createMockAuth(tempDir: directory)
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth))

        let error = await #expect(throws: SyncEngineError.self) {
            try await engine.sync(localPath: localRoot.path, remoteFolderId: "root-b")
        }
        #expect(error == .rootBindingConflict(
            localPath: localRoot.path,
            remoteRootId: "root-b",
            existingLocalPath: localRoot.path,
            existingRemoteRootId: "root-a"))
    }

    @Test("Nested local roots cannot be bound to different remote roots")
    private func cannotBindNestedLocalRoots() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "nested-binding-\(UUID().uuidString)")
        let parent = directory.appendingPathComponent("parent")
        let child = parent.appendingPathComponent("child")
        try FileManager.default.createDirectory(at: child, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        _ = try await seedBinding(store: store, localRoot: parent, remoteRootID: "parent-root")

        let childError = await #expect(throws: SyncEngineError.self) {
            try await store.read { conn in
                try SyncEngine.validateRootBinding(
                    conn: conn,
                    localPath: RootSyncCoordinator.normalizedPath(child.path),
                    remoteRootID: "child-root")
            }
        }
        #expect(childError == .rootBindingConflict(
            localPath: child.path,
            remoteRootId: "child-root",
            existingLocalPath: parent.path,
            existingRemoteRootId: "parent-root"))

        let separateStore = try await StateStore(
            path: directory.appendingPathComponent("other-state.sqlite").path)
        _ = try await seedBinding(
            store: separateStore, localRoot: child, remoteRootID: "child-root")
        let parentError = await #expect(throws: SyncEngineError.self) {
            try await separateStore.read { conn in
                try SyncEngine.validateRootBinding(
                    conn: conn,
                    localPath: RootSyncCoordinator.normalizedPath(parent.path),
                    remoteRootID: "parent-root")
            }
        }
        #expect(parentError == .rootBindingConflict(
            localPath: parent.path,
            remoteRootId: "parent-root",
            existingLocalPath: child.path,
            existingRemoteRootId: "child-root"))
    }

    @Test("The same root binding can resume bootstrap")
    private func sameBindingCanResume() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "same-binding-\(UUID().uuidString)")
        let localRoot = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        _ = try await seedBinding(
            store: store, localRoot: localRoot, remoteRootID: "root", bootstrapState: "freshCreated")
        context.value.requestHandler = { request in
            let url = try #require(request.url)
            guard url.path.hasSuffix("/files/root") else { throw URLError(.badURL) }
            let response = try #require(HTTPURLResponse(
                url: url, statusCode: 200, httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]))
            return (response, Data(
                #"{"id":"root","name":"root","mimeType":"application/vnd.google-apps.folder"}"#.utf8))
        }
        defer { context.value.requestHandler = nil }
        let auth = try createMockAuth(tempDir: directory)
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth))

        let stats = try await engine.syncLocalToRemoteEmpty(
            localPath: localRoot.path, remoteRootId: "root")
        #expect(stats.filesUploaded == 0)
        #expect(stats.filesFailed == 0)
        let state = try await store.read { conn in
            let query = try conn.prepare("SELECT bootstrap_state FROM roots WHERE remote_root_id = 'root';")
            _ = try #require(try query.step())
            return query.columnText(at: 0)
        }
        #expect(state == "existingKnown")
    }
}
