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
