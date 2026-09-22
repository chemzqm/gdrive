import Foundation
import Testing
import os
@testable import GDrive

private struct ConflictRemoteFile: Sendable {
    let id: String
    var name: String
    var parentID = "root"
    var content: Data
    var trashed = false
    var json: [String: Any] {
        ["id": id, "name": name, "parents": [parentID], "size": String(content.count),
         "sha256Checksum": SyncEngine.computeSha256(of: content), "version": "2", "trashed": trashed]
    }
}
private struct ConflictServerState: Sendable {
    var files: [String: ConflictRemoteFile] = [:]
    var uploads = 0
    var downloads = 0
    var loseResponse = false
    var sessions: [String: ConflictRemoteFile] = [:]
    var sessionSizes: [String: Int] = [:]
    var removed: Set<String> = []
    var emitChanges = true
    var metadataPatches = 0
}
private struct ConflictProtocolState: Sendable {
    let state = OSAllocatedUnfairLock(initialState: ConflictServerState())
    let downloadHook = OSAllocatedUnfairLock<(@Sendable () -> Void)?>(initialState: nil)
}
private final class ConflictProtocol: URLProtocol, @unchecked Sendable {
    private lazy var server = TestHTTPContext<ConflictProtocolState>.value(for: request)!
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    private struct Response {
        let status: Int
        let data: Data
        let headers: [String: String]
    }
    override func startLoading() {
        do {
            let url = request.url!
            let response = try self.server.state.withLock { state in
                try self.routedResponse(for: url, state: &state)
            }
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: url, statusCode: response.status, httpVersion: nil, headerFields: response.headers)!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: response.data)
            client?.urlProtocolDidFinishLoading(self)
        } catch { client?.urlProtocol(self, didFailWithError: error) }
    }
    private func trashResponse(for url: URL, state: inout ConflictServerState) throws -> Response? {
        guard request.httpMethod == "PATCH",
              let trashed = (try JSONSerialization.jsonObject(
                with: request.extractBodyData ?? Data()) as? [String: Bool])?["trashed"] else {
            return nil
        }
        state.metadataPatches += 1
        let id = url.lastPathComponent
        guard var file = state.files[id] else {
            return Response(status: 404, data: Data(), headers: [:])
        }
        file.trashed = trashed
        state.files[id] = file
        return Response(status: 200,
            data: try JSONSerialization.data(withJSONObject: file.json), headers: [:])
    }
    private func routedResponse(for url: URL, state: inout ConflictServerState) throws -> Response {
        if let response = try trashResponse(for: url, state: &state) { return response }
        return try response(for: url, state: &state)
    }
    private func response(for url: URL, state: inout ConflictServerState) throws -> Response {
        var headers: [String: String] = [:]
        var json: [String: Any] = [:]
        func receiveChunk() throws -> Response {
            let id = url.lastPathComponent
            var file = try #require(state.sessions[id])
            let chunk = request.extractBodyData ?? Data()
            file.content.append(chunk)
            state.sessions[id] = file
            if file.content.count == state.sessionSizes[id] {
                state.files[id] = file
                state.uploads += 1
                json = file.json
            } else {
                headers["Range"] = "bytes=0-\(file.content.count - 1)"
                return Response(status: 308, data: Data(), headers: headers)
            }
            return Response(status: 200, data: try JSONSerialization.data(withJSONObject: json), headers: headers)
        }

        func receiveMultipart() throws -> Response {
            let body = (String(bytes: request.extractBodyData ?? Data(), encoding: .utf8) ?? "Invalid UTF-8 data")
            let start = try #require(body.firstIndex(of: "{"))
            let end = try #require(body[start...].firstIndex(of: "}"))
            let meta = try #require(JSONSerialization.jsonObject(with: Data(body[start...end].utf8)) as? [String: Any])
            let id = try #require(meta["id"] as? String)
            let name = try #require(meta["name"] as? String)
            let marker = "Content-Type: application/octet-stream\r\n\r\n"
            let contentStart = try #require(body.range(of: marker)?.upperBound)
            let contentEnd = try #require(body.range(of: "\r\n--", range: contentStart..<body.endIndex)?.lowerBound)
            let file = ConflictRemoteFile(id: id, name: name, content: Data(body[contentStart..<contentEnd].utf8))
            state.files[id] = file
            state.uploads += 1
            if state.loseResponse { state.loseResponse = false; return Response(status: 400, data: Data(), headers: headers) }
            json = file.json
            return Response(status: 200, data: try JSONSerialization.data(withJSONObject: json), headers: headers)
        }

        if url.path.hasSuffix("/changes/startPageToken") {
            json = ["startPageToken": "start"]
        } else if url.path.hasSuffix("/changes") {
            let present = state.emitChanges
                ? state.files.values.map { ["fileId": $0.id, "file": $0.json] as [String: Any] }
                : []
            let removed = state.emitChanges
                ? state.removed.map { ["fileId": $0, "removed": true] as [String: Any] }
                : []
            json = ["changes": present + removed, "newStartPageToken": "next"]
        } else if url.path.hasSuffix("/generateIds") {
            json = ["ids": (0..<1000).map { "generated-\($0)" }]
        } else if url.path.hasPrefix("/session/") {
            return try receiveChunk()
        } else if url.path.contains("/upload/"), url.query?.contains("uploadType=resumable") == true {
            let meta = try #require(JSONSerialization.jsonObject(with: request.extractBodyData ?? Data()) as? [String: Any])
            let id = try #require(meta["id"] as? String)
            let name = try #require(meta["name"] as? String)
            state.sessions[id] = ConflictRemoteFile(id: id, name: name, content: Data())
            state.sessionSizes[id] = Int(request.value(forHTTPHeaderField: "X-Upload-Content-Length") ?? "")
            headers["Location"] = "https://www.googleapis.com/session/\(id)"
        } else if url.path.contains("/upload/") {
            return try receiveMultipart()
        } else if url.path.hasSuffix("/files/root") {
            json = ["id": "root", "name": "root", "mimeType": "application/vnd.google-apps.folder", "trashed": false]
        } else if url.path.hasSuffix("/files") {
            json = ["files": []]
        } else if let file = state.files[url.lastPathComponent] {
            if URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems?.contains(where: { $0.name == "alt" && $0.value == "media" }) == true {
                state.downloads += 1
                server.downloadHook.withLock { hook in
                    hook?()
                    hook = nil
                }
                return Response(status: 200, data: file.content, headers: headers)
            }
            json = file.json
        } else { return Response(status: 404, data: Data(), headers: headers) }
        return Response(status: 200, data: try JSONSerialization.data(withJSONObject: json), headers: headers)
    }
    override func stopLoading() {}
}

@Suite("Durable sync conflicts")
struct SyncConflictTests {
    private let context = TestHTTPContext(ConflictProtocolState())
    private struct Fixture {
        let directory: URL
        let local: URL
        let original: URL
        let engine: SyncEngine
        let store: StateStore
        let remoteID: String
    }
    private func fixture(
        localContent: Data = Data("local edited content".utf8),
        remoteContent: Data = Data("remote edited content".utf8),
        filePublisher: @escaping SyncEngine.FilePublisher = {
            try LocalFilePublication.publish($0, to: $1, expected: $2,
                                             expectedSHA256: $3, expectedLocalSHA256: $4)
        }
    ) async throws -> Fixture {
        context.value.state.withLock { $0 = ConflictServerState() }
        context.value.downloadHook.withLock { $0 = nil }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("a12-\(UUID().uuidString)")
        let local = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        let original = local.appendingPathComponent("file.txt")
        try Data("baseline".utf8).write(to: original)
        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(clientId: "test", accessToken: "test", expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)
        let configuration = URLSessionConfiguration.ephemeral
        context.configure(configuration)
        configuration.protocolClasses = [ConflictProtocol.self]
        let client = DriveClient(auth: auth, session: URLSession(configuration: configuration), requestsPerSecond: nil)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let engine = try await SyncEngine(auth: auth, store: store, client: client,
            idPool: IDPool(initialIds: (0..<1000).map { "id-\($0)" }),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            conflictDirectory: directory.appendingPathComponent("conflicts"),
            incrementalScan: SyncEngine.defaultDirectoryScan, filePublisher: filePublisher)
        let first = try await engine.syncLocalToRemoteEmpty(localPath: local.path, remoteRootId: "root")
        #expect(first.filesUploaded == 1)
        struct ItemIdentity: Sendable {
            let remoteID: String
        }
        let ids = try await store.read { conn -> ItemIdentity in
            let queryStatement = try conn.prepare(
                "SELECT remote_file_id FROM items WHERE entry_kind = 'file';")
            _ = try queryStatement.step()
            return ItemIdentity(remoteID: queryStatement.columnText(at: 0)!)
        }
        try localContent.write(to: original)
        context.value.state.withLock { $0.files[ids.remoteID]!.content = remoteContent }
        return Fixture(
            directory: directory, local: local, original: original,
            engine: engine, store: store, remoteID: ids.remoteID)
    }

    private func recreatedEngine(for fixture: Fixture) async throws -> SyncEngine {
        let auth = try Auth(path: fixture.directory.appendingPathComponent("auth.json").path)
        let configuration = URLSessionConfiguration.ephemeral
        context.configure(configuration)
        configuration.protocolClasses = [ConflictProtocol.self]
        let client = DriveClient(
            auth: auth, session: URLSession(configuration: configuration), requestsPerSecond: nil)
        let store = try await StateStore(path: fixture.store.path)
        return try await SyncEngine(
            auth: auth, store: store, client: client, idPool: IDPool(api: nil, initialIds: []),
            downloadTemporaryDirectory: fixture.directory.appendingPathComponent("downloads"),
            conflictDirectory: fixture.directory.appendingPathComponent("conflicts"),
            incrementalScan: SyncEngine.defaultDirectoryScan)
    }

    private func pendingChangeCount(_ store: StateStore, remoteID: String) async throws -> Int64 {
        try await store.read { conn in
            let query = try conn.prepare(
                "SELECT count(*) FROM remote_change_inbox WHERE remote_id = ?;")
            defer { query.reset() }
            query.bindText(remoteID, at: 1)
            _ = try query.step()
            return query.columnInt64(at: 0) ?? 0
        }
    }

    private func conflictRevision(_ store: StateStore, id: String) async throws -> Int64 {
        try await store.read { conn in
            let query = try conn.prepare(
                "SELECT revision FROM sync_conflicts WHERE conflict_id = ?;")
            defer { query.reset() }
            query.bindText(id, at: 1)
            _ = try #require(try query.step())
            return try #require(query.columnInt64(at: 0))
        }
    }

    private struct ItemTopology: Sendable {
        let name: String?
        let parentName: String?
        let remoteName: String?
        let remoteParentID: String?
    }

    private func itemTopology(_ store: StateStore, remoteID: String) async throws -> ItemTopology {
        try await store.read { conn in
            let query = try conn.prepare("""
            SELECT i.name, p.name, i.remote_name, i.remote_parent_file_id
            FROM items i LEFT JOIN items p ON p.item_id = i.parent_id
            WHERE i.remote_file_id = ?;
            """)
            defer { query.reset() }
            query.bindText(remoteID, at: 1)
            _ = try query.step()
            return ItemTopology(
                name: query.columnText(at: 0), parentName: query.columnText(at: 1),
                remoteName: query.columnText(at: 2), remoteParentID: query.columnText(at: 3))
        }
    }

    private func addMappedRemoteDirectory(
        to fixture: Fixture, name: String, remoteID: String
    ) async throws {
        let url = fixture.local.appendingPathComponent(name)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: false)
        guard let identity = try LocalDirectoryIdentity.read(at: url) else {
            throw SyncEngineError.general("Unable to read test directory identity")
        }
        try await fixture.store.write { conn in
            let insert = try conn.prepare("""
            INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                local_device, local_inode, remote_parent_file_id, remote_name,
                local_status, remote_status, phase, created_at, updated_at)
            SELECT root_id, item_id, ?, 'directory', ?, ?, ?, remote_file_id, ?,
                'present', 'present', 'committed', 0, 0
            FROM items WHERE root_id = (SELECT root_id FROM roots WHERE remote_root_id = 'root')
                AND parent_id IS NULL;
            """)
            defer { insert.reset() }
            insert.bindText(name, at: 1)
            insert.bindText(remoteID, at: 2)
            insert.bindInt64(identity.device, at: 3)
            insert.bindInt64(identity.inode, at: 4)
            insert.bindText(name, at: 5)
            _ = try insert.step()
        }
    }

    @Test("Incremental conflict is durable until the caller selects the remote version")
    func convergence() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(stats.filesFailed == 0)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        let localVersion = try #require(try LocalFileVersion.read(at: testFixture.original))
        #expect(try Data(contentsOf: testFixture.original) == Data("local edited content".utf8))
        #expect(try Data(contentsOf: URL(fileURLWithPath: conflictPath)) == Data("remote edited content".utf8))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path) == stats.conflicts)
        let downloads = context.value.state.withLock { $0.downloads }
        let pending = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(pending.conflicts == stats.conflicts)
        #expect(context.value.state.withLock { $0.downloads } == downloads)
        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)
        #expect(try Data(contentsOf: testFixture.original) == Data("remote edited content".utf8))
        #expect(try LocalFileVersion.read(at: testFixture.original)?.inode == localVersion.inode)
        #expect(!FileManager.default.fileExists(atPath: conflictPath))
        let localEntries = try FileManager.default.contentsOfDirectory(
            at: testFixture.local, includingPropertiesForKeys: nil)
        #expect(localEntries.map(\.lastPathComponent) == ["file.txt"])
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).isEmpty)
    }

    @Test("Large remote conflict resolution consumes its copy without reporting cleanup failure")
    func largeRemoteResolution() async throws {
        let remote = Data(repeating: 0x52, count: 8 * 1024 * 1024 + 1)
        let testFixture = try await fixture(remoteContent: remote)
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)

        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)

        #expect(try Data(contentsOf: testFixture.original) == remote)
        #expect(!FileManager.default.fileExists(atPath: conflictPath))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).isEmpty)
    }

    @Test("Large remote conflict resolution publishes when the local target is missing")
    func largeRemoteResolutionWithoutLocalTarget() async throws {
        let remote = Data(repeating: 0x53, count: 8 * 1024 * 1024 + 1)
        let testFixture = try await fixture(remoteContent: remote)
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        try FileManager.default.removeItem(at: testFixture.original)

        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)

        #expect(try Data(contentsOf: testFixture.original) == remote)
        #expect(!FileManager.default.fileExists(atPath: conflictPath))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).isEmpty)
    }

    @Test("Large remote conflict resolution retries after its database commit fails")
    func largeRemoteResolutionRetriesCommit() async throws {
        let remote = Data(repeating: 0x54, count: 8 * 1024 * 1024 + 1)
        let testFixture = try await fixture(remoteContent: remote)
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        try await testFixture.store.write { conn in
            try conn.execute("""
            CREATE TRIGGER reject_conflict_resolution BEFORE DELETE ON sync_conflicts
            BEGIN SELECT RAISE(ABORT, 'injected conflict resolution failure'); END;
            """)
        }

        await #expect(throws: (any Error).self) {
            try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)
        }
        #expect(!FileManager.default.fileExists(atPath: conflictPath))
        #expect(try Data(contentsOf: testFixture.original) == remote)
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).count == 1)
        try await testFixture.store.write {
            try $0.execute("DROP TRIGGER reject_conflict_resolution;")
        }

        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)

        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).isEmpty)
    }

    @Test("Conflict resolution rejects a revision change during its receipt")
    func conflictResolutionRejectsStaleRevision() async throws {
        let remote = Data(repeating: 0x56, count: 8 * 1024 * 1024 + 1)
        let testFixture = try await fixture(remoteContent: remote)
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        let originalRevision = try await conflictRevision(testFixture.store, id: conflict.id)
        try await testFixture.store.write { conn in
            try conn.execute("""
            CREATE TRIGGER advance_conflict_revision AFTER UPDATE ON items
            WHEN OLD.phase = 'blocked'
            BEGIN
                UPDATE sync_conflicts SET revision = revision + 1 WHERE item_id = NEW.item_id;
            END;
            """)
        }

        await #expect(throws: (any Error).self) {
            try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)
        }
        #expect(try await conflictRevision(testFixture.store, id: conflict.id) == originalRevision)
        let phase = try await testFixture.store.read { conn in
            let query = try conn.prepare(
                "SELECT phase FROM items WHERE remote_file_id = ?;")
            defer { query.reset() }
            query.bindText(testFixture.remoteID, at: 1)
            _ = try #require(try query.step())
            return query.columnText(at: 0)
        }
        #expect(phase == "blocked")
        try await testFixture.store.write {
            try $0.execute("DROP TRIGGER advance_conflict_revision;")
        }

        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)

        #expect(try Data(contentsOf: testFixture.original) == remote)
        #expect(try await testFixture.engine.listConflicts(
            localPath: testFixture.local.path).isEmpty)
    }

    @Test("A recreated engine repairs a large conflict receipt after publication")
    func recreatedEngineRepairsLargeRemoteResolution() async throws {
        let remote = Data(repeating: 0x55, count: 8 * 1024 * 1024 + 1)
        let testFixture = try await fixture(remoteContent: remote)
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        try await testFixture.store.write { conn in
            try conn.execute("""
            CREATE TRIGGER reject_conflict_resolution BEFORE DELETE ON sync_conflicts
            BEGIN SELECT RAISE(ABORT, 'injected conflict resolution failure'); END;
            """)
        }
        await #expect(throws: (any Error).self) {
            try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)
        }
        #expect(!FileManager.default.fileExists(atPath: conflictPath))
        try await testFixture.store.write {
            try $0.execute("DROP TRIGGER reject_conflict_resolution;")
        }
        let recreated = try await recreatedEngine(for: testFixture)

        try await recreated.resolveConflict(id: conflict.id, resolution: .remote)

        #expect(try Data(contentsOf: testFixture.original) == remote)
        #expect(try await recreated.listConflicts(localPath: testFixture.local.path).isEmpty)
    }

    @Test("A one-shot remote rename is replayed after resolving a conflict")
    func remoteRenameDuringConflictIsReplayed() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let first = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(first.conflicts.first)
        context.value.state.withLock { $0.files[testFixture.remoteID]!.name = "renamed.txt" }

        let refreshed = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        #expect(refreshed.remoteWorkPending > 0)
        #expect(try await pendingChangeCount(testFixture.store, remoteID: testFixture.remoteID) == 1)
        context.value.state.withLock { $0.emitChanges = false }
        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)
        let recreated = try await recreatedEngine(for: testFixture)

        let replayed = try await recreated.syncIncremental(localPath: testFixture.local.path)

        let renamed = testFixture.local.appendingPathComponent("renamed.txt")
        #expect(!FileManager.default.fileExists(atPath: testFixture.original.path))
        #expect(try Data(contentsOf: renamed) == Data("remote edited content".utf8))
        #expect(replayed.remoteWorkPending == 0)
        #expect(try await pendingChangeCount(testFixture.store, remoteID: testFixture.remoteID) == 0)
        let topology = try await itemTopology(testFixture.store, remoteID: testFixture.remoteID)
        #expect(topology.name == "renamed.txt")
        #expect(topology.remoteName == "renamed.txt")
        #expect(topology.remoteParentID == "root")
    }

    @Test("A one-shot remote move is replayed after resolving a conflict")
    func remoteMoveDuringConflictIsReplayed() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let first = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(first.conflicts.first)
        try await addMappedRemoteDirectory(to: testFixture, name: "folder", remoteID: "remote-folder")
        context.value.state.withLock {
            $0.files[testFixture.remoteID]!.parentID = "remote-folder"
        }

        let refreshed = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        #expect(refreshed.remoteWorkPending > 0)
        #expect(try await pendingChangeCount(testFixture.store, remoteID: testFixture.remoteID) == 1)
        context.value.state.withLock { $0.emitChanges = false }
        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)

        let replayed = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        let moved = testFixture.local.appendingPathComponent("folder/file.txt")
        #expect(!FileManager.default.fileExists(atPath: testFixture.original.path))
        #expect(try Data(contentsOf: moved) == Data("remote edited content".utf8))
        #expect(replayed.remoteWorkPending == 0)
        #expect(try await pendingChangeCount(testFixture.store, remoteID: testFixture.remoteID) == 0)
        let topology = try await itemTopology(testFixture.store, remoteID: testFixture.remoteID)
        #expect(topology.name == "file.txt")
        #expect(topology.parentName == "folder")
        #expect(topology.remoteParentID == "remote-folder")
    }

    @Test("A local edit during a download becomes a queryable conflict")
    func downloadRaceBecomesConflict() async throws {
        let testFixture = try await fixture(localContent: Data("baseline".utf8))
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let localEdit = Data("edited during download".utf8)
        context.value.downloadHook.withLock { hook in
            hook = { try? localEdit.write(to: testFixture.original) }
        }

        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        #expect(stats.filesFailed == 0)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        #expect(try Data(contentsOf: testFixture.original) == localEdit)
        #expect(try Data(contentsOf: URL(fileURLWithPath: conflictPath))
            == Data("remote edited content".utf8))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path)
            == [conflict])
    }

    @Test("Large publication rollback reuses the download for a durable conflict")
    func publicationRollbackBecomesConflict() async throws {
        let localEdit = Data("edited at publication".utf8)
        let testFixture = try await fixture(localContent: Data("baseline".utf8),
            filePublisher: { source, destination, expected, remoteSHA256, localSHA256 in
                #expect(localSHA256 == SyncEngine.computeSha256(of: Data("baseline".utf8)))
                return try LocalFilePublication.publish(
                    source, to: destination, expected: expected, expectedSHA256: remoteSHA256,
                    expectedLocalSHA256: localSHA256,
                    renameHook: { stage in
                        if stage == .beforeSwap { try localEdit.write(to: destination) }
                    })
            })
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let remote = Data(repeating: 0x52, count: 8 * 1024 * 1024 + 1)
        context.value.state.withLock { $0.files[testFixture.remoteID]!.content = remote }
        let downloads = context.value.state.withLock { $0.downloads }

        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        #expect(stats.filesFailed == 0)
        #expect(stats.filesDownloaded == 0)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        #expect(try Data(contentsOf: testFixture.original) == localEdit)
        #expect(try Data(contentsOf: URL(fileURLWithPath: conflictPath)) == remote)
        #expect(context.value.state.withLock { $0.downloads } == downloads + 1)
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path) == [conflict])
    }

    @Test("Selecting the local conflict version keeps the conflict when overwrite is blocked")
    func chooseLocalKeepsConflictWhenOverwriteIsBlocked() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        let uploadsBeforeResolution = context.value.state.withLock { $0.uploads }

        await #expect(throws: DriveError.unsafeOverwrite(fileId: testFixture.remoteID)) {
            try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .local)
        }

        #expect(try Data(contentsOf: testFixture.original) == Data("local edited content".utf8))
        #expect(FileManager.default.fileExists(atPath: conflictPath))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path) == [conflict])

        let retry = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(retry.filesUploaded == 0)
        #expect(context.value.state.withLock { $0.uploads } == uploadsBeforeResolution)
        #expect(context.value.state.withLock { $0.files[testFixture.remoteID]?.content }
            == Data("remote edited content".utf8))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path) == [conflict])
    }

    @Test("Large incremental conflict is downloaded without uploading a conflict copy")
    func largeConflict() async throws {
        let testFixture = try await fixture(
            localContent: Data(repeating: 65, count: 8 * 1024 * 1024 + 1))
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(stats.conflicts.count == 1)
        #expect(stats.filesFailed == 0)
        #expect(context.value.state.withLock { $0.sessions.isEmpty })
        #expect(context.value.state.withLock { $0.uploads } == 1)
    }

    @Test("Unresolved conflict refreshes remote content and blocks deletion propagation")
    func refreshAndDeletionBarrier() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let first = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(first.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        let initialRevision = try await conflictRevision(testFixture.store, id: conflict.id)

        try FileManager.default.removeItem(at: testFixture.original)
        let remoteCount = context.value.state.withLock { $0.files.count }
        let blocked = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(blocked.conflicts.count == 1)
        #expect(context.value.state.withLock { $0.files.count } == remoteCount)

        let newer = Data("newer remote conflict".utf8)
        context.value.state.withLock { $0.files[testFixture.remoteID]!.content = newer }
        let refreshed = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(refreshed.conflicts.first?.remoteSHA256 == SyncEngine.computeSha256(of: newer))
        #expect(try Data(contentsOf: URL(fileURLWithPath: conflictPath)) == newer)
        let refreshedRevision = try await conflictRevision(testFixture.store, id: conflict.id)
        #expect(refreshedRevision > initialRevision)

        context.value.state.withLock {
            $0.files.removeValue(forKey: testFixture.remoteID)
            $0.removed.insert(testFixture.remoteID)
        }
        let removed = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(removed.conflicts.first?.remoteStatus == .removed)
        #expect(try Data(contentsOf: URL(fileURLWithPath: conflictPath)) == newer)
        #expect(try await conflictRevision(testFixture.store, id: conflict.id) > refreshedRevision)
    }

    @Test("Selecting a local deletion trashes the remote conflict file")
    func chooseLocalDeletion() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        try FileManager.default.removeItem(at: testFixture.original)
        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .local)
        #expect(context.value.state.withLock { $0.files[testFixture.remoteID]?.trashed } == true)
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).isEmpty)
    }

    @Test("Local deletion against a remote modification can select the remote version")
    func localDeletionAgainstRemoteModification() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        try FileManager.default.removeItem(at: testFixture.original)

        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        let conflict = try #require(stats.conflicts.first)
        #expect(conflict.remoteStatus == .present)
        #expect(context.value.state.withLock { $0.files[testFixture.remoteID]?.trashed } == false)
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path) == [conflict])
        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)
        #expect(try Data(contentsOf: testFixture.original) == Data("remote edited content".utf8))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).isEmpty)
    }

    @Test("Selecting a remote deletion trashes the local conflict file")
    func chooseRemoteDeletion() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        _ = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        context.value.state.withLock { $0.files[testFixture.remoteID]!.trashed = true }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        #expect(conflict.remoteStatus == .trashed)
        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)
        #expect(!FileManager.default.fileExists(atPath: testFixture.original.path))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).isEmpty)
    }

    @Test("Selecting a local modification keeps a remotely trashed conflict actionable")
    func chooseLocalModificationAfterRemoteDeletionKeepsConflict() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        context.value.state.withLock { $0.files[testFixture.remoteID]!.trashed = true }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        #expect(conflict.remoteStatus == .trashed)
        #expect(conflict.conflictPath == nil)
        let patches = context.value.state.withLock { $0.metadataPatches }

        await #expect(throws: DriveError.unsafeOverwrite(fileId: testFixture.remoteID)) {
            try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .local)
        }

        #expect(context.value.state.withLock { $0.files[testFixture.remoteID]?.trashed } == true)
        #expect(context.value.state.withLock { $0.metadataPatches } == patches)
        #expect(try Data(contentsOf: testFixture.original) == Data("local edited content".utf8))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path) == [conflict])

        let retry = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(retry.filesUploaded == 0)
        #expect(retry.filesFailed == 0)
        #expect(retry.conflicts == [conflict])
    }

}
