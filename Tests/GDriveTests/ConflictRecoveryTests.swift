import Foundation
import Testing
import os
@testable import GDrive

private struct ConflictRemoteFile: Sendable {
    let id: String
    let name: String
    var content: Data
    var json: [String: Any] {
        ["id": id, "name": name, "parents": ["root"], "size": String(content.count),
         "sha256Checksum": SyncEngine.computeSha256(of: content), "version": "2", "trashed": false]
    }
}
private struct ConflictServerState: Sendable {
    var files: [String: ConflictRemoteFile] = [:]
    var uploads = 0
    var downloads = 0
    var loseResponse = false
    var sessions: [String: ConflictRemoteFile] = [:]
    var sessionSizes: [String: Int] = [:]
}
private final class ConflictProtocol: URLProtocol, @unchecked Sendable {
    static let state = OSAllocatedUnfairLock(initialState: ConflictServerState())
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
            let response = try Self.state.withLock { state in
                try self.response(for: url, state: &state)
            }
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: url, statusCode: response.status, httpVersion: nil, headerFields: response.headers)!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: response.data)
            client?.urlProtocolDidFinishLoading(self)
        } catch { client?.urlProtocol(self, didFailWithError: error) }
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
            json = ["changes": state.files.values.map { ["fileId": $0.id, "file": $0.json] }, "newStartPageToken": "next"]
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
                return Response(status: 200, data: file.content, headers: headers)
            }
            json = file.json
        } else { return Response(status: 404, data: Data(), headers: headers) }
        return Response(status: 200, data: try JSONSerialization.data(withJSONObject: json), headers: headers)
    }
    override func stopLoading() {}
}

@Suite("Durable conflict convergence (A12)", .serialized)
struct ConflictRecoveryTests {
    private struct Fixture {
        let directory: URL
        let local: URL
        let original: URL
        let auth: Auth
        let client: DriveClient
        let store: StateStore
        let engine: SyncEngine
        let rootID: Int64
        let parentID: Int64
        let itemID: Int64
        let remoteID: String
    }
    private func fixture(localContent: Data = Data("local edited content".utf8)) async throws -> Fixture {
        ConflictProtocol.state.withLock { $0 = ConflictServerState() }
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
        configuration.protocolClasses = [ConflictProtocol.self]
        let client = DriveClient(auth: auth, session: URLSession(configuration: configuration))
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let engine = try await SyncEngine(auth: auth, store: store, client: client,
            idPool: IDPool(initialIds: (0..<1000).map { "id-\($0)" }),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"))
        let first = try await engine.syncLocalToRemoteEmpty(localPath: local.path, remoteRootId: "root")
        #expect(first.filesUploaded == 1)
        struct ItemIdentity: Sendable {
            let rootID: Int64
            let parentID: Int64
            let itemID: Int64
            let remoteID: String
        }
        let ids = try await store.read { conn -> ItemIdentity in
            let queryStatement = try conn.prepare("SELECT root_id, parent_id, item_id, remote_file_id FROM items WHERE entry_kind = 'file';")
            _ = try queryStatement.step()
            return ItemIdentity(rootID: queryStatement.columnInt64(at: 0)!, parentID: queryStatement.columnInt64(at: 1)!, itemID: queryStatement.columnInt64(at: 2)!, remoteID: queryStatement.columnText(at: 3)!)
        }
        try localContent.write(to: original)
        ConflictProtocol.state.withLock { $0.files[ids.remoteID]!.content = Data("remote edited content".utf8) }
        return Fixture(directory: directory, local: local, original: original, auth: auth, client: client,
                       store: store, engine: engine, rootID: ids.rootID, parentID: ids.parentID, itemID: ids.itemID, remoteID: ids.remoteID)
    }
    private func assertConverged(_ testFixture: Fixture) async throws {
        let remote = ConflictProtocol.state.withLock { Array($0.files.values) }
        #expect(remote.count == 2)
        for file in remote {
            #expect(try Data(contentsOf: testFixture.local.appendingPathComponent(file.name)) == file.content)
        }
        let before = ConflictProtocol.state.withLock { ($0.uploads, $0.downloads) }
        let next = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path, remoteRootId: "root")
        #expect(next.conflictsResolved == 0)
        #expect(next.filesFailed == 0)
        #expect(next.filesUploaded == 0)
        #expect(next.filesDownloaded == 0)
        #expect(ConflictProtocol.state.withLock { $0.uploads } == before.0)
        #expect(ConflictProtocol.state.withLock { $0.downloads } == before.1)
        let pending = try await ConflictOperation.pending(store: testFixture.store, rootID: testFixture.rootID)
        #expect(pending.isEmpty)
        let committed = try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT count(*) FROM items WHERE entry_kind = 'file' AND base_sha256 = local_sha256 AND base_sha256 = remote_sha256 AND dirty_generation = 0;")
            _ = try queryStatement.step()
            return queryStatement.columnInt64(at: 0)
        }
        #expect(committed == 2)
        #expect(!FileManager.default.fileExists(atPath: testFixture.directory.appendingPathComponent("downloads/root").path))
    }

    @Test("One incremental round publishes both versions; next round transfers nothing")
    func convergence() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path, remoteRootId: "root")
        #expect(stats.conflictsResolved == 1)
        #expect(stats.filesFailed == 0)
        try await assertConverged(testFixture)
    }

    @Test("Large conflict copy uses resumable upload and converges without later transfers")
    func largeConflict() async throws {
        let testFixture = try await fixture(localContent: Data(repeating: 65, count: 9 * 1024 * 1024))
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path, remoteRootId: "root")
        #expect(stats.conflictsResolved == 1)
        #expect(stats.filesFailed == 0)
        #expect(ConflictProtocol.state.withLock { $0.sessions.count } == 1)
        try await assertConverged(testFixture)
    }

    @Test("Successful create with lost response stays pending and recovers the same ID")
    func lostResponse() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        ConflictProtocol.state.withLock { $0.loseResponse = true }
        let failed = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path, remoteRootId: "root")
        #expect(failed.conflictsResolved == 0)
        #expect(failed.filesFailed == 1)
        #expect(try await ConflictOperation.pending(store: testFixture.store, rootID: testFixture.rootID).count == 1)
        let recovered = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path, remoteRootId: "root")
        #expect(recovered.conflictsResolved == 1)
        #expect(ConflictProtocol.state.withLock { $0.uploads } == 2) // baseline + one copy
        try await assertConverged(testFixture)
    }

    @Test("Reopen SQLite after each interrupted stage reuses both identities", arguments: ConflictCheckpoint.allCases)
    func recovery(stage: ConflictCheckpoint) async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let localSHA = SyncEngine.computeSha256(of: Data("local edited content".utf8))
        let remoteSHA = SyncEngine.computeSha256(of: Data("remote edited content".utf8))
        struct Generations: Sendable {
            let local: Int64
            let remote: Int64
            let dirty: Int64
        }
        let generations = try await testFixture.store.read { conn -> Generations in
            let queryStatement = try conn.prepare("SELECT local_generation, remote_generation, dirty_generation FROM items WHERE item_id = \(testFixture.itemID);")
            _ = try queryStatement.step()
            return Generations(local: queryStatement.columnInt64(at: 0)!, remote: queryStatement.columnInt64(at: 1)!, dirty: queryStatement.columnInt64(at: 2)!)
        }
        let operationStatement = try await ConflictOperation.prepare(store: testFixture.store, rootID: testFixture.rootID, itemID: testFixture.itemID,
            parentID: testFixture.parentID, original: testFixture.original, remoteID: testFixture.remoteID, parentRemoteID: "root",
            copyRemoteID: "copy-id", conflictID: "deterministic", localSHA: localSHA, remoteSHA: remoteSHA,
            localGeneration: generations.local, remoteGeneration: generations.remote, dirtyGeneration: generations.dirty)
        struct Stop: Error {}
        do {
            try await testFixture.engine.resolveConflict(operationStatement) { if $0 == stage { throw Stop() } }
            Issue.record("Missing checkpoint")
        } catch is Stop {}
        let reopened = try await StateStore(path: testFixture.store.path)
        let pending = try await ConflictOperation.pending(store: reopened, rootID: testFixture.rootID)
        #expect(pending.count == 1)
        #expect(pending.first?.id == operationStatement.id)
        #expect(pending.first?.copyRemoteID == "copy-id")
        let engine = try await SyncEngine(auth: testFixture.auth, store: reopened, client: testFixture.client,
            downloadTemporaryDirectory: testFixture.engine.downloadTemporaryDirectory)
        let resumed = try await engine.syncIncremental(localPath: testFixture.local.path, remoteRootId: "root")
        #expect(resumed.conflictsResolved == 1)
        try await assertConverged(testFixture)
    }
    @Test("Changed originals, copies, generations and remote copies remain pending", arguments: ["original", "copy", "generation", "copyGeneration", "remoteCopy"])
    func changedDuringRecovery(kind: String) async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        ConflictProtocol.state.withLock { $0.loseResponse = true }
        _ = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path, remoteRootId: "root")
        let operationStatement = try #require(try await ConflictOperation.pending(store: testFixture.store, rootID: testFixture.rootID).first)
        let newer = Data("newer user data".utf8)
        switch kind {
        case "original": try newer.write(to: testFixture.original)
        case "copy": try newer.write(to: URL(fileURLWithPath: operationStatement.copyPath))
        case "generation", "copyGeneration":
            let itemID = kind == "generation" ? testFixture.itemID : operationStatement.copyItemID
            try await testFixture.store.write { conn in
                try conn.execute("UPDATE items SET local_generation = local_generation + 1 WHERE item_id = \(itemID);")
            }
        default: ConflictProtocol.state.withLock { $0.files[operationStatement.copyRemoteID]!.content = newer }
        }
        do {
            _ = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path, remoteRootId: "root")
            Issue.record("Changed conflict inputs must not be marked resolved")
        } catch {}
        #expect(try await ConflictOperation.pending(store: testFixture.store, rootID: testFixture.rootID).count == 1)
        #expect(ConflictProtocol.state.withLock { $0.uploads } == 2)
        if kind == "original" { #expect(try Data(contentsOf: testFixture.original) == newer) }
        if kind == "copy" { #expect(try Data(contentsOf: URL(fileURLWithPath: operationStatement.copyPath)) == newer) }
        let base = try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT base_sha256 FROM items WHERE item_id = \(testFixture.itemID);")
            _ = try queryStatement.step()
            return queryStatement.columnText(at: 0)
        }
        #expect(base == SyncEngine.computeSha256(of: Data("baseline".utf8)))
    }

}
