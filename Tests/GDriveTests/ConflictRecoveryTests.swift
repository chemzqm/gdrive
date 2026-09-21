import Foundation
import Testing
import os
@testable import GDrive

private struct ConflictRemoteFile: Sendable {
    let id: String
    let name: String
    var content: Data
    var trashed = false
    var json: [String: Any] {
        ["id": id, "name": name, "parents": ["root"], "size": String(content.count),
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
}
private struct ConflictProtocolState: Sendable {
    let state = OSAllocatedUnfairLock(initialState: ConflictServerState())
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
            let present = state.files.values.map { ["fileId": $0.id, "file": $0.json] as [String: Any] }
            let removed = state.removed.map { ["fileId": $0, "removed": true] as [String: Any] }
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
                return Response(status: 200, data: file.content, headers: headers)
            }
            json = file.json
        } else { return Response(status: 404, data: Data(), headers: headers) }
        return Response(status: 200, data: try JSONSerialization.data(withJSONObject: json), headers: headers)
    }
    override func stopLoading() {}
}

@Suite("Durable conflict convergence (A12)")
struct ConflictRecoveryTests {
    private let context = TestHTTPContext(ConflictProtocolState())
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
    private struct ConflictGenerations {
        let local: Int64
        let remote: Int64
        let dirty: Int64
    }
    private func fixture(localContent: Data = Data("local edited content".utf8)) async throws -> Fixture {
        context.value.state.withLock { $0 = ConflictServerState() }
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
            conflictDirectory: directory.appendingPathComponent("conflicts"))
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
        context.value.state.withLock { $0.files[ids.remoteID]!.content = Data("remote edited content".utf8) }
        return Fixture(directory: directory, local: local, original: original, auth: auth, client: client,
                       store: store, engine: engine, rootID: ids.rootID, parentID: ids.parentID, itemID: ids.itemID, remoteID: ids.remoteID)
    }
    @Test("Incremental conflict is durable until the caller selects the remote version")
    func convergence() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(stats.conflictsResolved == 0)
        #expect(stats.filesFailed == 0)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        #expect(try Data(contentsOf: testFixture.original) == Data("local edited content".utf8))
        #expect(try Data(contentsOf: URL(fileURLWithPath: conflictPath)) == Data("remote edited content".utf8))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path) == stats.conflicts)
        let downloads = context.value.state.withLock { $0.downloads }
        let pending = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(pending.conflicts == stats.conflicts)
        #expect(context.value.state.withLock { $0.downloads } == downloads)
        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .remote)
        #expect(try Data(contentsOf: testFixture.original) == Data("remote edited content".utf8))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).isEmpty)
    }

    @Test("Selecting the local conflict version keeps the conflict when overwrite is blocked")
    func chooseLocalKeepsConflictWhenOverwriteIsBlocked() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        let conflictPath = try #require(conflict.conflictPath)
        let uploadsBeforeResolution = context.value.state.withLock { $0.uploads }

        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .local)

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
        let testFixture = try await fixture(localContent: Data(repeating: 65, count: 9 * 1024 * 1024))
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

        context.value.state.withLock {
            $0.files.removeValue(forKey: testFixture.remoteID)
            $0.removed.insert(testFixture.remoteID)
        }
        let removed = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(removed.conflicts.first?.remoteStatus == .removed)
        #expect(try Data(contentsOf: URL(fileURLWithPath: conflictPath)) == newer)
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

    @Test("Local deletion against a remote modification creates a conflict")
    func localDeletionAgainstRemoteModification() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        try FileManager.default.removeItem(at: testFixture.original)

        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        let conflict = try #require(stats.conflicts.first)
        #expect(conflict.remoteStatus == .present)
        #expect(context.value.state.withLock { $0.files[testFixture.remoteID]?.trashed } == false)
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path) == [conflict])
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

    @Test("Selecting a local modification restores a remotely trashed file")
    func chooseLocalModificationAfterRemoteDeletion() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        context.value.state.withLock { $0.files[testFixture.remoteID]!.trashed = true }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let conflict = try #require(stats.conflicts.first)
        #expect(conflict.remoteStatus == .trashed)
        #expect(conflict.conflictPath == nil)
        try await testFixture.engine.resolveConflict(id: conflict.id, resolution: .local)
        #expect(context.value.state.withLock { $0.files[testFixture.remoteID]?.trashed } == false)
        #expect(try Data(contentsOf: testFixture.original) == Data("local edited content".utf8))
        #expect(try await testFixture.engine.listConflicts(localPath: testFixture.local.path).isEmpty)
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
        let resumed = try await engine.syncIncremental(localPath: testFixture.local.path)
        #expect(resumed.conflictsResolved == 1)
        #expect(resumed.filesFailed == 0)
    }

    @Test(
        "A stale pending conflict does not block an unrelated healthy file",
        arguments: ["original", "copy", "remote"])
    func stalePendingConflictDoesNotBlockRoot(changedInput: String) async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let localSHA = SyncEngine.computeSha256(of: Data("local edited content".utf8))
        let remoteSHA = SyncEngine.computeSha256(of: Data("remote edited content".utf8))
        let generations = try await testFixture.store.read { conn -> ConflictGenerations in
            let query = try conn.prepare(
                "SELECT local_generation, remote_generation, dirty_generation FROM items WHERE item_id = \(testFixture.itemID);")
            defer { query.reset() }
            #expect(try query.step())
            return ConflictGenerations(
                local: query.columnInt64(at: 0) ?? 0,
                remote: query.columnInt64(at: 1) ?? 0,
                dirty: query.columnInt64(at: 2) ?? 0)
        }
        let operation = try await ConflictOperation.prepare(
            store: testFixture.store, rootID: testFixture.rootID, itemID: testFixture.itemID,
            parentID: testFixture.parentID, original: testFixture.original,
            remoteID: testFixture.remoteID, parentRemoteID: "root",
            copyRemoteID: "stale-copy-id", conflictID: "stale-conflict",
            localSHA: localSHA, remoteSHA: remoteSHA,
            localGeneration: generations.local, remoteGeneration: generations.remote,
            dirtyGeneration: generations.dirty)
        struct Stop: Error {}
        do {
            try await testFixture.engine.resolveConflict(operation) {
                if $0 == .copy { throw Stop() }
            }
            Issue.record("Missing copy checkpoint")
        } catch is Stop {}
        let edited = Data("edited after interruption".utf8)
        switch changedInput {
        case "original":
            try edited.write(to: testFixture.original)
        case "copy":
            try edited.write(to: URL(fileURLWithPath: operation.copyPath))
        default:
            context.value.state.withLock { $0.files[testFixture.remoteID]!.content = edited }
        }
        let healthy = testFixture.local.appendingPathComponent("healthy.txt")
        try Data("healthy".utf8).write(to: healthy)

        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        #expect(stats.filesUploaded == 1)
        #expect(context.value.state.withLock {
            $0.files.values.contains { $0.name == "healthy.txt" }
        })
        if changedInput == "original" {
            #expect(try Data(contentsOf: testFixture.original) == edited)
        } else if changedInput == "copy" {
            #expect(try Data(contentsOf: URL(fileURLWithPath: operation.copyPath)) == edited)
        } else {
            #expect(context.value.state.withLock {
                $0.files[testFixture.remoteID]?.content
            } == edited)
        }
        #expect(try await ConflictOperation.pending(
            store: testFixture.store, rootID: testFixture.rootID).map(\.id).contains(operation.id))
    }

}
