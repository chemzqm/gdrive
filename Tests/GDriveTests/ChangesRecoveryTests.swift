import Foundation
import Testing
import os
import Darwin
import DirectoryScanner
@testable import GDrive

private struct ChangesServer: Sendable {
    var files: [String: DriveFile] = [:]
    var contents: [String: Data] = [:]
    var pages: [String: DriveChangesPage] = [:]
    var failToken: String?
    var failStart = false
    var rejectListingToken: String?
    var rejectToken: String?
    var failFolder: String?
    var incompleteFolder: String?
    var listingPages: [String: Data] = [:]
    var resumableContents: [String: Data] = [:]
    var resumableFiles: [String: DriveFile] = [:]
    var requests: [String] = []
    var verifyUpload: (@Sendable (String) throws -> Void)?
    var uploadDelay: TimeInterval = 0
    var activeUploads = 0
    var peakUploads = 0
}
private struct ChangesProtocolState: Sendable {
    let heldDownload = OSAllocatedUnfairLock<(@Sendable () -> Void)?>(initialState: nil)
    let holdDownloads = OSAllocatedUnfairLock(initialState: false)
    let uploadStarted = AsyncSemaphore(count: 0)
    let state = OSAllocatedUnfairLock(initialState: ChangesServer())
}
private final class ChangesProtocol: URLProtocol, @unchecked Sendable {
    private lazy var server = TestHTTPContext<ChangesProtocolState>.value(for: request)!
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        let delay = self.server.state.withLock { state -> TimeInterval in
            guard request.url!.path.contains("/upload/"), state.uploadDelay > 0 else { return 0 }
            state.activeUploads += 1
            state.peakUploads = max(state.peakUploads, state.activeUploads)
            return state.uploadDelay
        }
        if delay > 0 {
            self.server.uploadStarted.signal()
            DispatchQueue.global().asyncAfter(deadline: .now() + delay) {
                self.server.state.withLock { $0.activeUploads -= 1 }
                self.respond()
            }
        } else { respond() }
    }
    private struct ResumableResponse {
        let status: Int
        let data: Data
        let headers: [String: String]?
    }
    private func respond() {
        do {
            let url = request.url!
            if request.httpMethod == "POST", url.query?.contains("uploadType=resumable") == true {
                let metadata = try #require(JSONSerialization.jsonObject(with: request.extractBodyData ?? Data()) as? [String: Any])
                let id = try #require(metadata["id"] as? String)
                let name = try #require(metadata["name"] as? String)
                let parent = (metadata["parents"] as? [String])?.first ?? "root"
                let size = request.value(forHTTPHeaderField: "X-Upload-Content-Length") ?? "0"
                self.server.state.withLock {
                    $0.requests.append("POST \(url.path)?\(url.query ?? "")")
                    $0.resumableFiles[id] = DriveFile(id: id, name: name, parents: [parent], size: size, version: "1")
                    $0.resumableContents[id] = Data()
                }
                let response = HTTPURLResponse(
                    url: url,
                    statusCode: 200,
                    httpVersion: nil,
                    headerFields: ["Location": "https://upload.test/resumable/\(id)"]
                )!
                client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
                client?.urlProtocol(self, didLoad: Data())
                client?.urlProtocolDidFinishLoading(self)
                return
            }
            func respondToChunk() throws {
                let id = url.lastPathComponent
                let body = request.extractBodyData ?? Data()
                let result = try self.server.state.withLock { state -> ResumableResponse in
                    state.requests.append("PUT \(url.path)?\(url.query ?? "")")
                    state.resumableContents[id, default: Data()].append(body)
                    let file = try #require(state.resumableFiles[id])
                    let total = try #require(file.sizeBytes)
                    let received = Int64(state.resumableContents[id]?.count ?? 0)
                    if received < total {
                        return ResumableResponse(status: 308, data: Data(), headers: ["Range": "bytes=0-\(received - 1)"])
                    }
                    let bytes = state.resumableContents[id] ?? Data()
                    let completed = DriveFile(
                        id: file.id,
                        name: file.name,
                        parents: file.parents,
                        size: String(bytes.count),
                        sha256Checksum: SyncEngine.computeSha256(of: bytes),
                        version: "1"
                    )
                    state.files[id] = completed
                    state.contents[id] = bytes
                    return ResumableResponse(status: 200, data: try JSONEncoder().encode(completed), headers: nil)
                }
                let response = HTTPURLResponse(url: url, statusCode: result.status, httpVersion: nil, headerFields: result.headers)!
                client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
                client?.urlProtocol(self, didLoad: result.data)
                client?.urlProtocolDidFinishLoading(self)
            }
            if request.httpMethod == "PUT", url.host == "upload.test" {
                try respondToChunk()
                return
            }

            let response = try self.server.state.withLock { state in
                try self.response(for: url, state: &state)
            }
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: url, statusCode: response.0, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: response.1)
            if request.url!.query?.contains("alt=media") == true, self.server.holdDownloads.withLock({ $0 }) {
                self.server.heldDownload.withLock { $0 = { self.client?.urlProtocolDidFinishLoading(self) } }
                return
            }
            client?.urlProtocolDidFinishLoading(self)
        } catch { client?.urlProtocol(self, didFailWithError: error) }
    }
    private func response(for url: URL, state: inout ChangesServer) throws -> (Int, Data) {
        let query = URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems ?? []
        func value(_ key: String) -> String? { query.first { $0.name == key }?.value }
        state.requests.append("\(request.httpMethod ?? "GET") \(url.path)?\(url.query ?? "")")
        let encoder = JSONEncoder()
        if url.path.hasSuffix("/changes/startPageToken") {
            return (state.failStart ? 400 : 200, Data(#"{"startPageToken":"fresh"}"#.utf8))
        }
        if url.path.hasSuffix("/changes") {
            let token = value("pageToken") ?? ""
            if token == state.failToken { return (400, Data("temporary page failure".utf8)) }
            if token == state.rejectToken { return (400, Data("invalid pageToken".utf8)) }
            return (200, try encoder.encode(state.pages[token] ?? DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [])))
        }
        if url.path.hasSuffix("/generateIds") {
            return (200, try JSONSerialization.data(withJSONObject: ["ids": (0..<1000).map { "generated-\($0)" }]))
        }
        if url.path.contains("/upload/") {
            let body = (String(bytes: request.extractBodyData ?? Data(), encoding: .utf8) ?? "Invalid UTF-8 data")
            let start = try #require(body.firstIndex(of: "{"))
            let end = try #require(body[start...].firstIndex(of: "}"))
            let metadata = try #require(JSONSerialization.jsonObject(with: Data(body[start...end].utf8)) as? [String: Any])
            let id = try #require(metadata["id"] as? String)
            let name = try #require(metadata["name"] as? String)
            try state.verifyUpload?(name)
            let parent = (metadata["parents"] as? [String])?.first ?? "root"
            let contentStart = try #require(body.range(of: "Content-Type: application/octet-stream\r\n\r\n")?.upperBound)
            let contentEnd = try #require(body.range(of: "\r\n--", range: contentStart..<body.endIndex)?.lowerBound)
            let bytes = Data(body[contentStart..<contentEnd].utf8)
            let file = DriveFile(id: id, name: name, parents: [parent], size: String(bytes.count), sha256Checksum: SyncEngine.computeSha256(of: bytes), version: "1")
            state.files[id] = file
            state.contents[id] = bytes
            return (200, try encoder.encode(file))
        }
        func listOrCreateFile() throws -> (Int, Data) {
            if request.httpMethod == "POST" {
                let metadata = try #require(JSONSerialization.jsonObject(with: request.extractBodyData ?? Data()) as? [String: Any])
                let id = try #require(metadata["id"] as? String)
                let name = try #require(metadata["name"] as? String)
                let file = DriveFile(id: id, name: name, mimeType: "application/vnd.google-apps.folder", parents: metadata["parents"] as? [String])
                state.files[id] = file
                return (200, try encoder.encode(file))
            }
            let parent = (value("q") ?? "").split(separator: "'").first.map(String.init) ?? ""
            if let rejected = state.rejectListingToken, value("pageToken") == rejected { return (400, Data("invalid pageToken".utf8)) }
            if state.failFolder == parent { return (400, Data("listing failure".utf8)) }
            if let page = state.listingPages["\(parent):\(value("pageToken") ?? "first")"] { return (200, page) }
            let files = state.files.values.filter { $0.parents?.first == parent && $0.trashed != true }
            let fileJSON = try files.map { try JSONSerialization.jsonObject(with: encoder.encode($0)) }
            return (200, try JSONSerialization.data(withJSONObject: ["files": fileJSON, "incompleteSearch": state.incompleteFolder == parent]))
        }
        if url.path.hasSuffix("/files") { return try listOrCreateFile() }
        if value("alt") == "media", let bytes = state.contents[url.lastPathComponent] { return (200, bytes) }
        if let file = state.files[url.lastPathComponent] {
            let current = request.httpMethod == "PATCH" ? try applyingMetadataPatch(to: file, url: url) : file
            state.files[file.id] = current
            return (200, try encoder.encode(current))
        }
        return (404, Data())
    }
    override func stopLoading() {}

    private func applyingMetadataPatch(to file: DriveFile, url: URL) throws -> DriveFile {
        let encoder = JSONEncoder()
        var fields = try #require(JSONSerialization.jsonObject(with: encoder.encode(file)) as? [String: Any])
        let update = try #require(JSONSerialization.jsonObject(with: request.extractBodyData ?? Data()) as? [String: Any])
        for key in ["name", "trashed"] {
            if let value = update[key] { fields[key] = value }
        }
        let query = URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems ?? []
        var parents = file.parents ?? []
        if let removed = query.first(where: { $0.name == "removeParents" })?.value {
            parents.removeAll { $0 == removed }
        }
        if let added = query.first(where: { $0.name == "addParents" })?.value, !parents.contains(added) {
            parents.append(added)
        }
        fields["parents"] = parents
        return try JSONDecoder().decode(DriveFile.self, from: JSONSerialization.data(withJSONObject: fields))
    }
}

@Suite("Changes durability and reconstruction (A13)")
struct ChangesRecoveryTests {
    private let context = TestHTTPContext(ChangesProtocolState())
    private struct Fixture {
        let directory: URL
        let local: URL
        let store: StateStore
        let client: DriveClient
        let auth: Auth
        let engine: SyncEngine
        let rootID: Int64
        let rootItemID: Int64
        func cleanup() {
            try? FileManager.default.removeItem(at: directory)
        }
        var changes: RemoteChanges { RemoteChanges(store: store, client: client, rootID: rootID, remoteRootID: "root", rootURL: local) }
    }
    private struct StoredBaseline {
        let kind: String?
        let remoteID: String?
        let dirtyGeneration: Int64?
    }
    private func folder(_ id: String, _ parent: String?, name: String? = nil) -> DriveFile {
        DriveFile(id: id, name: name ?? id, mimeType: "application/vnd.google-apps.folder", parents: parent.map { [$0] }, version: "1")
    }
    @discardableResult
    private func remoteFile(_ id: String, parent: String, content: String = "remote content", name: String? = nil) -> DriveFile {
        let bytes = Data(content.utf8)
        let file = DriveFile(id: id, name: name ?? id, parents: [parent], size: String(bytes.count), sha256Checksum: SyncEngine.computeSha256(of: bytes), version: "1")
        context.value.state.withLock { $0.files[id] = file; $0.contents[id] = bytes }
        return file
    }
    private func fixture(cursor: Bool = true) async throws -> Fixture {
        context.value.state.withLock { $0 = ChangesServer(files: ["root": folder("root", nil)]) }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("a13-\(UUID().uuidString)")
        let local = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(clientId: "test", accessToken: "test", expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)
        let config = URLSessionConfiguration.ephemeral
        context.configure(config)
        config.protocolClasses = [ChangesProtocol.self]
        let client = DriveClient(auth: auth, session: URLSession(configuration: config), requestsPerSecond: nil)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let ids = try await store.write { conn -> (Int64, Int64) in
            let rootStatement = try conn.prepare("INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at) VALUES ('default', ?, 1, 1, 'root', 'localToRemoteEmpty', 'existingKnown', 1, 1);")
            rootStatement.bindText(local.path, at: 1)
            _ = try rootStatement.step()
            let root = conn.lastInsertRowId
            try conn.execute("INSERT INTO items(root_id, name, entry_kind, remote_file_id, local_status, remote_status, phase, created_at, updated_at) VALUES (\(root), 'local', 'directory', 'root', 'present', 'present', 'committed', 1, 1);")
            let item = conn.lastInsertRowId
            if cursor { try conn.execute("INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) VALUES (\(root), 'default', 'drive_changes', 'start', 1);") }
            return (root, item)
        }
        try await setStoredRootIdentity(store: store, rootID: ids.0, localURL: local)
        let engine = try await SyncEngine(auth: auth, store: store, client: client, idPool: IDPool(initialIds: (0..<1000).map { "new-\($0)" }),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"))
        return Fixture(directory: directory, local: local, store: store, client: client, auth: auth, engine: engine, rootID: ids.0, rootItemID: ids.1)
    }
    private func token(_ testFixture: Fixture) async throws -> String? {
        try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT token_value FROM cursors WHERE root_id = \(testFixture.rootID);")
            return try queryStatement.step() ? queryStatement.columnText(at: 0) : nil
        }
    }
    private func converge(_ testFixture: Fixture, limit: Int = 12) async throws {
        for _ in 0..<limit {
            let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
            #expect(stats.filesFailed == 0)
            if stats.remoteWorkPending == 0 { return }
        }
        Issue.record("Remote work did not converge within the bounded fixture")
    }

    @Test("Restored mtime cannot hide same-size content changes from the baseline cache")
    func restoredMtimeStillHashesChangedContent() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let local = testFixture.local.appendingPathComponent("same-size.txt")
        let original = Data("AAAA".utf8)
        let changed = Data("BBBB".utf8)
        try original.write(to: local)

        let first = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(first.filesUploaded == 1)
        let baseline = try #require(try LocalFileVersion.read(at: local))

        try changed.write(to: local)
        let seconds = Int(baseline.mtime / 1_000_000_000)
        let nanoseconds = Int(baseline.mtime % 1_000_000_000)
        let times = [
            timespec(tv_sec: seconds, tv_nsec: nanoseconds),
            timespec(tv_sec: seconds, tv_nsec: nanoseconds)
        ]
        let result = times.withUnsafeBufferPointer {
            utimensat(AT_FDCWD, local.path, $0.baseAddress, 0)
        }
        #expect(result == 0)
        let modified = try #require(try LocalFileVersion.read(at: local))
        #expect(modified.device == baseline.device)
        #expect(modified.inode == baseline.inode)
        #expect(modified.mtime == baseline.mtime)
        #expect(modified.size == baseline.size)
        #expect(modified.ctime != baseline.ctime)

        let second = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(second.filesSkipped == 0)
        #expect(second.filesFailed == 1)
        let stored = try await testFixture.store.read { conn in
            let query = try conn.prepare(
                "SELECT local_sha256, dirty_generation FROM items WHERE name = 'same-size.txt';")
            _ = try #require(try query.step())
            return (query.columnText(at: 0), query.columnInt64(at: 1))
        }
        #expect(stored.0 == SyncEngine.computeSha256(of: changed))
        #expect((stored.1 ?? 0) > 0)
    }

    @Test("An incremental upload receipt SQLite failure drains then propagates")
    func incrementalUploadReceiptSQLiteFailurePropagates() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let local = testFixture.local.appendingPathComponent("receipt-failure.txt")
        let content = Data("receipt body".utf8)
        try content.write(to: local)
        try await testFixture.store.write { conn in
            try conn.execute("""
                CREATE TRIGGER fail_incremental_upload_receipt
                BEFORE UPDATE OF remote_file_id ON items
                WHEN NEW.name = 'receipt-failure.txt'
                BEGIN SELECT RAISE(ABORT, 'injected upload receipt failure'); END;
                """)
        }

        let error = await #expect(throws: (any Error).self) {
            try await testFixture.engine.syncIncrementalUnlocked(
                rootId: testFixture.rootID, rootItemId: testFixture.rootItemID,
                localPath: testFixture.local.path, remoteRootId: "root", maxConcurrency: 1,
                onProgress: nil)
        }
        let sqlite = try #require(error) as NSError
        #expect(sqlite.domain == "SQLiteStatement")
        let state = try await testFixture.store.read { conn -> (String?, Int64?) in
            let statement = try conn.prepare(
                "SELECT local_sha256, dirty_generation FROM items WHERE name = 'receipt-failure.txt';")
            defer { statement.reset() }
            #expect(try statement.step())
            return (statement.columnText(at: 0), statement.columnInt64(at: 1))
        }
        #expect(state.0 == SyncEngine.computeSha256(of: content))
        #expect((state.1 ?? 0) > 0)
    }

    @Test("A known remote directory event preserves a local deletion for reconciliation")
    func knownDirectoryEventPreservesLocalDeletion() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let localDirectory = testFixture.local.appendingPathComponent("deleted")
        try FileManager.default.createDirectory(at: localDirectory, withIntermediateDirectories: true)
        let attrs = try FileManager.default.attributesOfItem(atPath: localDirectory.path)
        let device = (attrs[.systemNumber] as? NSNumber)?.int64Value ?? 0
        let inode = (attrs[.systemFileNumber] as? NSNumber)?.int64Value ?? 0
        let remoteDirectory = folder("known-directory", "root", name: "deleted")
        try await testFixture.store.write { conn in
            let queryStatement = try conn.prepare("""
                INSERT INTO items(
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status,
                    phase, created_at, updated_at
                ) VALUES (?, ?, 'deleted', 'directory', 'known-directory', ?, ?,
                    'present', 'present', 'committed', 1, 1);
                """)
            queryStatement.bindInt64(testFixture.rootID, at: 1)
            queryStatement.bindInt64(testFixture.rootItemID, at: 2)
            queryStatement.bindInt64(device, at: 3)
            queryStatement.bindInt64(inode, at: 4)
            _ = try queryStatement.step()
        }
        try FileManager.default.removeItem(at: localDirectory)
        context.value.state.withLock {
            $0.files[remoteDirectory.id] = remoteDirectory
            $0.pages["start"] = DriveChangesPage(
                nextPageToken: nil,
                newStartPageToken: "steady",
                changes: [DriveChange(fileId: remoteDirectory.id, removed: false, file: remoteDirectory)]
            )
        }

        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        #expect(stats.filesDeleted == 1)
        #expect(!FileManager.default.fileExists(atPath: localDirectory.path))
        let retained = try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare(
                "SELECT COUNT(*) FROM items WHERE remote_file_id = 'known-directory';")
            guard try queryStatement.step() else { return false }
            return queryStatement.columnInt64(at: 0) == 0
        }
        #expect(retained)
    }

    @Test("Child before parent across pages survives a page failure and database reopen")
    func childBeforeParent() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        try Data("ready upload".utf8).write(to: testFixture.local.appendingPathComponent("ready.txt"))
        let parent = folder("dir", "root")
        let child = remoteFile("child", parent: "dir")
        context.value.state.withLock {
            $0.files[parent.id] = parent
            $0.pages["start"] = DriveChangesPage(nextPageToken: "page2", newStartPageToken: nil, changes: [DriveChange(fileId: child.id, removed: false, file: child)])
            $0.pages["page2"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: parent.id, removed: false, file: parent)])
            $0.failToken = "page2"
        }
        do { _ = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path); Issue.record("Expected page failure") } catch {}
        #expect(try await token(testFixture) == "page2")
        #expect(!context.value.state.withLock { $0.requests.contains { $0.contains("/upload/") } })
        let reopened = try await StateStore(path: testFixture.store.path)
        let pending = try await reopened.read { conn in
            let queryStatement = try conn.prepare("SELECT count(*) FROM remote_change_inbox;"); _ = try queryStatement.step(); return queryStatement.columnInt64(at: 0)
        }
        #expect(pending == 1)
        context.value.state.withLock { $0.failToken = nil }
        let engine = try await SyncEngine(auth: testFixture.auth, store: reopened, client: testFixture.client)
        _ = try await engine.syncIncremental(localPath: testFixture.local.path)
        try await converge(testFixture)
        #expect(try String(contentsOf: testFixture.local.appendingPathComponent("dir/child"), encoding: .utf8) == "remote content")
    }

    @Test("Moved-in directory enumerates an existing deep subtree; ready uploads precede listing")
    func movedSubtree() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let parent = folder("incoming", "root")
        let nested = folder("nested", "incoming")
        context.value.state.withLock {
            $0.files[parent.id] = parent; $0.files[nested.id] = nested
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: parent.id, removed: false, file: parent)])
        }
        remoteFile("deep", parent: "nested")
        try Data("local new".utf8).write(to: testFixture.local.appendingPathComponent("upload.txt"))
        let first = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(first.filesUploaded == 1)
        #expect(first.remoteWorkPending > 0)
        let requests = context.value.state.withLock { $0.requests }
        let upload = try #require(requests.firstIndex { $0.contains("/upload/") })
        let listing = try #require(requests.firstIndex { $0.contains("/files?q=") })
        #expect(upload < listing)
        try await converge(testFixture)
        #expect(try String(contentsOf: testFixture.local.appendingPathComponent("incoming/nested/deep"), encoding: .utf8) == "remote content")
    }

    @Test("A directory rename and child move in one Changes page use the renamed path", arguments: [true, false])
    func parentRenameAndChildMoveInOnePage(directoryFirst: Bool) async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let originalDirectory = folder("directory", "root", name: "A")
        let originalFile = remoteFile("incoming", parent: "root", name: "incoming.txt")
        context.value.state.withLock {
            $0.files[originalDirectory.id] = originalDirectory
            $0.pages["start"] = DriveChangesPage(
                nextPageToken: nil, newStartPageToken: "steady",
                changes: [
                    DriveChange(fileId: originalDirectory.id, removed: false, file: originalDirectory),
                    DriveChange(fileId: originalFile.id, removed: false, file: originalFile)
                ])
        }
        try await converge(testFixture)
        let localA = testFixture.local.appendingPathComponent("A")
        let localIncoming = testFixture.local.appendingPathComponent("incoming.txt")
        #expect(FileManager.default.fileExists(atPath: localA.path))
        #expect(try Data(contentsOf: localIncoming) == Data("remote content".utf8))

        let renamedDirectory = DriveFile(
            id: originalDirectory.id, name: "B", mimeType: "application/vnd.google-apps.folder",
            parents: ["root"], version: "2")
        let movedFile = DriveFile(
            id: originalFile.id, name: originalFile.name, parents: [originalDirectory.id],
            size: originalFile.size, sha256Checksum: originalFile.sha256Checksum, version: "2")
        let directoryChange = DriveChange(fileId: renamedDirectory.id, removed: false, file: renamedDirectory)
        let fileChange = DriveChange(fileId: movedFile.id, removed: false, file: movedFile)
        context.value.state.withLock {
            $0.files[renamedDirectory.id] = renamedDirectory
            $0.files[movedFile.id] = movedFile
            $0.pages["steady"] = DriveChangesPage(
                nextPageToken: nil, newStartPageToken: "after-move",
                changes: directoryFirst ? [directoryChange, fileChange] : [fileChange, directoryChange])
        }

        _ = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let localBFile = testFixture.local.appendingPathComponent("B/incoming.txt")
        #expect((try? Data(contentsOf: localBFile)) == Data("remote content".utf8))
        #expect(!FileManager.default.fileExists(atPath: localA.path))
        let retry = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(retry.filesUploaded == 0)
        #expect((try? Data(contentsOf: localBFile)) == Data("remote content".utf8))
        #expect(!FileManager.default.fileExists(atPath: localA.path))
        let inboxIDs = try await testFixture.store.read { conn in
            let query = try conn.prepare("""
                SELECT remote_id FROM remote_change_inbox
                WHERE root_id = ? AND remote_id IN ('directory', 'incoming');
                """)
            defer { query.reset() }
            query.bindInt64(testFixture.rootID, at: 1)
            var ids: [String] = []
            while try query.step() {
                if let id = query.columnText(at: 0) { ids.append(id) }
            }
            return ids
        }
        #expect(inboxIDs.isEmpty)
        #expect(context.value.state.withLock {
            $0.files.values.filter { $0.name == "incoming.txt" }.count
        } == 1)
    }

    @Test("Missing and explicitly rejected cursors reconstruct pre-existing remote files", arguments: ["missing", "rejected", "invalid"])
    func missingCursor(kind: String) async throws {
        let testFixture = try await fixture(cursor: kind != "missing")
        defer { testFixture.cleanup() }
        remoteFile("before-token", parent: "root")
        if kind == "rejected" { context.value.state.withLock { $0.rejectToken = "start" } }
        if kind == "invalid" { try await testFixture.store.write { try $0.execute("UPDATE cursors SET is_valid = 0;") } }
        try await converge(testFixture)
        #expect(try String(contentsOf: testFixture.local.appendingPathComponent("before-token"), encoding: .utf8) == "remote content")
        #expect(try await token(testFixture) == "steady")
    }

    @Test("An incomplete or failed directory listing retains its job", arguments: [false, true])
    func listingFailure(incomplete: Bool) async throws {
        let testFixture = try await fixture(cursor: false)
        defer { testFixture.cleanup() }
        remoteFile("retained", parent: "root")
        context.value.state.withLock {
            if incomplete { $0.incompleteFolder = "root" } else { $0.failFolder = "root" }
        }
        do { _ = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path); Issue.record("Expected listing failure") } catch {}
        #expect(try await testFixture.changes.pendingCount() > 0)
        context.value.state.withLock { $0.failFolder = nil; $0.incompleteFolder = nil }
        try await converge(testFixture)
        #expect(FileManager.default.fileExists(atPath: testFixture.local.appendingPathComponent("retained").path))
    }

    @Test("Missing ancestor metadata stays durable after the final Changes token")
    func unknownParent() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let child = remoteFile("child", parent: "unavailable")
        context.value.state.withLock {
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: child.id, removed: false, file: child)])
        }
        let first = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(first.remoteWorkPending > 0)
        #expect(try await token(testFixture) == "steady")
        context.value.state.withLock { $0.files["unavailable"] = folder("unavailable", "root") }
        try await converge(testFixture)
        #expect(FileManager.default.fileExists(atPath: testFixture.local.appendingPathComponent("unavailable/child").path))
    }

    @Test("Known file moved outside preserves local bytes and cannot be written back")
    func movedOutside() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let file = remoteFile("known", parent: "root")
        context.value.state.withLock { $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: file.id, removed: false, file: file)]) }
        try await converge(testFixture)
        let outside = folder("outside", nil)
        let moved = DriveFile(id: file.id, name: file.name, parents: [outside.id], size: file.size, sha256Checksum: file.sha256Checksum, version: "2")
        context.value.state.withLock {
            $0.files[outside.id] = outside; $0.files[file.id] = moved
            $0.pages["steady"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "after-move", changes: [DriveChange(fileId: moved.id, removed: false, file: moved)])
        }
        let local = testFixture.local.appendingPathComponent("known")
        try Data("new local content".utf8).write(to: local)
        let stats = try await testFixture.engine.syncIncremental(
            localPath: testFixture.local.path)
        #expect(stats.filesUploaded == 0)
        #expect(stats.filesDeleted == 0)
        #expect(try String(contentsOf: local, encoding: .utf8) == "new local content")
        let status = try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT remote_status FROM items WHERE remote_file_id = 'known';"); _ = try queryStatement.step(); return queryStatement.columnText(at: 0)
        }
        #expect(status == "unknown")
    }

    @Test("Bootstrap token failures propagate; retries preserve an existing cursor")
    func bootstrapCursor() async throws {
        let testFixture = try await fixture(cursor: false)
        defer { testFixture.cleanup() }
        do {
            try await RemoteChanges.saveInitialCursor(store: testFixture.store, client: testFixture.client, rootID: testFixture.rootID, requireExisting: true)
            Issue.record("Existing roots must reconstruct missing cursor history")
        } catch {}
        #expect(try await token(testFixture) == nil)
        context.value.state.withLock { $0.failStart = true }
        do { try await RemoteChanges.saveInitialCursor(store: testFixture.store, client: testFixture.client, rootID: testFixture.rootID); Issue.record("Expected token error") } catch {}
        #expect(try await token(testFixture) == nil)
        context.value.state.withLock { $0.failStart = false }
        try await RemoteChanges.saveInitialCursor(store: testFixture.store, client: testFixture.client, rootID: testFixture.rootID)
        try await testFixture.store.write { try $0.execute("UPDATE cursors SET token_value = 'older-boundary';") }
        try await RemoteChanges.saveInitialCursor(store: testFixture.store, client: testFixture.client, rootID: testFixture.rootID)
        #expect(try await token(testFixture) == "older-boundary")
    }

    @Test("Bootstrap cursor failure can resume through sync", arguments: ["network", "interrupted"])
    func bootstrapCursorFailureCanResumeWithSync(scenario: String) async throws {
        let testFixture = try await fixture(cursor: false)
        defer { testFixture.cleanup() }
        let localData = Data("local bootstrap".utf8)
        try localData.write(to: testFixture.local.appendingPathComponent("local.txt"))

        if scenario == "network" {
            try await testFixture.store.write { try $0.execute("DELETE FROM roots;") }
            context.value.state.withLock { $0.failStart = true }
            await #expect(throws: (any Error).self) {
                try await testFixture.engine.sync(
                    localPath: testFixture.local.path, remoteFolderId: "root")
            }
            try await testFixture.store.read { conn in
                let queryStatement = try conn.prepare(
                    "SELECT (SELECT COUNT(*) FROM roots) + (SELECT COUNT(*) FROM cursors);")
                #expect(try queryStatement.step())
                #expect(queryStatement.columnInt64(at: 0) == 0)
            }
            context.value.state.withLock { $0.failStart = false }
        } else {
            try await testFixture.store.write {
                try $0.execute("UPDATE roots SET bootstrap_state = 'freshCreated';")
            }
            remoteFile("historical", parent: "root", content: "remote before recovery")
        }

        let resumed = try await testFixture.engine.sync(
            localPath: testFixture.local.path, remoteFolderId: "root")
        #expect(resumed.filesUploaded == 1)
        try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare(
                """
                SELECT (SELECT COUNT(*) FROM roots),
                    (SELECT COUNT(*) FROM cursors),
                    (SELECT bootstrap_state FROM roots LIMIT 1),
                    (SELECT COUNT(*) FROM items WHERE remote_file_id = 'historical');
                """)
            #expect(try queryStatement.step())
            #expect(queryStatement.columnInt64(at: 0) == 1)
            #expect(queryStatement.columnInt64(at: 1) == 1)
            #expect(queryStatement.columnText(at: 2) == "existingKnown")
            #expect(queryStatement.columnInt64(at: 3) == (scenario == "interrupted" ? 1 : 0))
        }
        #expect(context.value.state.withLock {
            $0.files.values.contains { $0.name == "local.txt" }
        })

        if scenario == "interrupted" {
            _ = try await testFixture.engine.sync(
                localPath: testFixture.local.path, remoteFolderId: "root")
            #expect(try String(
                contentsOf: testFixture.local.appendingPathComponent("historical"),
                encoding: .utf8) == "remote before recovery")
        }
    }
    @Test("Cursor commit failure rolls back the page observations")
    func atomicPage() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let file = remoteFile("atomic", parent: "root")
        context.value.state.withLock {
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: file.id, removed: false, file: file)])
        }
        try await testFixture.store.write { conn in
            try conn.execute("CREATE TRIGGER fail_cursor BEFORE UPDATE ON cursors BEGIN SELECT RAISE(ABORT, 'injected commit error'); END;")
        }
        do { try await testFixture.changes.consume(); Issue.record("Expected transaction failure") } catch {}
        #expect(try await token(testFixture) == "start")
        let rows = try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT count(*) FROM remote_change_inbox;"); _ = try queryStatement.step(); return queryStatement.columnInt64(at: 0)
        }
        #expect(rows == 0)
        try await testFixture.store.write { try $0.execute("DROP TRIGGER fail_cursor;") }
        try await converge(testFixture)
        #expect(FileManager.default.fileExists(atPath: testFixture.local.appendingPathComponent("atomic").path))
    }

    @Test("Listing page checkpoint survives failure without losing earlier children", arguments: [false, true])
    func directoryPages(rejected: Bool) async throws {
        let testFixture = try await fixture(cursor: false)
        defer { testFixture.cleanup() }
        let first = remoteFile("first", parent: "root")
        let second = remoteFile("second", parent: "root")
        let encoder = JSONEncoder()
        let firstPage = try JSONSerialization.data(withJSONObject: ["nextPageToken": "tail", "files": [JSONSerialization.jsonObject(with: encoder.encode(first))]])
        let secondPage = try JSONSerialization.data(withJSONObject: ["files": [JSONSerialization.jsonObject(with: encoder.encode(second))]])
        context.value.state.withLock {
            $0.listingPages["root:first"] = firstPage
            $0.listingPages["root:tail"] = secondPage
        }
        let round = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(round.remoteWorkPending > 0)
        context.value.state.withLock {
            if rejected { $0.rejectListingToken = "tail" } else { $0.failFolder = "root" }
        }
        do { _ = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path); Issue.record("Expected tail-page failure") } catch {}
        let checkpoint = try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT page_token FROM remote_directory_scans WHERE remote_id = 'root';"); _ = try queryStatement.step(); return queryStatement.columnText(at: 0)
        }
        #expect(checkpoint == (rejected ? nil : "tail"))
        context.value.state.withLock { $0.failFolder = nil; $0.rejectListingToken = nil }
        try await converge(testFixture)
        #expect(FileManager.default.fileExists(atPath: testFixture.local.appendingPathComponent("first").path))
        #expect(FileManager.default.fileExists(atPath: testFixture.local.appendingPathComponent("second").path))
    }

    @Test("A thousand known-parent observations use page-sized commits")
    func pageBatching() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let changes = (0..<1000).map { index -> DriveChange in
            let file = DriveFile(id: "id-\(index)", name: "file-\(index)", parents: ["root"], size: "1", sha256Checksum: String(repeating: "a", count: 64))
            return DriveChange(fileId: file.id, removed: false, file: file)
        }
        context.value.state.withLock { $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: changes) }
        let before = await testFixture.store.getWriterStats()
        try await testFixture.changes.consume()
        let after = await testFixture.store.getWriterStats()
        #expect(after.totalCommits - before.totalCommits <= 3)
        let count = try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT count(*) FROM items WHERE entry_kind = 'file' AND remote_status = 'present';"); _ = try queryStatement.step(); return queryStatement.columnInt64(at: 0)
        }
        #expect(count == 1000)
        #expect(try await testFixture.changes.pendingCount() == 0)
    }

    @Test("An empty incoming directory is not mistaken for a local deletion while listing")
    func emptyIncomingDirectory() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let dir = folder("empty", "root")
        context.value.state.withLock {
            $0.files[dir.id] = dir
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: dir.id, removed: false, file: dir)])
        }
        try await converge(testFixture)
        _ = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(FileManager.default.fileExists(atPath: testFixture.local.appendingPathComponent("empty").path))
        #expect(!context.value.state.withLock { $0.requests.contains { $0.hasPrefix("PATCH") } })
    }

    @Test("Moved-out directories also block new children after a local rename")
    func movedOutsideDirectory() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let dir = folder("dir", "root")
        context.value.state.withLock {
            $0.files[dir.id] = dir
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: dir.id, removed: false, file: dir)])
        }
        remoteFile("child", parent: "dir")
        try await converge(testFixture)
        let moved = folder("dir", "outside")
        context.value.state.withLock {
            $0.files["outside"] = folder("outside", nil)
            $0.files["dir"] = moved
            $0.pages["steady"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "after-move", changes: [DriveChange(fileId: moved.id, removed: false, file: moved)])
        }
        _ = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        try FileManager.default.moveItem(at: testFixture.local.appendingPathComponent("dir"), to: testFixture.local.appendingPathComponent("renamed"))
        try Data("local new".utf8).write(to: testFixture.local.appendingPathComponent("renamed/new.txt"))
        let stats = try await testFixture.engine.syncIncremental(
            localPath: testFixture.local.path)
        #expect(stats.filesFailed == 0)
        #expect(stats.filesUploaded == 0)
        #expect(stats.directoriesCreated == 0)
        #expect(stats.filesDeleted == 0)
        #expect(FileManager.default.fileExists(atPath: testFixture.local.appendingPathComponent("renamed/child").path))
    }

    @Test("A failed remote rename records a local path issue")
    func failedRemoteRenameRecordsIssue() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let original = remoteFile("renamed-file", parent: "root", name: "original.txt")
        context.value.state.withLock {
            $0.pages["start"] = DriveChangesPage(
                nextPageToken: nil, newStartPageToken: "steady",
                changes: [DriveChange(fileId: original.id, removed: false, file: original)])
        }
        try await converge(testFixture)
        let occupied = testFixture.local.appendingPathComponent("occupied.txt")
        try Data("untracked local file".utf8).write(to: occupied)

        let renamed = DriveFile(
            id: original.id, name: occupied.lastPathComponent, parents: ["root"],
            size: original.size, sha256Checksum: original.sha256Checksum, version: "2")
        context.value.state.withLock {
            $0.files[renamed.id] = renamed
            $0.pages["steady"] = DriveChangesPage(
                nextPageToken: nil, newStartPageToken: "done",
                changes: [DriveChange(fileId: renamed.id, removed: false, file: renamed)])
        }

        try await testFixture.changes.consume()

        let page = try await testFixture.engine.listSyncIssues(localPath: testFixture.local.path)
        let issue = try #require(page.issues.first { $0.remoteFileId == renamed.id })
        #expect(issue.relativePath == occupied.lastPathComponent)
        #expect(issue.stage == .pathUpdate)
        #expect(issue.category == .localIO)
        #expect(issue.suggestedAction == .inspectLocalFile)
        #expect(try await testFixture.changes.pendingCount() > 0)
        #expect(try String(contentsOf: occupied, encoding: .utf8) == "untracked local file")
    }

    @Test("A remote rename recovers a file whose first download never landed")
    func remoteRenameRecoversFileWithoutLocalIdentity() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let original = remoteFile("pending-file", parent: "root", name: "original.txt")
        let body = context.value.state.withLock { state -> Data in
            state.pages["start"] = DriveChangesPage(
                nextPageToken: nil, newStartPageToken: "steady",
                changes: [DriveChange(fileId: original.id, removed: false, file: original)])
            return state.contents.removeValue(forKey: original.id) ?? Data()
        }

        let first = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        #expect(first.filesDownloaded == 0)
        #expect(first.filesFailed == 1)
        #expect(!FileManager.default.fileExists(
            atPath: testFixture.local.appendingPathComponent(original.name).path))
        let renamed = DriveFile(
            id: original.id, name: "renamed.txt", parents: ["root"],
            size: original.size, sha256Checksum: original.sha256Checksum, version: "2")
        context.value.state.withLock {
            $0.files[renamed.id] = renamed
            $0.contents[renamed.id] = body
            $0.pages["steady"] = DriveChangesPage(
                nextPageToken: nil, newStartPageToken: "done",
                changes: [DriveChange(fileId: renamed.id, removed: false, file: renamed)])
        }

        let second = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)

        #expect(second.filesDownloaded == 1)
        #expect(second.filesFailed == 0)
        #expect(second.remoteWorkPending == 0)
        #expect(try Data(contentsOf: testFixture.local.appendingPathComponent(renamed.name)) == body)
        #expect(!FileManager.default.fileExists(
            atPath: testFixture.local.appendingPathComponent(original.name).path))
    }

    @Test("Rebuilding a lost cursor re-probes old queued payloads")
    func staleQueuedObservation() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let old = remoteFile("stale", parent: "root")
        let payload = (String(bytes: try JSONEncoder().encode(DriveChange(fileId: old.id, removed: false, file: old)), encoding: .utf8) ?? "Invalid UTF-8 data")
        try await testFixture.store.write { conn in
            try conn.execute("DELETE FROM cursors;")
            let queryStatement = try conn.prepare("INSERT INTO remote_change_inbox(root_id, remote_id, payload) VALUES (?, 'stale', ?);")
            queryStatement.bindInt64(testFixture.rootID, at: 1); queryStatement.bindText(payload, at: 2); _ = try queryStatement.step()
        }
        context.value.state.withLock { _ = $0.files.removeValue(forKey: "stale") }
        let stats = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(stats.remoteWorkPending > 0)
        #expect(stats.filesDownloaded == 0)
        #expect(!FileManager.default.fileExists(atPath: testFixture.local.appendingPathComponent("stale").path))
    }

    @Test("Empty root reconstruction still reports the deferred local upload round")
    func emptyRebuildWithLocalFiles() async throws {
        let testFixture = try await fixture(cursor: false)
        defer { testFixture.cleanup() }
        try Data("local new".utf8).write(to: testFixture.local.appendingPathComponent("local.txt"))
        let first = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(first.filesUploaded == 0)
        #expect(first.remoteWorkPending > 0)
        let second = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(second.filesUploaded == 1)
        #expect(second.remoteWorkPending == 0)
    }

    @Test("A14 bootstrap blocks equivalent sibling names before any publication", arguments: [
        ["same", "same"], ["a", "A"], ["é", "e\u{301}"]
    ])
    func bootstrapNameCollision(names: [String]) async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        _ = remoteFile("one", parent: "root", content: "first", name: names[0])
        _ = remoteFile("two", parent: "root", content: "second", name: names[1])
        await #expect(throws: (any Error).self) {
            _ = try await testFixture.engine.syncRemoteToLocalEmpty(localPath: testFixture.local.path, remoteRootId: "root")
        }
        #expect(try FileManager.default.contentsOfDirectory(atPath: testFixture.local.path).isEmpty)
        #expect(context.value.state.withLock { !$0.requests.contains { $0.contains("alt=media") } })
        #expect(try await testFixture.changes.pendingCount() > 0)
    }

    @Test("A14 directory identities cannot collapse on bootstrap", arguments: [false, true])
    func bootstrapDirectoryCollision(mixed: Bool) async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        context.value.state.withLock {
            $0.files["one"] = folder("one", "root", name: "same")
            $0.files["two"] = mixed ? DriveFile(id: "two", name: "same", parents: ["root"]) : folder("two", "root", name: "same")
        }
        await #expect(throws: (any Error).self) {
            _ = try await testFixture.engine.syncRemoteToLocalEmpty(localPath: testFixture.local.path, remoteRootId: "root")
        }
        #expect(try FileManager.default.contentsOfDirectory(atPath: testFixture.local.path).isEmpty)
        let count = try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT COUNT(*) FROM items WHERE parent_id IS NOT NULL;")
            _ = try queryStatement.step(); return queryStatement.columnInt64(at: 0)
        }
        #expect(count == 0)
    }

    @Test("A14 Changes retain colliding identity and gate both sides until rename", arguments: [
        ["same", "same"], ["a", "A"], ["é", "e\u{301}"]
    ])
    func changesNameCollision(names: [String]) async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let one = remoteFile("one", parent: "root", content: "first", name: names[0])
        let two = remoteFile("two", parent: "root", content: "second", name: names[1])
        context.value.state.withLock {
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [
                DriveChange(fileId: one.id, removed: false, file: one),
                DriveChange(fileId: two.id, removed: false, file: two)])
        }
        let blocked = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(blocked.remoteWorkPending > 0)
        #expect(blocked.remoteNameConflicts == 1)
        #expect(blocked.issueCount == 1)
        let issues = try await testFixture.engine.listSyncIssues(localPath: testFixture.local.path)
        #expect(issues.totalCount == 1)
        #expect(issues.issues.first?.category == .remoteConflict)
        #expect(issues.issues.first?.suggestedAction == .renameRemote)
        #expect(blocked.filesDownloaded == 0)
        #expect(try FileManager.default.contentsOfDirectory(atPath: testFixture.local.path).isEmpty)
        let renamed = remoteFile("two", parent: "root", content: "second", name: "unique")
        context.value.state.withLock {
            $0.pages["steady"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "done", changes: [DriveChange(fileId: renamed.id, removed: false, file: renamed)])
        }
        try await converge(testFixture)
        #expect(try await testFixture.engine.listSyncIssues(
            localPath: testFixture.local.path).issues.isEmpty)
        #expect(try String(contentsOf: testFixture.local.appendingPathComponent(names[0]), encoding: .utf8) == "first")
        #expect(try String(contentsOf: testFixture.local.appendingPathComponent("unique"), encoding: .utf8) == "second")
    }

    @Test("A14 unsafe names cannot escape the bootstrap root", arguments: ["../escape", "/absolute", ".", "..", "nul\0name"])
    func unsafeBootstrapName(name: String) async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        _ = remoteFile("bad", parent: "root", name: name)
        await #expect(throws: (any Error).self) {
            _ = try await testFixture.engine.syncRemoteToLocalEmpty(localPath: testFixture.local.path, remoteRootId: "root")
        }
        #expect(try FileManager.default.contentsOfDirectory(atPath: testFixture.local.path).isEmpty)
        #expect(!FileManager.default.fileExists(atPath: testFixture.directory.appendingPathComponent("escape").path))
        #expect(context.value.state.withLock { !$0.requests.contains { $0.contains("alt=media") } })
    }

    @Test("A14 Changes block symlinked parents without writing outside root")
    func symlinkedRemoteParent() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let parent = folder("parent", "root")
        context.value.state.withLock {
            $0.files[parent.id] = parent
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: parent.id, removed: false, file: parent)])
        }
        try await converge(testFixture)
        let outside = testFixture.directory.appendingPathComponent("outside")
        try FileManager.default.createDirectory(at: outside, withIntermediateDirectories: true)
        let localParent = testFixture.local.appendingPathComponent("parent")
        try FileManager.default.removeItem(at: localParent)
        try FileManager.default.createSymbolicLink(at: localParent, withDestinationURL: outside)
        #expect(throws: (any Error).self) {
            try RemoteNameMapping.validateDestination(localParent.appendingPathComponent("missing/child"), root: testFixture.local)
        }
        let child = remoteFile("child", parent: "parent")
        context.value.state.withLock {
            $0.pages["steady"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "done", changes: [DriveChange(fileId: child.id, removed: false, file: child)])
        }
        try await testFixture.changes.consume()
        #expect(try await testFixture.changes.pendingCount() > 0)
        #expect(try FileManager.default.contentsOfDirectory(atPath: outside.path).isEmpty)
    }

    @Test("A14 name lookup uses index and equivalent names retain distinct item identities")
    func nameIndex() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        try await testFixture.store.write { conn in
            for name in ["a", "A"] {
                let queryStatement = try conn.prepare("INSERT INTO items(root_id,parent_id,name,entry_kind,remote_file_id,created_at,updated_at) VALUES (?,? ,?,'file',?,1,1);")
                queryStatement.bindInt64(testFixture.rootID, at: 1); queryStatement.bindInt64(testFixture.rootItemID, at: 2)
                queryStatement.bindText(name, at: 3); queryStatement.bindText(name, at: 4)
                _ = try queryStatement.step()
            }
            let plan = try conn.prepare("EXPLAIN QUERY PLAN SELECT item_id FROM items INDEXED BY idx_items_local_name_key WHERE root_id = 1 AND parent_id = 1 AND gdrive_name_key(name) = gdrive_name_key('a');")
            #expect(try plan.step())
            #expect(plan.columnText(at: 3)?.contains("idx_items_local_name_key") == true)
            let queryStatement = try conn.prepare("SELECT COUNT(*) FROM items WHERE gdrive_name_key(name) = gdrive_name_key('A');")
            #expect(try queryStatement.step())
            #expect(queryStatement.columnInt64(at: 0) == 2)
        }
    }
    @Test("A15 empty binding is complete and accepts additions on either side", arguments: [false, true])
    func emptyBinding(remoteAddition: Bool) async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        try await testFixture.store.write { try $0.execute("DELETE FROM roots;") }
        let before = await testFixture.store.getWriterStats()
        _ = try await testFixture.engine.sync(localPath: testFixture.local.path, remoteFolderId: "root")
        let after = await testFixture.store.getWriterStats()
        #expect(after.totalCommits - before.totalCommits == 1)
        try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT r.bootstrap_state, i.phase, c.token_value FROM roots r JOIN items i USING(root_id) JOIN cursors c USING(root_id) WHERE i.parent_id IS NULL;")
            #expect(try queryStatement.step())
            #expect(queryStatement.columnText(at: 0) == "existingKnown")
            #expect(queryStatement.columnText(at: 1) == "committed")
            #expect(queryStatement.columnText(at: 2) == "fresh")
        }
        let requests = context.value.state.withLock { $0.requests }
        #expect(requests.count == 3)
        #expect(requests[1].contains("startPageToken"))
        #expect(requests[2].contains("/files?"))
        if remoteAddition {
            let file = remoteFile("new", parent: "root", content: "new content", name: ".env")
            context.value.state.withLock {
                $0.pages["fresh"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: file.id, removed: false, file: file)])
            }
        } else {
            try Data("new content".utf8).write(to: testFixture.local.appendingPathComponent(".env"))
        }
        let result = try await testFixture.engine.sync(localPath: testFixture.local.path, remoteFolderId: "root")
        #expect(result.filesFailed == 0)
        #expect(remoteAddition ? result.filesDownloaded == 1 : result.filesUploaded == 1)
        #expect(try String(contentsOf: testFixture.local.appendingPathComponent(".env"), encoding: .utf8) == "new content")
    }

    @Test("A15 hidden file initializes upload")
    func hiddenInitialUpload() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        try await testFixture.store.write { try $0.execute("DELETE FROM roots;") }
        try Data("secret".utf8).write(to: testFixture.local.appendingPathComponent(".env"))
        let result = try await testFixture.engine.sync(localPath: testFixture.local.path, remoteFolderId: "root")
        #expect(result.filesUploaded == 1)
        #expect(result.filesFailed == 0)
    }

    @Test("A15 probe includes hidden entries and excludes only git directories")
    func emptyProbeFiltering() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        #expect(try SyncEngine.isLocalRootEmpty(root.path))
        let git = root.appendingPathComponent(".git")
        try FileManager.default.createDirectory(at: git, withIntermediateDirectories: true)
        try Data("ignored".utf8).write(to: git.appendingPathComponent("config"))
        #expect(try SyncEngine.isLocalRootEmpty(root.path))
        try FileManager.default.removeItem(at: git)
        try Data("gitdir: elsewhere".utf8).write(to: git)
        #expect(try !SyncEngine.isLocalRootEmpty(root.path))
        #expect(throws: POSIXError.self) { try SyncEngine.isLocalRootEmpty(git.path) }
    }

    @Test("A15 unreadable local root never commits an empty baseline")
    func unreadableInitialRoot() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        try await testFixture.store.write { try $0.execute("DELETE FROM roots;") }
        try FileManager.default.setAttributes([.posixPermissions: 0], ofItemAtPath: testFixture.local.path)
        defer { try? FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: testFixture.local.path) }
        await #expect(throws: POSIXError.self) {
            try await testFixture.engine.sync(localPath: testFixture.local.path, remoteFolderId: "root")
        }
        try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT COUNT(*) FROM roots;")
            #expect(try queryStatement.step())
            #expect(queryStatement.columnInt64(at: 0) == 0)
        }
    }

    @Test("A15 cursor insert failure rolls back root and item")
    func emptyBindingRollback() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        try await testFixture.store.write {
            try $0.execute("DELETE FROM roots;")
            try $0.execute("CREATE TRIGGER reject_cursor BEFORE INSERT ON cursors BEGIN SELECT RAISE(ABORT, 'injected'); END;")
        }
        await #expect(throws: (any Error).self) {
            try await testFixture.engine.sync(localPath: testFixture.local.path, remoteFolderId: "root")
        }
        try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT (SELECT COUNT(*) FROM roots) + (SELECT COUNT(*) FROM items) + (SELECT COUNT(*) FROM cursors);")
            #expect(try queryStatement.step())
            #expect(queryStatement.columnInt64(at: 0) == 0)
        }
    }

    @Test("A15 absent local directory is created for an empty binding")
    func absentLocalEmptyBinding() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        try await testFixture.store.write { try $0.execute("DELETE FROM roots;") }
        try FileManager.default.removeItem(at: testFixture.local)
        _ = try await testFixture.engine.sync(localPath: testFixture.local.path, remoteFolderId: "root")
        _ = try await testFixture.engine.sync(localPath: testFixture.local.path, remoteFolderId: "root")
        #expect(FileManager.default.fileExists(atPath: testFixture.local.path))
    }

    @Test("A15 download bootstrap reuses the routing cursor")
    func downloadReusesCursor() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let staging = testFixture.directory.appendingPathComponent("bootstrap-downloads")
        try testFixture.engine.setDownloadTemporaryDirectory(staging)
        try await testFixture.store.write { try $0.execute("DELETE FROM roots;") }
        remoteFile("new", parent: "root")
        let result = try await testFixture.engine.sync(localPath: testFixture.local.path, remoteFolderId: "root")
        #expect(result.filesDownloaded == 1)
        #expect(result.filesFailed == 0)
        #expect(!FileManager.default.fileExists(atPath: staging.appendingPathComponent("root").path))
        #expect(context.value.state.withLock { $0.requests.filter { $0.contains("startPageToken") }.count } == 1)
    }

    private func oneEntry(_ url: URL, type: EntryType = .file) throws -> ScanBatch {
        try entries([(url, type)])
    }

    private func entries(_ entries: [(URL, EntryType)]) throws -> ScanBatch {
        var data: [UInt8] = []
        var records: [ScanBatch.Record] = []
        for (url, type) in entries {
            var info = stat()
            guard stat(url.path, &info) == 0 else { throw POSIXError(.EIO) }
            let bytes = Array(url.path.utf8)
            records.append(.init(offset: UInt32(data.count), length: UInt32(bytes.count), type: type,
                metadata: FileMetadata(identity: FileIdentity(device: info.st_dev, inode: info.st_ino),
                    modificationTime: FileTimestamp(seconds: Int64(info.st_mtimespec.tv_sec), nanoseconds: Int32(info.st_mtimespec.tv_nsec)),
                    changeTime: FileTimestamp(seconds: Int64(info.st_ctimespec.tv_sec), nanoseconds: Int32(info.st_ctimespec.tv_nsec)),
                    fileSize: Int64(info.st_size))))
            data.append(contentsOf: bytes)
            data.append(0)
        }
        return ScanBatch(pathData: data, records: records)
    }

    private func waitForUpload(_ name: String) async throws -> Bool {
        for _ in 0..<200 {
            if context.value.state.withLock({ $0.files.values.contains { $0.name == name } }) { return true }
            try await Task.sleep(for: .milliseconds(10))
        }
        return false
    }

    private func waitForCommittedUpload(_ name: String, store: StateStore) async throws {
        while true {
            try Task.checkCancellation()
            let committed = try await store.read { conn in
                let statement = try conn.prepare(
                    "SELECT phase, dirty_generation FROM items WHERE name = ?;")
                statement.bindText(name, at: 1)
                defer { statement.reset() }
                return try statement.step()
                    && statement.columnText(at: 0) == "committed"
                    && statement.columnInt64(at: 1) == 0
            }
            if committed { return }
            await Task.yield()
        }
    }

    @Test(
        "File and directory replacements converge in one incremental sync",
        arguments: [false, true])
    func typeReplacementConverges(
        replacementIsDirectory: Bool
    ) async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let name = "replaced"
        let local = testFixture.local.appendingPathComponent(name)
        let originalKind = replacementIsDirectory ? "file" : "directory"
        let remoteID = "remote-replaced"
        if replacementIsDirectory {
            try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
            try Data("child".utf8).write(to: local.appendingPathComponent("child.txt"))
            _ = remoteFile(remoteID, parent: "root", name: name)
        } else {
            try Data("replacement".utf8).write(to: local)
            context.value.state.withLock {
                $0.files[remoteID] = folder(remoteID, "root", name: name)
            }
        }
        let baselineSHA256 = SyncEngine.computeSha256(of: Data("remote content".utf8))
        try await testFixture.store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO items(
                    root_id,parent_id,name,entry_kind,remote_file_id,
                    local_sha256,local_size,base_sha256,base_size,remote_sha256,remote_size,
                    local_status,remote_status,phase,dirty_generation,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'present','present','committed',0,1,1);
                """)
            stmt.bindInt64(testFixture.rootID, at: 1)
            stmt.bindInt64(testFixture.rootItemID, at: 2)
            stmt.bindText(name, at: 3)
            stmt.bindText(originalKind, at: 4)
            stmt.bindText(remoteID, at: 5)
            if originalKind == "file" {
                stmt.bindText(baselineSHA256, at: 6)
                stmt.bindInt64(14, at: 7)
                stmt.bindText(baselineSHA256, at: 8)
                stmt.bindInt64(14, at: 9)
                stmt.bindText(baselineSHA256, at: 10)
                stmt.bindInt64(14, at: 11)
            }
            _ = try stmt.step()
        }

        let stats = try await testFixture.engine.syncIncrementalUnlocked(
            rootId: testFixture.rootID, rootItemId: testFixture.rootItemID,
            localPath: testFixture.local.path, remoteRootId: "root", maxConcurrency: 1,
            onProgress: nil)
        #expect(stats.filesFailed == 0)

        let stored = try await testFixture.store.read { conn -> StoredBaseline in
            let stmt = try conn.prepare(
                "SELECT entry_kind, remote_file_id, dirty_generation FROM items WHERE root_id = \(testFixture.rootID) AND parent_id = \(testFixture.rootItemID) AND name = '\(name)';")
            defer { stmt.reset() }
            #expect(try stmt.step())
            return StoredBaseline(
                kind: stmt.columnText(at: 0), remoteID: stmt.columnText(at: 1),
                dirtyGeneration: stmt.columnInt64(at: 2))
        }
        #expect(stored.kind == (replacementIsDirectory ? "directory" : "file"))
        #expect(stored.remoteID != remoteID)
        #expect(stored.dirtyGeneration == 0)
        let remoteState = context.value.state.withLock { state in
            (state.files[remoteID]?.trashed, state.files.values.filter {
                $0.id != remoteID && $0.name == name && $0.trashed != true
            })
        }
        #expect(remoteState.0 == true)
        #expect(remoteState.1.count == 1)
        #expect((remoteState.1.first?.mimeType == "application/vnd.google-apps.folder")
            == replacementIsDirectory)
        if replacementIsDirectory {
            let replacementID = try #require(remoteState.1.first?.id)
            #expect(context.value.state.withLock { state in
                state.files.values.contains {
                    $0.name == "child.txt" && $0.parents?.first == replacementID
                        && $0.trashed != true
                }
            })
        }
    }

    @Test("A conflicting type replacement records that another sync is required")
    func conflictingTypeReplacementRecordsRetry() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let name = "replaced-conflict"
        let local = testFixture.local.appendingPathComponent(name)
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        let baselineBytes = Data("baseline".utf8)
        let remoteID = "remote-replaced-conflict"
        _ = remoteFile(remoteID, parent: "root", content: "changed remotely", name: name)
        let baselineSHA256 = SyncEngine.computeSha256(of: baselineBytes)
        try await testFixture.store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO items(
                    root_id,parent_id,name,entry_kind,remote_file_id,
                    local_sha256,local_size,base_sha256,base_size,remote_sha256,remote_size,
                    local_status,remote_status,phase,dirty_generation,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'present','present','committed',0,1,1);
                """)
            stmt.bindInt64(testFixture.rootID, at: 1)
            stmt.bindInt64(testFixture.rootItemID, at: 2)
            stmt.bindText(name, at: 3)
            stmt.bindText("file", at: 4)
            stmt.bindText(remoteID, at: 5)
            stmt.bindText(baselineSHA256, at: 6)
            stmt.bindInt64(Int64(baselineBytes.count), at: 7)
            stmt.bindText(baselineSHA256, at: 8)
            stmt.bindInt64(Int64(baselineBytes.count), at: 9)
            let remoteBytes = Data("changed remotely".utf8)
            stmt.bindText(SyncEngine.computeSha256(of: remoteBytes), at: 10)
            stmt.bindInt64(Int64(remoteBytes.count), at: 11)
            _ = try stmt.step()
        }

        let stats = try await testFixture.engine.syncIncrementalUnlocked(
            rootId: testFixture.rootID, rootItemId: testFixture.rootItemID,
            localPath: testFixture.local.path, remoteRootId: "root", maxConcurrency: 1,
            onProgress: nil)

        #expect(stats.issueCount > 0)
        let page = try await testFixture.engine.listSyncIssues(localPath: testFixture.local.path)
        let issue = try #require(page.issues.first {
            $0.relativePath == name && $0.stage == .localScan
        })
        #expect(issue.category == .staleState)
        #expect(issue.suggestedAction == .retry)
        #expect(issue.message.contains("run synchronization again"))
        #expect(context.value.state.withLock { $0.files[remoteID]?.trashed } != true)
        let storedKind = try await testFixture.store.read { conn -> String? in
            let stmt = try conn.prepare(
                "SELECT entry_kind FROM items WHERE root_id = ? AND parent_id = ? AND name = ?;")
            defer { stmt.reset() }
            stmt.bindInt64(testFixture.rootID, at: 1)
            stmt.bindInt64(testFixture.rootItemID, at: 2)
            stmt.bindText(name, at: 3)
            return try stmt.step() ? stmt.columnText(at: 0) : nil
        }
        #expect(storedKind == "file")
    }

    @Test(
        "A hard link is uploaded as a separate path without renaming its sibling",
        arguments: [false, true])
    func hardLinkDoesNotBecomeRename(crossDirectory: Bool) async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let original = testFixture.local.appendingPathComponent("a.txt")
        let linkedParent = crossDirectory
            ? testFixture.local.appendingPathComponent("sub", isDirectory: true)
            : testFixture.local
        try FileManager.default.createDirectory(at: linkedParent, withIntermediateDirectories: true)
        let linked = linkedParent.appendingPathComponent("b.txt")
        let content = Data("shared inode".utf8)
        try content.write(to: original)
        #expect(link(original.path, linked.path) == 0)
        var metadata = stat()
        #expect(lstat(original.path, &metadata) == 0)
        let device = metadata.st_dev
        let inode = metadata.st_ino
        let mtime = metadata.st_mtimespec.tv_sec * 1_000_000_000
            + metadata.st_mtimespec.tv_nsec
        let sha = SyncEngine.computeSha256(of: content)
        let remote = remoteFile("remote-a", parent: "root", content: "shared inode", name: "a.txt")
        try await testFixture.store.write { conn in
            try conn.execute("""
                INSERT INTO items(
                    root_id,parent_id,name,entry_kind,remote_file_id,
                    local_device,local_inode,local_mtime,local_size,local_sha256,
                    base_sha256,base_size,remote_sha256,remote_size,
                    local_status,remote_status,phase,dirty_generation,created_at,updated_at)
                VALUES (
                    \(testFixture.rootID),\(testFixture.rootItemID),'a.txt','file','\(remote.id)',
                    \(device),\(inode),\(mtime),\(content.count),'\(sha)',
                    '\(sha)',\(content.count),'\(sha)',\(content.count),
                    'present','present','committed',0,1,1);
                """)
        }

        let stats = try await testFixture.engine.syncIncrementalUnlocked(
            rootId: testFixture.rootID, rootItemId: testFixture.rootItemID,
            localPath: testFixture.local.path, remoteRootId: "root", maxConcurrency: 1,
            onProgress: nil)

        #expect(stats.filesUploaded == 1)
        let names = context.value.state.withLock { Set($0.files.values.map(\.name)) }
        #expect(names.contains("a.txt"))
        #expect(names.contains("b.txt"))
        let storedNames = try await testFixture.store.read { conn -> Set<String> in
            let query = try conn.prepare(
                "SELECT name FROM items WHERE root_id = \(testFixture.rootID) AND entry_kind = 'file';")
            defer { query.reset() }
            var result = Set<String>()
            while try query.step(), let name = query.columnText(at: 0) { result.insert(name) }
            return result
        }
        #expect(storedNames.contains("a.txt"))
        #expect(storedNames.contains("b.txt"))
        let requestCount = context.value.state.withLock { $0.requests.count }
        let second = try await testFixture.engine.syncIncrementalUnlocked(
            rootId: testFixture.rootID, rootItemId: testFixture.rootItemID,
            localPath: testFixture.local.path, remoteRootId: "root", maxConcurrency: 1,
            onProgress: nil)
        #expect(second.filesUploaded == 0)
        #expect(context.value.state.withLock {
            !$0.requests.dropFirst(requestCount).contains { $0.hasPrefix("PATCH ") }
        })
    }

    @Test("A vanished observation does not abort later files or become a deletion")
    func vanishedObservationIsolated() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let vanished = testFixture.local.appendingPathComponent("vanished")
        let healthy = testFixture.local.appendingPathComponent("healthy")
        let old = Data("old baseline".utf8)
        try old.write(to: vanished)
        try Data("healthy body".utf8).write(to: healthy)
        let batch = try entries([(vanished, .file), (healthy, .file)])
        var version = stat()
        #expect(stat(vanished.path, &version) == 0)
        let device = Int64(version.st_dev)
        let inode = Int64(version.st_ino)
        let oldSHA = SyncEngine.computeSha256(of: old)
        try await testFixture.store.write { conn in
            let query = try conn.prepare("""
                INSERT INTO items(root_id,parent_id,name,entry_kind,remote_file_id,
                    local_device,local_inode,local_mtime,local_size,local_sha256,
                    base_sha256,base_size,remote_sha256,remote_size,
                    local_status,remote_status,phase,dirty_generation,created_at,updated_at)
                VALUES (?,?,?,'file','remote-vanished',?,?,?,?,?,?,?,?,?,
                    'present','present','committed',0,1,1);
                """)
            query.bindInt64(testFixture.rootID, at: 1)
            query.bindInt64(testFixture.rootItemID, at: 2)
            query.bindText("vanished", at: 3)
            query.bindInt64(device, at: 4)
            query.bindInt64(inode, at: 5)
            query.bindInt64(0, at: 6) // force hashing instead of the cache fast path
            query.bindInt64(Int64(old.count), at: 7)
            query.bindText(oldSHA, at: 8)
            query.bindText(oldSHA, at: 9)
            query.bindInt64(Int64(old.count), at: 10)
            query.bindText(oldSHA, at: 11)
            query.bindInt64(Int64(old.count), at: 12)
            _ = try query.step()
        }
        let engine = try await SyncEngine(auth: testFixture.auth, store: testFixture.store, client: testFixture.client,
            idPool: IDPool(initialIds: ["healthy-id"]), incrementalScan: { _, consume in
                try FileManager.default.removeItem(at: vanished)
                try await consume(batch)
            })

        let stats = try await engine.syncIncremental(localPath: testFixture.local.path)

        #expect(stats.filesFailed == 1)
        #expect(stats.filesUploaded == 1)
        #expect(context.value.state.withLock { $0.files.values.contains { $0.name == "healthy" } })
        let preserved = try await testFixture.store.read { conn in
            let query = try conn.prepare("SELECT local_status, base_sha256, dirty_generation, phase, local_sha256 FROM items WHERE name = 'vanished';")
            guard try query.step() else { return false }
            return query.columnText(at: 0) == "unknown"
                && query.columnText(at: 1) == oldSHA && (query.columnInt64(at: 2) ?? 0) > 0
                && query.columnText(at: 3) == "waitingEvidence" && query.columnText(at: 4) == oldSHA
        }
        #expect(preserved)
        #expect(!context.value.state.withLock { $0.requests.contains { $0.contains("remote-vanished") } })
    }

    @Test("Streaming downloads stay outside the active scan and use the configured remote-root folder")
    func downloadOutsideScan() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let first = testFixture.local.appendingPathComponent("first")
        let old = Data("old".utf8)
        try old.write(to: first)
        let remote = remoteFile("remote-first", parent: "root", content: String(repeating: "R", count: 131072), name: "first")
        let oldSHA = SyncEngine.computeSha256(of: old)
        let remoteSHA = try #require(remote.sha256Checksum)
        try await testFixture.store.write { conn in
            try conn.execute("INSERT INTO items(root_id,parent_id,name,entry_kind,remote_file_id,base_sha256,base_size,remote_sha256,remote_size,remote_status,phase,dirty_generation,created_at,updated_at) VALUES (\(testFixture.rootID),\(testFixture.rootItemID),'first','file','remote-first','\(oldSHA)',3,'\(remoteSHA)',131072,'present','ready',1,1,1);")
        }
        let batch = try oneEntry(first)
        let stagingBase = testFixture.directory.appendingPathComponent("downloads")
        let staging = stagingBase.appendingPathComponent("root")
        context.value.holdDownloads.withLock { $0 = true }
        defer { context.value.holdDownloads.withLock { $0 = false } }
        let engine = try await SyncEngine(auth: testFixture.auth, store: testFixture.store, client: testFixture.client,
            idPool: IDPool(initialIds: ["temp-upload-id"]), incrementalScan: { _, consume in
                defer { context.value.heldDownload.withLock { callback in callback?(); callback = nil } }
                try await consume(batch)
                var temporary: URL?
                for _ in 0..<500 {
                    // Check both places so this reproduces the original in-tree download defect.
                    let entries = (try? FileManager.default.contentsOfDirectory(at: staging, includingPropertiesForKeys: nil)) ?? []
                    let localEntries = try FileManager.default.contentsOfDirectory(at: testFixture.local, includingPropertiesForKeys: nil)
                    temporary = (entries + localEntries).first {
                        $0.lastPathComponent.hasPrefix(".tmp_") && ((try? LocalFileVersion.read(at: $0))?.size ?? 0) >= 131072
                    }
                    if temporary != nil { break }
                    try await Task.sleep(for: .milliseconds(10))
                }
                let observedTemporary = try #require(temporary)
                #expect(observedTemporary.deletingLastPathComponent().resolvingSymlinksInPath().path == staging.resolvingSymlinksInPath().path)
                for entry in try FileManager.default.contentsOfDirectory(at: testFixture.local, includingPropertiesForKeys: nil)
                    where entry.lastPathComponent != "first" {
                    try await consume(oneEntry(entry))
                    _ = try await waitForUpload(entry.lastPathComponent)
                }
            })
        try engine.setDownloadTemporaryDirectory(stagingBase)
        #expect(throws: (any Error).self) {
            try engine.setDownloadTemporaryDirectory(URL(string: "https://example.invalid/downloads")!)
        }
        #expect(engine.downloadTemporaryDirectory == stagingBase)
        let stats = try await engine.syncIncremental(localPath: testFixture.local.path)
        #expect(stats.filesDownloaded == 1)
        #expect(stats.filesUploaded == 0)
        #expect(stats.filesFailed == 0)
        #expect(try Data(contentsOf: first) == Data(String(repeating: "R", count: 131072).utf8))
        #expect(context.value.state.withLock { !$0.files.values.contains { $0.name.hasPrefix(".tmp_") } })
        #expect(try FileManager.default.contentsOfDirectory(atPath: testFixture.local.path) == ["first"])
        #expect(!FileManager.default.fileExists(atPath: staging.path))
    }

    @Test(
        "A16 first upload completes while the scanner's tail is paused",
        .timeLimit(.minutes(1)),
        arguments: ["complete", "error", "cancel"])
    func uploadBeforeScanEnd(ending: String) async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let first = testFixture.local.appendingPathComponent("first")
        try Data("first body".utf8).write(to: first)
        let batch = try oneEntry(first)
        let tail = testFixture.local.appendingPathComponent("slow-tail")
        try FileManager.default.createDirectory(at: tail, withIntermediateDirectories: true)
        let tailBatch = try oneEntry(tail, type: .directory)
        // A known tail directory avoids a fake directory creation; it is deliberately not emitted yet.
        try await testFixture.store.write { conn in
            try conn.execute("INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id, local_status, remote_status, phase, created_at, updated_at) VALUES (\(testFixture.rootID), \(testFixture.rootItemID), 'slow-tail', 'directory', 'tail', 'present', 'present', 'committed', 1, 1);")
        }
        // The HTTP boundary checks that local observation AND create intent were committed first.
        context.value.state.withLock { state in
            state.verifyUpload = { name in
                let conn = try SQLiteConnection(path: testFixture.store.path, readonly: true)
                let queryStatement = try conn.prepare("SELECT i.local_sha256, op.expected_sha256 FROM items i JOIN operations op ON op.item_id = i.item_id WHERE i.name = ? AND op.state != 'completed';")
                queryStatement.bindText(name, at: 1)
                #expect(try queryStatement.step())
                #expect(queryStatement.columnText(at: 0) == SyncEngine.computeSha256(of: Data("first body".utf8)))
                #expect(queryStatement.columnText(at: 0) == queryStatement.columnText(at: 1))
            }
        }
        let engine = try await SyncEngine(auth: testFixture.auth, store: testFixture.store, client: testFixture.client,
            idPool: IDPool(initialIds: ["first-id"]), incrementalScan: { _, consume in
                try await consume(batch)
                // Do not supply the tail until the upload receipt is committed.
                try await waitForCommittedUpload("first", store: testFixture.store)
                if ending == "error" { throw POSIXError(.EIO) }
                if ending == "cancel" {
                    withUnsafeCurrentTask { $0?.cancel() }
                    try Task.checkCancellation()
                }
                try await consume(tailBatch)
            })
        let run = Task { try await engine.syncIncremental(localPath: testFixture.local.path) }
        if ending == "error" {
            await #expect(throws: POSIXError.self) { try await run.value }
        } else if ending == "cancel" {
            await #expect(throws: CancellationError.self) { try await run.value }
        } else {
            let result = try await run.value
            #expect(result.filesUploaded == 1)
            #expect(result.filesFailed == 0)
        }
        try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT phase, dirty_generation FROM items WHERE name = 'first';")
            #expect(try queryStatement.step())
            #expect(queryStatement.columnText(at: 0) == "committed")
            #expect(queryStatement.columnInt64(at: 1) == 0)
            let tail = try conn.prepare("SELECT local_status FROM items WHERE name = 'slow-tail';")
            #expect(try tail.step())
            #expect(tail.columnText(at: 0) == "present")
        }
        #expect(context.value.state.withLock { $0.requests.filter { $0.contains("/upload/") }.count } == 1)
    }

    @Test("A16 1000 observations and matching receipts commit in bounded batches")
    func naturalObservationCommits() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let data = Data("same body".utf8)
        let sha = SyncEngine.computeSha256(of: data)
        for itemIndex in 0..<1000 { try data.write(to: testFixture.local.appendingPathComponent("file-\(itemIndex)")) }
        try await testFixture.store.write { conn in
            let queryStatement = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    base_sha256, base_size, remote_sha256, remote_size, remote_status,
                    phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, ?, 'file', ?, ?, 9, ?, 9, 'present', 'ready', 1, 1, 1);
                """)
            for itemIndex in 0..<1000 {
                queryStatement.bindInt64(testFixture.rootID, at: 1)
                queryStatement.bindInt64(testFixture.rootItemID, at: 2)
                queryStatement.bindText("file-\(itemIndex)", at: 3)
                queryStatement.bindText("remote-\(itemIndex)", at: 4)
                queryStatement.bindText(sha, at: 5)
                queryStatement.bindText(sha, at: 6)
                _ = try queryStatement.step()
                queryStatement.reset()
            }
        }
        let before = await testFixture.store.getWriterStats()
        let result = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        let after = await testFixture.store.getWriterStats()
        #expect(result.filesScanned == 1000)
        #expect(result.filesUploaded == 0)
        #expect(result.filesFailed == 0)
        #expect(after.totalCommits - before.totalCommits <= 45)
        #expect(after.timeoutTriggeredCommits == before.timeoutTriggeredCommits)
        try await testFixture.store.read { conn in
            let queryStatement = try conn.prepare("SELECT COUNT(*) FROM items WHERE entry_kind = 'file' AND local_sha256 = base_sha256 AND dirty_generation = 0 AND local_generation = 1;")
            #expect(try queryStatement.step())
            #expect(queryStatement.columnInt64(at: 0) == 1000)
            let plan = try conn.prepare("EXPLAIN QUERY PLAN SELECT items.item_id FROM json_each('[1,2,3]') selected CROSS JOIN items ON items.item_id = selected.value WHERE items.root_id = 1 AND items.dirty_generation > 0;")
            var details: [String] = []
            while try plan.step() { details.append(plan.columnText(at: 3) ?? "") }
            #expect(details.contains { $0.contains("SEARCH items USING INTEGER PRIMARY KEY") })
        }
        print("A16 1000 observations: commits=\(after.totalCommits - before.totalCommits), elapsed=\(result.elapsedSeconds)")
    }

    @Test("A16 transfer scheduling applies backpressure before creating tasks")
    func boundedIncrementalUploads() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        for itemIndex in 0..<80 { try Data("body".utf8).write(to: testFixture.local.appendingPathComponent("file-\(itemIndex)")) }
        context.value.state.withLock { $0.uploadDelay = 0.02 }
        let result = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path, maxConcurrency: 2)
        #expect(result.filesUploaded == 80)
        #expect(result.filesFailed == 0)
        #expect(context.value.state.withLock { $0.peakUploads } == 2)
        #expect(context.value.state.withLock { $0.activeUploads } == 0)
    }

    @Test("Same-named incremental uploads have distinct monitor entries", .timeLimit(.minutes(1)))
    func sameNamedIncrementalUploadsHaveDistinctMonitorEntries() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let first = testFixture.local.appendingPathComponent("dir-a/shared.txt")
        let second = testFixture.local.appendingPathComponent("dir-b/shared.txt")
        try FileManager.default.createDirectory(
            at: first.deletingLastPathComponent(), withIntermediateDirectories: true)
        try FileManager.default.createDirectory(
            at: second.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("first".utf8).write(to: first)
        try Data("second".utf8).write(to: second)
        context.value.state.withLock { $0.uploadDelay = 0.2 }

        let sync = Task {
            try await testFixture.engine.syncIncremental(
                localPath: testFixture.local.path, maxConcurrency: 2)
        }
        try await context.value.uploadStarted.wait()
        try await context.value.uploadStarted.wait()
        testFixture.engine.monitor.refreshSnapshot()
        let snapshot = testFixture.engine.transferStatus

        #expect(snapshot.activeUploads.count == 2)
        #expect(Set(snapshot.activeUploads.map(\.fileId)) == Set([first.path, second.path]))
        #expect(snapshot.activeUploads.allSatisfy { $0.name == "shared.txt" })
        let result = try await sync.value
        #expect(result.filesUploaded == 2)
        #expect(result.filesFailed == 0)
    }

    @Test("A17 incremental large files use bounded resumable chunks")
    func incrementalLargeFileUsesResumableChunks() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let file = testFixture.local.appendingPathComponent("large.bin")
        let mebibyte = Data(repeating: 0x5a, count: 1024 * 1024)
        FileManager.default.createFile(atPath: file.path, contents: nil)
        let handle = try FileHandle(forWritingTo: file)
        for _ in 0..<8 { try handle.write(contentsOf: mebibyte) }
        try handle.write(contentsOf: Data([0x5a]))
        try handle.close()

        let result = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path, maxConcurrency: 1)
        #expect(result.filesUploaded == 1)
        #expect(result.filesFailed == 0)
        let requests = context.value.state.withLock { $0.requests }
        #expect(requests.contains { $0.contains("uploadType=resumable") })
        #expect(requests.filter { $0.hasPrefix("PUT /resumable/") }.count == 2)
        #expect(!requests.contains { $0.contains("uploadType=multipart") })
        try await testFixture.store.read { conn in
            let item = try conn.prepare("SELECT phase, dirty_generation, base_size FROM items WHERE name = 'large.bin';")
            #expect(try item.step())
            #expect(item.columnText(at: 0) == "committed")
            #expect(item.columnInt64(at: 1) == 0)
            #expect(item.columnInt64(at: 2) == Int64(8 * 1024 * 1024 + 1))
        }
    }

    @Test("A16 early child upload waits for a pending parent create to recover")
    func pendingParentBeforeChild() async throws {
        let testFixture = try await fixture()
        defer { testFixture.cleanup() }
        let directory = testFixture.local.appendingPathComponent("pending-parent")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        try Data("child body".utf8).write(to: directory.appendingPathComponent("child"))
        var version = stat()
        #expect(stat(directory.path, &version) == 0)
        _ = try await DurableCreateIntentStore.prepareDirectory(store: testFixture.store,
            rootID: testFixture.rootID, parentItemID: testFixture.rootItemID, name: "pending-parent",
            targetParentRemoteID: "root", candidateRemoteID: "pending-id",
            device: Int64(version.st_dev), inode: Int64(version.st_ino))
        let result = try await testFixture.engine.syncIncremental(localPath: testFixture.local.path)
        #expect(result.filesUploaded == 1)
        #expect(result.filesFailed == 0)
        let requests = context.value.state.withLock { $0.requests }
        let creation = try #require(requests.firstIndex { $0.hasPrefix("POST /drive/v3/files?") })
        let upload = try #require(requests.firstIndex { $0.contains("/upload/") })
        #expect(creation < upload)
        #expect(context.value.state.withLock { $0.files.values.first { $0.name == "child" }?.parents } == ["pending-id"])
    }

}
