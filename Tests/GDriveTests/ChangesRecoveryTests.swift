import Foundation
import Testing
import os
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
    var requests: [String] = []
}
private final class ChangesProtocol: URLProtocol, @unchecked Sendable {
    static let state = OSAllocatedUnfairLock(initialState: ChangesServer())
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        do {
            let url = request.url!
            let response: (Int, Data) = try Self.state.withLock { state in
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
                    let body = String(decoding: request.extractBodyData ?? Data(), as: UTF8.self)
                    let start = try #require(body.firstIndex(of: "{"))
                    let end = try #require(body[start...].firstIndex(of: "}"))
                    let metadata = try #require(JSONSerialization.jsonObject(with: Data(body[start...end].utf8)) as? [String: Any])
                    let id = try #require(metadata["id"] as? String)
                    let name = try #require(metadata["name"] as? String)
                    let parent = (metadata["parents"] as? [String])?.first ?? "root"
                    let a = try #require(body.range(of: "Content-Type: application/octet-stream\r\n\r\n")?.upperBound)
                    let b = try #require(body.range(of: "\r\n--", range: a..<body.endIndex)?.lowerBound)
                    let bytes = Data(body[a..<b].utf8)
                    let file = DriveFile(id: id, name: name, parents: [parent], size: String(bytes.count), sha256Checksum: SyncEngine.computeSha256(of: bytes), version: "1")
                    state.files[id] = file
                    state.contents[id] = bytes
                    return (200, try encoder.encode(file))
                }
                if url.path.hasSuffix("/files") {
                    let parent = (value("q") ?? "").split(separator: "'").first.map(String.init) ?? ""
                    if let rejected = state.rejectListingToken, value("pageToken") == rejected { return (400, Data("invalid pageToken".utf8)) }
                    if state.failFolder == parent { return (400, Data("listing failure".utf8)) }
                    if let page = state.listingPages["\(parent):\(value("pageToken") ?? "first")"] { return (200, page) }
                    let files = state.files.values.filter { $0.parents?.first == parent && $0.trashed != true }
                    let fileJSON = try files.map { try JSONSerialization.jsonObject(with: encoder.encode($0)) }
                    return (200, try JSONSerialization.data(withJSONObject: ["files": fileJSON, "incompleteSearch": state.incompleteFolder == parent]))
                }
                if value("alt") == "media", let bytes = state.contents[url.lastPathComponent] { return (200, bytes) }
                if let file = state.files[url.lastPathComponent] { return (200, try encoder.encode(file)) }
                return (404, Data())
            }
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: url, statusCode: response.0, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: response.1)
            client?.urlProtocolDidFinishLoading(self)
        } catch { client?.urlProtocol(self, didFailWithError: error) }
    }
    override func stopLoading() {}
}

@Suite("Changes durability and scoped reconstruction (A13)", .serialized)
struct ChangesRecoveryTests {
    private struct Fixture {
        let directory: URL
        let local: URL
        let store: StateStore
        let client: DriveClient
        let auth: Auth
        let engine: SyncEngine
        let rootID: Int64
        let rootItemID: Int64
        var changes: RemoteChanges { RemoteChanges(store: store, client: client, rootID: rootID, remoteRootID: "root", rootURL: local) }
    }
    private func folder(_ id: String, _ parent: String?, name: String? = nil) -> DriveFile {
        DriveFile(id: id, name: name ?? id, mimeType: "application/vnd.google-apps.folder", parents: parent.map { [$0] }, version: "1")
    }
    @discardableResult
    private func remoteFile(_ id: String, parent: String, content: String = "remote content", name: String? = nil) -> DriveFile {
        let bytes = Data(content.utf8)
        let file = DriveFile(id: id, name: name ?? id, parents: [parent], size: String(bytes.count), sha256Checksum: SyncEngine.computeSha256(of: bytes), version: "1")
        ChangesProtocol.state.withLock { $0.files[id] = file; $0.contents[id] = bytes }
        return file
    }
    private func fixture(cursor: Bool = true) async throws -> Fixture {
        ChangesProtocol.state.withLock { $0 = ChangesServer(files: ["root": folder("root", nil)]) }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("a13-\(UUID().uuidString)")
        let local = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(clientId: "test", accessToken: "test", expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [ChangesProtocol.self]
        let client = DriveClient(auth: auth, session: URLSession(configuration: config))
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let ids = try await store.write { conn -> (Int64, Int64) in
            let r = try conn.prepare("INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at) VALUES ('default', ?, 1, 1, 'root', 'localToRemoteEmpty', 'existingKnown', 1, 1);")
            r.bindText(local.path, at: 1)
            _ = try r.step()
            let root = conn.lastInsertRowId
            try conn.execute("INSERT INTO items(root_id, name, entry_kind, remote_file_id, local_status, remote_status, phase, created_at, updated_at) VALUES (\(root), 'local', 'directory', 'root', 'present', 'present', 'committed', 1, 1);")
            let item = conn.lastInsertRowId
            if cursor { try conn.execute("INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) VALUES (\(root), 'default', 'drive_changes', 'start', 1);") }
            return (root, item)
        }
        let engine = try await SyncEngine(auth: auth, store: store, client: client, idPool: IDPool(initialIds: (0..<1000).map { "new-\($0)" }))
        return Fixture(directory: directory, local: local, store: store, client: client, auth: auth, engine: engine, rootID: ids.0, rootItemID: ids.1)
    }
    private func token(_ f: Fixture) async throws -> String? {
        try await f.store.read { conn in
            let q = try conn.prepare("SELECT token_value FROM cursors WHERE root_id = \(f.rootID);")
            return try q.step() ? q.columnText(at: 0) : nil
        }
    }
    private func converge(_ f: Fixture, limit: Int = 12) async throws {
        for _ in 0..<limit {
            let stats = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
            #expect(stats.filesFailed == 0)
            if stats.remoteWorkPending == 0 { return }
        }
        Issue.record("Remote work did not converge within the bounded fixture")
    }

    @Test("Child before parent across pages survives a page failure and database reopen")
    func childBeforeParent() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        try Data("ready upload".utf8).write(to: f.local.appendingPathComponent("ready.txt"))
        let parent = folder("dir", "root")
        let child = remoteFile("child", parent: "dir")
        ChangesProtocol.state.withLock {
            $0.files[parent.id] = parent
            $0.pages["start"] = DriveChangesPage(nextPageToken: "page2", newStartPageToken: nil, changes: [DriveChange(fileId: child.id, removed: false, file: child)])
            $0.pages["page2"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: parent.id, removed: false, file: parent)])
            $0.failToken = "page2"
        }
        do { _ = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root"); Issue.record("Expected page failure") } catch {}
        #expect(try await token(f) == "page2")
        #expect(!ChangesProtocol.state.withLock { $0.requests.contains { $0.contains("/upload/") } })
        let reopened = try await StateStore(path: f.store.path)
        let pending = try await reopened.read { conn in
            let q = try conn.prepare("SELECT count(*) FROM remote_change_inbox;"); _ = try q.step(); return q.columnInt64(at: 0)
        }
        #expect(pending == 1)
        ChangesProtocol.state.withLock { $0.failToken = nil }
        let engine = try await SyncEngine(auth: f.auth, store: reopened, client: f.client)
        _ = try await engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        try await converge(f)
        #expect(try String(contentsOf: f.local.appendingPathComponent("dir/child"), encoding: .utf8) == "remote content")
    }

    @Test("Moved-in directory enumerates an existing deep subtree; ready uploads precede listing")
    func movedSubtree() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let parent = folder("incoming", "root")
        let nested = folder("nested", "incoming")
        ChangesProtocol.state.withLock {
            $0.files[parent.id] = parent; $0.files[nested.id] = nested
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: parent.id, removed: false, file: parent)])
        }
        remoteFile("deep", parent: "nested")
        try Data("local new".utf8).write(to: f.local.appendingPathComponent("upload.txt"))
        let first = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(first.filesUploaded == 1)
        #expect(first.remoteWorkPending > 0)
        let requests = ChangesProtocol.state.withLock { $0.requests }
        let upload = try #require(requests.firstIndex { $0.contains("/upload/") })
        let listing = try #require(requests.firstIndex { $0.contains("/files?q=") })
        #expect(upload < listing)
        try await converge(f)
        #expect(try String(contentsOf: f.local.appendingPathComponent("incoming/nested/deep"), encoding: .utf8) == "remote content")
    }

    @Test("Missing and explicitly rejected cursors reconstruct pre-existing remote files", arguments: ["missing", "rejected", "invalid"])
    func missingCursor(kind: String) async throws {
        let f = try await fixture(cursor: kind != "missing")
        defer { try? FileManager.default.removeItem(at: f.directory) }
        remoteFile("before-token", parent: "root")
        if kind == "rejected" { ChangesProtocol.state.withLock { $0.rejectToken = "start" } }
        if kind == "invalid" { try await f.store.write { try $0.execute("UPDATE cursors SET is_valid = 0;") } }
        try await converge(f)
        #expect(try String(contentsOf: f.local.appendingPathComponent("before-token"), encoding: .utf8) == "remote content")
        #expect(try await token(f) == "steady")
    }

    @Test("An incomplete or failed directory listing retains its job", arguments: [false, true])
    func listingFailure(incomplete: Bool) async throws {
        let f = try await fixture(cursor: false)
        defer { try? FileManager.default.removeItem(at: f.directory) }
        remoteFile("retained", parent: "root")
        ChangesProtocol.state.withLock {
            if incomplete { $0.incompleteFolder = "root" } else { $0.failFolder = "root" }
        }
        do { _ = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root"); Issue.record("Expected listing failure") } catch {}
        #expect(try await f.changes.pendingCount() > 0)
        ChangesProtocol.state.withLock { $0.failFolder = nil; $0.incompleteFolder = nil }
        try await converge(f)
        #expect(FileManager.default.fileExists(atPath: f.local.appendingPathComponent("retained").path))
    }

    @Test("Missing ancestor metadata stays durable after the final Changes token")
    func unknownParent() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let child = remoteFile("child", parent: "unavailable")
        ChangesProtocol.state.withLock {
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: child.id, removed: false, file: child)])
        }
        let first = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(first.remoteWorkPending > 0)
        #expect(try await token(f) == "steady")
        ChangesProtocol.state.withLock { $0.files["unavailable"] = folder("unavailable", "root") }
        try await converge(f)
        #expect(FileManager.default.fileExists(atPath: f.local.appendingPathComponent("unavailable/child").path))
    }

    @Test("Known file moved outside preserves local bytes and cannot be written back")
    func movedOutside() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let file = remoteFile("known", parent: "root")
        ChangesProtocol.state.withLock { $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: file.id, removed: false, file: file)]) }
        try await converge(f)
        let outside = folder("outside", nil)
        let moved = DriveFile(id: file.id, name: file.name, parents: [outside.id], size: file.size, sha256Checksum: file.sha256Checksum, version: "2")
        ChangesProtocol.state.withLock {
            $0.files[outside.id] = outside; $0.files[file.id] = moved
            $0.pages["steady"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "after-move", changes: [DriveChange(fileId: moved.id, removed: false, file: moved)])
        }
        let local = f.local.appendingPathComponent("known")
        try Data("new local content".utf8).write(to: local)
        let stats = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(stats.filesUploaded == 0)
        #expect(stats.filesDeleted == 0)
        #expect(try String(contentsOf: local, encoding: .utf8) == "new local content")
        let status = try await f.store.read { conn in
            let q = try conn.prepare("SELECT remote_status FROM items WHERE remote_file_id = 'known';"); _ = try q.step(); return q.columnText(at: 0)
        }
        #expect(status == "unknown")
    }

    @Test("Bootstrap token failures propagate; retries preserve an existing cursor")
    func bootstrapCursor() async throws {
        let f = try await fixture(cursor: false)
        defer { try? FileManager.default.removeItem(at: f.directory) }
        do {
            try await RemoteChanges.saveInitialCursor(store: f.store, client: f.client, rootID: f.rootID, requireExisting: true)
            Issue.record("Existing roots must reconstruct missing cursor history")
        } catch {}
        #expect(try await token(f) == nil)
        ChangesProtocol.state.withLock { $0.failStart = true }
        do { try await RemoteChanges.saveInitialCursor(store: f.store, client: f.client, rootID: f.rootID); Issue.record("Expected token error") } catch {}
        #expect(try await token(f) == nil)
        ChangesProtocol.state.withLock { $0.failStart = false }
        try await RemoteChanges.saveInitialCursor(store: f.store, client: f.client, rootID: f.rootID)
        try await f.store.write { try $0.execute("UPDATE cursors SET token_value = 'older-boundary';") }
        try await RemoteChanges.saveInitialCursor(store: f.store, client: f.client, rootID: f.rootID)
        #expect(try await token(f) == "older-boundary")
    }
    @Test("Cursor commit failure rolls back the page observations")
    func atomicPage() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let file = remoteFile("atomic", parent: "root")
        ChangesProtocol.state.withLock {
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: file.id, removed: false, file: file)])
        }
        try await f.store.write { conn in
            try conn.execute("CREATE TRIGGER fail_cursor BEFORE UPDATE ON cursors BEGIN SELECT RAISE(ABORT, 'injected commit error'); END;")
        }
        do { try await f.changes.consume(); Issue.record("Expected transaction failure") } catch {}
        #expect(try await token(f) == "start")
        let rows = try await f.store.read { conn in
            let q = try conn.prepare("SELECT count(*) FROM remote_change_inbox;"); _ = try q.step(); return q.columnInt64(at: 0)
        }
        #expect(rows == 0)
        try await f.store.write { try $0.execute("DROP TRIGGER fail_cursor;") }
        try await converge(f)
        #expect(FileManager.default.fileExists(atPath: f.local.appendingPathComponent("atomic").path))
    }

    @Test("Listing page checkpoint survives failure without losing earlier children", arguments: [false, true])
    func directoryPages(rejected: Bool) async throws {
        let f = try await fixture(cursor: false)
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let first = remoteFile("first", parent: "root")
        let second = remoteFile("second", parent: "root")
        let encoder = JSONEncoder()
        let firstPage = try JSONSerialization.data(withJSONObject: ["nextPageToken": "tail", "files": [JSONSerialization.jsonObject(with: encoder.encode(first))]])
        let secondPage = try JSONSerialization.data(withJSONObject: ["files": [JSONSerialization.jsonObject(with: encoder.encode(second))]])
        ChangesProtocol.state.withLock {
            $0.listingPages["root:first"] = firstPage
            $0.listingPages["root:tail"] = secondPage
        }
        let round = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(round.remoteWorkPending > 0)
        ChangesProtocol.state.withLock {
            if rejected { $0.rejectListingToken = "tail" } else { $0.failFolder = "root" }
        }
        do { _ = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root"); Issue.record("Expected tail-page failure") } catch {}
        let checkpoint = try await f.store.read { conn in
            let q = try conn.prepare("SELECT page_token FROM remote_directory_scans WHERE remote_id = 'root';"); _ = try q.step(); return q.columnText(at: 0)
        }
        #expect(checkpoint == (rejected ? nil : "tail"))
        ChangesProtocol.state.withLock { $0.failFolder = nil; $0.rejectListingToken = nil }
        try await converge(f)
        #expect(FileManager.default.fileExists(atPath: f.local.appendingPathComponent("first").path))
        #expect(FileManager.default.fileExists(atPath: f.local.appendingPathComponent("second").path))
    }

    @Test("A thousand known-parent observations use page-sized commits")
    func pageBatching() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let changes = (0..<1000).map { index -> DriveChange in
            let file = DriveFile(id: "id-\(index)", name: "file-\(index)", parents: ["root"], size: "1", sha256Checksum: String(repeating: "a", count: 64))
            return DriveChange(fileId: file.id, removed: false, file: file)
        }
        ChangesProtocol.state.withLock { $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: changes) }
        let before = await f.store.getWriterStats()
        try await f.changes.consume()
        let after = await f.store.getWriterStats()
        #expect(after.totalCommits - before.totalCommits <= 3)
        let count = try await f.store.read { conn in
            let q = try conn.prepare("SELECT count(*) FROM items WHERE entry_kind = 'file' AND remote_status = 'present';"); _ = try q.step(); return q.columnInt64(at: 0)
        }
        #expect(count == 1000)
        #expect(try await f.changes.pendingCount() == 0)
    }

    @Test("An empty incoming directory is not mistaken for a local deletion while listing")
    func emptyIncomingDirectory() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let dir = folder("empty", "root")
        ChangesProtocol.state.withLock {
            $0.files[dir.id] = dir
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: dir.id, removed: false, file: dir)])
        }
        try await converge(f)
        _ = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(FileManager.default.fileExists(atPath: f.local.appendingPathComponent("empty").path))
        #expect(!ChangesProtocol.state.withLock { $0.requests.contains { $0.hasPrefix("PATCH") } })
    }

    @Test("Moved-out directories also block new children after a local rename")
    func movedOutsideDirectory() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let dir = folder("dir", "root")
        ChangesProtocol.state.withLock {
            $0.files[dir.id] = dir
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: dir.id, removed: false, file: dir)])
        }
        remoteFile("child", parent: "dir")
        try await converge(f)
        let moved = folder("dir", "outside")
        ChangesProtocol.state.withLock {
            $0.files["outside"] = folder("outside", nil)
            $0.files["dir"] = moved
            $0.pages["steady"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "after-move", changes: [DriveChange(fileId: moved.id, removed: false, file: moved)])
        }
        _ = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        try FileManager.default.moveItem(at: f.local.appendingPathComponent("dir"), to: f.local.appendingPathComponent("renamed"))
        try Data("local new".utf8).write(to: f.local.appendingPathComponent("renamed/new.txt"))
        let stats = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(stats.filesFailed == 0)
        #expect(stats.filesUploaded == 0)
        #expect(stats.directoriesCreated == 0)
        #expect(stats.filesDeleted == 0)
        #expect(FileManager.default.fileExists(atPath: f.local.appendingPathComponent("renamed/child").path))
    }

    @Test("Rebuilding a lost cursor re-probes old queued payloads")
    func staleQueuedObservation() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let old = remoteFile("stale", parent: "root")
        let payload = String(decoding: try JSONEncoder().encode(DriveChange(fileId: old.id, removed: false, file: old)), as: UTF8.self)
        try await f.store.write { conn in
            try conn.execute("DELETE FROM cursors;")
            let q = try conn.prepare("INSERT INTO remote_change_inbox(root_id, remote_id, payload) VALUES (?, 'stale', ?);")
            q.bindInt64(f.rootID, at: 1); q.bindText(payload, at: 2); _ = try q.step()
        }
        ChangesProtocol.state.withLock { _ = $0.files.removeValue(forKey: "stale") }
        let stats = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(stats.remoteWorkPending > 0)
        #expect(stats.filesDownloaded == 0)
        #expect(!FileManager.default.fileExists(atPath: f.local.appendingPathComponent("stale").path))
    }

    @Test("Empty root reconstruction still reports the deferred local upload round")
    func emptyRebuildWithLocalFiles() async throws {
        let f = try await fixture(cursor: false)
        defer { try? FileManager.default.removeItem(at: f.directory) }
        try Data("local new".utf8).write(to: f.local.appendingPathComponent("local.txt"))
        let first = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(first.filesUploaded == 0)
        #expect(first.remoteWorkPending > 0)
        let second = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(second.filesUploaded == 1)
        #expect(second.remoteWorkPending == 0)
    }

    @Test("A14 bootstrap blocks equivalent sibling names before any publication", arguments: [
        ["same", "same"], ["a", "A"], ["é", "e\u{301}"]
    ])
    func bootstrapNameCollision(names: [String]) async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        _ = remoteFile("one", parent: "root", content: "first", name: names[0])
        _ = remoteFile("two", parent: "root", content: "second", name: names[1])
        await #expect(throws: (any Error).self) {
            _ = try await f.engine.syncRemoteToLocalEmpty(localPath: f.local.path, remoteRootId: "root")
        }
        #expect(try FileManager.default.contentsOfDirectory(atPath: f.local.path).isEmpty)
        #expect(ChangesProtocol.state.withLock { !$0.requests.contains { $0.contains("alt=media") } })
        #expect(try await f.changes.pendingCount() > 0)
    }

    @Test("A14 directory identities cannot collapse on bootstrap", arguments: [false, true])
    func bootstrapDirectoryCollision(mixed: Bool) async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        ChangesProtocol.state.withLock {
            $0.files["one"] = folder("one", "root", name: "same")
            $0.files["two"] = mixed ? DriveFile(id: "two", name: "same", parents: ["root"]) : folder("two", "root", name: "same")
        }
        await #expect(throws: (any Error).self) {
            _ = try await f.engine.syncRemoteToLocalEmpty(localPath: f.local.path, remoteRootId: "root")
        }
        #expect(try FileManager.default.contentsOfDirectory(atPath: f.local.path).isEmpty)
        let count = try await f.store.read { conn in
            let q = try conn.prepare("SELECT COUNT(*) FROM items WHERE parent_id IS NOT NULL;")
            _ = try q.step(); return q.columnInt64(at: 0)
        }
        #expect(count == 0)
    }

    @Test("A14 Changes retain colliding identity and gate both sides until rename", arguments: [
        ["same", "same"], ["a", "A"], ["é", "e\u{301}"]
    ])
    func changesNameCollision(names: [String]) async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let one = remoteFile("one", parent: "root", content: "first", name: names[0])
        let two = remoteFile("two", parent: "root", content: "second", name: names[1])
        ChangesProtocol.state.withLock {
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [
                DriveChange(fileId: one.id, removed: false, file: one),
                DriveChange(fileId: two.id, removed: false, file: two)])
        }
        let blocked = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(blocked.remoteWorkPending > 0)
        #expect(blocked.remoteNameConflicts == 1)
        #expect(blocked.filesDownloaded == 0)
        #expect(try FileManager.default.contentsOfDirectory(atPath: f.local.path).isEmpty)
        let renamed = remoteFile("two", parent: "root", content: "second", name: "unique")
        ChangesProtocol.state.withLock {
            $0.pages["steady"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "done", changes: [DriveChange(fileId: renamed.id, removed: false, file: renamed)])
        }
        try await converge(f)
        #expect(try String(contentsOf: f.local.appendingPathComponent(names[0]), encoding: .utf8) == "first")
        #expect(try String(contentsOf: f.local.appendingPathComponent("unique"), encoding: .utf8) == "second")
    }

    @Test("A14 unsafe names cannot escape the bootstrap root", arguments: ["../escape", "/absolute", ".", "..", "nul\0name"])
    func unsafeBootstrapName(name: String) async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        _ = remoteFile("bad", parent: "root", name: name)
        await #expect(throws: (any Error).self) {
            _ = try await f.engine.syncRemoteToLocalEmpty(localPath: f.local.path, remoteRootId: "root")
        }
        #expect(try FileManager.default.contentsOfDirectory(atPath: f.local.path).isEmpty)
        #expect(!FileManager.default.fileExists(atPath: f.directory.appendingPathComponent("escape").path))
        #expect(ChangesProtocol.state.withLock { !$0.requests.contains { $0.contains("alt=media") } })
    }

    @Test("A14 Changes block symlinked parents without writing outside root")
    func symlinkedRemoteParent() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let parent = folder("parent", "root")
        ChangesProtocol.state.withLock {
            $0.files[parent.id] = parent
            $0.pages["start"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: parent.id, removed: false, file: parent)])
        }
        try await converge(f)
        let outside = f.directory.appendingPathComponent("outside")
        try FileManager.default.createDirectory(at: outside, withIntermediateDirectories: true)
        let localParent = f.local.appendingPathComponent("parent")
        try FileManager.default.removeItem(at: localParent)
        try FileManager.default.createSymbolicLink(at: localParent, withDestinationURL: outside)
        #expect(throws: (any Error).self) {
            try RemoteNameMapping.validateDestination(localParent.appendingPathComponent("missing/child"), root: f.local)
        }
        let child = remoteFile("child", parent: "parent")
        ChangesProtocol.state.withLock {
            $0.pages["steady"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "done", changes: [DriveChange(fileId: child.id, removed: false, file: child)])
        }
        try await f.changes.consume()
        #expect(try await f.changes.pendingCount() > 0)
        #expect(try FileManager.default.contentsOfDirectory(atPath: outside.path).isEmpty)
    }

    @Test("A14 name lookup uses index and equivalent names retain distinct item identities")
    func nameIndex() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        try await f.store.write { conn in
            for name in ["a", "A"] {
                let q = try conn.prepare("INSERT INTO items(root_id,parent_id,name,entry_kind,remote_file_id,created_at,updated_at) VALUES (?,? ,?,'file',?,1,1);")
                q.bindInt64(f.rootID, at: 1); q.bindInt64(f.rootItemID, at: 2)
                q.bindText(name, at: 3); q.bindText(name, at: 4)
                _ = try q.step()
            }
            let plan = try conn.prepare("EXPLAIN QUERY PLAN SELECT item_id FROM items INDEXED BY idx_items_local_name_key WHERE root_id = 1 AND parent_id = 1 AND gdrive_name_key(name) = gdrive_name_key('a') AND is_tombstone = 0;")
            #expect(try plan.step())
            #expect(plan.columnText(at: 3)?.contains("idx_items_local_name_key") == true)
            let q = try conn.prepare("SELECT COUNT(*) FROM items WHERE gdrive_name_key(name) = gdrive_name_key('A');")
            #expect(try q.step())
            #expect(q.columnInt64(at: 0) == 2)
        }
    }
}
