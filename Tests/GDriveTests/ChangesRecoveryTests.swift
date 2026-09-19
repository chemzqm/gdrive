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
    var requests: [String] = []
    var verifyUpload: (@Sendable (String) throws -> Void)?
    var uploadDelay: TimeInterval = 0
    var activeUploads = 0
    var peakUploads = 0
}
private final class ChangesProtocol: URLProtocol, @unchecked Sendable {
    static let heldDownload = OSAllocatedUnfairLock<(@Sendable () -> Void)?>(initialState: nil)
    static let holdDownloads = OSAllocatedUnfairLock(initialState: false)
    static let state = OSAllocatedUnfairLock(initialState: ChangesServer())
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        let delay = Self.state.withLock { state -> TimeInterval in
            guard request.url!.path.contains("/upload/"), state.uploadDelay > 0 else { return 0 }
            state.activeUploads += 1
            state.peakUploads = max(state.peakUploads, state.activeUploads)
            return state.uploadDelay
        }
        if delay > 0 {
            DispatchQueue.global().asyncAfter(deadline: .now() + delay) {
                Self.state.withLock { $0.activeUploads -= 1 }
                self.respond()
            }
        } else { respond() }
    }
    private func respond() {
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
                    try state.verifyUpload?(name)
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
                if value("alt") == "media", let bytes = state.contents[url.lastPathComponent] { return (200, bytes) }
                if let file = state.files[url.lastPathComponent] { return (200, try encoder.encode(file)) }
                return (404, Data())
            }
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: url, statusCode: response.0, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: response.1)
            if request.url!.query?.contains("alt=media") == true, Self.holdDownloads.withLock({ $0 }) {
                Self.heldDownload.withLock { $0 = { self.client?.urlProtocolDidFinishLoading(self) } }
                return
            }
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

    @Test("A known remote directory event preserves a local deletion for reconciliation")
    func knownDirectoryEventPreservesLocalDeletion() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let localDirectory = f.local.appendingPathComponent("deleted")
        try FileManager.default.createDirectory(at: localDirectory, withIntermediateDirectories: true)
        let attrs = try FileManager.default.attributesOfItem(atPath: localDirectory.path)
        let device = (attrs[.systemNumber] as? NSNumber)?.int64Value ?? 0
        let inode = (attrs[.systemFileNumber] as? NSNumber)?.int64Value ?? 0
        let remoteDirectory = folder("known-directory", "root", name: "deleted")
        try await f.store.write { conn in
            let q = try conn.prepare("""
                INSERT INTO items(
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status,
                    phase, created_at, updated_at
                ) VALUES (?, ?, 'deleted', 'directory', 'known-directory', ?, ?,
                    'present', 'present', 'committed', 1, 1);
                """)
            q.bindInt64(f.rootID, at: 1)
            q.bindInt64(f.rootItemID, at: 2)
            q.bindInt64(device, at: 3)
            q.bindInt64(inode, at: 4)
            _ = try q.step()
        }
        try FileManager.default.removeItem(at: localDirectory)
        ChangesProtocol.state.withLock {
            $0.files[remoteDirectory.id] = remoteDirectory
            $0.pages["start"] = DriveChangesPage(
                nextPageToken: nil,
                newStartPageToken: "steady",
                changes: [DriveChange(fileId: remoteDirectory.id, removed: false, file: remoteDirectory)]
            )
        }

        let stats = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")

        #expect(stats.filesDeleted == 1)
        #expect(!FileManager.default.fileExists(atPath: localDirectory.path))
        let tombstone = try await f.store.read { conn in
            let q = try conn.prepare("SELECT is_tombstone, local_status FROM items WHERE remote_file_id = 'known-directory';")
            guard try q.step() else { return false }
            return q.columnInt64(at: 0) == 1 && q.columnText(at: 1) == "absent"
        }
        #expect(tombstone)
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
    @Test("A15 empty binding is complete and accepts additions on either side", arguments: [false, true])
    func emptyBinding(remoteAddition: Bool) async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        try await f.store.write { try $0.execute("DELETE FROM roots;") }
        let before = await f.store.getWriterStats()
        _ = try await f.engine.sync(localPath: f.local.path, remoteFolderId: "root")
        let after = await f.store.getWriterStats()
        #expect(after.totalCommits - before.totalCommits == 1)
        try await f.store.read { conn in
            let q = try conn.prepare("SELECT r.bootstrap_state, i.phase, c.token_value FROM roots r JOIN items i USING(root_id) JOIN cursors c USING(root_id) WHERE i.parent_id IS NULL;")
            #expect(try q.step())
            #expect(q.columnText(at: 0) == "existingKnown")
            #expect(q.columnText(at: 1) == "committed")
            #expect(q.columnText(at: 2) == "fresh")
        }
        let requests = ChangesProtocol.state.withLock { $0.requests }
        #expect(requests.count == 3)
        #expect(requests[1].contains("startPageToken"))
        #expect(requests[2].contains("/files?"))
        if remoteAddition {
            let file = remoteFile("new", parent: "root", content: "new content", name: ".env")
            ChangesProtocol.state.withLock {
                $0.pages["fresh"] = DriveChangesPage(nextPageToken: nil, newStartPageToken: "steady", changes: [DriveChange(fileId: file.id, removed: false, file: file)])
            }
        } else {
            try Data("new content".utf8).write(to: f.local.appendingPathComponent(".env"))
        }
        let result = try await f.engine.sync(localPath: f.local.path, remoteFolderId: "root")
        #expect(result.filesFailed == 0)
        #expect(remoteAddition ? result.filesDownloaded == 1 : result.filesUploaded == 1)
        #expect(try String(contentsOf: f.local.appendingPathComponent(".env"), encoding: .utf8) == "new content")
    }

    @Test("A15 hidden file initializes upload")
    func hiddenInitialUpload() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        try await f.store.write { try $0.execute("DELETE FROM roots;") }
        try Data("secret".utf8).write(to: f.local.appendingPathComponent(".env"))
        let result = try await f.engine.sync(localPath: f.local.path, remoteFolderId: "root")
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
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        try await f.store.write { try $0.execute("DELETE FROM roots;") }
        try FileManager.default.setAttributes([.posixPermissions: 0], ofItemAtPath: f.local.path)
        defer { try? FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: f.local.path) }
        await #expect(throws: POSIXError.self) {
            try await f.engine.sync(localPath: f.local.path, remoteFolderId: "root")
        }
        try await f.store.read { conn in
            let q = try conn.prepare("SELECT COUNT(*) FROM roots;")
            #expect(try q.step())
            #expect(q.columnInt64(at: 0) == 0)
        }
    }

    @Test("A15 cursor insert failure rolls back root and item")
    func emptyBindingRollback() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        try await f.store.write {
            try $0.execute("DELETE FROM roots;")
            try $0.execute("CREATE TRIGGER reject_cursor BEFORE INSERT ON cursors BEGIN SELECT RAISE(ABORT, 'injected'); END;")
        }
        await #expect(throws: (any Error).self) {
            try await f.engine.sync(localPath: f.local.path, remoteFolderId: "root")
        }
        try await f.store.read { conn in
            let q = try conn.prepare("SELECT (SELECT COUNT(*) FROM roots) + (SELECT COUNT(*) FROM items) + (SELECT COUNT(*) FROM cursors);")
            #expect(try q.step())
            #expect(q.columnInt64(at: 0) == 0)
        }
    }

    @Test("A15 absent local directory is created for an empty binding")
    func absentLocalEmptyBinding() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        try await f.store.write { try $0.execute("DELETE FROM roots;") }
        try FileManager.default.removeItem(at: f.local)
        _ = try await f.engine.sync(localPath: f.local.path, remoteFolderId: "root")
        _ = try await f.engine.sync(localPath: f.local.path, remoteFolderId: "root")
        #expect(FileManager.default.fileExists(atPath: f.local.path))
    }

    @Test("A15 download bootstrap reuses the routing cursor")
    func downloadReusesCursor() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let staging = f.directory.appendingPathComponent("bootstrap-downloads")
        try f.engine.setDownloadTemporaryDirectory(staging)
        try await f.store.write { try $0.execute("DELETE FROM roots;") }
        remoteFile("new", parent: "root")
        let result = try await f.engine.sync(localPath: f.local.path, remoteFolderId: "root")
        #expect(result.filesDownloaded == 1)
        #expect(result.filesFailed == 0)
        #expect(try FileManager.default.contentsOfDirectory(atPath: staging.appendingPathComponent("root").path).isEmpty)
        #expect(ChangesProtocol.state.withLock { $0.requests.filter { $0.contains("startPageToken") }.count } == 1)
    }

    private func oneEntry(_ url: URL, type: EntryType = .file) throws -> ScanBatch {
        var info = stat()
        guard stat(url.path, &info) == 0 else { throw POSIXError(.EIO) }
        let bytes = Array(url.path.utf8)
        return ScanBatch(pathData: bytes + [0], records: [.init(offset: 0, length: UInt32(bytes.count), type: type,
            metadata: FileMetadata(identity: FileIdentity(device: info.st_dev, inode: info.st_ino),
                modificationTime: FileTimestamp(seconds: Int64(info.st_mtimespec.tv_sec), nanoseconds: Int32(info.st_mtimespec.tv_nsec)),
                changeTime: FileTimestamp(seconds: Int64(info.st_ctimespec.tv_sec), nanoseconds: Int32(info.st_ctimespec.tv_nsec)),
                fileSize: Int64(info.st_size)))])
    }

    private func waitForUpload(_ name: String) async throws -> Bool {
        for _ in 0..<200 {
            if ChangesProtocol.state.withLock({ $0.files.values.contains { $0.name == name } }) { return true }
            try await Task.sleep(for: .milliseconds(10))
        }
        return false
    }

    @Test("Streaming downloads stay outside the active scan and use the configured remote-root folder")
    func downloadOutsideScan() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let first = f.local.appendingPathComponent("first")
        let old = Data("old".utf8)
        try old.write(to: first)
        let remote = remoteFile("remote-first", parent: "root", content: String(repeating: "R", count: 131072), name: "first")
        let oldSHA = SyncEngine.computeSha256(of: old)
        let remoteSHA = try #require(remote.sha256Checksum)
        try await f.store.write { conn in
            try conn.execute("INSERT INTO items(root_id,parent_id,name,entry_kind,remote_file_id,base_sha256,base_size,remote_sha256,remote_size,remote_status,phase,dirty_generation,created_at,updated_at) VALUES (\(f.rootID),\(f.rootItemID),'first','file','remote-first','\(oldSHA)',3,'\(remoteSHA)',131072,'present','ready',1,1,1);")
        }
        let batch = try oneEntry(first)
        let stagingBase = f.directory.appendingPathComponent("downloads")
        let staging = stagingBase.appendingPathComponent("root")
        ChangesProtocol.holdDownloads.withLock { $0 = true }
        defer { ChangesProtocol.holdDownloads.withLock { $0 = false } }
        let engine = try await SyncEngine(auth: f.auth, store: f.store, client: f.client,
            idPool: IDPool(initialIds: ["temp-upload-id"]), incrementalScan: { _, consume in
                defer { ChangesProtocol.heldDownload.withLock { callback in callback?(); callback = nil } }
                try await consume(batch)
                var temporary: URL?
                for _ in 0..<500 {
                    // Check both places so this reproduces the original in-tree download defect.
                    let entries = (try? FileManager.default.contentsOfDirectory(at: staging, includingPropertiesForKeys: nil)) ?? []
                    let localEntries = try FileManager.default.contentsOfDirectory(at: f.local, includingPropertiesForKeys: nil)
                    temporary = (entries + localEntries).first {
                        $0.lastPathComponent.hasPrefix(".tmp_") && ((try? LocalFileVersion.read(at: $0))?.size ?? 0) >= 131072
                    }
                    if temporary != nil { break }
                    try await Task.sleep(for: .milliseconds(10))
                }
                let observedTemporary = try #require(temporary)
                #expect(observedTemporary.deletingLastPathComponent().resolvingSymlinksInPath().path == staging.resolvingSymlinksInPath().path)
                for entry in try FileManager.default.contentsOfDirectory(at: f.local, includingPropertiesForKeys: nil)
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
        let stats = try await engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(stats.filesDownloaded == 1)
        #expect(stats.filesUploaded == 0)
        #expect(stats.filesFailed == 0)
        #expect(try Data(contentsOf: first) == Data(String(repeating: "R", count: 131072).utf8))
        #expect(ChangesProtocol.state.withLock { !$0.files.values.contains { $0.name.hasPrefix(".tmp_") } })
        #expect(try FileManager.default.contentsOfDirectory(atPath: f.local.path) == ["first"])
        #expect(try FileManager.default.contentsOfDirectory(atPath: staging.path).allSatisfy { !$0.hasPrefix(".tmp_") || $0.contains(".local-conflict-") })
    }

    @Test("A16 first upload completes while the scanner's tail is paused", arguments: ["complete", "error", "cancel"])
    func uploadBeforeScanEnd(ending: String) async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let first = f.local.appendingPathComponent("first")
        try Data("first body".utf8).write(to: first)
        let batch = try oneEntry(first)
        let tail = f.local.appendingPathComponent("slow-tail")
        try FileManager.default.createDirectory(at: tail, withIntermediateDirectories: true)
        let tailBatch = try oneEntry(tail, type: .directory)
        // A known tail directory avoids a fake directory creation; it is deliberately not emitted yet.
        try await f.store.write { conn in
            try conn.execute("INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id, local_status, remote_status, phase, created_at, updated_at) VALUES (\(f.rootID), \(f.rootItemID), 'slow-tail', 'directory', 'tail', 'present', 'present', 'committed', 1, 1);")
        }
        // The HTTP boundary checks that local observation AND create intent were committed first.
        ChangesProtocol.state.withLock { state in
            state.verifyUpload = { name in
                let conn = try SQLiteConnection(path: f.store.path, readonly: true)
                let q = try conn.prepare("SELECT i.local_sha256, op.expected_sha256 FROM items i JOIN operations op ON op.item_id = i.item_id WHERE i.name = ? AND op.state != 'completed';")
                q.bindText(name, at: 1)
                #expect(try q.step())
                #expect(q.columnText(at: 0) == SyncEngine.computeSha256(of: Data("first body".utf8)))
                #expect(q.columnText(at: 0) == q.columnText(at: 1))
            }
        }
        let engine = try await SyncEngine(auth: f.auth, store: f.store, client: f.client,
            idPool: IDPool(initialIds: ["first-id"]), incrementalScan: { _, consume in
                try await consume(batch)
                // Do not supply the tail until the real multipart request has completed.
                #expect(try await waitForUpload("first"))
                if ending == "error" { throw POSIXError(.EIO) }
                if ending == "cancel" {
                    withUnsafeCurrentTask { $0?.cancel() }
                    try Task.checkCancellation()
                }
                try await consume(tailBatch)
            })
        let run = Task { try await engine.syncIncremental(localPath: f.local.path, remoteRootId: "root") }
        if ending == "error" {
            await #expect(throws: POSIXError.self) { try await run.value }
        } else if ending == "cancel" {
            await #expect(throws: CancellationError.self) { try await run.value }
        } else {
            let result = try await run.value
            #expect(result.filesUploaded == 1)
            #expect(result.filesFailed == 0)
        }
        try await f.store.read { conn in
            let q = try conn.prepare("SELECT phase, dirty_generation FROM items WHERE name = 'first';")
            #expect(try q.step())
            #expect(q.columnText(at: 0) == "committed")
            #expect(q.columnInt64(at: 1) == 0)
            let tail = try conn.prepare("SELECT local_status, is_tombstone FROM items WHERE name = 'slow-tail';")
            #expect(try tail.step())
            #expect(tail.columnText(at: 0) == "present")
            #expect(tail.columnInt64(at: 1) == 0)
        }
        #expect(ChangesProtocol.state.withLock { $0.requests.filter { $0.contains("/upload/") }.count } == 1)
    }

    @Test("A16 1000 observations and matching receipts commit in bounded batches")
    func naturalObservationCommits() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let data = Data("same body".utf8)
        let sha = SyncEngine.computeSha256(of: data)
        for i in 0..<1000 { try data.write(to: f.local.appendingPathComponent("file-\(i)")) }
        try await f.store.write { conn in
            let q = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    base_sha256, base_size, remote_sha256, remote_size, remote_status,
                    phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, ?, 'file', ?, ?, 9, ?, 9, 'present', 'ready', 1, 1, 1);
                """)
            for i in 0..<1000 {
                q.bindInt64(f.rootID, at: 1)
                q.bindInt64(f.rootItemID, at: 2)
                q.bindText("file-\(i)", at: 3)
                q.bindText("remote-\(i)", at: 4)
                q.bindText(sha, at: 5)
                q.bindText(sha, at: 6)
                _ = try q.step()
                q.reset()
            }
        }
        let before = await f.store.getWriterStats()
        let result = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        let after = await f.store.getWriterStats()
        #expect(result.filesScanned == 1000)
        #expect(result.filesUploaded == 0)
        #expect(result.filesFailed == 0)
        #expect(after.totalCommits - before.totalCommits <= 45)
        #expect(after.timeoutTriggeredCommits == before.timeoutTriggeredCommits)
        try await f.store.read { conn in
            let q = try conn.prepare("SELECT COUNT(*) FROM items WHERE entry_kind = 'file' AND local_sha256 = base_sha256 AND dirty_generation = 0 AND local_generation = 1;")
            #expect(try q.step())
            #expect(q.columnInt64(at: 0) == 1000)
            let plan = try conn.prepare("EXPLAIN QUERY PLAN SELECT items.item_id FROM json_each('[1,2,3]') selected CROSS JOIN items ON items.item_id = selected.value WHERE items.root_id = 1 AND items.dirty_generation > 0 AND items.is_tombstone = 0;")
            var details: [String] = []
            while try plan.step() { details.append(plan.columnText(at: 3) ?? "") }
            #expect(details.contains { $0.contains("SEARCH items USING INTEGER PRIMARY KEY") })
        }
        print("A16 1000 observations: commits=\(after.totalCommits - before.totalCommits), elapsed=\(result.elapsedSeconds)")
    }

    @Test("A16 transfer scheduling applies backpressure before creating tasks")
    func boundedIncrementalUploads() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        for i in 0..<80 { try Data("body".utf8).write(to: f.local.appendingPathComponent("file-\(i)")) }
        ChangesProtocol.state.withLock { $0.uploadDelay = 0.02 }
        let result = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root", maxConcurrency: 2)
        #expect(result.filesUploaded == 80)
        #expect(result.filesFailed == 0)
        #expect(ChangesProtocol.state.withLock { $0.peakUploads } == 2)
        #expect(ChangesProtocol.state.withLock { $0.activeUploads } == 0)
    }

    @Test("A16 early child upload waits for a pending parent create to recover")
    func pendingParentBeforeChild() async throws {
        let f = try await fixture()
        defer { try? FileManager.default.removeItem(at: f.directory) }
        let directory = f.local.appendingPathComponent("pending-parent")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        try Data("child body".utf8).write(to: directory.appendingPathComponent("child"))
        var version = stat()
        #expect(stat(directory.path, &version) == 0)
        _ = try await DurableCreateIntentStore.prepareDirectory(store: f.store,
            rootID: f.rootID, parentItemID: f.rootItemID, name: "pending-parent",
            targetParentRemoteID: "root", candidateRemoteID: "pending-id",
            device: Int64(version.st_dev), inode: Int64(version.st_ino))
        let result = try await f.engine.syncIncremental(localPath: f.local.path, remoteRootId: "root")
        #expect(result.filesUploaded == 1)
        #expect(result.filesFailed == 0)
        let requests = ChangesProtocol.state.withLock { $0.requests }
        let creation = try #require(requests.firstIndex { $0.hasPrefix("POST /drive/v3/files?") })
        let upload = try #require(requests.firstIndex { $0.contains("/upload/") })
        #expect(creation < upload)
        #expect(ChangesProtocol.state.withLock { $0.files.values.first { $0.name == "child" }?.parents } == ["pending-id"])
    }

}
