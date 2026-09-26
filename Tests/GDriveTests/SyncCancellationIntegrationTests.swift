import Foundation
import os
import Testing
@testable import GDrive

private enum IncrementalCancellationMode: Sendable {
    case queuedDownloads
    case conflictRefresh
}

private final class IncrementalCancellationState: Sendable {
    let mode: IncrementalCancellationMode
    let firstDownloadStarted = AsyncSemaphore(count: 0)
    let firstDownloadStopped = OSAllocatedUnfairLock(initialState: false)
    let timedOut = OSAllocatedUnfairLock(initialState: false)
    let requests = OSAllocatedUnfairLock(initialState: [String]())

    init(mode: IncrementalCancellationMode) {
        self.mode = mode
    }
}

private struct UnlinkRemaining: Sendable {
    let roots: Int64
    let items: Int64
    let cursors: Int64
    let storage: Int64
}

private struct PreservedConflict: Sendable {
    let sha256: String?
    let inboxCount: Int64
    let dirtyGeneration: Int64
}

private final class IncrementalCancellationURLProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let state = TestHTTPContext<IncrementalCancellationState>.value(for: request)!
        let url = request.url!
        state.requests.withLock { $0.append("\(request.httpMethod ?? "GET") \(url.path)?\(url.query ?? "")") }

        if url.path.hasSuffix("/files/first"), url.query?.contains("alt=media") == true {
            state.firstDownloadStarted.signal()
            // Bounds a broken cancellation path; this is not synchronization.
            DispatchQueue.global().asyncAfter(deadline: .now() + 10) { [weak self] in
                guard let self, !state.firstDownloadStopped.withLock({ $0 }) else { return }
                state.timedOut.withLock { $0 = true }
                self.client?.urlProtocol(self, didFailWithError: URLError(.timedOut))
            }
            return
        }

        let body: Data
        if url.path.hasSuffix("/files/root") {
            body = Data(#"{"id":"root","name":"root","mimeType":"application/vnd.google-apps.folder"}"#.utf8)
        } else if url.path.hasSuffix("/changes") {
            let page: DriveChangesPage
            switch state.mode {
            case .queuedDownloads:
                let first = Self.file(id: "first", name: "first.txt", content: "first")
                let second = Self.file(id: "second", name: "second.txt", content: "second")
                page = DriveChangesPage(nextPageToken: nil, newStartPageToken: "after", changes: [
                    DriveChange(fileId: first.id, removed: false, file: first),
                    DriveChange(fileId: second.id, removed: false, file: second)
                ])
            case .conflictRefresh:
                page = DriveChangesPage(nextPageToken: nil, newStartPageToken: "after", changes: [])
            }
            do {
                body = try JSONEncoder().encode(page)
            } catch {
                client?.urlProtocol(self, didFailWithError: error)
                return
            }
        } else if url.path.hasSuffix("/files/second"), url.query?.contains("alt=media") == true {
            body = Data("second".utf8)
        } else {
            client?.urlProtocol(self, didFailWithError: URLError(.badURL))
            return
        }

        let response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {
        guard request.url?.path.hasSuffix("/files/first") == true,
              request.url?.query?.contains("alt=media") == true else { return }
        let state = TestHTTPContext<IncrementalCancellationState>.value(for: request)!
        state.firstDownloadStopped.withLock { $0 = true }
        client?.urlProtocol(self, didFailWithError: URLError(.cancelled))
    }

    private static func file(id: String, name: String, content: String) -> DriveFile {
        let data = Data(content.utf8)
        return DriveFile(id: id, name: name, parents: ["root"], size: String(data.count),
            sha256Checksum: SyncEngine.computeSha256(of: data), version: "1")
    }
}

@Suite("Public incremental cancellation", .timeLimit(.minutes(1)))
struct SyncCancellationIntegrationTests {
    @Test("Public stop APIs drain an active incremental download before queued work starts",
          arguments: [false, true])
    func stopActiveIncrementalDownload(unlink: Bool) async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("incremental-cancel-\(UUID().uuidString)")
        let local = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(clientId: "test", accessToken: "test",
            expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)

        let context = TestHTTPContext(IncrementalCancellationState(mode: .queuedDownloads))
        let configuration = URLSessionConfiguration.ephemeral
        context.configure(configuration)
        configuration.protocolClasses = [IncrementalCancellationURLProtocol.self]
        let client = DriveClient(auth: auth, session: URLSession(configuration: configuration),
            requestsPerSecond: nil, maxRetries: 0, retrySleep: { _ in })
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let ids = try await store.write { connection -> (root: Int64, item: Int64) in
            let root = try connection.prepare("""
            INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
            VALUES ('default', ?, 1, 1, 'root', 'localToRemoteEmpty', 'existingKnown', 1, 1);
            """)
            root.bindText(local.path, at: 1)
            _ = try root.step()
            let rootID = connection.lastInsertRowId
            try connection.execute("""
            INSERT INTO items(root_id, name, entry_kind, remote_file_id, local_status, remote_status,
                phase, created_at, updated_at)
            VALUES (\(rootID), 'local', 'directory', 'root', 'present', 'present', 'committed', 1, 1);
            """)
            let itemID = connection.lastInsertRowId
            try connection.execute("""
            INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at)
            VALUES (\(rootID), 'default', 'drive_changes', 'start', 1);
            """)
            try connection.execute("""
            INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id, local_status,
                remote_status, remote_name, remote_parent_file_id, remote_generation, phase,
                dirty_generation, created_at, updated_at)
            VALUES (\(rootID), \(itemID), 'old-directory', 'directory', 'old-directory', 'absent',
                'present', 'old-directory', 'root', 1, 'ready', 1, 1, 1);
            """)
            return (rootID, itemID)
        }
        try await setStoredRootIdentity(store: store, rootID: ids.root, localURL: local)

        let engine = try await SyncEngine(auth: auth, store: store, client: client,
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            conflictDirectory: directory.appendingPathComponent("conflicts"),
            incrementalScan: SyncEngine.defaultDirectoryScan)

        let sync = Task { try await engine.syncIncremental(localPath: local.path, maxConcurrency: 1) }
        try await context.value.firstDownloadStarted.wait()
        if unlink {
            let otherEngine = try await SyncEngine(auth: auth, store: store, client: client,
                downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
                conflictDirectory: directory.appendingPathComponent("conflicts"),
                incrementalScan: SyncEngine.defaultDirectoryScan)
            let request = Task { try await otherEngine.unlink(localPath: local.path) }
            await #expect(throws: CancellationError.self) { _ = try await sync.value }
            try await request.value
        } else {
            let cancellation = Task { await engine.cancelSync(localPath: local.path) }
            await #expect(throws: CancellationError.self) { _ = try await sync.value }
            await cancellation.value
        }

        #expect(context.value.firstDownloadStopped.withLock { $0 })
        #expect(!context.value.timedOut.withLock { $0 })
        let requests = context.value.requests.withLock { $0 }
        #expect(!requests.contains { $0.contains("/files/second?") && $0.contains("alt=media") })
        #expect(!requests.contains { $0.hasPrefix("PATCH /drive/v3/files/old-directory?") })
        #expect(!FileManager.default.fileExists(atPath: local.appendingPathComponent("first.txt").path))
        #expect(!FileManager.default.fileExists(atPath: local.appendingPathComponent("second.txt").path))

        if unlink {
            let remaining = try await store.read { connection -> UnlinkRemaining in
                let roots = try connection.prepare("SELECT COUNT(*) FROM roots;")
                _ = try roots.step()
                let items = try connection.prepare("SELECT COUNT(*) FROM items;")
                _ = try items.step()
                let cursors = try connection.prepare("SELECT COUNT(*) FROM cursors;")
                _ = try cursors.step()
                let storage = try connection.prepare("SELECT COUNT(*) FROM root_storage_directories;")
                _ = try storage.step()
                return UnlinkRemaining(
                    roots: roots.columnInt64(at: 0) ?? -1,
                    items: items.columnInt64(at: 0) ?? -1,
                    cursors: cursors.columnInt64(at: 0) ?? -1,
                    storage: storage.columnInt64(at: 0) ?? -1)
            }
            #expect(remaining.roots == 0)
            #expect(remaining.items == 0)
            #expect(remaining.cursors == 0)
            #expect(remaining.storage == 0)
            #expect(FileManager.default.fileExists(atPath: local.path))
            #expect(!FileManager.default.fileExists(atPath: directory.appendingPathComponent("downloads/root").path))
            #expect(!FileManager.default.fileExists(atPath: directory.appendingPathComponent("conflicts/root").path))
        } else {
            let durable = try await store.read { connection -> (Int64, String?) in
                let items = try connection.prepare(
                    "SELECT COUNT(*) FROM items WHERE root_id = ? AND remote_file_id IN ('first', 'second');")
                items.bindInt64(ids.root, at: 1)
                _ = try items.step()
                let cursor = try connection.prepare("SELECT token_value FROM cursors WHERE root_id = ?;")
                cursor.bindInt64(ids.root, at: 1)
                _ = try cursor.step()
                return (items.columnInt64(at: 0) ?? 0, cursor.columnText(at: 0))
            }
            #expect(durable.0 == 2)
            #expect(durable.1 == "after")
        }

        let snapshot = engine.transferStatus
        #expect(snapshot.activeDownloads.isEmpty)
        #expect(snapshot.queuedDownloads.isEmpty)
        let token = try await RootSyncCoordinator.shared.acquire(localRootPath: local.path)
        await RootSyncCoordinator.shared.release(token)
    }

    @Test("Cancellation stops a conflict refresh before incremental scanning begins")
    func cancellationDuringConflictRefreshPreservesConflictAndBaseline() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("conflict-refresh-cancel-\(UUID().uuidString)")
        let local = directory.appendingPathComponent("local")
        let localFile = local.appendingPathComponent("conflict.txt")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        try Data("baseline".utf8).write(to: localFile)
        defer { try? FileManager.default.removeItem(at: directory) }

        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(clientId: "test", accessToken: "test",
            expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)

        let context = TestHTTPContext(IncrementalCancellationState(mode: .conflictRefresh))
        let configuration = URLSessionConfiguration.ephemeral
        context.configure(configuration)
        configuration.protocolClasses = [IncrementalCancellationURLProtocol.self]
        let client = DriveClient(auth: auth, session: URLSession(configuration: configuration),
            requestsPerSecond: nil, maxRetries: 0, retrySleep: { _ in })
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let originalSHA256 = SyncEngine.computeSha256(of: Data("old remote".utf8))
        let refreshed = DriveFile(id: "first", name: "conflict.txt", parents: ["root"], size: "5",
            sha256Checksum: SyncEngine.computeSha256(of: Data("first".utf8)), version: "2")
        let payloadData = try JSONEncoder().encode(
            DriveChange(fileId: refreshed.id, removed: false, file: refreshed))
        let payload = try #require(String(bytes: payloadData, encoding: .utf8))
        let rootID = try await store.write { connection -> Int64 in
            let root = try connection.prepare("""
            INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
            VALUES ('default', ?, 1, 1, 'root', 'localToRemoteEmpty', 'existingKnown', 1, 1);
            """)
            root.bindText(local.path, at: 1)
            _ = try root.step()
            let id = connection.lastInsertRowId
            try connection.execute("""
            INSERT INTO items(root_id, name, entry_kind, remote_file_id, local_status, remote_status,
                phase, created_at, updated_at)
            VALUES (\(id), 'local', 'directory', 'root', 'present', 'present', 'committed', 1, 1);
            """)
            let rootItemID = connection.lastInsertRowId
            try connection.execute("""
            INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id, local_status,
                remote_status, remote_name, remote_parent_file_id, local_generation, remote_generation,
                phase, dirty_generation, created_at, updated_at)
            VALUES (\(id), \(rootItemID), 'conflict.txt', 'file', 'first', 'present', 'present',
                'conflict.txt', 'root', 1, 1, 'blocked', 0, 1, 1);
            """)
            let itemID = connection.lastInsertRowId
            let conflict = try connection.prepare("""
            INSERT INTO sync_conflicts(conflict_id, root_id, item_id, remote_file_id, relative_path,
                local_path, remote_sha256, remote_size, remote_version, remote_status, created_at, updated_at)
            VALUES ('conflict', ?, ?, 'first', 'conflict.txt', ?, ?, 10, 1, 'present', 1, 1);
            """)
            conflict.bindInt64(id, at: 1)
            conflict.bindInt64(itemID, at: 2)
            conflict.bindText(localFile.path, at: 3)
            conflict.bindText(originalSHA256, at: 4)
            _ = try conflict.step()
            let inbox = try connection.prepare("""
            INSERT INTO remote_change_inbox(root_id, remote_id, payload, attempted_at)
            VALUES (?, 'first', ?, 0);
            """)
            inbox.bindInt64(id, at: 1)
            inbox.bindText(payload, at: 2)
            _ = try inbox.step()
            try connection.execute("""
            INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at)
            VALUES (\(id), 'default', 'drive_changes', 'start', 1);
            """)
            return id
        }
        try await setStoredRootIdentity(store: store, rootID: rootID, localURL: local)

        let scanStarted = OSAllocatedUnfairLock(initialState: false)
        let engine = try await SyncEngine(auth: auth, store: store, client: client,
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            conflictDirectory: directory.appendingPathComponent("conflicts"),
            incrementalScan: { _, _ in scanStarted.withLock { $0 = true } })
        let sync = Task { try await engine.syncIncremental(localPath: local.path) }
        try await context.value.firstDownloadStarted.wait()
        let cancellation = Task { await engine.cancelSync(localPath: local.path) }
        await #expect(throws: CancellationError.self) { _ = try await sync.value }
        await cancellation.value

        #expect(context.value.firstDownloadStopped.withLock { $0 })
        #expect(!context.value.timedOut.withLock { $0 })
        #expect(!scanStarted.withLock { $0 })
        #expect(try Data(contentsOf: localFile) == Data("baseline".utf8))
        let preserved = try await store.read { connection -> PreservedConflict in
            let conflict = try connection.prepare(
                "SELECT remote_sha256 FROM sync_conflicts WHERE conflict_id = 'conflict';")
            _ = try conflict.step()
            let inbox = try connection.prepare(
                "SELECT COUNT(*) FROM remote_change_inbox WHERE root_id = ? AND remote_id = 'first';")
            inbox.bindInt64(rootID, at: 1)
            _ = try inbox.step()
            let item = try connection.prepare(
                "SELECT dirty_generation FROM items WHERE root_id = ? AND remote_file_id = 'first';")
            item.bindInt64(rootID, at: 1)
            _ = try item.step()
            return PreservedConflict(
                sha256: conflict.columnText(at: 0),
                inboxCount: inbox.columnInt64(at: 0) ?? 0,
                dirtyGeneration: item.columnInt64(at: 0) ?? -1)
        }
        #expect(preserved.sha256 == originalSHA256)
        #expect(preserved.inboxCount == 1)
        #expect(preserved.dirtyGeneration == 0)
        #expect(engine.transferStatus.activeDownloads.isEmpty)
        #expect(engine.transferStatus.queuedDownloads.isEmpty)
    }
}
