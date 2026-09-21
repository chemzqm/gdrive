import Foundation
import CryptoKit
import Testing
import DirectoryScanner
import os
@testable import GDrive

final class MockDirectoryRecoveryURLProtocol: URLProtocol, @unchecked Sendable {
    private var isStopped = false
    private let lock = NSLock()

    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = TestHTTPContext<TestRequestHandler>.value(for: request)?.requestHandler else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
            return
        }
        do {
            let (response, data) = try handler(request)
            lock.lock()
            defer { lock.unlock() }
            guard !isStopped else { return }
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            lock.lock()
            defer { lock.unlock() }
            guard !isStopped else { return }
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {
        lock.lock()
        isStopped = true
        lock.unlock()
        client?.urlProtocol(self, didFailWithError: URLError(.cancelled))
    }
}

extension URLRequest {
    var extractBodyData: Data? {
        if let httpBody = self.httpBody {
            return httpBody
        }
        guard let stream = self.httpBodyStream else { return nil }
        stream.open()
        defer { stream.close() }
        var data = Data()
        let bufferSize = 4096
        let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
        defer { buffer.deallocate() }
        while stream.hasBytesAvailable {
            let read = stream.read(buffer, maxLength: bufferSize)
            if read > 0 {
                data.append(buffer, count: read)
            } else {
                break
            }
        }
        return data
    }
}

@Suite("Bootstrap Failure and Cancellation Recovery")
struct BootstrapRecoveryTests {
    private let context = TestHTTPContext(TestRequestHandler())

    private static func bootstrapFailureResponse(
        request: URLRequest, failure: String, failing: OSAllocatedUnfairLock<Bool>,
        body: Data, digest: String
    ) throws -> (HTTPURLResponse, Data) {
        let url = try #require(request.url)
        func response(_ status: Int = 200) -> HTTPURLResponse {
            HTTPURLResponse(
                url: url, statusCode: status, httpVersion: nil,
                headerFields: ["Content-Type": "application/json"])!
        }
        if url.path.hasSuffix("/changes/startPageToken") {
            return (response(), Data(#"{"startPageToken":"C0"}"#.utf8))
        }
        if url.path.hasSuffix("/changes") {
            return (response(), Data(#"{"changes":[],"newStartPageToken":"C1"}"#.utf8))
        }
        if url.path.hasSuffix("/files/root") {
            return (response(), Data(
                #"{"id":"root","name":"root","mimeType":"application/vnd.google-apps.folder"}"#.utf8))
        }
        if url.path.hasSuffix("/files/file") {
            if failing.withLock({ $0 }) {
                if failure == "network" {
                    return (response(400), Data(#"{"error":{"code":400,"message":"download failed"}}"#.utf8))
                }
                if failure == "checksum" { return (response(), Data("wrong bytes".utf8)) }
            }
            return (response(), body)
        }
        if url.path.hasSuffix("/files") {
            return (response(), try JSONSerialization.data(withJSONObject: ["files": [[
                "id": "file", "name": "file.txt", "mimeType": "text/plain",
                "size": String(body.count), "sha256Checksum": digest, "parents": ["root"]
            ]]]))
        }
        throw URLError(.badURL)
    }

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
        config.protocolClasses = [MockDirectoryRecoveryURLProtocol.self]
        let session = URLSession(configuration: config)
        return DriveClient(auth: auth, session: session, requestsPerSecond: nil)
    }

    @Test("Concurrent bootstrap downloads report exact file and byte totals")
    func concurrentDownloadStatsAreExact() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "download-stats-\(UUID().uuidString)")
        let local = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let auth = try createMockAuth(tempDir: directory)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let fileCount = 256
        let contents = Dictionary(uniqueKeysWithValues: (0..<fileCount).map { index in
            let id = "file-\(index)"
            return (id, Data(repeating: UInt8(index % 251), count: 1024 + index))
        })
        let listing = try JSONSerialization.data(withJSONObject: ["files": contents.map { id, data in
            [
                "id": id, "name": "\(id).bin", "mimeType": "application/octet-stream",
                "size": String(data.count), "sha256Checksum": SyncEngine.computeSha256(of: data),
                "parents": ["root"]
            ]
        }])
        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let response = try #require(HTTPURLResponse(
                url: url, statusCode: 200, httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]))
            if url.path.hasSuffix("/changes/startPageToken") {
                return (response, Data(#"{"startPageToken":"initial"}"#.utf8))
            }
            if url.path.hasSuffix("/files/root") {
                return (response, Data(
                    #"{"id":"root","name":"root","mimeType":"application/vnd.google-apps.folder"}"#.utf8))
            }
            if url.path.hasSuffix("/files") { return (response, listing) }
            if let data = contents[url.lastPathComponent] { return (response, data) }
            throw URLError(.badURL)
        }
        defer { context.value.requestHandler = nil }
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            conflictDirectory: directory.appendingPathComponent("conflicts"))

        let stats = try await engine.syncRemoteToLocalEmpty(
            localPath: local.path, remoteRootId: "root", maxDownloadConcurrency: 64)

        #expect(stats.filesDownloaded == fileCount)
        #expect(stats.bytesDownloaded == contents.values.reduce(0) { $0 + Int64($1.count) })
    }

    // MARK: - Bootstrap download recovery

    @Test("A bootstrap download receipt database failure remains recoverable after reopening")
    func downloadReceiptDatabaseFailureThrows() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("download-db-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let local = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        let auth = try createMockAuth(tempDir: directory)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        try await store.write { conn in
            try conn.execute("""
                CREATE TRIGGER reject_download_receipt BEFORE INSERT ON items
                WHEN NEW.entry_kind = 'file'
                BEGIN SELECT RAISE(ABORT, 'download receipt failure'); END;
                """)
        }
        let body = Data("downloaded bytes".utf8)
        let digest = SyncEngine.computeSha256(of: body)
        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let response = try #require(HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]))
            if url.path.hasSuffix("/changes/startPageToken") {
                return (response, Data(#"{"startPageToken":"initial"}"#.utf8))
            }
            if url.path.hasSuffix("/files/root") {
                return (response, Data(#"{"id":"root","name":"root","mimeType":"application/vnd.google-apps.folder"}"#.utf8))
            }
            if url.path.hasSuffix("/files/file") { return (response, body) }
            if url.path.hasSuffix("/files") {
                return (response, try JSONSerialization.data(withJSONObject: ["files": [[
                    "id": "file", "name": "file.txt", "mimeType": "text/plain",
                    "size": String(body.count), "sha256Checksum": digest, "parents": ["root"]
                ]]]))
            }
            throw URLError(.badURL)
        }
        defer { context.value.requestHandler = nil }
        let engine = try await SyncEngine(
            auth: auth, store: store, client: createMockClient(auth: auth),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            conflictDirectory: directory.appendingPathComponent("conflicts"))
        do {
            _ = try await engine.syncRemoteToLocalEmpty(localPath: local.path, remoteRootId: "root")
            Issue.record("Expected the database receipt failure to escape")
        } catch {
            #expect((error as NSError).domain == "SQLiteStatement")
            #expect(error.localizedDescription.contains("download receipt failure"))
        }
        #expect(try Data(contentsOf: local.appendingPathComponent("file.txt")) == body)
        let completed = try await store.read { conn in
            let query = try conn.prepare("SELECT bootstrap_state FROM roots;")
            defer { query.reset() }
            guard try query.step() else { return false }
            return query.columnText(at: 0) == "existingKnown"
        }
        #expect(!completed)
        let pending = try await store.read { conn in
            let query = try conn.prepare("SELECT count(*) FROM remote_change_inbox WHERE remote_id = 'file';")
            defer { query.reset() }
            return try query.step() ? query.columnInt64(at: 0) : nil
        }
        #expect(pending == 1)

        try await store.write { conn in
            try conn.execute("DROP TRIGGER reject_download_receipt;")
        }
        let reopenedStore = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let reopened = try await SyncEngine(
            auth: auth, store: reopenedStore, client: createMockClient(auth: auth),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            conflictDirectory: directory.appendingPathComponent("conflicts"))
        let recovered = try await reopened.sync(localPath: local.path, remoteFolderId: "root")
        #expect(recovered.filesFailed == 0)
        #expect(try Data(contentsOf: local.appendingPathComponent("file.txt")) == body)
        let recoveryState = try await reopenedStore.read { conn in
            let query = try conn.prepare("""
                SELECT r.bootstrap_state,
                    (SELECT count(*) FROM remote_change_inbox WHERE root_id = r.root_id),
                    (SELECT count(*) FROM items WHERE root_id = r.root_id AND remote_file_id = 'file')
                FROM roots r;
                """)
            defer { query.reset() }
            #expect(try query.step())
            return (query.columnText(at: 0), query.columnInt64(at: 1), query.columnInt64(at: 2))
        }
        #expect(recoveryState.0 == "existingKnown")
        #expect(recoveryState.1 == 0)
        #expect(recoveryState.2 == 1)
    }

    @Test("Bootstrap download failures remain pending and recover without a new Change")
    func bootstrapDownloadFailureRecovers() async throws {
        func run(_ failure: String) async throws {
            let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
                "download-recovery-\(failure)-\(UUID().uuidString)")
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            defer { try? FileManager.default.removeItem(at: directory) }
            let local = directory.appendingPathComponent("local")
            try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
            let auth = try createMockAuth(tempDir: directory)
            let databasePath = directory.appendingPathComponent("state.sqlite").path
            let store = try await StateStore(path: databasePath)
            let body = Data("recoverable remote bytes".utf8)
            let digest = SyncEngine.computeSha256(of: body)
            let failing = OSAllocatedUnfairLock(initialState: true)
            if failure == "publication" {
                try Data("local collision".utf8).write(to: local.appendingPathComponent("file.txt"))
            }
            context.value.requestHandler = { request in
                try Self.bootstrapFailureResponse(
                    request: request, failure: failure, failing: failing,
                    body: body, digest: digest)
            }
            defer { context.value.requestHandler = nil }

            let engine = try await SyncEngine(
                auth: auth, store: store, client: createMockClient(auth: auth),
                downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
                conflictDirectory: directory.appendingPathComponent("conflicts"))
            let failed = try await engine.syncRemoteToLocalEmpty(
                localPath: local.path, remoteRootId: "root")
            let expectedFailures = ["network": 1, "checksum": 1, "publication": 0]
            let expectedStates = [
                "network": ("freshCreated", Int64(1)),
                "checksum": ("freshCreated", Int64(1)),
                "publication": ("existingKnown", Int64(0))
            ]
            #expect(failed.filesFailed == expectedFailures[failure]!)
            #expect(failed.remoteWorkPending == 1)
            let pendingState = try await store.read { conn in
            let query = try conn.prepare("""
                SELECT r.bootstrap_state,
                    (SELECT count(*) FROM remote_change_inbox WHERE root_id = r.root_id)
                FROM roots r;
                """)
            defer { query.reset() }
            #expect(try query.step())
            return (query.columnText(at: 0), query.columnInt64(at: 1))
            }
            #expect(pendingState.0 == expectedStates[failure]!.0)
            #expect(pendingState.1 == expectedStates[failure]!.1)

            failing.withLock { $0 = false }
            if failure == "publication" {
                #expect(failed.conflicts.count == 1)
                #expect(try Data(contentsOf: local.appendingPathComponent("file.txt")) == Data("local collision".utf8))
                let conflictPath = try #require(failed.conflicts[0].conflictPath)
                #expect(try Data(contentsOf: URL(fileURLWithPath: conflictPath)) == body)
            }
            let reopenedStore = try await StateStore(path: databasePath)
            let reopened = try await SyncEngine(
                auth: auth, store: reopenedStore, client: createMockClient(auth: auth),
                downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
                conflictDirectory: directory.appendingPathComponent("conflicts"))
            if failure == "publication" {
                let conflicts = try await reopened.listConflicts(localPath: local.path)
                #expect(conflicts == failed.conflicts)
                try await reopened.resolveConflict(id: conflicts[0].id, resolution: .remote)
            } else {
                let recovered = try await reopened.sync(localPath: local.path, remoteFolderId: "root")
                #expect(recovered.filesFailed == 0)
            }
            #expect(try Data(contentsOf: local.appendingPathComponent("file.txt")) == body)
            let finalState = try await reopenedStore.read { conn in
            let query = try conn.prepare("""
                SELECT r.bootstrap_state,
                    (SELECT count(*) FROM remote_change_inbox WHERE root_id = r.root_id),
                    (SELECT count(*) FROM items WHERE root_id = r.root_id AND remote_file_id = 'file'),
                    (SELECT count(*) FROM sync_conflicts WHERE root_id = r.root_id)
                FROM roots r;
                """)
            defer { query.reset() }
            #expect(try query.step())
            return (query.columnText(at: 0), query.columnInt64(at: 1),
                query.columnInt64(at: 2), query.columnInt64(at: 3))
            }
            #expect(finalState.0 == "existingKnown")
            #expect(finalState.1 == 0)
            #expect(finalState.2 == 1)
            #expect(finalState.3 == 0)
        }

        for failure in ["network", "checksum", "publication"] {
            try await run(failure)
        }
    }

    // MARK: - Integration Tests: syncLocalToRemoteEmpty with Injections (A06 Swift Acceptance)

    @Test("Swift Acceptance 1: Parent directory 403 injection: children fail promptly, sibling directory succeeds, sync returns complete stats")
    func testParentDir403AllowsSiblingToComplete() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a06_test403_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        let brokenDir = localRootDir.appendingPathComponent("broken_dir")
        let brokenSubDir = brokenDir.appendingPathComponent("sub")
        let healthyDir = localRootDir.appendingPathComponent("healthy_dir")

        try FileManager.default.createDirectory(at: brokenSubDir, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: healthyDir, withIntermediateDirectories: true)

        let file1Content = "file1 in broken dir"
        let file2Content = "file2 in broken sub dir"
        let file3Content = "file3 in healthy dir"

        try file1Content.write(to: brokenDir.appendingPathComponent("file1.txt"), atomically: true, encoding: .utf8)
        try file2Content.write(to: brokenSubDir.appendingPathComponent("file2.txt"), atomically: true, encoding: .utf8)
        try file3Content.write(to: healthyDir.appendingPathComponent("file3.txt"), atomically: true, encoding: .utf8)

        let healthyData = file3Content.data(using: .utf8)!
        let healthySha256 = SHA256.hash(data: healthyData).map { String(format: "%02x", $0) }.joined()
        let healthySize = healthyData.count

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        let remoteRootId = "remote_root_123"

        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let path = url.path

            if path.hasSuffix("/changes/startPageToken") {
                let json = Data(#"{"startPageToken": "token_1"}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if path.hasSuffix("/files/generateIds") {
                let ids = (0..<100).map { "mock_id_\($0)" }
                let json = try JSONSerialization.data(withJSONObject: ["ids": ids])
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if request.httpMethod == "GET", path.contains("/files/\(remoteRootId)") {
                let json = Data("""
                {
                    "id": "\(remoteRootId)",
                    "name": "local_root",
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [],
                    "trashed": false
                }
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if request.httpMethod == "GET", path.hasSuffix("/drive/v3/files") {
                let json = Data(#"{"files": []}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if request.httpMethod == "POST" {
                if let bodyData = request.extractBodyData,
                   let bodyStr = String(data: bodyData, encoding: .utf8),
                   bodyStr.contains("broken_dir") {
                    // Inject 403 Forbidden on broken_dir creation
                    let errJson = Data(#"{"error": {"code": 403, "message": "The user does not have sufficient permissions for broken_dir"}}"#.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 403, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, errJson)
                }

                if path.contains("/upload/drive/v3/files") {
                    // Multipart upload of healthy file
                    let json = Data("""
                    {
                        "id": "file3_uploaded_id",
                        "name": "file3.txt",
                        "mimeType": "text/plain",
                        "size": "\(healthySize)",
                        "sha256Checksum": "\(healthySha256)"
                    }
                    """.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
                }

                // Healthy directory creation
                let json = Data("""
                {
                    "id": "healthy_dir_id",
                    "name": "healthy_dir",
                    "mimeType": "application/vnd.google-apps.folder"
                }
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            return (HTTPURLResponse(url: url, statusCode: 404, httpVersion: nil, headerFields: nil)!, Data())
        }

        // Run syncLocalToRemoteEmpty. It must not deadlock!
        let startTime = DispatchTime.now()
        let stats = try await engine.syncLocalToRemoteEmpty(
            localPath: localRootDir.path,
            remoteRootId: remoteRootId
        )
        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000

        #expect(elapsed < 10.0, "Sync should finish in bounded time without hanging on broken parent")
        #expect(stats.directoriesCreated == 1, "healthy_dir was created")
        #expect(stats.filesUploaded == 1, "file3 in healthy_dir was uploaded")
        #expect(stats.filesFailed == 2, "file1 and file2 under broken_dir were recorded as failed")
    }

    @Test("Swift Acceptance 2: Parent directory 409 conflict failure: children wake up and fail, sibling completes")
    func testParentDir409ConflictAllowsSiblingToComplete() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a06_test409_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        let conflictDir = localRootDir.appendingPathComponent("conflict_dir")
        let healthyDir = localRootDir.appendingPathComponent("healthy_dir")

        try FileManager.default.createDirectory(at: conflictDir, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: healthyDir, withIntermediateDirectories: true)

        try "file in conflict".write(to: conflictDir.appendingPathComponent("conflict_file.txt"), atomically: true, encoding: .utf8)
        let healthyContent = "healthy file content"
        try healthyContent.write(to: healthyDir.appendingPathComponent("healthy_file.txt"), atomically: true, encoding: .utf8)

        let healthyData = healthyContent.data(using: .utf8)!
        let healthySha256 = SHA256.hash(data: healthyData).map { String(format: "%02x", $0) }.joined()
        let healthySize = healthyData.count

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        let remoteRootId = "remote_root_409"

        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let path = url.path

            if path.hasSuffix("/changes/startPageToken") {
                let json = Data(#"{"startPageToken": "token_1"}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if path.hasSuffix("/files/generateIds") {
                let ids = (0..<100).map { "mock_id_\($0)" }
                let json = try JSONSerialization.data(withJSONObject: ["ids": ids])
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if request.httpMethod == "GET", path.contains("/files/\(remoteRootId)") {
                let json = Data("""
                {
                    "id": "\(remoteRootId)",
                    "name": "local_root",
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [],
                    "trashed": false
                }
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if request.httpMethod == "GET", path.hasSuffix("/drive/v3/files") {
                let json = Data(#"{"files": []}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            if request.httpMethod == "POST" {
                if let bodyData = request.extractBodyData,
                   let bodyStr = String(data: bodyData, encoding: .utf8),
                   bodyStr.contains("conflict_dir") {
                    // Inject 409 Conflict that cannot be verified (e.g. 500 on verify or non-folder)
                    let errJson = Data(#"{"error": {"code": 409, "message": "Conflict on conflict_dir"}}"#.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 409, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, errJson)
                }

                if path.contains("/upload/drive/v3/files") {
                    let json = Data("""
                    {
                        "id": "healthy_file_id",
                        "name": "healthy_file.txt",
                        "mimeType": "text/plain",
                        "size": "\(healthySize)",
                        "sha256Checksum": "\(healthySha256)"
                    }
                    """.utf8)
                    return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
                }

                let json = Data("""
                {
                    "id": "healthy_dir_id",
                    "name": "healthy_dir",
                    "mimeType": "application/vnd.google-apps.folder"
                }
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, json)
            }

            // In case verifyExistingFolder tries to get file
            if request.httpMethod == "GET", path.contains("/files/") {
                return (HTTPURLResponse(url: url, statusCode: 404, httpVersion: nil, headerFields: nil)!, Data())
            }

            return (HTTPURLResponse(url: url, statusCode: 404, httpVersion: nil, headerFields: nil)!, Data())
        }

        let stats = try await engine.syncLocalToRemoteEmpty(
            localPath: localRootDir.path,
            remoteRootId: remoteRootId
        )

        #expect(stats.directoriesCreated == 1, "healthy_dir succeeded")
        #expect(stats.filesUploaded == 1, "healthy_file succeeded")
        #expect(stats.filesFailed == 1, "conflict_file failed")
    }

    @Test("Swift Acceptance 3: Cancellation injection terminates sync without deadlock")
    func testCancellationOfSyncTerminatesPromptly() async throws {
        defer { context.value.requestHandler = nil }
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("a06_cancel_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let localRootDir = tempDir.appendingPathComponent("local_root")
        for directoryIndex in 1...3 {
            let sub = localRootDir.appendingPathComponent("dir_\(directoryIndex)")
            try FileManager.default.createDirectory(at: sub, withIntermediateDirectories: true)
            for fileIndex in 1...2 {
                try "data".write(to: sub.appendingPathComponent("file_\(fileIndex).txt"), atomically: true, encoding: .utf8)
            }
        }

        let auth = try createMockAuth(tempDir: tempDir)
        let client = createMockClient(auth: auth)
        let dbPath = tempDir.appendingPathComponent("test.sqlite").path
        let store = try await StateStore(path: dbPath)
        let engine = try await SyncEngine(auth: auth, store: store, client: client)

        let remoteRootId = "remote_root_cancel"

        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let path = url.path
            if path.hasSuffix("/changes/startPageToken") {
                let json = Data(#"{"startPageToken": "token_1"}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if path.hasSuffix("/files/generateIds") {
                let ids = (0..<100).map { "id_\($0)" }
                let json = try JSONSerialization.data(withJSONObject: ["ids": ids])
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if request.httpMethod == "GET", path.contains("/files/\(remoteRootId)") {
                let json = Data("""
                {
                    "id": "\(remoteRootId)",
                    "name": "local_root",
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [],
                    "trashed": false
                }
                """.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }
            if request.httpMethod == "GET", path.hasSuffix("/drive/v3/files") {
                let json = Data(#"{"files": []}"#.utf8)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
            }

            // For creation requests, sleep slightly to allow cancellation to happen mid-flight
            Thread.sleep(forTimeInterval: 0.05)
            let json = Data("""
            {
                "id": "dir_mock",
                "name": "dir",
                "mimeType": "application/vnd.google-apps.folder"
            }
            """.utf8)
            return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, json)
        }

        let syncTask = Task<SyncStats, Error> {
            try await engine.syncLocalToRemoteEmpty(
                localPath: localRootDir.path,
                remoteRootId: remoteRootId
            )
        }

        // Cancel after 20ms
        try await Task.sleep(nanoseconds: 20_000_000)
        syncTask.cancel()

        let startTime = DispatchTime.now()
        _ = try? await syncTask.value
        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        #expect(elapsed < 3.0, "Cancelled sync should terminate promptly without deadlock")
    }
    @Test("Bootstrap drains admitted tasks after scan failure or cancellation", arguments: [false, true])
    func scanInterruptionDrainsTasks(cancel: Bool) async throws {
        defer { context.value.requestHandler = nil }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("bootstrap-scan-\(UUID().uuidString)")
        let local = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: local.appendingPathComponent("parent"), withIntermediateDirectories: true)
        let file = local.appendingPathComponent("parent/file.txt")
        try Data("content".utf8).write(to: file)
        defer {
            context.value.requestHandler = nil
            try? FileManager.default.removeItem(at: directory)
        }
        context.value.requestHandler = { request in
            let url = try #require(request.url)
            let response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
            if url.path.hasSuffix("/changes/startPageToken") {
                return (response, Data(#"{"startPageToken":"start"}"#.utf8))
            }
            if request.httpMethod == "GET", url.path.hasSuffix("/files/root") {
                return (response, Data(#"{"id":"root","name":"root","mimeType":"application/vnd.google-apps.folder"}"#.utf8))
            }
            if request.httpMethod == "GET", url.path.hasSuffix("/files") {
                return (response, Data(#"{"files":[]}"#.utf8))
            }
            return (HTTPURLResponse(url: url, statusCode: 403, httpVersion: nil, headerFields: nil)!, Data())
        }
        let auth = try createMockAuth(tempDir: directory)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        enum ScanFailure: Error { case injected }
        let engine = try await SyncEngine(auth: auth, store: store, client: createMockClient(auth: auth),
            idPool: IDPool(initialIds: ["directory-id", "file-id"]), incrementalScan: { _, consume in
                var bytes: [UInt8] = []
                var records: [ScanBatch.Record] = []
                for (url, type) in [(local.appendingPathComponent("parent"), EntryType.directory), (file, EntryType.file)] {
                    let path = Array(url.path.utf8)
                    records.append(.init(offset: UInt32(bytes.count), length: UInt32(path.count), type: type, metadata: nil))
                    bytes.append(contentsOf: path)
                    bytes.append(0)
                }
                try await consume(ScanBatch(pathData: bytes, records: records))
                if cancel {
                    withUnsafeCurrentTask { $0?.cancel() }
                    try Task.checkCancellation()
                }
                throw ScanFailure.injected
            })
        let task = Task {
            try await engine.syncLocalToRemoteEmpty(localPath: local.path, remoteRootId: "root")
        }
        do {
            _ = try await task.value
            Issue.record("Interrupted scan should throw")
        } catch {
            if cancel {
                #expect(error is CancellationError)
            } else {
                #expect(error is ScanFailure)
            }
        }
        engine.monitor.refreshSnapshot()
        let status = engine.transferStatus
        #expect(status.queuedUploads.isEmpty)
        #expect(status.activeUploads.isEmpty)
        try await store.read { conn in
            let query = try conn.prepare("SELECT bootstrap_state FROM roots;")
            #expect(try query.step())
            #expect(query.columnText(at: 0) == "freshCreated")
        }
    }

}
