import CryptoKit
import Foundation
import Testing
@testable import GDrive

final class DurableIntentURLProtocol: URLProtocol, @unchecked Sendable {
    nonisolated(unsafe) static var requestHandler: (@Sendable (URLRequest) throws -> (HTTPURLResponse, Data))?

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.requestHandler else {
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

@Suite("Durable create intents (A05)", .serialized)
struct DurableIntentTests {
    private func makeAuth(in directory: URL) throws -> Auth {
        let path = directory.appendingPathComponent("auth.json")
        let authData = AuthData(
            clientId: "mock-client",
            rootID: "mock-root",
            accessToken: "mock-token",
            expiresAt: Date().addingTimeInterval(3600)
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(authData).write(to: path)
        return try Auth(path: path.path)
    }

    private func seedRoot(store: StateStore, localPath: String, remoteID: String) async throws -> (Int64, Int64) {
        try await store.write { conn in
            let root = try conn.prepare("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'freshCreated', 1, 1);
            """)
            root.bindText(localPath, at: 1)
            root.bindText(remoteID, at: 2)
            _ = try root.step()
            let rootID = conn.lastInsertRowId
            // A persisted baseline includes its Changes cursor; missing-cursor recovery
            // has a separate reconstruction contract and dedicated coverage.
            try conn.execute("INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) VALUES (\(rootID), 'default', 'drive_changes', 'start', 1);")

            let item = try conn.prepare("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id,
                local_status, remote_status, phase, created_at, updated_at
            ) VALUES (?, NULL, 'root', 'directory', ?, 'present', 'present', 'committed', 1, 1);
            """)
            item.bindInt64(rootID, at: 1)
            item.bindText(remoteID, at: 2)
            _ = try item.step()
            return (rootID, conn.lastInsertRowId)
        }
    }

    @Test("An unfinished multipart intent survives a fresh store and reuses its operation and Drive IDs")
    func unfinishedIntentReusesIdentity() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive-a05-reuse-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let databasePath = directory.appendingPathComponent("state.sqlite").path
        let firstStore = try await StateStore(path: databasePath)
        let (rootID, rootItemID) = try await seedRoot(store: firstStore, localPath: directory.path, remoteID: "remote-root")
        let sha = String(repeating: "a", count: 64)

        let first = try await DurableCreateIntentStore.prepareMultipartUpload(
            store: firstStore,
            rootID: rootID,
            parentItemID: rootItemID,
            name: "small.txt",
            targetParentRemoteID: "remote-root",
            candidateRemoteID: "drive-id-original",
            device: 1,
            inode: 2,
            mtime: 3,
            size: 4,
            sha256: sha
        )

        // A new StateStore represents a new process opening the durable database.
        let restartedStore = try await StateStore(path: databasePath)
        let recovered = try await DurableCreateIntentStore.prepareMultipartUpload(
            store: restartedStore,
            rootID: rootID,
            parentItemID: rootItemID,
            name: "small.txt",
            targetParentRemoteID: "remote-root",
            candidateRemoteID: "drive-id-must-not-replace-original",
            device: 1,
            inode: 2,
            mtime: 3,
            size: 4,
            sha256: sha
        )

        #expect(recovered.operationID == first.operationID)
        #expect(recovered.itemID == first.itemID)
        #expect(recovered.targetRemoteID == "drive-id-original")
        #expect(recovered.expectedSHA256 == sha)
        #expect(recovered.totalBytes == 4)

        let firstDirectory = try await DurableCreateIntentStore.prepareDirectory(
            store: firstStore,
            rootID: rootID,
            parentItemID: rootItemID,
            name: "folder",
            targetParentRemoteID: "remote-root",
            candidateRemoteID: "directory-id-original",
            device: 1,
            inode: 20
        )
        let recoveredDirectory = try await DurableCreateIntentStore.prepareDirectory(
            store: restartedStore,
            rootID: rootID,
            parentItemID: rootItemID,
            name: "folder",
            targetParentRemoteID: "remote-root",
            candidateRemoteID: "directory-id-must-not-replace-original",
            device: 1,
            inode: 20
        )
        #expect(recoveredDirectory.operationID == firstDirectory.operationID)
        #expect(recoveredDirectory.targetRemoteID == "directory-id-original")
    }

    @Test("Durable requests commit only their own generation (A05/A11)", arguments: ["none", "local_generation", "remote_generation", "dirty_generation", "source"], [false, true])
    func requestsStartAfterIntentCommit(invalidation: String, incremental: Bool) async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive-a05-order-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            DurableIntentURLProtocol.requestHandler = nil
            try? FileManager.default.removeItem(at: directory)
        }

        let localRoot = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: localRoot.appendingPathComponent("empty-dir"), withIntermediateDirectories: true)
        let content = Data("durable intent".utf8)
        try content.write(to: localRoot.appendingPathComponent("small.txt"))
        let sha = SHA256.hash(data: content).map { String(format: "%02x", $0) }.joined()

        let databasePath = directory.appendingPathComponent("state.sqlite").path
        let store = try await StateStore(path: databasePath, batchCapacity: 256, batchTimeoutMs: 20)
        let auth = try makeAuth(in: directory)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [DurableIntentURLProtocol.self]
        let client = DriveClient(auth: auth, session: URLSession(configuration: config))
        let idPool = IDPool(initialIds: ["small-file-id", "empty-dir-id"])

        DurableIntentURLProtocol.requestHandler = { request in
            let url = try #require(request.url)
            let response: HTTPURLResponse
            let data: Data

            if url.path.hasSuffix("/changes/startPageToken") {
                response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
                data = Data(#"{"startPageToken":"start-token"}"#.utf8)
                return (response, data)
            }
            if url.path.hasSuffix("/changes") {
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!,
                        Data(#"{"changes":[],"newStartPageToken":"next"}"#.utf8))
            }
            if url.path.hasSuffix("/files/generateIds") {
                response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
                data = try JSONSerialization.data(withJSONObject: [
                    "ids": (0..<1000).map { "prefetched-id-\($0)" }
                ])
                return (response, data)
            }
            if request.httpMethod == "GET", url.path.hasSuffix("/files/remote-root") {
                response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
                data = try JSONSerialization.data(withJSONObject: [
                    "id": "remote-root",
                    "name": "root",
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [],
                    "trashed": false
                ])
                return (response, data)
            }
            if request.httpMethod == "GET", url.path.hasSuffix("/drive/v3/files") {
                response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
                data = Data(#"{"files":[]}"#.utf8)
                return (response, data)
            }

            let operationType = url.path.contains("/upload/") ? "uploadMultipart" : "createDirectory"
            let conn = try SQLiteConnection(path: databasePath, readonly: true)
            let query = try conn.prepare("""
            SELECT target_remote_id, state
            FROM operations
            WHERE operation_type = ?
            ORDER BY created_at DESC
            LIMIT 1;
            """)
            query.bindText(operationType, at: 1)
            guard try query.step(),
                  let targetID = query.columnText(at: 0),
                  query.columnText(at: 1) == "inFlight" else {
                throw SyncEngineError.general("HTTP request escaped before its durable intent committed")
            }

            response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
            if operationType == "uploadMultipart" {
                if invalidation == "source" {
                    try Data("newer local contents".utf8).write(to: localRoot.appendingPathComponent("small.txt"))
                } else if invalidation != "none" {
                    let writer = try SQLiteConnection(path: databasePath)
                    // invalidation is a closed list of column names from the test arguments.
                    try writer.execute("UPDATE items SET \(invalidation) = \(invalidation) + 1 WHERE entry_kind = 'file';")
                }
                data = try JSONSerialization.data(withJSONObject: [
                    "id": targetID,
                    "name": "small.txt",
                    "mimeType": "application/octet-stream",
                    "parents": ["remote-root"],
                    "size": String(content.count),
                    "sha256Checksum": sha,
                    "trashed": false
                ])
            } else {
                data = try JSONSerialization.data(withJSONObject: [
                    "id": targetID,
                    "name": "empty-dir",
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": ["remote-root"],
                    "trashed": false
                ])
            }
            return (response, data)
        }

        let engine = try await SyncEngine(auth: auth, store: store, client: client, idPool: idPool)
        let stats: SyncStats
        if incremental {
            _ = try await seedRoot(store: store, localPath: localRoot.path, remoteID: "remote-root")
            stats = try await engine.syncIncremental(localPath: localRoot.path, remoteRootId: "remote-root", maxConcurrency: 8)
        } else {
            stats = try await engine.syncLocalToRemoteEmpty(localPath: localRoot.path, remoteRootId: "remote-root", maxUploadConcurrency: 8)
        }
        #expect(stats.filesUploaded == (invalidation == "none" ? 1 : 0))
        #expect(stats.filesFailed == (invalidation == "none" ? 0 : 1))
        #expect(stats.directoriesCreated == 1)

        let states: [(String, String)] = try await store.read { conn in
            let query = try conn.prepare("SELECT operation_type, state FROM operations ORDER BY operation_type;")
            var result: [(String, String)] = []
            while try query.step() {
                if let type = query.columnText(at: 0), let state = query.columnText(at: 1) {
                    result.append((type, state))
                }
            }
            return result
        }
        #expect(states.count == 2)
        #expect(states.first { $0.0 == "createDirectory" }?.1 == "completed")
        #expect(states.first { $0.0 == "uploadMultipart" }?.1 == (invalidation == "none" ? "completed" : "unknownOutcome"))
        try await store.read { conn in
            let query = try conn.prepare("SELECT dirty_generation, base_sha256 FROM items WHERE entry_kind = 'file';")
            #expect(try query.step())
            if invalidation == "none" {
                #expect(query.columnInt64(at: 0) == 0)
                #expect(query.columnText(at: 1) == sha)
            } else {
                #expect((query.columnInt64(at: 0) ?? 0) > 0)
                #expect(query.columnText(at: 1) == nil)
            }
        }
    }

    @Test("A fresh engine retries an unknown multipart result with the persisted Drive ID and accepts 409 verification")
    func incrementalRecoveryUsesPersistedID() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive-a05-recover-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            DurableIntentURLProtocol.requestHandler = nil
            try? FileManager.default.removeItem(at: directory)
        }

        let localRoot = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        let fileURL = localRoot.appendingPathComponent("small.txt")
        let content = Data("server already accepted this upload".utf8)
        try content.write(to: fileURL)
        let sha = SHA256.hash(data: content).map { String(format: "%02x", $0) }.joined()
        let attrs = try FileManager.default.attributesOfItem(atPath: fileURL.path)
        let device = (attrs[.systemNumber] as? NSNumber)?.int64Value ?? 0
        let inode = (attrs[.systemFileNumber] as? NSNumber)?.int64Value ?? 0
        let mtime = Int64(((attrs[.modificationDate] as? Date)?.timeIntervalSince1970 ?? 0) * 1_000_000_000)

        let databasePath = directory.appendingPathComponent("state.sqlite").path
        let firstStore = try await StateStore(path: databasePath)
        let (rootID, rootItemID) = try await seedRoot(store: firstStore, localPath: localRoot.path, remoteID: "remote-root")
        let intent = try await DurableCreateIntentStore.prepareMultipartUpload(
            store: firstStore,
            rootID: rootID,
            parentItemID: rootItemID,
            name: "small.txt",
            targetParentRemoteID: "remote-root",
            candidateRemoteID: "persisted-file-id",
            device: device,
            inode: inode,
            mtime: mtime,
            size: Int64(content.count),
            sha256: sha
        )
        await DurableCreateIntentStore.markUnknownOutcome(
            store: firstStore,
            operationID: intent.operationID,
            error: URLError(.networkConnectionLost)
        )

        let auth = try makeAuth(in: directory)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [DurableIntentURLProtocol.self]
        let client = DriveClient(auth: auth, session: URLSession(configuration: config))

        DurableIntentURLProtocol.requestHandler = { request in
            let url = try #require(request.url)
            if url.path.hasSuffix("/changes/startPageToken") {
                return (
                    HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!,
                    Data(#"{"startPageToken":"start-token"}"#.utf8)
                )
            }
            if url.path.hasSuffix("/drive/v3/changes") {
                return (
                    HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!,
                    Data(#"{"newStartPageToken":"next-token","changes":[]}"#.utf8)
                )
            }
            if request.httpMethod == "GET", url.path.hasSuffix("/files/remote-root") {
                let response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
                let data = try JSONSerialization.data(withJSONObject: [
                    "id": "remote-root",
                    "name": "root",
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [],
                    "trashed": false
                ])
                return (response, data)
            }
            if request.httpMethod == "POST", url.path.contains("/upload/") {
                // Models the first process having succeeded remotely before losing its response.
                return (HTTPURLResponse(url: url, statusCode: 409, httpVersion: nil, headerFields: nil)!, Data())
            }
            if request.httpMethod == "GET", url.path.hasSuffix("/files/persisted-file-id") {
                let response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
                let data = try JSONSerialization.data(withJSONObject: [
                    "id": "persisted-file-id",
                    "name": "small.txt",
                    "mimeType": "application/octet-stream",
                    "parents": ["remote-root"],
                    "size": String(content.count),
                    "sha256Checksum": sha,
                    "trashed": false
                ])
                return (response, data)
            }
            throw SyncEngineError.general("Unexpected recovery request: \(request.httpMethod ?? "GET") \(url)")
        }

        let restartedStore = try await StateStore(path: databasePath)
        let engine = try await SyncEngine(
            auth: auth,
            store: restartedStore,
            client: client,
            idPool: IDPool(initialIds: [])
        )
        let stats = try await engine.syncIncremental(
            localPath: localRoot.path,
            remoteRootId: "remote-root",
            maxConcurrency: 4
        )
        #expect(stats.filesUploaded == 1)

        let result: (String?, String?) = try await restartedStore.read { conn in
            let query = try conn.prepare("""
            SELECT operations.state, items.remote_file_id
            FROM operations JOIN items ON items.item_id = operations.item_id
            WHERE operations.operation_id = ?;
            """)
            query.bindText(intent.operationID, at: 1)
            guard try query.step() else { return (nil, nil) }
            return (query.columnText(at: 0), query.columnText(at: 1))
        }
        #expect(result.0 == "completed")
        #expect(result.1 == "persisted-file-id")
    }

    @Test("Concurrent intents use group commit instead of one fsync per small file")
    func intentsRemainBatched() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("gdrive-a05-batch-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = try await StateStore(
            path: directory.appendingPathComponent("state.sqlite").path,
            batchCapacity: 256,
            batchTimeoutMs: 50
        )
        let (rootID, rootItemID) = try await seedRoot(store: store, localPath: directory.path, remoteID: "remote-root")
        await store.writer.resetStats()
        let sha = String(repeating: "b", count: 64)
        let count = 128

        try await withThrowingTaskGroup(of: Void.self) { group in
            for index in 0..<count {
                group.addTask {
                    _ = try await DurableCreateIntentStore.prepareMultipartUpload(
                        store: store,
                        rootID: rootID,
                        parentItemID: rootItemID,
                        name: "file-\(index).txt",
                        targetParentRemoteID: "remote-root",
                        candidateRemoteID: "drive-id-\(index)",
                        device: 1,
                        inode: Int64(index + 1),
                        mtime: 1,
                        size: 1,
                        sha256: sha
                    )
                }
            }
            try await group.waitForAll()
        }

        let stats = await store.getWriterStats()
        #expect(stats.totalItems == count)
        #expect(stats.immediateCommits == 0)
        #expect(stats.totalCommits < count / 4)
    }
}
