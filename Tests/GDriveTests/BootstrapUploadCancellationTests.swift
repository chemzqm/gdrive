import Foundation
import os
import Testing
@testable import GDrive

private final class UploadCancellationState: Sendable {
    let started = AsyncSemaphore(count: 0)
    let stopped = OSAllocatedUnfairLock(initialState: false)
    let timedOut = OSAllocatedUnfairLock(initialState: false)
    let multipartRequests = OSAllocatedUnfairLock(initialState: 0)
}

private final class BlockedBootstrapUploadProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let state = TestHTTPContext<UploadCancellationState>.value(for: request)!
        let url = request.url!
        if url.query?.contains("uploadType=multipart") == true {
            state.multipartRequests.withLock { $0 += 1 }
            state.started.signal()
            DispatchQueue.global().asyncAfter(deadline: .now() + 10) { [weak self] in
                guard let self, !state.stopped.withLock({ $0 }) else { return }
                state.timedOut.withLock { $0 = true }
                self.client?.urlProtocol(self, didFailWithError: URLError(.timedOut))
            }
            return
        }
        let json: String
        if url.path.hasSuffix("/changes/startPageToken") {
            json = #"{"startPageToken":"initial"}"#
        } else if url.path.hasSuffix("/files/root") {
            json = #"{"id":"root","name":"root","mimeType":"application/vnd.google-apps.folder"}"#
        } else if url.path.hasSuffix("/files") {
            json = #"{"files":[]}"#
        } else {
            client?.urlProtocol(self, didFailWithError: URLError(.badURL))
            return
        }
        let response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Data(json.utf8))
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {
        guard request.url?.query?.contains("uploadType=multipart") == true else { return }
        TestHTTPContext<UploadCancellationState>.value(for: request)?.stopped.withLock { $0 = true }
        client?.urlProtocol(self, didFailWithError: URLError(.cancelled))
    }
}

@Suite("Bootstrap upload cancellation", .timeLimit(.minutes(1)))
struct BootstrapUploadCancellationTests {
    @Test("Public cancellation stops one active multipart request and admits no queued file")
    func cancellationStopsMultipartAndQueuedUpload() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let local = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        try Data("first".utf8).write(to: local.appendingPathComponent("first.txt"))
        try Data("second".utf8).write(to: local.appendingPathComponent("second.txt"))
        defer { try? FileManager.default.removeItem(at: directory) }

        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(
            clientId: "test", accessToken: "test",
            expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)
        let context = TestHTTPContext(UploadCancellationState())
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [BlockedBootstrapUploadProtocol.self]
        context.configure(configuration)
        let client = DriveClient(auth: auth, session: URLSession(configuration: configuration),
                                 requestsPerSecond: nil, maxRetries: 0, retrySleep: { _ in })
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let engine = try await SyncEngine(
            auth: auth, store: store, client: client,
            idPool: IDPool(api: nil, initialIds: ["first-id", "second-id"]),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            conflictDirectory: directory.appendingPathComponent("conflicts"))

        let sync = Task {
            try await engine.syncLocalToRemoteEmpty(
                localPath: local.path, remoteRootId: "root", maxUploadConcurrency: 1)
        }
        try await context.value.started.wait()
        let cancellation = Task { await engine.cancelSync(localPath: local.path) }
        await #expect(throws: CancellationError.self) { _ = try await sync.value }
        await cancellation.value

        #expect(context.value.stopped.withLock { $0 })
        #expect(!context.value.timedOut.withLock { $0 })
        #expect(context.value.multipartRequests.withLock { $0 } == 1)
        #expect(engine.transferStatus.activeUploads.isEmpty)
        #expect(engine.transferStatus.queuedUploads.isEmpty)
    }
}

private final class TransferCancellationState: Sendable {
    let started = AsyncSemaphore(count: 0)
    let stopped = OSAllocatedUnfairLock(initialState: false)
    let timedOut = OSAllocatedUnfairLock(initialState: false)
}

private final class BlockedTransferProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let state = TestHTTPContext<TransferCancellationState>.value(for: request)!
        let url = request.url!
        if url.query?.contains("uploadType=resumable") == true {
            let response = HTTPURLResponse(
                url: url, statusCode: 200, httpVersion: nil,
                headerFields: ["Location": "https://upload.test/session"])!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocolDidFinishLoading(self)
            return
        }
        state.started.signal()
        DispatchQueue.global().asyncAfter(deadline: .now() + 10) { [weak self] in
            guard let self, !state.stopped.withLock({ $0 }) else { return }
            state.timedOut.withLock { $0 = true }
            self.client?.urlProtocol(self, didFailWithError: URLError(.timedOut))
        }
    }

    override func stopLoading() {
        TestHTTPContext<TransferCancellationState>.value(for: request)?.stopped.withLock { $0 = true }
        client?.urlProtocol(self, didFailWithError: URLError(.cancelled))
    }
}

@Suite("Managed transfer cancellation", .timeLimit(.minutes(1)))
struct ManagedTransferCancellationTests {
    @Test("Public cancellation stops multipart and resumable body transports", arguments: ["multipart", "resumable"])
    func cancelsBodyTransport(kind: String) async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(
            clientId: "test", accessToken: "test",
            expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)
        let context = TestHTTPContext(TransferCancellationState())
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [BlockedTransferProtocol.self]
        context.configure(configuration)
        let client = DriveClient(auth: auth, session: URLSession(configuration: configuration),
                                 requestsPerSecond: nil, maxRetries: 0, retrySleep: { _ in })
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let engine = try await SyncEngine(auth: auth, store: store, client: client)
        let localRoot = directory.appendingPathComponent("root").path
        let control = SyncRunControl()
        let token = try await RootSyncCoordinator.shared.acquire(
            localRootPath: localRoot, control: control)
        let body = Data("body".utf8)
        let sha = SyncEngine.computeSha256(of: body)
        let transfer = Task {
            try await SyncRunControl.$current.withValue(control) {
                if kind == "multipart" {
                    _ = try await client.uploadMultipart(
                        name: "file", parentId: "parent", remoteId: "file-id",
                        content: body, expectedSha256: sha)
                } else {
                    let session = try await client.initiateResumableUpload(
                        name: "file", parentId: "parent", remoteId: "file-id", totalBytes: Int64(body.count))
                    _ = try await client.uploadResumableChunk(
                        sessionURL: session, chunkData: body, offset: 0, totalBytes: Int64(body.count))
                }
            }
        }
        try await context.value.started.wait()
        let cancellation = Task { await engine.cancelSync(localPath: localRoot) }
        await #expect(throws: CancellationError.self) { try await transfer.value }
        await RootSyncCoordinator.shared.release(token)
        await cancellation.value
        #expect(context.value.stopped.withLock { $0 })
        #expect(!context.value.timedOut.withLock { $0 })
    }
}

private final class MetadataAndTransferState: @unchecked Sendable {
    let metadataStarted = AsyncSemaphore(count: 0)
    let bodyStarted = AsyncSemaphore(count: 0)
    let bodyStopped = AsyncSemaphore(count: 0)
    let metadataStopped = OSAllocatedUnfairLock(initialState: false)
    private let metadataResponse = OSAllocatedUnfairLock<(@Sendable () -> Void)?>(initialState: nil)

    func setMetadataResponse(_ response: @escaping @Sendable () -> Void) {
        metadataResponse.withLock { $0 = response }
    }

    func releaseMetadataResponse() {
        let response = metadataResponse.withLock { response -> (@Sendable () -> Void)? in
            defer { response = nil }
            return response
        }
        response?()
    }
}

private final class MetadataAndTransferProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let state = TestHTTPContext<MetadataAndTransferState>.value(for: request)!
        let url = request.url!
        if url.query?.contains("uploadType=multipart") == true {
            state.bodyStarted.signal()
            return
        }
        guard request.httpMethod == "POST", url.path.hasSuffix("/files") else {
            client?.urlProtocol(self, didFailWithError: URLError(.badURL))
            return
        }
        state.setMetadataResponse { [weak self] in
            guard let self, let client = self.client else { return }
            let response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil,
                headerFields: ["Content-Type": "application/json"])!
            let body = Data(#"{"id":"directory-id","name":"directory","mimeType":"application/vnd.google-apps.folder","parents":["parent"]}"#.utf8)
            client.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client.urlProtocol(self, didLoad: body)
            client.urlProtocolDidFinishLoading(self)
        }
        state.metadataStarted.signal()
    }

    override func stopLoading() {
        let state = TestHTTPContext<MetadataAndTransferState>.value(for: request)!
        if request.url?.query?.contains("uploadType=multipart") == true {
            state.bodyStopped.signal()
        } else {
            state.metadataStopped.withLock { $0 = true }
        }
        client?.urlProtocol(self, didFailWithError: URLError(.cancelled))
    }
}

extension ManagedTransferCancellationTests {
    @Test("Cancellation preserves an in-flight directory request and its receipt")
    func cancellationLeavesMetadataAndReceiptAlive() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(
            clientId: "test", accessToken: "test",
            expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)
        let context = TestHTTPContext(MetadataAndTransferState())
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MetadataAndTransferProtocol.self]
        context.configure(configuration)
        let client = DriveClient(auth: auth, session: URLSession(configuration: configuration),
                                 requestsPerSecond: nil, maxRetries: 0, retrySleep: { _ in })
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let engine = try await SyncEngine(auth: auth, store: store, client: client)
        let localRoot = directory.appendingPathComponent("root").path
        let receiptTarget = try await store.write { conn -> (Int64, Int64) in
            let root = try conn.prepare("""
                INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, 'remote-root', 'localToRemoteEmpty', 'freshCreated', 1, 1);
                """)
            defer { root.reset() }
            root.bindText(localRoot, at: 1)
            _ = try root.step()
            let rootID = conn.lastInsertRowId
            let item = try conn.prepare("""
                INSERT INTO items(root_id, name, entry_kind, remote_file_id, created_at, updated_at)
                VALUES (?, 'receipt', 'directory', 'receipt-id', 1, 1);
                """)
            defer { item.reset() }
            item.bindInt64(rootID, at: 1)
            _ = try item.step()
            return (rootID, conn.lastInsertRowId)
        }
        let control = SyncRunControl()
        let token = try await RootSyncCoordinator.shared.acquire(
            localRootPath: localRoot, control: control)
        let receiptFinished = AsyncSemaphore(count: 0)
        let metadata = Task {
            try await SyncRunControl.$current.withValue(control) {
                _ = try await client.createDirectory(
                    name: "directory", parentId: "parent", remoteId: "directory-id")
                try await store.write { conn in
                    let receipt = try conn.prepare("""
                        INSERT INTO operations(operation_id, root_id, item_id, operation_type, state, created_at, updated_at)
                        VALUES ('metadata-receipt', ?, ?, 'createDirectory', 'completed', 1, 1);
                        """)
                    defer { receipt.reset() }
                    receipt.bindInt64(receiptTarget.0, at: 1)
                    receipt.bindInt64(receiptTarget.1, at: 2)
                    _ = try receipt.step()
                }
                receiptFinished.signal()
            }
        }
        let body = Data("body".utf8)
        let transfer = Task {
            try await SyncRunControl.$current.withValue(control) {
                _ = try await client.uploadMultipart(
                    name: "file", parentId: "parent", remoteId: "file-id",
                    content: body, expectedSha256: SyncEngine.computeSha256(of: body))
            }
        }

        try await context.value.metadataStarted.wait()
        try await context.value.bodyStarted.wait()
        let firstCancellation = Task { await engine.cancelSync(localPath: localRoot) }
        let secondCancellation = Task { await engine.cancelSync(localPath: localRoot) }
        try await context.value.bodyStopped.wait()
        #expect(!context.value.metadataStopped.withLock { $0 })
        context.value.releaseMetadataResponse()
        try await metadata.value
        try await receiptFinished.wait()
        await #expect(throws: CancellationError.self) { try await transfer.value }
        let receiptCount = try await store.read { conn in
            let query = try conn.prepare("SELECT COUNT(*) FROM operations WHERE operation_id = 'metadata-receipt';")
            defer { query.reset() }
            _ = try query.step()
            return query.columnInt64(at: 0)
        }
        #expect(receiptCount == 1)

        await RootSyncCoordinator.shared.release(token)
        await firstCancellation.value
        await secondCancellation.value
    }
}
