import Foundation
import os
import Testing
@testable import GDrive

private final class DownloadCancellationState: Sendable {
    let started = AsyncSemaphore(count: 0)
    let secondObserved = AsyncSemaphore(count: 0)
    let stopped = OSAllocatedUnfairLock(initialState: false)
    let timedOut = OSAllocatedUnfairLock(initialState: false)
    let mediaRequests = OSAllocatedUnfairLock(initialState: 0)
}

private final class BlockedBootstrapDownloadProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let state = TestHTTPContext<DownloadCancellationState>.value(for: request)!
        let url = request.url!
        if url.query?.contains("alt=media") == true {
            state.mediaRequests.withLock { $0 += 1 }
            state.started.signal()
            // A watchdog bounds a broken cancellation path; it is not synchronization.
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
            json = #"{"files":[{"id":"first","name":"first","mimeType":"text/plain","size":"1","parents":["root"]},{"id":"second","name":"second","mimeType":"text/plain","size":"1","parents":["root"]}]}"#
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
        if request.url?.query?.contains("alt=media") == true {
            TestHTTPContext<DownloadCancellationState>.value(for: request)?.stopped.withLock { $0 = true }
        }
        client?.urlProtocol(self, didFailWithError: URLError(.cancelled))
    }
}

@Suite("Bootstrap download cancellation", .timeLimit(.minutes(1)))
struct BootstrapDownloadCancellationTests {
    @Test("Cancellation reaches active downloads while traversal waits for a slot", arguments: [false, true])
    func cancellationDuringTraversal(usesPublicAPI: Bool) async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let local = directory.appendingPathComponent("local")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(clientId: "test", accessToken: "test",
            expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)
        let context = TestHTTPContext(DownloadCancellationState())
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [BlockedBootstrapDownloadProtocol.self]
        context.configure(configuration)
        let client = DriveClient(auth: auth, session: URLSession(configuration: configuration),
            requestsPerSecond: nil, maxRetries: 0, retrySleep: { _ in })
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let engine = try await SyncEngine(auth: auth, store: store, client: client,
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            conflictDirectory: directory.appendingPathComponent("conflicts"),
            incrementalScan: SyncEngine.defaultDirectoryScan,
            stableFileDigestCapture: { url in
                if url.lastPathComponent == "second" { context.value.secondObserved.signal() }
                return try StableLocalFileDigest.capture(at: url)
            })
        let sync = Task {
            try await engine.syncRemoteToLocalEmpty(localPath: local.path,
                remoteRootId: "root", maxDownloadConcurrency: 1)
        }
        try await context.value.started.wait()
        try await context.value.secondObserved.wait()
        let cancellation: Task<Void, Never>?
        if usesPublicAPI {
            cancellation = Task {
                await engine.cancelSync(localPath: local.path)
            }
        } else {
            cancellation = nil
            sync.cancel()
        }
        await #expect(throws: CancellationError.self) { _ = try await sync.value }
        await cancellation?.value
        #expect(context.value.stopped.withLock { $0 })
        #expect(!context.value.timedOut.withLock { $0 })
        #expect(context.value.mediaRequests.withLock { $0 } == 1)
        let snapshot = engine.transferStatus
        #expect(snapshot.activeDownloads.isEmpty)
        #expect(snapshot.queuedDownloads.isEmpty)
        let token = try await RootSyncCoordinator.shared.acquire(localRootPath: local.path)
        await RootSyncCoordinator.shared.release(token)
    }
}
