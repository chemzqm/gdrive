import Foundation
import Testing
import os

@testable import GDrive

private final class RetrySemanticsURLProtocol: URLProtocol, @unchecked Sendable {
    typealias Handler = @Sendable (URLRequest) throws -> (HTTPURLResponse, Data)

    static let handler = OSAllocatedUnfairLock<Handler?>(initialState: nil)

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler.withLock({ $0 }) else {
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

@Suite("Retry semantics", .serialized)
struct RetrySemanticsTests {
    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [RetrySemanticsURLProtocol.self]
        return URLSession(configuration: configuration)
    }

    private func makeAuth(session: URLSession) throws -> (Auth, URL) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("gdrive-retry-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let file = directory.appendingPathComponent("auth.json")
        let credentials = AuthData(
            clientId: "client", accessToken: "stale-token", refreshToken: "refresh-token",
            expiresAt: Date().addingTimeInterval(3600))
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(credentials).write(to: file)
        return (try Auth(path: file.path, session: session), directory)
    }

    @Test("401 forces one refresh and retries with the new token")
    func unauthorizedForcesRefresh() async throws {
        struct State {
            var refreshes = 0
            var authorizationHeaders: [String] = []
        }
        let state = OSAllocatedUnfairLock(initialState: State())
        RetrySemanticsURLProtocol.handler.withLock { handler in
            handler = { request in
                let url = try #require(request.url)
                if url.host == "oauth2.googleapis.com" {
                    state.withLock { $0.refreshes += 1 }
                    let response = HTTPURLResponse(
                        url: url, statusCode: 200, httpVersion: nil,
                        headerFields: ["Content-Type": "application/json"])!
                    return (
                        response,
                        Data(#"{"access_token":"fresh-token","expires_in":3600}"#.utf8)
                    )
                }

                let authorization = request.value(forHTTPHeaderField: "Authorization") ?? ""
                state.withLock { $0.authorizationHeaders.append(authorization) }
                let status = authorization == "Bearer stale-token" ? 401 : 200
                return (
                    HTTPURLResponse(
                        url: url, statusCode: status, httpVersion: nil, headerFields: nil)!, Data()
                )
            }
        }
        defer { RetrySemanticsURLProtocol.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session)
        var request = URLRequest(url: URL(string: "https://www.googleapis.com/drive/v3/files")!)
        request.setValue("Bearer stale-token", forHTTPHeaderField: "Authorization")

        let (_, response) = try await client.executeRequest(request, maxRetries: 3)
        let result = state.withLock { $0 }
        #expect(response.statusCode == 200)
        #expect(result.refreshes == 1)
        #expect(result.authorizationHeaders == ["Bearer stale-token", "Bearer fresh-token"])
        #expect(await auth.authData().accessToken == "fresh-token")
    }

    @Test("Cancellation during retry backoff stops before another request")
    func cancellationStopsRetry() async throws {
        let requestCount = OSAllocatedUnfairLock(initialState: 0)
        RetrySemanticsURLProtocol.handler.withLock { handler in
            handler = { request in
                requestCount.withLock { $0 += 1 }
                return (
                    HTTPURLResponse(
                        url: request.url!, statusCode: 503, httpVersion: nil,
                        headerFields: ["Retry-After": "5"])!,
                    Data()
                )
            }
        }
        defer { RetrySemanticsURLProtocol.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session)
        let request = URLRequest(url: URL(string: "https://www.googleapis.com/drive/v3/files")!)
        let task = Task { try await client.executeRequest(request) }

        while requestCount.withLock({ $0 }) == 0 {
            try await Task.sleep(for: .milliseconds(5))
        }
        task.cancel()
        await #expect(throws: CancellationError.self) { try await task.value }
        try await Task.sleep(for: .milliseconds(50))
        #expect(requestCount.withLock { $0 } == 1)
    }

    @Test("Streaming download retries a rate-limited response")
    func downloadRetriesRateLimit() async throws {
        let requestCount = OSAllocatedUnfairLock(initialState: 0)
        let body = Data("downloaded".utf8)
        RetrySemanticsURLProtocol.handler.withLock { handler in
            handler = { request in
                let count = requestCount.withLock { count in
                    count += 1
                    return count
                }
                if count == 1 {
                    return (
                        HTTPURLResponse(
                            url: request.url!, statusCode: 429, httpVersion: nil,
                            headerFields: ["Retry-After": "0"])!,
                        Data()
                    )
                }
                return (
                    HTTPURLResponse(
                        url: request.url!, statusCode: 200, httpVersion: nil,
                        headerFields: ["Content-Length": String(body.count)])!,
                    body
                )
            }
        }
        defer { RetrySemanticsURLProtocol.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session)
        let destination = directory.appendingPathComponent("destination")
        let staging = directory.appendingPathComponent("staging", isDirectory: true)

        try await client.downloadFile(
            remoteId: "file", destinationURL: destination, temporaryDirectory: staging)

        #expect(requestCount.withLock { $0 } == 2)
        #expect(try Data(contentsOf: destination) == body)
    }
}
