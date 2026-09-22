import Foundation
import Testing
import os

@testable import GDrive

private final class RetrySemanticsURLProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = TestHTTPContext<TestRequestHandler>.value(for: request)?.requestHandler else {
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

@Suite("Retry semantics")
struct RetrySemanticsTests {
    private let context = TestHTTPContext(TestRequestHandler())

    private func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        context.configure(configuration)
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

    @Test("HTTP error text preserves UTF-8 and explicitly reports invalid UTF-8", arguments: [false, true])
    func errorBodyDecoding(invalidUTF8: Bool) async throws {
        defer { context.value.requestHandler = nil }
        let expected = invalidUTF8 ? "Invalid UTF-8 data" : "服务器错误: café"
        let body = invalidUTF8 ? Data([0x61, 0xFF, 0x62]) : Data(expected.utf8)
        context.value.handler.withLock { handler in
            handler = { request in
                (HTTPURLResponse(url: request.url!, statusCode: 400, httpVersion: nil, headerFields: nil)!, body)
            }
        }
        defer { context.value.handler.withLock { $0 = nil } }
        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session, requestsPerSecond: nil)
        let request = URLRequest(url: URL(string: "https://www.googleapis.com/drive/v3/files")!)
        do {
            _ = try await client.executeRequest(request, maxRetries: 0)
            Issue.record("Expected HTTP error")
        } catch DriveError.serverError(let status, let message) {
            #expect(status == 400)
            #expect(message == expected)
        }
    }

    @Test("401 forces one refresh and retries with the new token")
    func unauthorizedForcesRefresh() async throws {
        defer { context.value.requestHandler = nil }
        struct State {
            var refreshes = 0
            var authorizationHeaders: [String] = []
        }
        let state = OSAllocatedUnfairLock(initialState: State())
        context.value.handler.withLock { handler in
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
        defer { context.value.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session, requestsPerSecond: nil)
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
        defer { context.value.requestHandler = nil }
        let requestCount = OSAllocatedUnfairLock(initialState: 0)
        context.value.handler.withLock { handler in
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
        defer { context.value.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session, requestsPerSecond: nil)
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
        defer { context.value.requestHandler = nil }
        let requestCount = OSAllocatedUnfairLock(initialState: 0)
        let body = Data("downloaded".utf8)
        context.value.handler.withLock { handler in
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
        defer { context.value.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session, requestsPerSecond: nil)
        let destination = directory.appendingPathComponent("destination")
        let staging = directory.appendingPathComponent("staging", isDirectory: true)

        try await client.downloadFile(
            remoteId: "file", destinationURL: destination, temporaryDirectory: staging)

        #expect(requestCount.withLock { $0 } == 2)
        #expect(try Data(contentsOf: destination) == body)
    }

    @Test("Streaming download retries after a partial response body")
    func downloadRetriesPartialBody() async throws {
        let body = Data("complete download".utf8)
        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let requestCount = OSAllocatedUnfairLock(initialState: 0)
        let client = DriveClient(
            auth: auth, session: session, requestsPerSecond: nil, maxRetries: 1,
            retrySleep: { _ in },
            downloadAttempt: { request, destination, onProgress in
                let attempt = requestCount.withLock { count in
                    count += 1
                    return count
                }
                if attempt == 1 {
                    let partial = Data(body.prefix(4))
                    try partial.write(to: destination)
                    onProgress?(Int64(partial.count))
                    throw URLError(.networkConnectionLost)
                }
                try body.write(to: destination)
                onProgress?(6)
                onProgress?(Int64(body.count - 6))
                return HTTPURLResponse(
                    url: request.url!, statusCode: 200, httpVersion: nil,
                    headerFields: ["Content-Length": String(body.count)])!
            })
        let destination = directory.appendingPathComponent("destination")
        let staging = directory.appendingPathComponent("staging", isDirectory: true)
        let progress = OSAllocatedUnfairLock<Int64>(initialState: 0)

        try await client.downloadFile(
            remoteId: "file", destinationURL: destination, temporaryDirectory: staging,
            onProgress: { delta in progress.withLock { $0 += delta } })

        #expect(requestCount.withLock { $0 } == 2)
        #expect(try Data(contentsOf: destination) == body)
        #expect(progress.withLock { $0 } == Int64(body.count))
    }

    @Test("executeRequest throws distinct rateLimited429 error when retries are exhausted")
    func executeRequestThrowsRateLimited429() async throws {
        defer { context.value.requestHandler = nil }
        context.value.handler.withLock { handler in
            handler = { request in
                (
                    HTTPURLResponse(
                        url: request.url!, statusCode: 429, httpVersion: nil,
                        headerFields: ["Retry-After": "3"])!,
                    Data(#"{"error":{"code":429,"message":"Too Many Requests"}}"#.utf8)
                )
            }
        }
        defer { context.value.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session, requestsPerSecond: nil, maxRetries: 0, retrySleep: { _ in })
        let request = URLRequest(url: URL(string: "https://www.googleapis.com/drive/v3/files")!)

        await #expect(throws: DriveError.rateLimited429(retryAfter: 3.0)) {
            try await client.executeRequest(request)
        }
    }

    @Test("executeRequest throws distinct rateLimited403 error for userRateLimitExceeded")
    func executeRequestThrowsRateLimited403UserQuota() async throws {
        defer { context.value.requestHandler = nil }
        let errorBody = """
        {
          "error": {
            "code": 403,
            "message": "User Rate Limit Exceeded",
            "errors": [
              {
                "domain": "usageLimits",
                "reason": "userRateLimitExceeded",
                "message": "User Rate Limit Exceeded"
              }
            ]
          }
        }
        """
        context.value.handler.withLock { handler in
            handler = { request in
                (
                    HTTPURLResponse(
                        url: request.url!, statusCode: 403, httpVersion: nil,
                        headerFields: ["Retry-After": "5"])!,
                    Data(errorBody.utf8)
                )
            }
        }
        defer { context.value.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session, requestsPerSecond: nil, maxRetries: 0, retrySleep: { _ in })
        let request = URLRequest(url: URL(string: "https://www.googleapis.com/drive/v3/files")!)

        await #expect(throws: DriveError.rateLimited403(reason: "userRateLimitExceeded", retryAfter: 5.0)) {
            try await client.executeRequest(request)
        }
    }

    @Test("executeRequest throws distinct rateLimited403 error for rateLimitExceeded")
    func executeRequestThrowsRateLimited403ProjectLimit() async throws {
        defer { context.value.requestHandler = nil }
        let errorBody = """
        {
          "error": {
            "code": 403,
            "message": "Rate Limit Exceeded",
            "errors": [
              {
                "domain": "usageLimits",
                "reason": "rateLimitExceeded",
                "message": "Rate Limit Exceeded"
              }
            ]
          }
        }
        """
        context.value.handler.withLock { handler in
            handler = { request in
                (
                    HTTPURLResponse(
                        url: request.url!, statusCode: 403, httpVersion: nil,
                        headerFields: nil)!,
                    Data(errorBody.utf8)
                )
            }
        }
        defer { context.value.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session, requestsPerSecond: nil, maxRetries: 0, retrySleep: { _ in })
        let request = URLRequest(url: URL(string: "https://www.googleapis.com/drive/v3/files")!)

        await #expect(throws: DriveError.rateLimited403(reason: "rateLimitExceeded", retryAfter: 2.0)) {
            try await client.executeRequest(request)
        }
    }

    @Test("executeRequest non-rate-limit 403 throws serverError immediately without rate limiting")
    func executeRequestThrowsServerErrorForPermissionDenied() async throws {
        defer { context.value.requestHandler = nil }
        let errorBody = """
        {
          "error": {
            "code": 403,
            "message": "The user does not have sufficient permissions for this file.",
            "errors": [
              {
                "domain": "global",
                "reason": "insufficientFilePermissions",
                "message": "The user does not have sufficient permissions for this file."
              }
            ]
          }
        }
        """
        context.value.handler.withLock { handler in
            handler = { request in
                (
                    HTTPURLResponse(
                        url: request.url!, statusCode: 403, httpVersion: nil,
                        headerFields: nil)!,
                    Data(errorBody.utf8)
                )
            }
        }
        defer { context.value.handler.withLock { $0 = nil } }

        let session = makeSession()
        let (auth, directory) = try makeAuth(session: session)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = DriveClient(auth: auth, session: session, requestsPerSecond: nil, maxRetries: 3, retrySleep: { _ in })
        let request = URLRequest(url: URL(string: "https://www.googleapis.com/drive/v3/files")!)

        await #expect(throws: DriveError.serverError(statusCode: 403, message: errorBody)) {
            try await client.executeRequest(request)
        }
    }
}
