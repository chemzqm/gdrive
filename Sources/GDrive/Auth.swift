import CryptoKit
import Foundation
import os
#if canImport(Darwin)
import Darwin
#endif

private final class OAuthCallbackCancellation: Sendable {
    private struct State: Sendable {
        var writeFD: Int32
        var cancelled = false
    }

    private let state: OSAllocatedUnfairLock<State>

    init(writeFD: Int32) {
        state = OSAllocatedUnfairLock(initialState: State(writeFD: writeFD))
    }

    var isCancelled: Bool { state.withLock { $0.cancelled } }

    func cancel() {
        state.withLock { state in
            state.cancelled = true
            guard state.writeFD >= 0 else { return }
            var byte: UInt8 = 1
            _ = Darwin.write(state.writeFD, &byte, 1)
        }
    }

    func finish() {
        state.withLock { state in
            if state.writeFD >= 0 { close(state.writeFD) }
            state.writeFD = -1
        }
    }
}

/// Data stored in ~/.gdrive/auth.json.
public struct AuthData: Codable, Sendable {
    public var clientId: String
    public var clientSecret: String?
    public var rootID: String?
    public var scopes: [String]?
    public var accessToken: String?
    public var refreshToken: String?
    public var expiresAt: Date?

    public init(
        clientId: String,
        clientSecret: String? = nil,
        rootID: String? = nil,
        scopes: [String]? = nil,
        accessToken: String? = nil,
        refreshToken: String? = nil,
        expiresAt: Date? = nil
    ) {
        self.clientId = clientId
        self.clientSecret = clientSecret
        self.rootID = rootID
        self.scopes = scopes
        self.accessToken = accessToken
        self.refreshToken = refreshToken
        self.expiresAt = expiresAt
    }
}

/// Manages the minimal authorization flow required for Google Drive.
public actor Auth {
    public static let defaultPath: String = {
        (NSHomeDirectory() as NSString).appendingPathComponent(".gdrive/auth.json")
    }()

    public let fileURL: URL
    private var data: AuthData
    private let session: URLSession
    private var refreshTask: Task<String, Error>?

    public init(path: String = defaultPath) throws {
        try self.init(path: path, session: .shared)
    }

    init(path: String, session: URLSession) throws {
        self.fileURL = URL(fileURLWithPath: path)
        self.session = session
        guard FileManager.default.fileExists(atPath: path) else {
            throw NSError(domain: "GDriveAuth", code: 1, userInfo: [NSLocalizedDescriptionKey: "Credential file not found: \(path)"])
        }
        let rawData = try Data(contentsOf: fileURL)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        self.data = try decoder.decode(AuthData.self, from: rawData)
    }

    /// Get current credentials
    public func authData() -> AuthData {
        data
    }

    /// Returns a valid access token, refreshing it when necessary.
    public func token() async throws -> String {
        if let token = data.accessToken,
           let expiresAt = data.expiresAt,
           expiresAt.timeIntervalSinceNow > 60 {
            return token
        }
        return try await refreshAccessToken()
    }

    /// Refreshes the access token even when its recorded expiry is still valid.
    /// Concurrent callers share one refresh request. If another caller already
    /// replaced `rejectedToken`, its newer token is returned without refreshing again.
    func forceRefresh(rejecting rejectedToken: String? = nil) async throws -> String {
        if let rejectedToken,
           let currentToken = data.accessToken,
           currentToken != rejectedToken,
           let expiresAt = data.expiresAt,
           expiresAt.timeIntervalSinceNow > 60 {
            return currentToken
        }
        return try await refreshAccessToken()
    }

    /// Opens browser authorization and saves the resulting token.
    public func login(timeoutSeconds: Int = 300) async throws {
        let (verifier, challenge) = generatePKCE()
        let state = UUID().uuidString

        // Start a local loopback server to receive the callback.
        let (serverFD, port) = try Self.bindLoopback()
        let seconds = UInt64(max(0, timeoutSeconds))
        let (duration, durationOverflow) = seconds.multipliedReportingOverflow(by: 1_000_000_000)
        let (end, deadlineOverflow) = DispatchTime.now().uptimeNanoseconds.addingReportingOverflow(duration)
        let deadline = durationOverflow || deadlineOverflow ? UInt64.max : end
        let redirectURI = "http://127.0.0.1:\(port)/oauth2callback"

        var components = URLComponents(string: "https://accounts.google.com/o/oauth2/v2/auth")!
        let scopes = (data.scopes ?? [
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/drive"
        ]).joined(separator: " ")

        components.queryItems = [
            URLQueryItem(name: "client_id", value: data.clientId),
            URLQueryItem(name: "redirect_uri", value: redirectURI),
            URLQueryItem(name: "response_type", value: "code"),
            URLQueryItem(name: "scope", value: scopes),
            URLQueryItem(name: "state", value: state),
            URLQueryItem(name: "code_challenge", value: challenge),
            URLQueryItem(name: "code_challenge_method", value: "S256"),
            URLQueryItem(name: "access_type", value: "offline"),
            URLQueryItem(name: "prompt", value: "consent")
        ]
        guard let authURL = components.url else {
            close(serverFD)
            throw NSError(domain: "GDriveAuth", code: 3, userInfo: [NSLocalizedDescriptionKey: "Failed to build authorization link"])
        }

        print("Opening the browser for Google authorization:\n\(authURL.absoluteString)\n")
        let openProcess = Process()
        openProcess.executableURL = URL(fileURLWithPath: "/usr/bin/open")
        openProcess.arguments = [authURL.absoluteString]
        try? openProcess.run()

        // Receive the authorization code.
        let code = try await Self.waitForCode(serverFD: serverFD, expectedState: state, deadline: deadline)

        // Exchange the authorization code for tokens.
        var params = [
            "client_id": data.clientId,
            "code": code,
            "code_verifier": verifier,
            "grant_type": "authorization_code",
            "redirect_uri": redirectURI
        ]
        if let secret = data.clientSecret, !secret.isEmpty {
            params["client_secret"] = secret
        }

        try await exchangeToken(params: params)
    }

    // MARK: - Token HTTP

    private func refresh(with refreshToken: String) async throws -> String {
        var params = [
            "client_id": data.clientId,
            "refresh_token": refreshToken,
            "grant_type": "refresh_token"
        ]
        if let secret = data.clientSecret, !secret.isEmpty {
            params["client_secret"] = secret
        }
        return try await exchangeToken(params: params)
    }

    private func refreshAccessToken() async throws -> String {
        try Task.checkCancellation()
        if let refreshTask {
            let token = try await refreshTask.value
            try Task.checkCancellation()
            return token
        }
        guard let refreshToken = data.refreshToken, !refreshToken.isEmpty else {
            throw NSError(domain: "GDriveAuth", code: 2, userInfo: [NSLocalizedDescriptionKey: "Missing refresh token. Complete authorization first."])
        }
        let task = Task { try await self.refresh(with: refreshToken) }
        refreshTask = task
        defer { refreshTask = nil }
        let token = try await task.value
        try Task.checkCancellation()
        return token
    }

    @discardableResult
    private func exchangeToken(params: [String: String]) async throws -> String {
        var req = URLRequest(url: URL(string: "https://oauth2.googleapis.com/token")!)
        req.httpMethod = "POST"
        req.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        let bodyString = params.map { "\($0.key)=\(urlEncode($0.value))" }.joined(separator: "&")
        req.httpBody = Data(bodyString.utf8)

        let (respData, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let detail = (String(bytes: respData, encoding: .utf8) ?? "Invalid UTF-8 data")
            throw NSError(domain: "GDriveAuth", code: 4, userInfo: [NSLocalizedDescriptionKey: "Token request failed: \(detail)"])
        }

        struct TokenResponse: Decodable {
            let access_token: String
            let expires_in: Double
            let refresh_token: String?
        }

        let decoded = try JSONDecoder().decode(TokenResponse.self, from: respData)
        data.accessToken = decoded.access_token
        data.expiresAt = Date().addingTimeInterval(decoded.expires_in)
        if let refreshToken = decoded.refresh_token, !refreshToken.isEmpty {
            data.refreshToken = refreshToken
        }

        try save()
        return decoded.access_token
    }

    private func save() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let raw = try encoder.encode(data)
        let temporaryURL = fileURL.deletingLastPathComponent().appendingPathComponent(
            ".\(fileURL.lastPathComponent).\(UUID().uuidString).tmp")
        let descriptor = Darwin.open(
            temporaryURL.path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            S_IRUSR | S_IWUSR)
        guard descriptor >= 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }

        let handle = FileHandle(fileDescriptor: descriptor, closeOnDealloc: true)
        var shouldRemoveTemporaryFile = true
        defer {
            if shouldRemoveTemporaryFile {
                try? FileManager.default.removeItem(at: temporaryURL)
            }
        }
        do {
            guard fchmod(descriptor, S_IRUSR | S_IWUSR) == 0 else {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            try handle.write(contentsOf: raw)
            try handle.synchronize()
            try handle.close()
        } catch {
            try? handle.close()
            throw error
        }

        guard Darwin.rename(temporaryURL.path, fileURL.path) == 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        shouldRemoveTemporaryFile = false
    }

    // MARK: - Helpers

    private func generatePKCE() -> (verifier: String, challenge: String) {
        var bytes = [UInt8](repeating: 0, count: 32)
        _ = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
        let verifier = base64URL(bytes)
        let challenge = base64URL(Array(SHA256.hash(data: Data(verifier.utf8))))
        return (verifier, challenge)
    }

    private func base64URL(_ bytes: [UInt8]) -> String {
        Data(bytes).base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private func urlEncode(_ string: String) -> String {
        string.addingPercentEncoding(withAllowedCharacters: .alphanumerics.union(CharacterSet(charactersIn: "-._~"))) ?? string
    }

    static func bindLoopback() throws -> (serverFD: Int32, port: UInt16) {
        let socketFD = socket(AF_INET, SOCK_STREAM, 0)
        guard socketFD >= 0 else { throw NSError(domain: "GDriveAuth", code: 5, userInfo: [NSLocalizedDescriptionKey: "Failed to create socket"]) }
        _ = fcntl(socketFD, F_SETFD, FD_CLOEXEC)

        var reuse: Int32 = 1
        setsockopt(socketFD, SOL_SOCKET, SO_REUSEADDR, &reuse, socklen_t(MemoryLayout<Int32>.size))

        var addr = sockaddr_in()
        addr.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = 0
        addr.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))

        guard withUnsafePointer(to: &addr, {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(socketFD, $0, socklen_t(MemoryLayout<sockaddr_in>.size)) }
        }) == 0 else {
            close(socketFD)
            throw NSError(domain: "GDriveAuth", code: 6, userInfo: [NSLocalizedDescriptionKey: "Failed to bind port"])
        }

        guard listen(socketFD, 32) == 0 else {
            close(socketFD)
            throw NSError(domain: "GDriveAuth", code: 7, userInfo: [NSLocalizedDescriptionKey: "Failed to listen on port"])
        }

        var len = socklen_t(MemoryLayout<sockaddr_in>.size)
        getsockname(socketFD, withUnsafeMutablePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { $0 }
        }, &len)

        return (socketFD, UInt16(bigEndian: addr.sin_port))
    }

    static func waitForCode(serverFD: Int32, expectedState: String, deadline: UInt64) async throws -> String {
        _ = fcntl(serverFD, F_SETFD, FD_CLOEXEC)
        var wake = [Int32](repeating: -1, count: 2)
        guard pipe(&wake) == 0 else {
            let error = POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            close(serverFD)
            throw error
        }
        _ = fcntl(wake[0], F_SETFD, FD_CLOEXEC)
        _ = fcntl(wake[1], F_SETFD, FD_CLOEXEC)
        guard fcntl(wake[1], F_SETFL, O_NONBLOCK) >= 0 else {
            let error = POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            close(wake[0])
            close(wake[1])
            close(serverFD)
            throw error
        }
        let cancellation = OAuthCallbackCancellation(writeFD: wake[1])
        let wakeReadFD = wake[0]
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                DispatchQueue.global(qos: .userInitiated).async {
                    let result: Result<String, Error>
                    do {
                        result = .success(try receiveCode(
                            serverFD: serverFD, wakeFD: wakeReadFD, expectedState: expectedState,
                            deadline: deadline, cancellation: cancellation))
                    } catch {
                        result = .failure(error)
                    }
                    closeQueuedCallbacks(serverFD: serverFD)
                    cancellation.finish()
                    close(wakeReadFD)
                    close(serverFD)
                    continuation.resume(with: result)
                }
            }
        } onCancel: {
            cancellation.cancel()
        }
    }

    private static func closeQueuedCallbacks(serverFD: Int32) {
        guard fcntl(serverFD, F_SETFL, O_NONBLOCK) >= 0 else { return }
        for _ in 0..<32 {
            let clientFD = accept(serverFD, nil, nil)
            if clientFD < 0 { break }
            var drainBuffer = [UInt8](repeating: 0, count: 1024)
            var drained = 0
            while drained < 16384 && read(clientFD, &drainBuffer, drainBuffer.count) > 0 {
                drained += 1024
            }
            close(clientFD)
        }
    }

    private static func closeClient(
        _ clientFD: Int32, clients: inout [Int32: [UInt8]], order: inout [Int32]
    ) {
        clients.removeValue(forKey: clientFD)
        order.removeAll { $0 == clientFD }
        var drainBuffer = [UInt8](repeating: 0, count: 1024)
        var drained = 0
        while drained < 16384 && read(clientFD, &drainBuffer, drainBuffer.count) > 0 {
            drained += 1024
        }
        close(clientFD)
    }

    private static func acceptReadyConnections(
        serverFD: Int32, clients: inout [Int32: [UInt8]], order: inout [Int32]
    ) throws {
        for _ in 0..<32 {
            let clientFD = accept(serverFD, nil, nil)
            if clientFD < 0 {
                if errno == EAGAIN || errno == EWOULDBLOCK || errno == EMFILE || errno == ENFILE { break }
                if errno == EINTR || errno == ECONNABORTED { continue }
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            guard fcntl(clientFD, F_SETFL, O_NONBLOCK) >= 0 else { close(clientFD); continue }
            _ = fcntl(clientFD, F_SETFD, FD_CLOEXEC)
            var noSignal: Int32 = 1
            guard setsockopt(clientFD, SOL_SOCKET, SO_NOSIGPIPE,
                             &noSignal, socklen_t(MemoryLayout<Int32>.size)) == 0 else {
                close(clientFD)
                continue
            }
            if order.count == 32 {
                let oldest = order[0]
                closeClient(oldest, clients: &clients, order: &order)
            }
            clients[clientFD] = []
            order.append(clientFD)
        }
    }

    private static func readReadyConnections(
        _ descriptors: ArraySlice<pollfd>, expectedState: String,
        clients: inout [Int32: [UInt8]], order: inout [Int32]
    ) throws -> String? {
        for descriptor in descriptors where descriptor.revents != 0 {
            let clientFD = descriptor.fd
            guard var received = clients[clientFD] else { continue }
            var buffer = [UInt8](repeating: 0, count: 4096)
            let count = read(clientFD, &buffer, buffer.count)
            if count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) { continue }
            guard count > 0 else {
                closeClient(clientFD, clients: &clients, order: &order)
                continue
            }
            received.append(contentsOf: buffer[0..<count])
            while let first = received.first, first == 10 || first == 13 || first == 32 {
                received.removeFirst()
            }
            if let newline = received.firstIndex(of: 10) {
                let result = callbackResult(in: received[0...newline], expectedState: expectedState)
                switch result {
                case .code(let code):
                    sendCallbackResponse(
                        clientFD: clientFD, status: "200 OK",
                        body: "Google Drive authorization succeeded. You can close this window and return to the terminal.")
                    closeClient(clientFD, clients: &clients, order: &order)
                    return code
                case .error(let err):
                    sendCallbackResponse(
                        clientFD: clientFD, status: "400 Bad Request",
                        body: "Google Drive authorization failed: \(err). You can close this window and return to the terminal.")
                    closeClient(clientFD, clients: &clients, order: &order)
                    throw NSError(domain: "GDriveAuth", code: 13, userInfo: [
                        NSLocalizedDescriptionKey: "Authorization failed: \(err)"])
                case .invalid:
                    sendCallbackResponse(
                        clientFD: clientFD, status: "400 Bad Request",
                        body: "Invalid authorization callback. Please retry.")
                    closeClient(clientFD, clients: &clients, order: &order)
                }
            } else if received.count > 8192 {
                closeClient(clientFD, clients: &clients, order: &order)
            } else {
                clients[clientFD] = received
            }
        }
        return nil
    }

    private static func pollCallbacks(
        serverFD: Int32, wakeFD: Int32, order: [Int32],
        deadline: UInt64, cancellation: OAuthCallbackCancellation
    ) throws -> [pollfd] {
        if cancellation.isCancelled { throw CancellationError() }
        let now = DispatchTime.now().uptimeNanoseconds
        guard now < deadline else {
            throw NSError(domain: "GDriveAuth", code: 8, userInfo: [
                NSLocalizedDescriptionKey: "Timed out waiting for authorization"])
        }
        let remainingMilliseconds = (deadline - now) / 1_000_000 + 1
        let waitMilliseconds = Int32(min(remainingMilliseconds, UInt64(Int32.max)))
        var descriptors = [
            pollfd(fd: serverFD, events: Int16(POLLIN), revents: 0),
            pollfd(fd: wakeFD, events: Int16(POLLIN), revents: 0)
        ]
        descriptors += order.map { pollfd(fd: $0, events: Int16(POLLIN), revents: 0) }
        let ready = poll(&descriptors, nfds_t(descriptors.count), waitMilliseconds)
        if ready < 0 {
            if errno == EINTR {
                if cancellation.isCancelled { throw CancellationError() }
                return []
            }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        if cancellation.isCancelled { throw CancellationError() }
        return ready == 0 ? [] : descriptors
    }

    private static func receiveCode(
        serverFD: Int32, wakeFD: Int32, expectedState: String,
        deadline: UInt64, cancellation: OAuthCallbackCancellation
    ) throws -> String {
        guard fcntl(serverFD, F_SETFL, O_NONBLOCK) >= 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        var clients: [Int32: [UInt8]] = [:]
        var order: [Int32] = []
        defer {
            for clientFD in order {
                var drainBuffer = [UInt8](repeating: 0, count: 1024)
                var drained = 0
                while drained < 16384 && read(clientFD, &drainBuffer, drainBuffer.count) > 0 {
                    drained += 1024
                }
                close(clientFD)
            }
        }

        while true {
            let descriptors = try pollCallbacks(
                serverFD: serverFD, wakeFD: wakeFD, order: order,
                deadline: deadline, cancellation: cancellation)
            if descriptors.isEmpty { continue }
            if descriptors[1].revents != 0 { throw CancellationError() }

            if let code = try readReadyConnections(
                descriptors.dropFirst(2), expectedState: expectedState,
                clients: &clients, order: &order
            ) {
                return code
            }

            if descriptors[0].revents & Int16(POLLIN) != 0 {
                try acceptReadyConnections(serverFD: serverFD, clients: &clients, order: &order)
            } else if descriptors[0].revents != 0 {
                throw NSError(domain: "GDriveAuth", code: 9, userInfo: [
                    NSLocalizedDescriptionKey: "Failed to receive callback"])
            }
        }
    }

    private enum CallbackResult {
        case code(String)
        case error(String)
        case invalid
    }

    private static func callbackResult(in lineBytes: ArraySlice<UInt8>, expectedState: String) -> CallbackResult {
        guard let line = String(bytes: lineBytes, encoding: .utf8) else { return .invalid }
        let parts = line.split(separator: " ")
        guard parts.count == 3, parts[0] == "GET", parts[2].hasPrefix("HTTP/1.") else { return .invalid }
        let urlString = parts[1].hasPrefix("/") ? "http://127.0.0.1" + parts[1] : String(parts[1])
        guard let components = URLComponents(string: urlString),
              components.path == "/oauth2callback" || components.path == "/oauth2callback/",
              let queryItems = components.queryItems,
              queryItems.first(where: { $0.name == "state" })?.value == expectedState else { return .invalid }
        if let code = queryItems.first(where: { $0.name == "code" })?.value, !code.isEmpty {
            return .code(code)
        }
        let err = queryItems.first(where: { $0.name == "error" })?.value?.replacingOccurrences(of: "+", with: " ") ?? "unknown"
        if let desc = queryItems.first(where: { $0.name == "error_description" })?.value, !desc.isEmpty {
            let cleanDesc = desc.replacingOccurrences(of: "+", with: " ")
            return .error("\(err): \(cleanDesc)")
        }
        return .error(err)
    }

    private static func sendCallbackResponse(clientFD: Int32, status: String, body: String) {
        let response = "HTTP/1.1 \(status)\r\nContent-Type: text/plain; charset=utf-8\r\n"
            + "Content-Length: \(body.utf8.count)\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n\(body)"
        let bytes = [UInt8](response.utf8)
        bytes.withUnsafeBufferPointer { buffer in
            guard let base = buffer.baseAddress else { return }
            var offset = 0
            while offset < buffer.count {
                let sent = Darwin.write(clientFD, base + offset, buffer.count - offset)
                if sent > 0 {
                    offset += sent
                } else if sent < 0 && errno == EINTR {
                    continue
                } else if sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) {
                    var pfd = pollfd(fd: clientFD, events: Int16(POLLOUT), revents: 0)
                    if poll(&pfd, 1, 500) <= 0 { break }
                    continue
                } else {
                    break
                }
            }
        }
        _ = shutdown(clientFD, SHUT_WR)
    }
}
