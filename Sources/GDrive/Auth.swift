import CryptoKit
import Foundation
#if canImport(Darwin)
import Darwin
#endif

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

    public init(path: String = defaultPath) throws {
        self.fileURL = URL(fileURLWithPath: path)
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
        guard let refreshToken = data.refreshToken, !refreshToken.isEmpty else {
            throw NSError(domain: "GDriveAuth", code: 2, userInfo: [NSLocalizedDescriptionKey: "Missing refresh token. Complete authorization first."])
        }
        return try await refresh(with: refreshToken)
    }

    /// Opens browser authorization and saves the resulting token.
    public func login(timeoutSeconds: Int = 300) async throws {
        let (verifier, challenge) = generatePKCE()
        let state = UUID().uuidString

        // Start a local loopback server to receive the callback.
        let (serverFD, port) = try bindLoopback()
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
        let code = try await waitForCode(serverFD: serverFD, expectedState: state, timeout: timeoutSeconds)

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

    @discardableResult
    private func exchangeToken(params: [String: String]) async throws -> String {
        var req = URLRequest(url: URL(string: "https://oauth2.googleapis.com/token")!)
        req.httpMethod = "POST"
        req.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        let bodyString = params.map { "\($0.key)=\(urlEncode($0.value))" }.joined(separator: "&")
        req.httpBody = Data(bodyString.utf8)

        let (respData, response) = try await URLSession.shared.data(for: req)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let detail = String(decoding: respData, as: UTF8.self)
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
        if let rt = decoded.refresh_token, !rt.isEmpty {
            data.refreshToken = rt
        }

        try save()
        return decoded.access_token
    }

    private func save() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let raw = try encoder.encode(data)
        try raw.write(to: fileURL, options: .atomic)
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

    private func urlEncode(_ s: String) -> String {
        s.addingPercentEncoding(withAllowedCharacters: .alphanumerics.union(CharacterSet(charactersIn: "-._~"))) ?? s
    }

    private func bindLoopback() throws -> (serverFD: Int32, port: UInt16) {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw NSError(domain: "GDriveAuth", code: 5, userInfo: [NSLocalizedDescriptionKey: "Failed to create socket"]) }

        var reuse: Int32 = 1
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, socklen_t(MemoryLayout<Int32>.size))

        var addr = sockaddr_in()
        addr.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = 0
        addr.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))

        guard withUnsafePointer(to: &addr, {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size)) }
        }) == 0 else {
            close(fd)
            throw NSError(domain: "GDriveAuth", code: 6, userInfo: [NSLocalizedDescriptionKey: "Failed to bind port"])
        }

        guard listen(fd, 1) == 0 else {
            close(fd)
            throw NSError(domain: "GDriveAuth", code: 7, userInfo: [NSLocalizedDescriptionKey: "Failed to listen on port"])
        }

        var len = socklen_t(MemoryLayout<sockaddr_in>.size)
        getsockname(fd, withUnsafeMutablePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { $0 }
        }, &len)

        return (fd, UInt16(bigEndian: addr.sin_port))
    }

    private func waitForCode(serverFD: Int32, expectedState: String, timeout: Int) async throws -> String {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                defer { close(serverFD) }

                var pfd = pollfd(fd: serverFD, events: Int16(POLLIN), revents: 0)
                let pollRet = poll(&pfd, 1, Int32(timeout * 1000))
                guard pollRet > 0 else {
                    continuation.resume(throwing: NSError(domain: "GDriveAuth", code: 8, userInfo: [NSLocalizedDescriptionKey: "Timed out waiting for authorization"]))
                    return
                }

                let clientFD = accept(serverFD, nil, nil)
                guard clientFD >= 0 else {
                    continuation.resume(throwing: NSError(domain: "GDriveAuth", code: 9, userInfo: [NSLocalizedDescriptionKey: "Failed to receive callback"]))
                    return
                }
                defer { close(clientFD) }

                var buffer = [UInt8](repeating: 0, count: 4096)
                let n = read(clientFD, &buffer, buffer.count)
                guard n > 0, let reqText = String(bytes: buffer[0..<n], encoding: .utf8) else {
                    continuation.resume(throwing: NSError(domain: "GDriveAuth", code: 10, userInfo: [NSLocalizedDescriptionKey: "Failed to read callback data"]))
                    return
                }

                guard let line = reqText.split(separator: "\r\n").first,
                      let urlPart = line.split(separator: " ").dropFirst().first,
                      let components = URLComponents(string: "http://127.0.0.1" + urlPart),
                      let queryItems = components.queryItems else {
                    continuation.resume(throwing: NSError(domain: "GDriveAuth", code: 11, userInfo: [NSLocalizedDescriptionKey: "Failed to parse callback link"]))
                    return
                }

                let query = Dictionary(queryItems.compactMap { item in item.value.map { (item.name, $0) } }, uniquingKeysWith: { a, _ in a })
                guard query["state"] == expectedState else {
                    continuation.resume(throwing: NSError(domain: "GDriveAuth", code: 12, userInfo: [NSLocalizedDescriptionKey: "OAuth state validation failed"]))
                    return
                }

                guard let code = query["code"], !code.isEmpty else {
                    let err = query["error"] ?? "unknown"
                    continuation.resume(throwing: NSError(domain: "GDriveAuth", code: 13, userInfo: [NSLocalizedDescriptionKey: "Authorization failed: \(err)"]))
                    return
                }

                let body = "Google Drive authorization succeeded. You can close this window and return to the terminal."
                let resp = "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: \(body.utf8.count)\r\nConnection: close\r\n\r\n\(body)"
                _ = resp.withCString { Darwin.write(clientFD, $0, strlen($0)) }

                continuation.resume(returning: code)
            }
        }
    }
}
