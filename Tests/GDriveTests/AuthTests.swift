import Darwin
import Foundation
import Testing
@testable import GDrive

private func connectToCallback(port: UInt16) throws -> Int32 {
    let clientFD = socket(AF_INET, SOCK_STREAM, 0)
    guard clientFD >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
    var address = sockaddr_in()
    address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
    address.sin_family = sa_family_t(AF_INET)
    address.sin_port = port.bigEndian
    address.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))
    let connected = withUnsafePointer(to: &address) {
        $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
            connect(clientFD, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
        }
    }
    guard connected == 0 else {
        let error = POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        close(clientFD)
        throw error
    }
    return clientFD
}

private func sendCallback(port: UInt16, target: String) throws {
    let clientFD = try connectToCallback(port: port)
    defer { close(clientFD) }
    let request = "GET \(target) HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
    let sent = request.withCString { Darwin.write(clientFD, $0, strlen($0)) }
    #expect(sent == request.utf8.count)
}

private func callbackSocketWasClosed(_ clientFD: Int32) -> Bool {
    var descriptor = pollfd(fd: clientFD, events: Int16(POLLIN), revents: 0)
    guard poll(&descriptor, 1, 1000) > 0 else { return false }
    var byte: UInt8 = 0
    return read(clientFD, &byte, 1) == 0
}

private func callbackListenerWasClosed(port: UInt16) -> Bool {
    guard let clientFD = try? connectToCallback(port: port) else { return true }
    close(clientFD)
    return false
}

@Suite("Auth Tests")
struct AuthTests {
    @Test("A silent or invalid loopback connection does not block a later valid callback")
    func callbackWaitsForValidConnection() async throws {
        let (serverFD, port) = try Auth.bindLoopback()
        let deadline = DispatchTime.now().uptimeNanoseconds + 3_000_000_000
        let wait = Task {
            try await Auth.waitForCode(serverFD: serverFD, expectedState: "expected", deadline: deadline)
        }
        let silentFD = try connectToCallback(port: port)
        defer { close(silentFD) }
        try sendCallback(port: port, target: "/oauth2callback?state=wrong&code=early")
        try sendCallback(port: port, target: "/oauth2callback?state=expected&code=accepted")

        #expect(try await wait.value == "accepted")
        #expect(callbackSocketWasClosed(silentFD))
        #expect(callbackListenerWasClosed(port: port))
    }

    @Test("The callback deadline closes a connected client that sends no data")
    func callbackTimesOutWithSilentClient() async throws {
        let (serverFD, port) = try Auth.bindLoopback()
        let deadline = DispatchTime.now().uptimeNanoseconds + 300_000_000
        let wait = Task {
            try await Auth.waitForCode(serverFD: serverFD, expectedState: "expected", deadline: deadline)
        }
        let silentFD = try connectToCallback(port: port)
        defer { close(silentFD) }

        do {
            _ = try await wait.value
            Issue.record("Expected the callback deadline to expire")
        } catch {
            #expect((error as NSError).domain == "GDriveAuth")
            #expect((error as NSError).code == 8)
        }
        #expect(callbackSocketWasClosed(silentFD))
        #expect(callbackListenerWasClosed(port: port))
    }

    @Test("Cancelling callback wait closes all accepted clients")
    func callbackCancellationClosesClients() async throws {
        let (serverFD, port) = try Auth.bindLoopback()
        let deadline = DispatchTime.now().uptimeNanoseconds + 3_000_000_000
        let wait = Task {
            try await Auth.waitForCode(serverFD: serverFD, expectedState: "expected", deadline: deadline)
        }
        let first = try connectToCallback(port: port)
        let second = try connectToCallback(port: port)
        defer { close(first); close(second) }
        wait.cancel()

        await #expect(throws: CancellationError.self) { try await wait.value }
        #expect(callbackSocketWasClosed(first))
        #expect(callbackSocketWasClosed(second))
        #expect(callbackListenerWasClosed(port: port))
    }

    @Test("OAuth error callback fails fast with authorization error")
    func callbackFailsFastOnOAuthError() async throws {
        let (serverFD, port) = try Auth.bindLoopback()
        let deadline = DispatchTime.now().uptimeNanoseconds + 5_000_000_000
        let wait = Task {
            try await Auth.waitForCode(serverFD: serverFD, expectedState: "expected", deadline: deadline)
        }
        let silentFD = try connectToCallback(port: port)
        defer { close(silentFD) }
        try sendCallback(port: port, target: "/oauth2callback?state=expected&error=access_denied")

        do {
            _ = try await wait.value
            Issue.record("Expected authorization to fail immediately on OAuth error")
        } catch {
            let nsError = error as NSError
            #expect(nsError.domain == "GDriveAuth")
            #expect(nsError.code == 13)
            #expect(nsError.localizedDescription.contains("access_denied"))
        }
        #expect(callbackSocketWasClosed(silentFD))
        #expect(callbackListenerWasClosed(port: port))
    }

    @Test("AuthData round trip")
    func testAuthData() throws {
        let auth = AuthData(
            clientId: "test_id",
            clientSecret: "test_secret",
            rootID: "test_root",
            scopes: ["openid"],
            accessToken: "access",
            refreshToken: "refresh",
            expiresAt: Date()
        )
        let data = try JSONEncoder().encode(auth)
        let decoded = try JSONDecoder().decode(AuthData.self, from: data)
        #expect(decoded.clientId == "test_id")
        #expect(decoded.rootID == "test_root")
        #expect(decoded.accessToken == "access")
    }

    @Test("Fetch 1000 IDs from Google Drive files.generateIds")
    func testGenerate1000Ids() async throws {
        let auth = try Auth()
        let api = DriveClient(auth: auth, requestsPerSecond: nil)
        let ids = try await api.generateIds(count: 1000)
        #expect(ids.count == 1000)
        #expect(Set(ids).count == 1000)
        print("Retrieved 1000 Google Drive IDs: \(ids.prefix(3).joined(separator: ", ")) ... \(ids.suffix(3).joined(separator: ", "))")
    }
}
