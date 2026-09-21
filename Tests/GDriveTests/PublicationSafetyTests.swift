import Foundation
import Testing
import os
@testable import GDrive

private class PublicationURLProtocol: URLProtocol, @unchecked Sendable {
    static let content = Data(repeating: 0x72, count: 65_544)
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        client?.urlProtocol(self, didReceive: HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Self.content)
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
}

// Only blocksOverwrite uses this protocol, so concurrent downloads cannot affect its count.
private final class BlockedPublicationURLProtocol: PublicationURLProtocol, @unchecked Sendable {
    static let requests = OSAllocatedUnfairLock(initialState: 0)

    override func startLoading() {
        Self.requests.withLock { $0 += 1 }
        super.startLoading()
    }
}

private final class NonRetryableDownloadURLProtocol: URLProtocol, @unchecked Sendable {
    static let errorBody = Data(repeating: 0x78, count: 64 * 1024)

    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        client?.urlProtocol(
            self,
            didReceive: HTTPURLResponse(
                url: request.url!, statusCode: 400, httpVersion: nil, headerFields: nil)!,
            cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Self.errorBody)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}

@Suite("Stable transfer inputs and local publication (A11)")
struct PublicationSafetyTests {
    private func client(in directory: URL, protocolClass: URLProtocol.Type = PublicationURLProtocol.self) throws -> DriveClient {
        let credentials = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(clientId: "test", accessToken: "test", expiresAt: Date().addingTimeInterval(3600))).write(to: credentials)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [protocolClass]
        return DriveClient(auth: try Auth(path: credentials.path), session: URLSession(configuration: configuration), requestsPerSecond: nil)
    }

    @Test("Unsupported remote overwrites fail before any request")
    func blocksOverwrite() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("a11-block-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let client = try client(in: directory, protocolClass: BlockedPublicationURLProtocol.self)
        BlockedPublicationURLProtocol.requests.withLock { $0 = 0 }
        do {
            _ = try await client.updateMultipart(remoteId: "existing", content: Data(), expectedSha256: "unused")
            Issue.record("Unconditional overwrite was allowed")
        } catch DriveError.unsafeOverwrite { }
        #expect(BlockedPublicationURLProtocol.requests.withLock { $0 } == 0)
    }

    @Test("Non-retryable download errors throw and discard their body")
    func nonRetryableDownloadError() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("a11-download-error-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let staging = directory.appendingPathComponent("downloads")
        let destination = directory.appendingPathComponent("file")
        let client = try client(in: directory, protocolClass: NonRetryableDownloadURLProtocol.self)
        let progress = OSAllocatedUnfairLock(initialState: Int64(0))

        do {
            try await client.downloadFile(
                remoteId: "remote",
                destinationURL: destination,
                expectedSha256: nil,
                temporaryDirectory: staging,
                onProgress: { count in progress.withLock { $0 += count } })
            Issue.record("Non-retryable HTTP error was returned as a successful download")
        } catch DriveError.serverError(let statusCode, let message) {
            #expect(statusCode == 400)
            #expect(message.utf8.count == 4096)
        }

        #expect(progress.withLock { $0 } == 0)
        #expect(!FileManager.default.fileExists(atPath: destination.path))
        #expect(try FileManager.default.contentsOfDirectory(atPath: staging.path).isEmpty)
    }

    @Test("A modification during streaming download survives publication and temporary cleanup")
    func downloadRace() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("a11-download-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let destination = directory.appendingPathComponent("file")
        try Data("original".utf8).write(to: destination)
        let newer = Data("concurrent local edit".utf8)
        let client = try client(in: directory)
        let changed = OSAllocatedUnfairLock(initialState: false)
        let staging = directory.appendingPathComponent("downloads")
        do {
            try await client.downloadFile(remoteId: "remote", destinationURL: destination,
                expectedSha256: SyncEngine.computeSha256(of: PublicationURLProtocol.content), temporaryDirectory: staging, onProgress: { _ in
                    changed.withLock { done in
                        if !done {
                            do { try newer.write(to: destination) } catch { Issue.record(error) }
                            done = true
                        }
                    }
                })
            Issue.record("Changed destination was replaced")
        } catch SyncEngineError.localFileModified { }
        #expect(try Data(contentsOf: destination) == newer)
        #expect(try FileManager.default.contentsOfDirectory(atPath: directory.path).allSatisfy { !$0.hasPrefix(".tmp_") })
        #expect(try FileManager.default.contentsOfDirectory(atPath: staging.path).isEmpty)
    }

    @Test("Captured small and large inputs do not follow later source writes",
          arguments: [12, 8 * 1024 * 1024 + 1])
    func stableInput(size: Int) throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("a11-input-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let source = directory.appendingPathComponent("source")
        let original = Data(repeating: 0x41, count: size)
        try original.write(to: source)
        let input = try StableUploadInput.capture(at: source)
        let handle = try FileHandle(forWritingTo: source)
        try handle.write(contentsOf: Data(repeating: 0x42, count: size))
        try handle.close()
        #expect(input.sha256 == SyncEngine.computeSha256(of: original))
        #expect(try (input.data ?? Data(contentsOf: input.fileURL)) == original)
        #expect(throws: (any Error).self) { try input.version.validate(at: source) }
    }

    @Test("Download publication rejects changed, replaced and newly created destinations", arguments: ["changed", "replaced", "created"])
    func rejectsStaleDestination(kind: String) throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("a11-publish-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let destination = directory.appendingPathComponent("file")
        let staging = directory.appendingPathComponent("staging")
        try FileManager.default.createDirectory(at: staging, withIntermediateDirectories: true)
        let download = staging.appendingPathComponent("download")
        if kind != "created" { try Data("original".utf8).write(to: destination) }
        let expected = try LocalFileVersion.read(at: destination)
        if kind == "replaced" { try FileManager.default.removeItem(at: destination) }
        let newer = Data("new local version".utf8)
        try newer.write(to: destination)
        try Data("remote version".utf8).write(to: download)
        let result = try LocalFilePublication.publish(
            download, to: destination, expected: expected,
            expectedSHA256: SyncEngine.computeSha256(of: Data("remote version".utf8)))
        #expect(result == .destinationChanged)
        #expect(try Data(contentsOf: destination) == newer)
        #expect(FileManager.default.fileExists(atPath: download.path))
    }

    @Test("Downloaded bytes are written through the destination path", arguments: [false, true])
    func writesThroughDestination(existing: Bool) throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("a11-publish-ok-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let destination = directory.appendingPathComponent("file")
        let staging = directory.appendingPathComponent("staging")
        try FileManager.default.createDirectory(at: staging, withIntermediateDirectories: true)
        let download = staging.appendingPathComponent("download")
        if existing { try Data("original".utf8).write(to: destination) }
        let expected = try LocalFileVersion.read(at: destination)
        let content = Data("remote version".utf8)
        try content.write(to: download)
        let result = try LocalFilePublication.publish(
            download, to: destination, expected: expected,
            expectedSHA256: SyncEngine.computeSha256(of: content))
        guard case .published(let published) = result else {
            Issue.record("Unchanged destination was rejected")
            return
        }
        if let expected {
            #expect(published.inode == expected.inode)
        }
        #expect(try Data(contentsOf: destination) == content)
        #expect(FileManager.default.fileExists(atPath: download.path))
    }

    @Test("A failure after publication starts is not reported as a local modification")
    func postWriteFailureClassification() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "a11-publish-failure-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let destination = directory.appendingPathComponent("file")
        let source = directory.appendingPathComponent("download")
        let remote = Data("remote version".utf8)
        try Data("original".utf8).write(to: destination)
        try remote.write(to: source)

        do {
            _ = try LocalFilePublication.publish(
                source, to: destination, expected: try LocalFileVersion.read(at: destination),
                expectedSHA256: "incorrect")
            Issue.record("Publication with an incorrect checksum unexpectedly succeeded")
        } catch DriveError.checksumMismatch { }

        #expect(try Data(contentsOf: destination) == remote)
    }

    @Test("Checksum and pre-publication cancellation clean the configured staging folder", arguments: [false, true])
    func failedDownloadCleanup(cancel: Bool) async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("staging-failure-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let staging = directory.appendingPathComponent("downloads/remote-root")
        let destination = directory.appendingPathComponent("file")
        let original = Data("original".utf8)
        try original.write(to: destination)
        let client = try client(in: directory)
        let expected = try LocalFileVersion.read(at: destination)
        do {
            try await client.downloadFileSafely(remoteId: "remote", destinationURL: destination,
                expectedSha256: cancel ? SyncEngine.computeSha256(of: PublicationURLProtocol.content) : "wrong",
                expectedDestination: expected, temporaryDirectory: staging,
                beforePublish: { if cancel { throw CancellationError() } })
            Issue.record("Failed download unexpectedly published")
        } catch is CancellationError { #expect(cancel) } catch DriveError.checksumMismatch { #expect(!cancel) }
        #expect(try Data(contentsOf: destination) == original)
        #expect(try FileManager.default.contentsOfDirectory(atPath: staging.path).isEmpty)
    }
}
