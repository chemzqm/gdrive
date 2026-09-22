import Darwin
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

    @Test("Large downloads use atomic namespace publication", arguments: [false, true])
    func atomicallyPublishesLargeDownload(existing: Bool) throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "a11-publish-large-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let destination = directory.appendingPathComponent("file")
        let download = directory.appendingPathComponent("download")
        if existing { try Data("original".utf8).write(to: destination) }
        let expected = try LocalFileVersion.read(at: destination)
        let content = Data(repeating: 0x52, count: 8 * 1024 * 1024 + 1)
        try content.write(to: download)
        let downloaded = try #require(try LocalFileVersion.read(at: download))

        let result = try LocalFilePublication.publish(
            download, to: destination, expected: expected,
            expectedSHA256: SyncEngine.computeSha256(of: content))
        guard case .published(let published) = result else {
            Issue.record("Unchanged destination was rejected")
            return
        }

        #expect(published.inode == downloaded.inode)
        if let expected { #expect(published.inode != expected.inode) }
        #expect(try Data(contentsOf: destination) == content)
        #expect(!FileManager.default.fileExists(atPath: download.path))
    }

    @Test("Large publication rolls back edits around the swap",
          arguments: [LocalFilePublication.RenameStage.beforeSwap, .afterSwap], [false, true])
    func largePublicationRestoresLocalEdit(stage: LocalFilePublication.RenameStage, databaseHash: Bool) throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("publish-race-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let destination = directory.appendingPathComponent("file")
        let source = directory.appendingPathComponent("download")
        let original = Data("original".utf8)
        let edited = Data("modified".utf8)
        let remote = Data(repeating: 0x52, count: 8 * 1024 * 1024 + 1)
        try original.write(to: destination)
        try remote.write(to: source)
        let expected = try LocalFileVersion.read(at: destination)
        let result = try LocalFilePublication.publish(
            source, to: destination, expected: expected,
            expectedSHA256: SyncEngine.computeSha256(of: remote),
            expectedLocalSHA256: databaseHash ? SyncEngine.computeSha256(of: original) : nil,
            renameHook: { current in
                guard current == stage else { return }
                try edited.write(to: current == .beforeSwap ? destination : source)
            })
        #expect(result == .destinationChanged)
        #expect(try Data(contentsOf: destination) == edited)
        #expect(try Data(contentsOf: source) == remote)
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

        let result = try LocalFilePublication.publish(
            source, to: destination, expected: try LocalFileVersion.read(at: destination),
            expectedSHA256: "incorrect")
        guard case .failedAfterWriteStarted = result else {
            Issue.record("Publication with an incorrect checksum unexpectedly succeeded")
            return
        }

        #expect(try Data(contentsOf: destination) == remote)
    }

    @Test(
        "Publication I/O errors retain verified recovery bytes",
        arguments: [false, true], LocalFilePublication.WriteStage.allCases)
    func publicationIOErrorDoesNotBecomeUserConflict(
        existing: Bool, stage: LocalFilePublication.WriteStage
    ) async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "a11-publish-io-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let credentials = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(
            clientId: "test", accessToken: "test",
            expiresAt: Date().addingTimeInterval(3600))).write(to: credentials)
        let auth = try Auth(path: credentials.path)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [PublicationURLProtocol.self]
        let client = DriveClient(
            auth: auth, session: URLSession(configuration: configuration), requestsPerSecond: nil)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let destination = directory.appendingPathComponent("file")
        if existing { try Data("original local bytes".utf8).write(to: destination) }
        let expected = try LocalFileVersion.read(at: destination)
        let staging = directory.appendingPathComponent("downloads")
        let engine = try await SyncEngine(
            auth: auth, store: store, client: client,
            downloadTemporaryDirectory: staging,
            incrementalScan: SyncEngine.defaultDirectoryScan,
            filePublisher: { source, target, expectedVersion, sha256, localSHA256 in
                try LocalFilePublication.publish(
                    source, to: target, expected: expectedVersion, expectedSHA256: sha256,
                    expectedLocalSHA256: localSHA256,
                    writeHook: { current, _, output in
                        guard current == stage else { return }
                        if current == .duringCopy {
                            let prefix = PublicationURLProtocol.content.prefix(1024)
                            let count = prefix.withUnsafeBytes { bytes in
                                write(output, bytes.baseAddress, bytes.count)
                            }
                            guard count == prefix.count else {
                                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
                            }
                        }
                        throw POSIXError(.EIO)
                    })
            })

        do {
            _ = try await engine.executeFileDownload(
                remoteID: "remote",
                expectedSHA256: SyncEngine.computeSha256(of: PublicationURLProtocol.content),
                destination: destination,
                expectedDestination: expected,
                temporaryDirectory: staging)
            Issue.record("Injected publication failure unexpectedly succeeded")
        } catch let error as LocalFilePublicationRecoveryError {
            #expect(error.destinationPath == destination.path)
            #expect(error.sha256 == SyncEngine.computeSha256(of: PublicationURLProtocol.content))
            #expect(error.size == Int64(PublicationURLProtocol.content.count))
            #expect(try Data(contentsOf: error.stagingURL) == PublicationURLProtocol.content)
        }
        #expect(FileManager.default.fileExists(atPath: destination.path))
        #expect(try FileManager.default.contentsOfDirectory(atPath: staging.path).count == 1)
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
