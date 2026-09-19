import Foundation
import Testing
import os
@testable import GDrive

private final class PerformanceURLProtocol: URLProtocol, @unchecked Sendable {
    static let firstUpload = OSAllocatedUnfairLock(initialState: UInt64(0))
    static let content = Data(repeating: 65, count: 4096)
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        do {
            let url = request.url!
            let data: Data
            if url.path.hasSuffix("/changes/startPageToken") {
                data = Data(#"{"startPageToken":"start"}"#.utf8)
            } else if url.path.hasSuffix("/changes") {
                data = Data(#"{"changes":[],"newStartPageToken":"next"}"#.utf8)
            } else if url.path.hasSuffix("/files/generateIds") {
                data = try JSONSerialization.data(withJSONObject: ["ids": (0..<1000).map { "prefetch-\($0)" }])
            } else if url.path.contains("/upload/") {
                Self.firstUpload.withLock { if $0 == 0 { $0 = DispatchTime.now().uptimeNanoseconds } }
                let body = (String(bytes: request.extractBodyData ?? Data(), encoding: .utf8) ?? "Invalid UTF-8 data")
                let start = try #require(body.firstIndex(of: "{"))
                let end = try #require(body[start...].firstIndex(of: "}"))
                let metadata = try #require(JSONSerialization.jsonObject(with: Data(body[start...end].utf8)) as? [String: Any])
                data = try JSONSerialization.data(withJSONObject: [
                    "id": metadata["id"]!, "name": metadata["name"]!, "size": "4096",
                    "sha256Checksum": SyncEngine.computeSha256(of: Self.content)
                ])
            } else {
                data = Data(#"{"id":"root","name":"root","mimeType":"application/vnd.google-apps.folder","trashed":false,"files":[]}"#.utf8)
            }
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch { client?.urlProtocol(self, didFailWithError: error) }
    }
    override func stopLoading() {}
}

@Suite("Bounded transfer performance", .enabled(if: ProcessInfo.processInfo.environment["GDRIVE_PERF"] == "1"))
struct TransferPerformanceTests {
    @Test("128 small uploads at production pacing and unchanged scan, five samples")
    func transferAndFastSkip() async throws {
        var uploads: [Double] = []
        var skips: [Double] = []
        var firstUploads: [Double] = []
        var commits: [Int] = []
        let incremental = ProcessInfo.processInfo.environment["GDRIVE_PERF_INCREMENTAL"] == "1"
        let unpaced = ProcessInfo.processInfo.environment["GDRIVE_PERF_UNPACED"] == "1"
        for _ in 0..<5 {
            let directory = FileManager.default.temporaryDirectory.appendingPathComponent("a11-perf-\(UUID().uuidString)")
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            defer { try? FileManager.default.removeItem(at: directory) }
            let local = directory.appendingPathComponent("local")
            try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
            for index in 0..<128 {
                let url = local.appendingPathComponent("file-\(index)")
                try PerformanceURLProtocol.content.write(to: url)
                try FileManager.default.setAttributes([.modificationDate: Date(timeIntervalSince1970: 1_700_000_000)], ofItemAtPath: url.path)
            }
            let authPath = directory.appendingPathComponent("auth.json")
            let encoder = JSONEncoder()
            encoder.dateEncodingStrategy = .iso8601
            try encoder.encode(AuthData(clientId: "test", accessToken: "test", expiresAt: Date().addingTimeInterval(3600))).write(to: authPath)
            let auth = try Auth(path: authPath.path)
            let config = URLSessionConfiguration.ephemeral
            config.protocolClasses = [PerformanceURLProtocol.self]
            let limiter = unpaced
                ? DriveRateLimiter(targetRate: 100_000, burstCapacity: 100_000, minRate: 100_000, maxRate: 100_000)
                : DriveRateLimiter()
            let client = DriveClient(auth: auth, session: URLSession(configuration: config), rateLimiter: limiter)
            let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
            let engine = try await SyncEngine(auth: auth, store: store, client: client, idPool: IDPool(initialIds: (0..<1000).map { "id-\($0)" }))
            if incremental {
                try await store.write { conn in
                    let rootStatement = try conn.prepare("INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at) VALUES ('default', ?, 1, 1, 'root', 'localToRemoteEmpty', 'existingKnown', 1, 1);")
                    rootStatement.bindText(local.path, at: 1)
                    _ = try rootStatement.step()
                    let rootID = conn.lastInsertRowId
                    try conn.execute("INSERT INTO items(root_id, name, entry_kind, remote_file_id, local_status, remote_status, phase, created_at, updated_at) VALUES (\(rootID), 'local', 'directory', 'root', 'present', 'present', 'committed', 1, 1);")
                    try conn.execute("INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) VALUES (\(rootID), 'default', 'drive_changes', 'start', 1);")
                }
            }
            let before = await store.getWriterStats()
            PerformanceURLProtocol.firstUpload.withLock { $0 = 0 }
            let started = DispatchTime.now().uptimeNanoseconds
            let first = incremental
                ? try await engine.syncIncremental(localPath: local.path, remoteRootId: "root", maxConcurrency: unpaced ? 1 : 64)
                : try await engine.syncLocalToRemoteEmpty(localPath: local.path, remoteRootId: "root", maxUploadConcurrency: 64)
            let firstRequest = PerformanceURLProtocol.firstUpload.withLock { $0 }
            #expect(firstRequest > started)
            firstUploads.append(Double(firstRequest - started) / 1e9)
            commits.append(await store.getWriterStats().totalCommits - before.totalCommits)
            let scanCount = max(1, Int(ProcessInfo.processInfo.environment["GDRIVE_PERF_SCANS"] ?? "1") ?? 1)
            #expect(first.filesUploaded == 128)
            #expect(first.filesFailed == 0)
            for _ in 0..<scanCount {
                let second = try await engine.syncIncremental(localPath: local.path, remoteRootId: "root")
                #expect(second.filesSkipped == 128)
                #expect(second.filesUploaded == 0)
                skips.append(second.elapsedSeconds)
            }
            uploads.append(first.elapsedSeconds)
        }
        print("PERF incremental=\(incremental) unpaced=\(unpaced) first uploads=\(firstUploads) median=\(firstUploads.sorted()[2]); commits=\(commits)")
        print("A11 PERF upload seconds: \(uploads); median=\(uploads.sorted()[2])")
        print("A11 PERF unchanged seconds: \(skips); median=\(skips.sorted()[skips.count / 2])")
    }
}
