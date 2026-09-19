import Foundation
import Testing
@testable import GDrive

private actor BootstrapScanBarrier {
    private var batchesConsumed = 0
    private var scanCompleted = false
    private var released = false
    private var waiter: CheckedContinuation<Void, Never>?

    func pauseAfterFirstBatchIfNeeded() async {
        batchesConsumed += 1
        guard batchesConsumed == 1, !released else { return }
        await withCheckedContinuation { continuation in
            if released {
                continuation.resume()
            } else {
                waiter = continuation
            }
        }
    }

    func markScanCompleted() {
        scanCompleted = true
    }

    func snapshot() -> (batchesConsumed: Int, scanCompleted: Bool) {
        (batchesConsumed, scanCompleted)
    }

    func release() {
        released = true
        let continuation = waiter
        waiter = nil
        continuation?.resume()
    }
}

@Suite("Bootstrap streaming live regression")
struct StreamingLiveTests {
    @Test("Bootstrap uploads the first scanned file before the directory scan completes")
    func bootstrapUploadsBeforeScanCompletion() async throws {
        let auth = try Auth()
        let configuredRootIDValue = await auth.authData().rootID
        let configuredRootID = try #require(
            configuredRootIDValue,
            "Live streaming regression requires rootID in the configured GDrive credentials"
        )
        let client = DriveClient(auth: auth)

        let localRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("gdrive_streaming_live_\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: localRoot) }

        let contents: [String: Data] = [
            "first.txt": Data("first streaming payload".utf8),
            "second.txt": Data("second streaming payload".utf8),
            "third.txt": Data("third streaming payload".utf8)
        ]
        for (name, data) in contents {
            try data.write(to: localRoot.appendingPathComponent(name))
        }
        let expectedSHA = contents.mapValues(SyncEngine.computeSha256(of:))

        let databasePath = FileManager.default.temporaryDirectory
            .appendingPathComponent("gdrive_streaming_live_\(UUID().uuidString).sqlite").path
        defer {
            for suffix in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: databasePath + suffix)
            }
        }

        let barrier = BootstrapScanBarrier()
        let store = try await StateStore(path: databasePath)
        let engine = try await SyncEngine(
            auth: auth,
            store: store,
            client: client,
            incrementalScan: { request, consume in
                var singleEntryRequest = request
                singleEntryRequest.options.workers = 1
                singleEntryRequest.options.batchCapacity = 1
                do {
                    try await SyncEngine.defaultDirectoryScan(singleEntryRequest) { batch in
                        try await consume(batch)
                        await barrier.pauseAfterFirstBatchIfNeeded()
                    }
                    await barrier.markScanCompleted()
                } catch {
                    await barrier.markScanCompleted()
                    throw error
                }
            }
        )

        // Create the remote fixture only after all local setup that can throw, so
        // every subsequent exit path can await its cleanup explicitly.
        let remoteID = try #require(try await client.generateIds(count: 1).first)
        let remoteName = "streaming_live_\(UUID().uuidString)"
        let remoteRoot = try await client.createDirectory(
            name: remoteName,
            parentId: configuredRootID,
            remoteId: remoteID
        )

        let syncTask = Task {
            try await engine.syncLocalToRemoteEmpty(
                localPath: localRoot.path,
                remoteRootId: remoteRoot.id,
                maxUploadConcurrency: 1
            )
        }

        var firstUploaded: DriveFile?
        var pollingError: Error?
        let deadline = Date().addingTimeInterval(30)
        func pollFirstUpload() async {
            do {
                while Date() < deadline {
                    let children = try await client.listChildren(parentId: remoteRoot.id)
                    if let file = children.first(where: { !$0.isDirectory }) {
                        firstUploaded = file
                        break
                    }
                    try await Task.sleep(for: .milliseconds(200))
                }
            } catch {
                pollingError = error
            }
        }
        await pollFirstUpload()

        func verifyFirstUpload() async {
            let heldSnapshot = await barrier.snapshot()
            if let firstUploaded {
                #expect(heldSnapshot.batchesConsumed == 1, "A later scan batch was consumed before the barrier was released")
                #expect(!heldSnapshot.scanCompleted, "The directory scan completed before the first upload became visible")
                if let expectedFirstSHA = expectedSHA[firstUploaded.name] {
                    #expect(
                        firstUploaded.sha256Checksum?.lowercased() == expectedFirstSHA.lowercased(),
                        "The first visible remote file must match its local SHA-256"
                    )
                } else {
                    Issue.record("Unexpected first remote file: \(firstUploaded.name)")
                }
            } else if let pollingError {
                Issue.record("Polling the real Drive directory failed before the first upload was observed: \(pollingError)")
            } else {
                Issue.record("No file became visible in the real Drive directory within 30 seconds while the scan remained blocked")
            }
        }
        await verifyFirstUpload()

        await barrier.release()

        var syncResult: Result<SyncStats, Error>
        do {
            syncResult = .success(try await syncTask.value)
        } catch {
            syncResult = .failure(error)
        }

        let finalChildren: [DriveFile]
        do {
            finalChildren = try await client.listChildren(parentId: remoteRoot.id)
        } catch {
            do {
                try await client.trash(remoteId: remoteRoot.id)
            } catch {
                Issue.record("Failed to clean up real Drive streaming fixture \(remoteRoot.id): \(error)")
            }
            throw error
        }
        do {
            try await client.trash(remoteId: remoteRoot.id)
        } catch {
            Issue.record("Failed to clean up real Drive streaming fixture \(remoteRoot.id): \(error)")
        }

        let stats = try syncResult.get()
        #expect(stats.filesUploaded == contents.count)
        #expect(stats.filesFailed == 0)

        let finalFiles = finalChildren.filter { !$0.isDirectory }
        #expect(finalChildren.count == contents.count, "Remote fixture must contain exactly the expected files and no directories")
        #expect(finalFiles.count == contents.count, "Remote fixture must not contain duplicate or non-file entries")
        #expect(Set(finalFiles.map(\.name)) == Set(contents.keys))
        for file in finalFiles {
            #expect(
                file.sha256Checksum?.lowercased() == expectedSHA[file.name]?.lowercased(),
                "Remote SHA-256 mismatch for \(file.name)"
            )
        }
    }
}
