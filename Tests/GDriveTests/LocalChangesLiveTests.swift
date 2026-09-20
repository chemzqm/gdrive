import Foundation
import Testing
@testable import GDrive

private struct LocalChangesLiveTimeout: Error, LocalizedError {
    let waitingFor: String

    var errorDescription: String? {
        "Timed out waiting for \(waitingFor)"
    }
}

/// Holds selected scanner completions after their input has been fully enumerated.
/// This controls only scheduling; every upload and Drive observation remains live.
private actor PendingLocalChangesScanGate {
    private var completedScans = 0
    private var heldScans = Set<Int>()
    private var releasedScans = Set<Int>()
    private var waiters: [Int: CheckedContinuation<Void, Never>] = [:]
    private var permanentlyReleased = false

    func pauseAfterScanCompletion() async {
        completedScans += 1
        let scan = completedScans
        heldScans.insert(scan)

        guard scan <= 2, !permanentlyReleased, !releasedScans.contains(scan) else { return }
        await withCheckedContinuation { continuation in
            if releasedScans.contains(scan) {
                continuation.resume()
            } else {
                waiters[scan] = continuation
            }
        }
    }

    func isHolding(_ scan: Int) -> Bool {
        heldScans.contains(scan) && !releasedScans.contains(scan)
    }

    func release(_ scan: Int) {
        releasedScans.insert(scan)
        let waiter = waiters.removeValue(forKey: scan)
        waiter?.resume()
    }

    func releaseAll() {
        permanentlyReleased = true
        for scan in waiters.keys {
            releasedScans.insert(scan)
        }
        let currentWaiters = waiters
        waiters.removeAll()
        for waiter in currentWaiters.values {
            waiter.resume()
        }
    }
}

private actor LocalChangeAcceptance {
    private var finished = false
    private var errorDescription: String?

    func succeed() {
        finished = true
    }

    func fail(_ error: Error) {
        errorDescription = String(describing: error)
        finished = true
    }

    func snapshot() -> (finished: Bool, errorDescription: String?) {
        (finished, errorDescription)
    }
}

private func waitUntil(
    _ description: String,
    timeout: Duration = .seconds(30),
    condition: @escaping @Sendable () async -> Bool
) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now.advanced(by: timeout)
    while !(await condition()) {
        guard clock.now < deadline else {
            throw LocalChangesLiveTimeout(waitingFor: description)
        }
        try await Task.sleep(for: .milliseconds(50))
    }
}

@Suite("Pending local changes live integration")
struct LocalChangesLiveTests {
    @Test("Accepted local changes drain after bootstrap and a following pending round")
    func acceptedChangesDrainSeriallyAndDropStaleCreates() async throws {
        let auth = try Auth()
        let configuredRootID = try #require(
            await auth.authData().rootID,
            "Live pending-local-change tests require rootID in the configured GDrive credentials"
        )
        let client = DriveClient(auth: auth, requestsPerSecond: nil)

        let localRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("gdrive_local_changes_live_\(UUID().uuidString)", isDirectory: true)
        let trackedRootPath = localRoot.standardizedFileURL.path
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: localRoot) }

        let seedData = Data("seed present before bootstrap scan".utf8)
        try seedData.write(to: localRoot.appendingPathComponent("seed.txt"))

        let databasePath = FileManager.default.temporaryDirectory
            .appendingPathComponent("gdrive_local_changes_live_\(UUID().uuidString).sqlite").path
        defer {
            for suffix in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: databasePath + suffix)
            }
        }

        let gate = PendingLocalChangesScanGate()
        let store = try await StateStore(path: databasePath)
        let engine = try await SyncEngine(
            auth: auth,
            store: store,
            client: client,
            incrementalScan: { request, consume in
                // The scanner has completed its real traversal before the gate
                // closes, so later writes cannot leak into this scan's input.
                try await SyncEngine.defaultDirectoryScan(request, consume)
                await gate.pauseAfterScanCompletion()
            }
        )

        let remoteID = try #require(try await client.generateIds(count: 1).first)
        let remoteRoot = try await client.createDirectory(
            name: "local_changes_live_\(UUID().uuidString)",
            parentId: configuredRootID,
            remoteId: remoteID
        )

        var bootstrapTask: Task<SyncStats, Error>?
        do {
            let task = Task {
                try await engine.syncLocalToRemoteEmpty(
                    localPath: localRoot.path,
                    remoteRootId: remoteRoot.id,
                    maxUploadConcurrency: 1
                )
            }
            bootstrapTask = task

            try await waitUntil("the bootstrap scanner to finish its original enumeration") {
                await gate.isHolding(1)
            }

            let firstDirectory = localRoot.appendingPathComponent("first-pending", isDirectory: true)
            let firstFile = firstDirectory.appendingPathComponent("first.txt")
            let firstData = Data("first change admitted during bootstrap".utf8)
            try FileManager.default.createDirectory(at: firstDirectory, withIntermediateDirectories: true)
            try firstData.write(to: firstFile)

            let firstAcceptance = LocalChangeAcceptance()
            let firstNotice = Task {
                do {
                    try await engine.notifyLocalChanges([
                        .created(path: firstDirectory.path, isDirectory: true)
                    ])
                    await firstAcceptance.succeed()
                } catch {
                    await firstAcceptance.fail(error)
                }
            }
            try await waitUntil("the first local-change notification to be accepted") {
                await firstAcceptance.snapshot().finished
            }
            await firstNotice.value
            #expect(await firstAcceptance.snapshot().errorDescription == nil)

            let heldChildren = try await client.listChildren(parentId: remoteRoot.id)
            #expect(
                !heldChildren.contains(where: { $0.name == firstDirectory.lastPathComponent }),
                "Accepted work must stay queued until the held bootstrap round completes"
            )
            let bootstrapStatus = try #require(
                await engine.pendingLocalChangeStatus().first(where: { $0.localRootPath == trackedRootPath })
            )
            #expect(bootstrapStatus.isRunning)
            #expect(bootstrapStatus.pendingChangeCount > 0)

            await gate.release(1)
            try await waitUntil("the first pending scanner to finish its enumeration") {
                await gate.isHolding(2)
            }
            // A watcher-owned pending round holds the same root lock as an
            // explicit sync. Both public entry points must fail before any
            // network work while this scanner is deliberately suspended.
            await #expect(throws: SyncEngineError.rootBusy(path: trackedRootPath)) {
                _ = try await engine.syncIncremental(
                    localPath: localRoot.path,
                    remoteRootId: remoteRoot.id
                )
            }
            await #expect(throws: SyncEngineError.rootBusy(path: trackedRootPath)) {
                _ = try await engine.sync(
                    localPath: localRoot.path,
                    remoteFolderId: remoteRoot.id
                )
            }

            let secondDirectory = localRoot.appendingPathComponent("second-pending", isDirectory: true)
            let secondFile = secondDirectory.appendingPathComponent("second.txt")
            let secondData = Data("second change admitted while pending work runs".utf8)
            let staleFile = localRoot.appendingPathComponent("stale.txt")
            try FileManager.default.createDirectory(at: secondDirectory, withIntermediateDirectories: true)
            try secondData.write(to: secondFile)
            try Data("must not reach Drive".utf8).write(to: staleFile)

            let secondAcceptance = LocalChangeAcceptance()
            let secondNotice = Task {
                do {
                    try await engine.notifyLocalChanges([
                        .created(path: secondDirectory.path, isDirectory: true),
                        .created(path: staleFile.path, isDirectory: false)
                    ])
                    await secondAcceptance.succeed()
                } catch {
                    await secondAcceptance.fail(error)
                }
            }
            try await waitUntil("the second local-change notification to be accepted") {
                await secondAcceptance.snapshot().finished
            }
            await secondNotice.value
            #expect(await secondAcceptance.snapshot().errorDescription == nil)
            try FileManager.default.removeItem(at: staleFile)

            let pendingStatus = try #require(
                await engine.pendingLocalChangeStatus().first(where: { $0.localRootPath == trackedRootPath })
            )
            #expect(pendingStatus.isRunning)
            #expect(pendingStatus.pendingChangeCount > 0)

            await gate.release(2)
            _ = try await task.value

            try await waitUntil("all accepted pending local changes to drain") {
                let status = await engine.pendingLocalChangeStatus()
                    .first(where: { $0.localRootPath == trackedRootPath })
                // A completed drain removes the coordinator state, so absence
                // is the public idle state.
                return status == nil
            }

            let rootChildren = try await client.listChildren(parentId: remoteRoot.id)
            #expect(rootChildren.count == 3)
            #expect(!rootChildren.contains(where: { $0.name == staleFile.lastPathComponent }))

            let seedRemote = try #require(rootChildren.first(where: { $0.name == "seed.txt" }))
            let firstRemoteDirectory = try #require(rootChildren.first(where: { $0.name == firstDirectory.lastPathComponent }))
            let secondRemoteDirectory = try #require(rootChildren.first(where: { $0.name == secondDirectory.lastPathComponent }))
            #expect(!seedRemote.isDirectory)
            #expect(firstRemoteDirectory.isDirectory)
            #expect(secondRemoteDirectory.isDirectory)

            let firstRemoteFile = try #require(
                try await client.listChildren(parentId: firstRemoteDirectory.id)
                    .first(where: { $0.name == firstFile.lastPathComponent })
            )
            let secondRemoteFile = try #require(
                try await client.listChildren(parentId: secondRemoteDirectory.id)
                    .first(where: { $0.name == secondFile.lastPathComponent })
            )
            let expectedSHAByName = [
                "seed.txt": SyncEngine.computeSha256(of: seedData),
                firstFile.lastPathComponent: SyncEngine.computeSha256(of: firstData),
                secondFile.lastPathComponent: SyncEngine.computeSha256(of: secondData)
            ]
            for file in [seedRemote, firstRemoteFile, secondRemoteFile] {
                #expect(file.sha256Checksum?.lowercased() == expectedSHAByName[file.name]?.lowercased())
            }

            struct CommittedFile: Sendable {
                let name: String
                let remoteID: String?
                let baseSHA: String?
                let phase: String?
            }
            let storedFiles: [CommittedFile] = try await store.read { conn in
                let statement = try conn.cachedStatement("""
                SELECT name, remote_file_id, base_sha256, phase
                FROM items
                WHERE entry_kind = 'file' AND is_tombstone = 0
                ORDER BY name;
                """)
                defer { statement.reset() }
                var files: [CommittedFile] = []
                while try statement.step() {
                    files.append(CommittedFile(
                        name: statement.columnText(at: 0) ?? "",
                        remoteID: statement.columnText(at: 1),
                        baseSHA: statement.columnText(at: 2),
                        phase: statement.columnText(at: 3)
                    ))
                }
                return files
            }
            #expect(storedFiles.count == 3)
            #expect(!storedFiles.contains(where: { $0.name == staleFile.lastPathComponent }))
            let expectedRemoteIDByName = [
                "seed.txt": seedRemote.id,
                firstFile.lastPathComponent: firstRemoteFile.id,
                secondFile.lastPathComponent: secondRemoteFile.id
            ]
            for file in storedFiles {
                #expect(file.remoteID == expectedRemoteIDByName[file.name])
                #expect(file.baseSHA?.lowercased() == expectedSHAByName[file.name]?.lowercased())
            }
            // RemoteChanges consumes root-wide Drive echoes before watcher work
            // is filtered to its local scope. An earlier file can therefore be
            // ready for a later full round; only this final notified subtree is
            // required to reach committed in this scoped pending pass.
            let finalScopedFile = try #require(
                storedFiles.first(where: { $0.name == secondFile.lastPathComponent })
            )
            #expect(finalScopedFile.phase == "committed")

            await gate.releaseAll()
            try await client.trash(remoteId: remoteRoot.id)
        } catch {
            await gate.releaseAll()
            if let bootstrapTask {
                bootstrapTask.cancel()
                _ = try? await bootstrapTask.value
            }
            do {
                try await client.trash(remoteId: remoteRoot.id)
            } catch {
                Issue.record("Failed to clean up real Drive pending-local-changes fixture \(remoteRoot.id): \(error)")
            }
            throw error
        }
    }

    @Test("A11 blocked modification stays durable and a later deletion drains without retrying it")
    func blockedModificationDrainsAfterDeletion() async throws {
        let auth = try Auth()
        let configuredRootID = try #require(
            await auth.authData().rootID,
            "Live pending-local-change tests require rootID in the configured GDrive credentials"
        )
        let client = DriveClient(auth: auth, requestsPerSecond: nil)

        let localRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("gdrive_local_changes_a11_live_\(UUID().uuidString)", isDirectory: true)
        let trackedRootPath = localRoot.standardizedFileURL.path
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: localRoot) }

        let guardedFile = localRoot.appendingPathComponent("guarded.txt")
        let originalData = Data("A11 baseline body".utf8)
        try originalData.write(to: guardedFile)

        let databasePath = FileManager.default.temporaryDirectory
            .appendingPathComponent("gdrive_local_changes_a11_live_\(UUID().uuidString).sqlite").path
        defer {
            for suffix in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: databasePath + suffix)
            }
        }

        let store = try await StateStore(path: databasePath)
        let engine = try await SyncEngine(auth: auth, store: store, client: client)
        let remoteID = try #require(try await client.generateIds(count: 1).first)
        let remoteRoot = try await client.createDirectory(
            name: "local_changes_a11_live_\(UUID().uuidString)",
            parentId: configuredRootID,
            remoteId: remoteID
        )

        do {
            _ = try await engine.syncLocalToRemoteEmpty(
                localPath: localRoot.path,
                remoteRootId: remoteRoot.id,
                maxUploadConcurrency: 1
            )
            let initialChildren = try await client.listChildren(parentId: remoteRoot.id)
            let guardedRemote = try #require(initialChildren.first(where: { $0.name == guardedFile.lastPathComponent }))
            let originalSHA = SyncEngine.computeSha256(of: originalData)
            #expect(guardedRemote.sha256Checksum?.lowercased() == originalSHA.lowercased())

            let modifiedData = Data("A11 modified body which must not overwrite remotely".utf8)
            let healthyFile = localRoot.appendingPathComponent("healthy.txt")
            let healthyData = Data("healthy watcher change".utf8)
            try modifiedData.write(to: guardedFile)
            try healthyData.write(to: healthyFile)
            try await engine.notifyLocalChanges([
                .modified(path: guardedFile.path, isDirectory: false),
                .created(path: healthyFile.path, isDirectory: false)
            ])

            try await waitUntil("the A11 modification failure to finish its pending round") {
                await engine.pendingLocalChangeStatus()
                    .first(where: { $0.localRootPath == trackedRootPath }) == nil
            }

            struct GuardedRecord: Sendable {
                let remoteID: String?
                let localSHA: String?
                let baseSHA: String?
                let localGeneration: Int64
                let dirtyGeneration: Int64
                let phase: String?
                let isTombstone: Bool
            }
            // A fresh StateStore must see the failed local observation; watcher
            // queue state is intentionally gone once the round has finished.
            let durableStore = try await StateStore(path: databasePath)
            let blockedRecord: GuardedRecord = try await durableStore.read { conn in
                let statement = try conn.cachedStatement("""
                SELECT remote_file_id, local_sha256, base_sha256, local_generation,
                       dirty_generation, phase, is_tombstone
                FROM items
                WHERE entry_kind = 'file' AND name = ? AND is_tombstone = 0;
                """)
                statement.bindText(guardedFile.lastPathComponent, at: 1)
                defer { statement.reset() }
                guard try statement.step() else {
                    throw SyncEngineError.general("Missing blocked A11 file row")
                }
                return GuardedRecord(
                    remoteID: statement.columnText(at: 0),
                    localSHA: statement.columnText(at: 1),
                    baseSHA: statement.columnText(at: 2),
                    localGeneration: statement.columnInt64(at: 3) ?? 0,
                    dirtyGeneration: statement.columnInt64(at: 4) ?? 0,
                    phase: statement.columnText(at: 5),
                    isTombstone: (statement.columnInt64(at: 6) ?? 0) != 0
                )
            }
            // updateMultipart is deliberately blocked by A11; the old remote
            // identity and SHA remain the baseline while SQLite records the
            // unsynced local content.
            let modifiedSHA = SyncEngine.computeSha256(of: modifiedData)
            #expect(blockedRecord.remoteID == guardedRemote.id)
            #expect(blockedRecord.localSHA?.lowercased() == modifiedSHA.lowercased())
            #expect(blockedRecord.baseSHA?.lowercased() == originalSHA.lowercased())
            #expect(blockedRecord.phase == "blocked")
            #expect(blockedRecord.dirtyGeneration > 0)
            #expect(!blockedRecord.isTombstone)
            let unchangedRemote = try await client.getFile(remoteId: guardedRemote.id)
            #expect(unchangedRemote.trashed != true)
            #expect(unchangedRemote.sha256Checksum?.lowercased() == originalSHA.lowercased())

            let childrenAfterFailure = try await client.listChildren(parentId: remoteRoot.id)
            let healthyRemote = try #require(childrenAfterFailure.first(where: { $0.name == healthyFile.lastPathComponent }))
            #expect(healthyRemote.sha256Checksum?.lowercased() == SyncEngine.computeSha256(of: healthyData).lowercased())

            let laterHealthyFile = localRoot.appendingPathComponent("later-healthy.txt")
            let laterHealthyData = Data("healthy watcher change after the A11 failure".utf8)
            try laterHealthyData.write(to: laterHealthyFile)
            try await engine.notifyLocalChanges([
                .created(path: laterHealthyFile.path, isDirectory: false)
            ])
            try await waitUntil("the later healthy watcher change to reach Drive") {
                do {
                    let children = try await client.listChildren(parentId: remoteRoot.id)
                    return children.contains {
                        $0.name == laterHealthyFile.lastPathComponent
                            && $0.sha256Checksum?.lowercased()
                                == SyncEngine.computeSha256(of: laterHealthyData).lowercased()
                    }
                } catch {
                    return false
                }
            }
            try await waitUntil("the later healthy watcher round to drain") {
                await engine.pendingLocalChangeStatus()
                    .first(where: { $0.localRootPath == trackedRootPath }) == nil
            }
            let afterLaterHealthyRecord: GuardedRecord = try await durableStore.read { conn in
                let statement = try conn.cachedStatement("""
                SELECT remote_file_id, local_sha256, base_sha256, local_generation,
                       dirty_generation, phase, is_tombstone
                FROM items
                WHERE entry_kind = 'file' AND name = ? AND is_tombstone = 0;
                """)
                statement.bindText(guardedFile.lastPathComponent, at: 1)
                defer { statement.reset() }
                guard try statement.step() else {
                    throw SyncEngineError.general("Missing blocked A11 file row after healthy notification")
                }
                return GuardedRecord(
                    remoteID: statement.columnText(at: 0),
                    localSHA: statement.columnText(at: 1),
                    baseSHA: statement.columnText(at: 2),
                    localGeneration: statement.columnInt64(at: 3) ?? 0,
                    dirtyGeneration: statement.columnInt64(at: 4) ?? 0,
                    phase: statement.columnText(at: 5),
                    isTombstone: (statement.columnInt64(at: 6) ?? 0) != 0
                )
            }
            #expect(afterLaterHealthyRecord.localGeneration == blockedRecord.localGeneration)
            #expect(afterLaterHealthyRecord.localSHA?.lowercased() == modifiedSHA.lowercased())
            #expect(afterLaterHealthyRecord.baseSHA?.lowercased() == originalSHA.lowercased())
            #expect(afterLaterHealthyRecord.dirtyGeneration > 0)

            try FileManager.default.removeItem(at: guardedFile)
            try await engine.notifyLocalChanges([
                .deleted(path: guardedFile.path, isDirectory: false)
            ])
            try await waitUntil("the A11 deletion watcher round to drain") {
                await engine.pendingLocalChangeStatus()
                    .first(where: { $0.localRootPath == trackedRootPath }) == nil
            }

            let remainingChildren = try await client.listChildren(parentId: remoteRoot.id)
            #expect(!remainingChildren.contains(where: { $0.id == guardedRemote.id }))
            let trashedGuardedRemote = try await client.getFile(remoteId: guardedRemote.id)
            #expect(trashedGuardedRemote.trashed == true)
            let healthyAfterDrain = try #require(remainingChildren.first(where: { $0.id == healthyRemote.id }))
            #expect(healthyAfterDrain.sha256Checksum?.lowercased() == SyncEngine.computeSha256(of: healthyData).lowercased())

            let deletedRecord: GuardedRecord = try await durableStore.read { conn in
                let statement = try conn.cachedStatement("""
                SELECT remote_file_id, local_sha256, base_sha256, local_generation,
                       dirty_generation, phase, is_tombstone
                FROM items
                WHERE entry_kind = 'file' AND name = ?
                ORDER BY item_id DESC LIMIT 1;
                """)
                statement.bindText(guardedFile.lastPathComponent, at: 1)
                defer { statement.reset() }
                guard try statement.step() else {
                    throw SyncEngineError.general("Missing deleted A11 file row")
                }
                return GuardedRecord(
                    remoteID: statement.columnText(at: 0),
                    localSHA: statement.columnText(at: 1),
                    baseSHA: statement.columnText(at: 2),
                    localGeneration: statement.columnInt64(at: 3) ?? 0,
                    dirtyGeneration: statement.columnInt64(at: 4) ?? 0,
                    phase: statement.columnText(at: 5),
                    isTombstone: (statement.columnInt64(at: 6) ?? 0) != 0
                )
            }
            #expect(deletedRecord.remoteID == guardedRemote.id)
            #expect(deletedRecord.baseSHA?.lowercased() == originalSHA.lowercased())
            #expect(deletedRecord.phase == "committed")
            #expect(deletedRecord.dirtyGeneration == 0)
            #expect(deletedRecord.isTombstone)

            try await client.trash(remoteId: remoteRoot.id)
        } catch {
            // Do not remove the local fixture or database while a notification
            // round is still active and could touch either one.
            _ = try? await waitUntil("pending local work to stop after a failure", timeout: .seconds(10)) {
                guard let status = await engine.pendingLocalChangeStatus()
                    .first(where: { $0.localRootPath == trackedRootPath })
                else {
                    return true
                }
                return !status.isRunning
            }
            do {
                try await client.trash(remoteId: remoteRoot.id)
            } catch {
                Issue.record("Failed to clean up real Drive A11 pending-local-changes fixture \(remoteRoot.id): \(error)")
            }
            throw error
        }
    }
}
