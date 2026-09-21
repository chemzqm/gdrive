import Foundation
import Testing
@testable import GDrive

@Suite("Root sync coordination")
struct RootSyncCoordinatorTests {
    private func makeEngine(in directory: URL) async throws -> SyncEngine {
        let authPath = directory.appendingPathComponent("auth.json")
        let data = AuthData(clientId: "test", accessToken: "token", expiresAt: Date().addingTimeInterval(3600))
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(data).write(to: authPath)
        return try await SyncEngine(auth: Auth(path: authPath.path),
            store: StateStore(path: directory.appendingPathComponent("state.sqlite").path))
    }

    @Test("The same local root is rejected while a run is active")
    func rejectsConcurrentRunForSameLocalRoot() async throws {
        let path = "/tmp/gdrive-root-lock-\(UUID().uuidString)"
        try await RootSyncCoordinator.shared.acquire(localRootPath: path)

        do {
            try await RootSyncCoordinator.shared.acquire(localRootPath: path)
            Issue.record("Expected the second run to be rejected")
        } catch let error as SyncEngineError {
            #expect(error == .rootBusy(path: path))
        }
        await RootSyncCoordinator.shared.release(localRootPath: path)
    }

    @Test("Different local roots remain independent")
    func allowsDifferentLocalRoots() async throws {
        let first = "/tmp/gdrive-root-lock-\(UUID().uuidString)"
        let second = "/tmp/gdrive-root-lock-\(UUID().uuidString)"
        try await RootSyncCoordinator.shared.acquire(localRootPath: first)
        try await RootSyncCoordinator.shared.acquire(localRootPath: second)

        await RootSyncCoordinator.shared.release(localRootPath: first)
        await RootSyncCoordinator.shared.release(localRootPath: second)

        try await RootSyncCoordinator.shared.acquire(localRootPath: first)
        await RootSyncCoordinator.shared.release(localRootPath: first)
    }

    @Test("Notifications coalesce and hand off without releasing the root")
    func handsPendingBatchToCurrentRound() async throws {
        let path = "/tmp/gdrive-pending-\(UUID().uuidString)"
        let change = LocalChange.modified(path: path + "/file", isDirectory: false)
        try await RootSyncCoordinator.shared.acquire(localRootPath: path)

        #expect(await RootSyncCoordinator.shared.enqueue([change, change], for: path) == false)
        let first = try #require(await RootSyncCoordinator.shared.takePending(for: path))
        #expect(first.changes == [change])
        #expect(await RootSyncCoordinator.shared.finishIfIdle(localRootPath: path))
        let statuses = await RootSyncCoordinator.shared.statuses()
        #expect(!statuses.contains { $0.localRootPath == path }, "\(statuses)")
    }

    @Test("Later queued work does not replay an already consumed failed batch")
    func laterBatchDoesNotReplayConsumedFailure() async throws {
        let root = "/tmp/gdrive-consumed-\(UUID().uuidString)"
        let first = LocalChange.modified(path: root + "/first", isDirectory: false)
        let later = LocalChange.modified(path: root + "/later", isDirectory: false)
        try await RootSyncCoordinator.shared.acquire(localRootPath: root)
        _ = await RootSyncCoordinator.shared.enqueue([first], for: root)
        _ = try #require(await RootSyncCoordinator.shared.takePending(for: root))
        _ = await RootSyncCoordinator.shared.enqueue([later], for: root)

        let next = try #require(await RootSyncCoordinator.shared.takePending(for: root))
        #expect(next.changes == [later])
        #expect(await RootSyncCoordinator.shared.finishIfIdle(localRootPath: root))
    }

    @Test("A notification accepted at idle starts one pending round")
    func idleNotificationStartsRound() async throws {
        let path = "/tmp/gdrive-pending-idle-\(UUID().uuidString)"
        let change = LocalChange.deleted(path: path + "/gone", isDirectory: false)

        #expect(await RootSyncCoordinator.shared.enqueue([change], for: path))
        #expect(await RootSyncCoordinator.shared.enqueue([change], for: path) == false)
        let batch = try #require(await RootSyncCoordinator.shared.takePending(for: path))
        #expect(batch.changes == [change])
        #expect(await RootSyncCoordinator.shared.finishIfIdle(localRootPath: path))
    }

    @Test("A notification arriving at the idle handoff remains drainable")
    func idleHandoffDoesNotLoseWakeup() async throws {
        let path = "/tmp/gdrive-pending-handoff-\(UUID().uuidString)"
        let first = LocalChange.created(path: path + "/first", isDirectory: false)
        let second = LocalChange.created(path: path + "/second", isDirectory: false)
        try await RootSyncCoordinator.shared.acquire(localRootPath: path)
        _ = await RootSyncCoordinator.shared.enqueue([first], for: path)
        _ = try #require(await RootSyncCoordinator.shared.takePending(for: path))

        #expect(await RootSyncCoordinator.shared.enqueue([second], for: path) == false)
        #expect(await RootSyncCoordinator.shared.finishIfIdle(localRootPath: path) == false)
        let batch = try #require(await RootSyncCoordinator.shared.takePending(for: path))
        #expect(batch.changes == [second])
        #expect(await RootSyncCoordinator.shared.finishIfIdle(localRootPath: path))
    }

    @Test("Cleanup discards only pending changes in its subtree")
    func discardsCleanupSubtree() async throws {
        let root = "/tmp/gdrive-cleanup-pending-\(UUID().uuidString)"
        let removed = root + "/removed"
        let keep = LocalChange.modified(path: root + "/keep.txt", isDirectory: false)
        let crossingMove = LocalChange.moved(
            from: root + "/outside.txt", to: removed + "/moved.txt", isDirectory: false)
        try await RootSyncCoordinator.shared.acquire(localRootPath: root)
        _ = await RootSyncCoordinator.shared.enqueue([
            .deleted(path: removed, isDirectory: true),
            .modified(path: removed + "/child.txt", isDirectory: false),
            .moved(
                from: removed + "/old.txt", to: removed + "/new.txt", isDirectory: false),
            crossingMove,
            keep
        ], for: root)

        await RootSyncCoordinator.shared.discardPendingChanges(for: root, under: removed)

        let batch = try #require(await RootSyncCoordinator.shared.takePending(for: root))
        #expect(batch.changes == [crossingMove, keep])
        #expect(await RootSyncCoordinator.shared.finishIfIdle(localRootPath: root))
    }

    @Test("Pre-baseline active roots route a multi-root watcher batch by path boundary")
    func routesActiveRootsBeforeDatabaseBinding() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let first = directory.appendingPathComponent("one").path
        let second = directory.appendingPathComponent("two").path
        let engine = try await makeEngine(in: directory)
        try await RootSyncCoordinator.shared.acquire(localRootPath: first)
        try await RootSyncCoordinator.shared.acquire(localRootPath: second)

        try await engine.notifyLocalChanges([
            .created(path: first + "/new", isDirectory: false),
            .deleted(path: second + "/gone", isDirectory: false)
        ])
        let statuses = await engine.pendingLocalChangeStatus()
        #expect(statuses.first { $0.localRootPath == first }?.pendingChangeCount == 1)
        #expect(statuses.first { $0.localRootPath == second }?.pendingChangeCount == 1)
        await #expect(throws: SyncEngineError.self) {
            try await engine.notifyLocalChanges([.created(path: first + "-other/file", isDirectory: false)])
        }
        _ = await RootSyncCoordinator.shared.takePending(for: first)
        _ = await RootSyncCoordinator.shared.takePending(for: second)
        _ = await RootSyncCoordinator.shared.finishIfIdle(localRootPath: first)
        _ = await RootSyncCoordinator.shared.finishIfIdle(localRootPath: second)
    }
}
