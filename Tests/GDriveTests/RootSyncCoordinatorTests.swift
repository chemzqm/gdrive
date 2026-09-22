import Foundation
import Testing
@testable import GDrive

@Suite("Root sync coordination")
struct RootSyncCoordinatorTests {
    private enum InjectedFailure: Error {
        case failed
    }

    private struct Fixture {
        let directory: URL
        let root: URL
        let backup: URL
        let alternate: URL
        let engine: SyncEngine
    }

    private func fixture() async throws -> Fixture {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("root-lock-\(UUID().uuidString)")
        let root = directory.appendingPathComponent("root")
        let backup = directory.appendingPathComponent("original")
        let alternate = directory.appendingPathComponent("alternate")
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: alternate, withIntermediateDirectories: true)
        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(
            clientId: "test", accessToken: "test",
            expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let auth = try Auth(path: authURL.path)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let engine = try await SyncEngine(
            auth: auth, store: store, idPool: IDPool(api: nil, initialIds: []),
            downloadTemporaryDirectory: directory.appendingPathComponent("downloads"),
            conflictDirectory: directory.appendingPathComponent("conflicts"))
        return Fixture(
            directory: directory, root: root, backup: backup,
            alternate: alternate, engine: engine)
    }

    private func replaceRootWithSymlink(_ fixture: Fixture) throws {
        try FileManager.default.moveItem(at: fixture.root, to: fixture.backup)
        try FileManager.default.createSymbolicLink(
            at: fixture.root, withDestinationURL: fixture.alternate)
    }

    private func restoreRoot(_ fixture: Fixture) throws {
        try FileManager.default.removeItem(at: fixture.root)
        try FileManager.default.moveItem(at: fixture.backup, to: fixture.root)
    }

    private func verifyLockCanBeReacquired(_ fixture: Fixture) async throws {
        try await fixture.engine.withRootSyncLock(localPath: fixture.root.path) {}
    }

    @Test("The same local root is rejected while a run is active")
    func rejectsConcurrentRunForSameLocalRoot() async throws {
        let path = "/tmp/gdrive-root-lock-\(UUID().uuidString)"
        let token = try await RootSyncCoordinator.shared.acquire(localRootPath: path)

        do {
            _ = try await RootSyncCoordinator.shared.acquire(localRootPath: path)
            Issue.record("Expected the second run to be rejected")
        } catch let error as SyncEngineError {
            #expect(error == .rootBusy(path: path))
        }
        await RootSyncCoordinator.shared.release(token)
    }

    @Test("Different local roots remain independent")
    func allowsDifferentLocalRoots() async throws {
        let first = "/tmp/gdrive-root-lock-\(UUID().uuidString)"
        let second = "/tmp/gdrive-root-lock-\(UUID().uuidString)"
        let firstToken = try await RootSyncCoordinator.shared.acquire(localRootPath: first)
        let secondToken = try await RootSyncCoordinator.shared.acquire(localRootPath: second)

        await RootSyncCoordinator.shared.release(firstToken)
        await RootSyncCoordinator.shared.release(secondToken)

        let retryToken = try await RootSyncCoordinator.shared.acquire(localRootPath: first)
        await RootSyncCoordinator.shared.release(retryToken)
    }

    @Test("Ancestor and descendant roots cannot run concurrently through symlink aliases")
    func rejectsConcurrentNestedRoots() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("nested-root-lock-\(UUID().uuidString)")
        let parent = directory.appendingPathComponent("parent")
        let child = parent.appendingPathComponent("child")
        let alias = directory.appendingPathComponent("child-alias")
        try FileManager.default.createDirectory(at: child, withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(at: alias, withDestinationURL: child)
        defer { try? FileManager.default.removeItem(at: directory) }

        let parentToken = try await RootSyncCoordinator.shared.acquire(localRootPath: parent.path)
        let childError = await #expect(throws: SyncEngineError.self) {
            _ = try await RootSyncCoordinator.shared.acquire(localRootPath: alias.path)
        }
        #expect(childError == .rootBusy(path: child.path))
        await RootSyncCoordinator.shared.release(parentToken)

        let childToken = try await RootSyncCoordinator.shared.acquire(localRootPath: alias.path)
        let parentError = await #expect(throws: SyncEngineError.self) {
            _ = try await RootSyncCoordinator.shared.acquire(localRootPath: parent.path)
        }
        #expect(parentError == .rootBusy(path: parent.path))
        await RootSyncCoordinator.shared.release(childToken)
    }

    @Test("A changed root symlink does not leak the lock after a successful run")
    func releasesOriginalKeyAfterSuccess() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }

        try await testFixture.engine.withRootSyncLock(localPath: testFixture.root.path) {
            try replaceRootWithSymlink(testFixture)
        }
        try restoreRoot(testFixture)

        try await verifyLockCanBeReacquired(testFixture)
    }

    @Test("A changed root symlink does not leak the lock after an error")
    func releasesOriginalKeyAfterError() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }

        await #expect(throws: InjectedFailure.self) {
            try await testFixture.engine.withRootSyncLock(localPath: testFixture.root.path) {
                try replaceRootWithSymlink(testFixture)
                throw InjectedFailure.failed
            }
        }
        try restoreRoot(testFixture)

        try await verifyLockCanBeReacquired(testFixture)
    }

    @Test("A changed root symlink does not leak the lock after cancellation")
    func releasesOriginalKeyAfterCancellation() async throws {
        let testFixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: testFixture.directory) }
        let entered = AsyncSemaphore(count: 0)
        let blocked = AsyncSemaphore(count: 0)
        let run = Task {
            try await testFixture.engine.withRootSyncLock(localPath: testFixture.root.path) {
                try replaceRootWithSymlink(testFixture)
                entered.signal()
                try await blocked.wait()
            }
        }
        try await entered.wait()

        run.cancel()
        await #expect(throws: CancellationError.self) { try await run.value }
        try restoreRoot(testFixture)

        try await verifyLockCanBeReacquired(testFixture)
    }

}
