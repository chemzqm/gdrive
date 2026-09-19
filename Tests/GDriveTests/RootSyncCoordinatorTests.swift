import Foundation
import Testing
@testable import GDrive

@Suite("Root sync coordination")
struct RootSyncCoordinatorTests {
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
}
