import Foundation

/// Process-local exclusion for sync runs that operate on the same local root.
actor RootSyncCoordinator {
    static let shared = RootSyncCoordinator()

    private var runningRoots: Set<String> = []

    func acquire(localRootPath: String) throws {
        guard runningRoots.insert(localRootPath).inserted else {
            throw SyncEngineError.rootBusy(path: localRootPath)
        }
    }

    func release(localRootPath: String) {
        runningRoots.remove(localRootPath)
    }
}
