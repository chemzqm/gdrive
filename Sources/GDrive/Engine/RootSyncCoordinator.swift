import Foundation

/// Process-local exclusion for sync runs that operate on the same local root.
actor RootSyncCoordinator {
    static let shared = RootSyncCoordinator()

    private var runningRoots: Set<String> = []

    func acquire(localRootPath: String) throws {
        let key = Self.normalizedPath(localRootPath)
        guard runningRoots.insert(key).inserted else {
            throw SyncEngineError.rootBusy(path: key)
        }
    }

    func release(localRootPath: String) {
        runningRoots.remove(Self.normalizedPath(localRootPath))
    }

    nonisolated static func normalizedPath(_ path: String) -> String {
        var existing = URL(fileURLWithPath: (path as NSString).expandingTildeInPath)
            .standardizedFileURL
        var missingComponents: [String] = []
        while existing.path != "/", !FileManager.default.fileExists(atPath: existing.path) {
            missingComponents.append(existing.lastPathComponent)
            existing.deleteLastPathComponent()
        }
        var resolved = existing.resolvingSymlinksInPath()
        for component in missingComponents.reversed() {
            resolved.appendPathComponent(component)
        }
        return resolved.standardizedFileURL.path
    }
}
