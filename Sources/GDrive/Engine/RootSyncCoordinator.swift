import Foundation

/// Process-local exclusion for sync runs that operate on the same local root.
actor RootSyncCoordinator {
    static let shared = RootSyncCoordinator()

    struct Token: Sendable {
        fileprivate let key: String
    }

    private var runningRoots: Set<String> = []

    func acquire(localRootPath: String) throws -> Token {
        let key = Self.normalizedPath(localRootPath)
        guard runningRoots.insert(key).inserted else {
            throw SyncEngineError.rootBusy(path: key)
        }
        return Token(key: key)
    }

    func release(_ token: Token) {
        runningRoots.remove(token.key)
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
