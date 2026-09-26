import Foundation

/// Process-local exclusion for sync runs that operate on the same local root.
actor RootSyncCoordinator {
    static let shared = RootSyncCoordinator()

    struct Token: Sendable {
        fileprivate let id: UUID
    }

    private struct Entry {
        let key: String
        let control: SyncRunControl?
        var waiters: [CheckedContinuation<Void, Never>] = []
    }

    private var runningRoots: [UUID: Entry] = [:]

    func acquire(localRootPath: String, control: SyncRunControl? = nil) throws -> Token {
        let key = Self.normalizedPath(localRootPath)
        guard !runningRoots.values.contains(where: { Self.overlaps($0.key, key) }) else {
            throw SyncEngineError.rootBusy(path: key)
        }
        let id = UUID()
        runningRoots[id] = Entry(key: key, control: control)
        return Token(id: id)
    }

    func release(_ token: Token) {
        guard let entry = runningRoots.removeValue(forKey: token.id) else { return }
        entry.waiters.forEach { $0.resume() }
    }

    /// Stops only a currently running sync for this exact root, then waits for
    /// that invocation to release its root token.  A later invocation cannot
    /// be mistaken for the cancelled one because the waiter is attached to its
    /// original token.
    func cancelSync(localRootPath: String) async {
        let key = Self.normalizedPath(localRootPath)
        guard let (id, control) = runningRoots.first(where: {
            $0.value.key == key && $0.value.control != nil
        }).map({ ($0.key, $0.value.control!) }) else { return }
        control.cancel()
        await waitForRelease(id)
    }

    private func waitForRelease(_ id: UUID) async {
        await withCheckedContinuation { continuation in
            guard var entry = runningRoots[id] else {
                continuation.resume()
                return
            }
            entry.waiters.append(continuation)
            runningRoots[id] = entry
        }
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

    nonisolated static func contains(_ candidatePath: String, in directoryPath: String) -> Bool {
        let directory = normalizedPath(directoryPath)
        let candidate = normalizedPath(candidatePath)
        guard candidate != directory else { return true }
        let prefix = directory.hasSuffix("/") ? directory : directory + "/"
        return candidate.hasPrefix(prefix)
    }

    nonisolated static func overlaps(_ firstPath: String, _ secondPath: String) -> Bool {
        contains(firstPath, in: secondPath) || contains(secondPath, in: firstPath)
    }
}
