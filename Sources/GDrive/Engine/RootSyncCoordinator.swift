import Foundation

/// Process-local coordination for one complete sync round per local root.
actor RootSyncCoordinator {
    static let shared = RootSyncCoordinator()

    struct Batch: Sendable { let changes: [LocalChange] }

    private struct State {
        var running = false
        var pending: [LocalChange] = []
    }

    private var states: [String: State] = [:]

    func acquire(localRootPath: String) throws {
        let key = Self.normalizedPath(localRootPath)
        var state = states[key] ?? State()
        guard !state.running else { throw SyncEngineError.rootBusy(path: key) }
        state.running = true
        states[key] = state
    }

    func enqueue(_ changes: [LocalChange], for localRootPath: String) -> Bool {
        let key = Self.normalizedPath(localRootPath)
        var state = states[key] ?? State()
        state.pending = LocalChange.coalescing(state.pending + changes)
        let shouldStart = !state.running
        state.running = true
        states[key] = state
        return shouldStart
    }

    func takePending(for localRootPath: String) -> Batch? {
        let key = Self.normalizedPath(localRootPath)
        guard var state = states[key], !state.pending.isEmpty else { return nil }
        let batch = Batch(changes: state.pending)
        state.pending.removeAll()
        states[key] = state
        return batch
    }

    func discardPendingChanges(for localRootPath: String, under path: String) {
        let key = Self.normalizedPath(localRootPath)
        let normalizedPath = Self.normalizedPath(path)
        guard var state = states[key] else { return }
        state.pending.removeAll { change in
            change.paths.allSatisfy { Self.contains(Self.normalizedPath($0), in: normalizedPath) }
        }
        states[key] = state
    }

    func finishIfIdle(localRootPath: String) -> Bool {
        let key = Self.normalizedPath(localRootPath)
        guard let state = states[key] else { return true }
        guard state.pending.isEmpty else { return false }
        states.removeValue(forKey: key)
        return true
    }

    /// Compatibility release for direct coordinator users and focused tests.
    /// Normal engine paths use `finishIfIdle` to preserve the pending handoff.
    func release(localRootPath: String) {
        let key = Self.normalizedPath(localRootPath)
        guard var state = states[key] else { return }
        state.running = false
        if state.pending.isEmpty { states.removeValue(forKey: key) } else { states[key] = state }
    }

    func activeRoots(containing paths: [String]) -> [String] {
        let normalizedPaths = paths.map(Self.normalizedPath)
        return states.keys.filter { root in normalizedPaths.contains { Self.contains($0, in: root) } }
    }

    func statuses() -> [PendingLocalChangeStatus] {
        states.map { path, state in
            PendingLocalChangeStatus(localRootPath: path, isRunning: state.running,
                pendingChangeCount: state.pending.count)
        }.sorted { $0.localRootPath < $1.localRootPath }
    }

    nonisolated static func contains(_ path: String, in root: String) -> Bool {
        path == root || path.hasPrefix(root.hasSuffix("/") ? root : root + "/")
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
