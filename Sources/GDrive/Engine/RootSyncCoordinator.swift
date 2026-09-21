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
        var state = states[localRootPath] ?? State()
        guard !state.running else { throw SyncEngineError.rootBusy(path: localRootPath) }
        state.running = true
        states[localRootPath] = state
    }

    func enqueue(_ changes: [LocalChange], for localRootPath: String) -> Bool {
        var state = states[localRootPath] ?? State()
        state.pending = LocalChange.coalescing(state.pending + changes)
        let shouldStart = !state.running
        state.running = true
        states[localRootPath] = state
        return shouldStart
    }

    func takePending(for localRootPath: String) -> Batch? {
        guard var state = states[localRootPath], !state.pending.isEmpty else { return nil }
        let batch = Batch(changes: state.pending)
        state.pending.removeAll()
        states[localRootPath] = state
        return batch
    }

    func discardPendingChanges(for localRootPath: String, under path: String) {
        guard var state = states[localRootPath] else { return }
        state.pending.removeAll { change in
            change.paths.allSatisfy { Self.contains($0, in: path) }
        }
        states[localRootPath] = state
    }

    func finishIfIdle(localRootPath: String) -> Bool {
        guard let state = states[localRootPath] else { return true }
        guard state.pending.isEmpty else { return false }
        states.removeValue(forKey: localRootPath)
        return true
    }

    /// Compatibility release for direct coordinator users and focused tests.
    /// Normal engine paths use `finishIfIdle` to preserve the pending handoff.
    func release(localRootPath: String) {
        guard var state = states[localRootPath] else { return }
        state.running = false
        if state.pending.isEmpty { states.removeValue(forKey: localRootPath) } else { states[localRootPath] = state }
    }

    func activeRoots(containing paths: [String]) -> [String] {
        states.keys.filter { root in paths.contains { Self.contains($0, in: root) } }
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
}
