import Foundation
import os

/// The paths a watcher-triggered round is allowed to observe.  The watcher
/// event is only a hint: `scanLocal()` records the paths that actually still
/// exist before this scope is used for reconciliation or absence detection.
final class LocalChangeScope: @unchecked Sendable {
    private struct State {
        var exact = Set<String>()
        var subtrees = Set<String>()
        var completeExact = Set<String>()
        var completeSubtrees = Set<String>()
    }

    let rootURL: URL
    let changes: [LocalChange]
    private let state = OSAllocatedUnfairLock(initialState: State())

    init(rootURL: URL, changes: [LocalChange]) {
        self.rootURL = rootURL.standardizedFileURL
        self.changes = changes
    }

    func relativePath(_ path: String) -> String? {
        let normalized = URL(fileURLWithPath: path).standardizedFileURL.path
        let root = rootURL.path
        let componentPrefix = root == "/" ? "/" : root + "/"
        guard normalized == root || normalized.hasPrefix(componentPrefix) else { return nil }
        let relative = normalized == root ? "" : String(normalized.dropFirst(componentPrefix.count))
        guard !relative.split(separator: "/").contains(".git") else { return nil }
        return relative
    }

    func markExistingFile(_ path: String) {
        state.withLock { _ = $0.exact.insert(path) }
    }

    func markExistingDirectory(_ path: String) {
        state.withLock { _ = $0.subtrees.insert(path) }
    }

    func markObservedDirectory(_ path: String) {
        state.withLock { _ = $0.exact.insert(path) }
    }

    func markMissingExact(_ path: String) {
        state.withLock {
            $0.exact.insert(path)
            $0.completeExact.insert(path)
        }
    }

    func markMissingSubtree(_ path: String) {
        state.withLock {
            $0.subtrees.insert(path)
            $0.completeSubtrees.insert(path)
        }
    }

    func markScannedSubtree(_ path: String) {
        state.withLock { _ = $0.completeSubtrees.insert(path) }
    }

    func includes(_ path: String) -> Bool {
        state.withLock { state in
            state.exact.contains(path) || state.subtrees.contains { root in
                root.isEmpty || path == root || path.hasPrefix(root + "/")
            }
        }
    }

    func coversMissing(_ path: String) -> Bool {
        state.withLock { state in
            state.completeExact.contains(path) || state.completeSubtrees.contains { root in
                root.isEmpty || path == root || path.hasPrefix(root + "/")
            }
        }
    }
}
