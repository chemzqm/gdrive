import Foundation
import os

struct BootstrapDirectoryTarget: Sendable {
    let remoteID: String
    let itemID: Int64
}

enum BootstrapDirectoryDependency: Sendable {
    case ready(BootstrapDirectoryTarget)
    case task(Task<BootstrapDirectoryTarget, Error>)

    func value() async throws -> BootstrapDirectoryTarget {
        switch self {
        case .ready(let target):
            return target
        case .task(let task):
            return try await task.value
        }
    }
}

enum BootstrapDirectoryDependencyError: Error, LocalizedError, Sendable {
    case missingParent(path: String)

    var errorDescription: String? {
        switch self {
        case .missingParent(let path):
            return "Parent directory was not emitted before its child: \(path)"
        }
    }
}

/// Owns the bootstrap's unstructured work and publishes a directory dependency
/// synchronously while a scan callback is handling that directory.
final class BootstrapTaskRegistry: @unchecked Sendable {
    private struct State {
        var directories: [String: BootstrapDirectoryDependency]
    }

    private let state: OSAllocatedUnfairLock<State>
    private let lifecycle = TaskLifecycle()

    init(root: BootstrapDirectoryTarget) {
        state = OSAllocatedUnfairLock(initialState: State(directories: ["": .ready(root)]))
    }

    func dependency(for path: String) throws -> BootstrapDirectoryDependency {
        try state.withLock { state in
            guard let dependency = state.directories[path] else {
                throw BootstrapDirectoryDependencyError.missingParent(path: path)
            }
            return dependency
        }
    }

    func registerReady(_ target: BootstrapDirectoryTarget, for path: String) {
        state.withLock { state in
            state.directories[path] = .ready(target)
        }
    }

    func startDirectoryTask(
        for path: String,
        operation: @escaping @Sendable () async throws -> BootstrapDirectoryTarget
    ) {
        lifecycle.startThrowing({
            let target = try await operation()
            self.registerReady(target, for: path)
            return target
        }, onRegistered: { task in
            state.withLock { state in
                state.directories[path] = .task(task)
            }
        })
    }

    func startFileTask(operation: @escaping @Sendable () async -> Void) {
        lifecycle.start(operation)
    }

    func cancelAll() {
        lifecycle.cancelAll()
    }

    func waitForAll() async {
        await lifecycle.waitForAll()
    }
}
