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
        var directoryTasks: [Task<BootstrapDirectoryTarget, Error>] = []
        var fileTasks: [Task<Void, Never>] = []
        var cancelled = false
    }

    private let state: OSAllocatedUnfairLock<State>

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

    func registerDirectoryTask(_ task: Task<BootstrapDirectoryTarget, Error>, for path: String) {
        let shouldCancel = state.withLock { state in
            state.directories[path] = .task(task)
            state.directoryTasks.append(task)
            return state.cancelled
        }
        if shouldCancel { task.cancel() }
    }

    func registerFileTask(_ task: Task<Void, Never>) {
        let shouldCancel = state.withLock { state in
            state.fileTasks.append(task)
            return state.cancelled
        }
        if shouldCancel { task.cancel() }
    }

    func cancelAll() {
        let tasks = state.withLock { state -> ([Task<BootstrapDirectoryTarget, Error>], [Task<Void, Never>]) in
            state.cancelled = true
            return (state.directoryTasks, state.fileTasks)
        }
        tasks.0.forEach { $0.cancel() }
        tasks.1.forEach { $0.cancel() }
    }

    func waitForAll() async {
        let tasks = state.withLock { ($0.directoryTasks, $0.fileTasks) }
        for task in tasks.0 { _ = try? await task.value }
        for task in tasks.1 { await task.value }
    }
}
