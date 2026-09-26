import Foundation
import os

private final class TaskStartGate: @unchecked Sendable {
    private struct State {
        var opened = false
        var waiters: [CheckedContinuation<Void, Never>] = []
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    func wait() async {
        await withCheckedContinuation { continuation in
            let resumeNow = state.withLock { state in
                guard !state.opened else { return true }
                state.waiters.append(continuation)
                return false
            }
            if resumeNow { continuation.resume() }
        }
    }

    func open() {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            state.opened = true
            let waiters = state.waiters
            state.waiters.removeAll()
            return waiters
        }
        waiters.forEach { $0.resume() }
    }
}

/// Tracks only active unstructured tasks and provides one cancellation/drain boundary.
final class TaskLifecycle: @unchecked Sendable {
    private struct Handle: @unchecked Sendable {
        let cancel: @Sendable () -> Void
    }

    private struct State {
        var tasks: [UUID: Handle] = [:]
        var cancelled = false
        var drainWaiters: [CheckedContinuation<Void, Never>] = []
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    @discardableResult
    func start(_ operation: @escaping @Sendable () async -> Void) -> Task<Void, Never> {
        let id = UUID()
        let gate = TaskStartGate()
        let task = Task {
            await gate.wait()
            await operation()
            self.finished(id)
        }
        register(id: id, cancel: { task.cancel() })
        gate.open()
        return task
    }

    @discardableResult
    func startThrowing<Success: Sendable>(
        _ operation: @escaping @Sendable () async throws -> Success,
        onRegistered: (Task<Success, Error>) -> Void = { _ in }
    ) -> Task<Success, Error> {
        let id = UUID()
        let gate = TaskStartGate()
        let task = Task {
            await gate.wait()
            defer { self.finished(id) }
            try Task.checkCancellation()
            return try await operation()
        }
        register(id: id, cancel: { task.cancel() })
        onRegistered(task)
        gate.open()
        return task
    }

    private func register(id: UUID, cancel: @escaping @Sendable () -> Void) {
        let cancelled = state.withLock { state in
            state.tasks[id] = Handle(cancel: cancel)
            return state.cancelled
        }
        if cancelled { cancel() }
    }

    private func finished(_ id: UUID) {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            state.tasks.removeValue(forKey: id)
            guard state.tasks.isEmpty else { return [] }
            let waiters = state.drainWaiters
            state.drainWaiters.removeAll()
            return waiters
        }
        waiters.forEach { $0.resume() }
    }

    func cancelAll() {
        let handles = state.withLock { state -> [Handle] in
            state.cancelled = true
            return Array(state.tasks.values)
        }
        handles.forEach { $0.cancel() }
    }

    var isCancelled: Bool {
        state.withLock { $0.cancelled }
    }

    func waitForAll() async {
        await withCheckedContinuation { continuation in
            let resumeNow = state.withLock { state in
                guard !state.tasks.isEmpty else { return true }
                state.drainWaiters.append(continuation)
                return false
            }
            if resumeNow { continuation.resume() }
        }
    }
}
