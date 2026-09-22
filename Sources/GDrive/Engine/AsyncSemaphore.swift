import Foundation

/// Thread-safe asynchronous semaphores to precisely limit the number of concurrent in-transit tasks and memory quotas
public final class AsyncSemaphore: @unchecked Sendable {
    private struct Waiter {
        let id: UUID
        let continuation: CheckedContinuation<Void, Error>
    }

    private var count: Int
    private var waiters: [Waiter] = []
    private var lock = os_unfair_lock()

    public init(count: Int) {
        self.count = count
    }

    public func wait() async throws {
        let waiterID = UUID()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                os_unfair_lock_lock(&lock)
                if Task.isCancelled {
                    os_unfair_lock_unlock(&lock)
                    continuation.resume(throwing: CancellationError())
                } else if count > 0 {
                    count -= 1
                    os_unfair_lock_unlock(&lock)
                    continuation.resume()
                } else {
                    waiters.append(Waiter(id: waiterID, continuation: continuation))
                    os_unfair_lock_unlock(&lock)
                }
            }
        } onCancel: {
            cancelWaiter(id: waiterID)
        }
    }

    public func signal() {
        os_unfair_lock_lock(&lock)
        if !waiters.isEmpty {
            let waiter = waiters.removeFirst()
            os_unfair_lock_unlock(&lock)
            waiter.continuation.resume()
        } else {
            count += 1
            os_unfair_lock_unlock(&lock)
        }
    }

    private func cancelWaiter(id: UUID) {
        os_unfair_lock_lock(&lock)
        guard let index = waiters.firstIndex(where: { $0.id == id }) else {
            os_unfair_lock_unlock(&lock)
            return
        }
        let waiter = waiters.remove(at: index)
        os_unfair_lock_unlock(&lock)
        waiter.continuation.resume(throwing: CancellationError())
    }
}
