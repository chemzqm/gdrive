import Foundation

/// Thread-safe asynchronous semaphores to precisely limit the number of concurrent in-transit tasks and memory quotas
public final class AsyncSemaphore: @unchecked Sendable {
    private var count: Int
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var lock = os_unfair_lock()

    public init(count: Int) {
        self.count = count
    }

    public func wait() async {
        await withCheckedContinuation { cont in
            os_unfair_lock_lock(&lock)
            if count > 0 {
                count -= 1
                os_unfair_lock_unlock(&lock)
                cont.resume()
            } else {
                waiters.append(cont)
                os_unfair_lock_unlock(&lock)
            }
        }
    }

    public func signal() {
        os_unfair_lock_lock(&lock)
        if !waiters.isEmpty {
            let waiter = waiters.removeFirst()
            os_unfair_lock_unlock(&lock)
            waiter.resume()
        } else {
            count += 1
            os_unfair_lock_unlock(&lock)
        }
    }
}
