import Foundation

/// 线程安全的异步信号量，用于精确限制并发在途任务数与内存配额
public final class AsyncSemaphore: @unchecked Sendable {
    private var count: Int
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var lock = os_unfair_lock()

    public init(count: Int) {
        self.count = count
    }

    public func wait() async {
        let shouldWait: Bool = {
            os_unfair_lock_lock(&lock)
            defer { os_unfair_lock_unlock(&lock) }
            if count > 0 {
                count -= 1
                return false
            }
            return true
        }()

        if shouldWait {
            await withCheckedContinuation { cont in
                os_unfair_lock_lock(&lock)
                waiters.append(cont)
                os_unfair_lock_unlock(&lock)
            }
        }
    }

    public func signal() {
        var waiter: CheckedContinuation<Void, Never>?
        os_unfair_lock_lock(&lock)
        if !waiters.isEmpty {
            waiter = waiters.removeFirst()
        } else {
            count += 1
        }
        os_unfair_lock_unlock(&lock)
        waiter?.resume()
    }
}
