import Darwin
import Foundation

/// 线程安全的有界目录任务调度器与工作线程协作器
public final class ScanScheduler: @unchecked Sendable {
    public enum EnqueueResult: Sendable {
        case enqueued
        case full
        case stopped
    }
    public enum ReservationResult: Sendable {
        case reserved
        case full
        case stopped
    }
    /// 待扫描目录工作项
    public struct DirectoryWork {
        public let dir: UnsafeMutablePointer<DIR>
        public let prefix: [UInt8]
        public let depth: Int

        public init(dir: UnsafeMutablePointer<DIR>, prefix: [UInt8], depth: Int) {
            self.dir = dir
            self.prefix = prefix
            self.depth = depth
        }
    }

    private var mutex = pthread_mutex_t()
    private var cond = pthread_cond_t()

    private let capacity: Int
    private var readyQueue: [DirectoryWork]
    private var reservedSlots: Int
    private var activeWorkers: Int
    private var isAborted: Bool
    private var isFinished: Bool

    public init(workers: Int) {
        self.capacity = max(4, workers * 2)
        self.readyQueue = []
        self.reservedSlots = 0
        self.activeWorkers = 0
        self.isAborted = false
        self.isFinished = false

        pthread_mutex_init(&mutex, nil)
        pthread_cond_init(&cond, nil)
    }

    deinit {
        drainAndCloseAll()
        pthread_mutex_destroy(&mutex)
        pthread_cond_destroy(&cond)
    }

    /// 尝试将子目录推入共享就绪队列
    /// - Returns: 成功时转移句柄所有权并返回 true；队列满或已停止时返回 false，所有权仍在调用方。
    public func tryEnqueue(_ work: DirectoryWork) -> Bool {
        enqueue(work) == .enqueued
    }

    /// 内部调用方需要区分背压和停止，以决定 DFS 或立即释放句柄。
    func enqueue(_ work: DirectoryWork) -> EnqueueResult {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }

        if isAborted || isFinished {
            return .stopped
        }

        if readyQueue.count + reservedSlots < capacity {
            readyQueue.append(work)
            pthread_cond_signal(&cond)
            return .enqueued
        }

        return .full
    }

    /// 尝试预留共享就绪队列的一个槽位（用于在分发子目录任务前先将当前批次刷入通道，保证父目录先于子节点输出）
    func tryReserve() -> ReservationResult {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }

        if isAborted || isFinished {
            return .stopped
        }

        if readyQueue.count + reservedSlots < capacity {
            reservedSlots += 1
            return .reserved
        }

        return .full
    }

    /// 提交预留的槽位并将任务推入共享就绪队列
    func commitReserved(_ work: DirectoryWork) {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }

        reservedSlots -= 1
        if isAborted || isFinished {
            closedir(work.dir)
            return
        }

        readyQueue.append(work)
        pthread_cond_signal(&cond)
    }

    /// 取消预留的槽位
    func cancelReservation() {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }

        reservedSlots -= 1
        if activeWorkers == 0 && readyQueue.isEmpty && reservedSlots == 0 {
            pthread_cond_broadcast(&cond)
        }
    }

    /// 工作线程尝试获取下一个目录任务
    /// - Parameter wasActive: 调用前该工作线程是否正持有活跃任务
    /// - Returns: 若获取到任务返回 DirectoryWork；若扫描已完成或取消返回 nil
    public func acquireWork(wasActive: Bool) -> DirectoryWork? {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }

        if wasActive {
            activeWorkers -= 1
        }

        while true {
            if isAborted || isFinished {
                return nil
            }

            if let work = readyQueue.popLast() {
                activeWorkers += 1
                return work
            }

            // 当就绪队列为空且没有其他活跃 worker 和预留槽位时，表示全树扫描完成
            if activeWorkers == 0 && reservedSlots == 0 {
                isFinished = true
                pthread_cond_broadcast(&cond)
                return nil
            }

            pthread_cond_wait(&cond, &mutex)
        }
    }

    /// 停止调度器，唤醒所有等待中的工作线程
    public func stop() {
        pthread_mutex_lock(&mutex)
        if !isAborted {
            isAborted = true
            pthread_cond_broadcast(&cond)
        }
        pthread_mutex_unlock(&mutex)
    }

    /// 仅表示异常停止；正常枚举完成仍允许 worker 提交尾批。
    public func shouldAbort() -> Bool {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }
        return isAborted
    }

    /// 清空就绪队列并关闭所有未领取的目录描述符
    public func drainAndCloseAll() {
        pthread_mutex_lock(&mutex)
        isAborted = true
        isFinished = true
        reservedSlots = 0
        let remaining = readyQueue
        readyQueue.removeAll()
        pthread_cond_broadcast(&cond)
        pthread_mutex_unlock(&mutex)

        for item in remaining {
            closedir(item.dir)
        }
    }
}
