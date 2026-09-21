import Darwin
import Foundation

/// 线程安全的有界批次通道，连接工作线程生产者与异步消费者
public final class BatchChannel: @unchecked Sendable {
    private var mutex = pthread_mutex_t()
    private var workerCond = pthread_cond_t()

    private let capacity: Int
    private var queue: [ScanBatch]
    private var isClosed: Bool
    private var error: Error?

    private var pendingContinuation: CheckedContinuation<ScanBatch?, Error>?

    public init(capacity: Int) {
        self.capacity = max(2, capacity)
        self.queue = []
        self.isClosed = false
        self.error = nil

        pthread_mutex_init(&mutex, nil)
        pthread_cond_init(&workerCond, nil)
    }

    deinit {
        pthread_mutex_destroy(&mutex)
        pthread_cond_destroy(&workerCond)
    }

    /// 工作线程提交一个批次（当队列满时阻塞等待消费者取走）
    /// - Returns: 若通道已关闭或发生错误返回 false
    public func send(_ batch: ScanBatch) -> Bool {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }

        while queue.count >= capacity && !isClosed && error == nil {
            pthread_cond_wait(&workerCond, &mutex)
        }

        if isClosed || error != nil {
            return false
        }

        if let cont = pendingContinuation {
            pendingContinuation = nil
            cont.resume(returning: batch)
            return true
        }

        queue.append(batch)
        return true
    }

    /// 异步消费者获取下一个批次
    /// - Returns: 下一个批次；通道结束时返回 nil；发生错误时抛错
    public func next() async throws -> ScanBatch? {
        try await withCheckedThrowingContinuation { continuation in
            pthread_mutex_lock(&mutex)
            defer { pthread_mutex_unlock(&mutex) }

            if let err = self.error {
                continuation.resume(throwing: err)
                return
            }

            if !self.queue.isEmpty {
                let batch = self.queue.removeFirst()
                pthread_cond_signal(&self.workerCond)
                continuation.resume(returning: batch)
                return
            }

            if self.isClosed {
                continuation.resume(returning: nil)
                return
            }

            // 队列为空且未关闭，挂起等待生产者
            self.pendingContinuation = continuation
        }
    }

    /// 正常关闭通道
    public func close() {
        pthread_mutex_lock(&mutex)
        isClosed = true
        let cont = pendingContinuation
        pendingContinuation = nil
        pthread_cond_broadcast(&workerCond)
        pthread_mutex_unlock(&mutex)

        cont?.resume(returning: nil)
    }

    /// 异常中断通道并向消费者传播错误
    public func fail(_ err: Error) {
        pthread_mutex_lock(&mutex)
        if error == nil {
            error = err
        }
        isClosed = true
        let cont = pendingContinuation
        pendingContinuation = nil
        pthread_cond_broadcast(&workerCond)
        pthread_mutex_unlock(&mutex)

        cont?.resume(throwing: err)
    }

    /// 首个失败由通道保存；后续取消或 worker 故障不得覆盖它。
    public func failure() -> Error? {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }
        return error
    }

    /// 取消通道
    public func cancel() {
        fail(ScanError.cancelled)
    }
}
