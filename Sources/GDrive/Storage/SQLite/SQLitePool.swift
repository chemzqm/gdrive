import Foundation

/// 并发只读连接池，多线程无锁并发读取
public actor ReaderPool {
    private let path: String
    private let maxConnections: Int
    private var available: [SQLiteConnection] = []
    private var totalCreated: Int = 0
    private var waiters: [CheckedContinuation<SQLiteConnection, Never>] = []

    public init(path: String, maxConnections: Int = max(4, ProcessInfo.processInfo.activeProcessorCount)) {
        self.path = path
        self.maxConnections = maxConnections
    }

    /// 从读连接池借用连接执行只读查询，完成后自动归还
    public func withReader<T: Sendable>(_ block: @Sendable (SQLiteConnection) throws -> T) async throws -> T {
        let conn = await acquire()
        defer { release(conn) }
        return try block(conn)
    }

    private func acquire() async -> SQLiteConnection {
        if let conn = available.popLast() {
            return conn
        }
        if totalCreated < maxConnections {
            do {
                let conn = try SQLiteConnection(path: path, readonly: true)
                totalCreated += 1
                return conn
            } catch {
                // 若只读连接创建失败，回退等待已有连接
            }
        }
        return await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    private func release(_ conn: SQLiteConnection) {
        if !waiters.isEmpty {
            let waiter = waiters.removeFirst()
            waiter.resume(returning: conn)
        } else {
            available.append(conn)
        }
    }
}

/// 写入统计指标
public struct WriterStats: Sendable {
    public var totalCommits: Int = 0
    public var totalItems: Int = 0
    public var capacityTriggeredCommits: Int = 0
    public var timeoutTriggeredCommits: Int = 0
    public var immediateCommits: Int = 0

    public var averageBatchSize: Double {
        totalCommits > 0 ? Double(totalItems) / Double(totalCommits) : 0
    }
}

/// 专职单写者 Actor，支持即时写与 Group Commit 自动批次合并提交
public actor DedicatedWriter {
    private let connection: SQLiteConnection
    public let batchCapacity: Int
    public let batchTimeoutMs: Int

    private enum FlushReason {
        case capacity
        case timeout
        case immediate
    }

    private struct PendingWriteTask {
        let block: (SQLiteConnection) throws -> Void
        let continuation: CheckedContinuation<Void, Error>
    }

    private var pendingQueue: [PendingWriteTask] = []
    private var timerTask: Task<Void, Never>?
    private var stats = WriterStats()

    public init(
        path: String,
        batchCapacity: Int = 256,
        batchTimeoutMs: Int = 5
    ) throws {
        self.connection = try SQLiteConnection(path: path, readonly: false)
        self.batchCapacity = batchCapacity
        self.batchTimeoutMs = batchTimeoutMs
    }

    /// 获取写入统计信息
    public func getStats() -> WriterStats {
        stats
    }

    /// 重置统计信息
    public func resetStats() {
        stats = WriterStats()
    }

    /// 立即独立提交事务（首文件意图、关键目录解锁等，绝不等待凑批）
    @discardableResult
    public func writeImmediate<T: Sendable>(_ block: @Sendable (SQLiteConnection) throws -> T) throws -> T {
        // 先将当前缓冲中的批次提交，保证顺序一致性
        flushPending(reason: .immediate)
        stats.immediateCommits += 1
        stats.totalCommits += 1
        stats.totalItems += 1
        return try connection.transaction {
            try block(connection)
        }
    }

    /// 别名：立即独立提交
    @discardableResult
    public func write<T: Sendable>(_ block: @Sendable (SQLiteConnection) throws -> T) throws -> T {
        try writeImmediate(block)
    }

    /// Group Commit 批量写入：多个并发 Worker 提交时自动合并为一个事务
    /// 满足任意条件即提交：
    /// 1. 缓冲区满 batchCapacity (默认 64 项)
    /// 2. 距离首个任务入队超时 batchTimeoutMs (默认 5ms)
    public func batchWrite(_ block: @escaping @Sendable (SQLiteConnection) throws -> Void) async throws {
        try await withCheckedThrowingContinuation { continuation in
            let task = PendingWriteTask(block: block, continuation: continuation)
            pendingQueue.append(task)

            if pendingQueue.count >= batchCapacity {
                flushPending(reason: .capacity)
            } else if pendingQueue.count == 1 {
                scheduleBatchTimer()
            }
        }
    }

    private func scheduleBatchTimer() {
        timerTask?.cancel()
        let timeout = batchTimeoutMs
        timerTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(timeout) * 1_000_000)
            await self?.timerTriggered()
        }
    }

    private func timerTriggered() {
        if !pendingQueue.isEmpty {
            flushPending(reason: .timeout)
        }
    }

    private func flushPending(reason: FlushReason) {
        timerTask?.cancel()
        timerTask = nil

        guard !pendingQueue.isEmpty else { return }
        let currentBatch = pendingQueue
        pendingQueue.removeAll(keepingCapacity: true)

        do {
            try connection.transaction {
                for task in currentBatch {
                    try task.block(connection)
                }
            }
            stats.totalCommits += 1
            stats.totalItems += currentBatch.count
            switch reason {
            case .capacity: stats.capacityTriggeredCommits += 1
            case .timeout: stats.timeoutTriggeredCommits += 1
            case .immediate: break
            }
            for task in currentBatch {
                task.continuation.resume()
            }
        } catch {
            for task in currentBatch {
                task.continuation.resume(throwing: error)
            }
        }
    }

    /// 执行直接 SQL（如 schema 初始化）
    public func executeDirect(_ sql: String) throws {
        try connection.execute(sql)
    }

    /// 强制清空当前缓冲区并提交事务
    public func flush() throws {
        flushPending(reason: .immediate)
    }

    /// 受控 WAL Checkpoint，将 WAL 日志刷回主库文件
    public func checkpoint() throws {
        flushPending(reason: .immediate)
        try connection.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    }
}
