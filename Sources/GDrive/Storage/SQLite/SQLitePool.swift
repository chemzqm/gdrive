import Foundation

/// Concurrent read-only connection pools, multi-threaded lockless concurrent reads
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

    /// Borrow the connection from the read connection pool to perform a read-only query, and automatically return it after completion
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
                // Fallback waiting for existing connection if read-only connection creation fails
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

/// Write statistics metrics
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

/// Full-time single writer Actor,Supports instant writing and Group Commit Automatic batch merge submissions
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

    /// Get write statistics
    public func getStats() -> WriterStats {
        stats
    }

    /// Reset stats
    public func resetStats() {
        stats = WriterStats()
    }

    /// Immediately submit transactions independently (first file intention, key directory unlock, etc., never wait for approval)
    @discardableResult
    public func writeImmediate<T: Sendable>(_ block: @Sendable (SQLiteConnection) throws -> T) throws -> T {
        // First commit the batches in the current buffer to ensure sequential consistency
        flushPending(reason: .immediate)
        stats.immediateCommits += 1
        stats.totalCommits += 1
        stats.totalItems += 1
        return try connection.transaction {
            try block(connection)
        }
    }

    /// Slug: Submit now independently
    @discardableResult
    public func write<T: Sendable>(_ block: @Sendable (SQLiteConnection) throws -> T) throws -> T {
        try writeImmediate(block)
    }

    /// Group Commit Batch Write: Multiple Concurrency Worker Automatically merge into one transaction on submission
    /// Submit when any condition is met:
    /// 1. Buffer Full batchCapacity (Default 64 Item)
    /// 2. Timeout before first mission enlistment batchTimeoutMs (Default 5ms)
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

    /// Execute Directly SQL(As schema Initialization)
    public func executeDirect(_ sql: String) throws {
        try connection.execute(sql)
    }

    /// Force current buffer to be emptied and transaction committed
    public func flush() throws {
        flushPending(reason: .immediate)
    }

    /// Controlled WAL Checkpoint,will WAL Log flush back to master library file
    public func checkpoint() throws {
        flushPending(reason: .immediate)
        try connection.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    }
}
