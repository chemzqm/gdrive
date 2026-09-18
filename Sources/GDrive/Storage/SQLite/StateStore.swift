import Foundation

/// SQLite 同步基线存储引擎（StateStore）
/// 遵循 v1.md §10.2 核心原则：
/// - 单专职写入者（DedicatedWriter）处理所有写事务，杜绝锁争用
/// - 并发只读连接池（ReaderPool）处理扫描校验与调度查询，零等待
/// - WAL 模式，内存有界，支持 group commit 与受控 checkpoint
public final class StateStore: Sendable {
    public let path: String
    public let writer: DedicatedWriter
    public let readerPool: ReaderPool

    public static var defaultDatabasePath: String {
        let home = FileManager.default.homeDirectoryForCurrentUser
        return home.appendingPathComponent(".gdrive/gdrive.sqlite").path
    }

    public init(
        path: String = StateStore.defaultDatabasePath,
        maxReaders: Int = max(4, ProcessInfo.processInfo.activeProcessorCount),
        batchCapacity: Int = 256,
        batchTimeoutMs: Int = 5
    ) async throws {
        let resolvedPath = (path as NSString).expandingTildeInPath
        self.path = resolvedPath

        // 确保目录存在
        let directory = URL(fileURLWithPath: resolvedPath).deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)

        // 1. 初始化单写者并应用 Schema
        let writer = try DedicatedWriter(path: path, batchCapacity: batchCapacity, batchTimeoutMs: batchTimeoutMs)
        try await Self.initSchema(writer: writer)
        self.writer = writer

        // 2. 初始化并发只读连接池
        self.readerPool = ReaderPool(path: path, maxConnections: maxReaders)
    }

    /// 获取底层写者统计指标
    public func getWriterStats() async -> WriterStats {
        await writer.getStats()
    }

    /// 执行并发只读查询
    public func read<T: Sendable>(_ block: @Sendable (SQLiteConnection) throws -> T) async throws -> T {
        try await readerPool.withReader(block)
    }

    /// 执行即时串行写操作（自动独立事务提交，首文件/目录创建立即提交）
    @discardableResult
    public func write<T: Sendable>(_ block: @Sendable (SQLiteConnection) throws -> T) async throws -> T {
        try await writer.writeImmediate(block)
    }

    /// Group Commit 批量写：并发任务自动合并至 64 条或 5ms 批次提交，大幅减少磁盘落盘次数
    public func batchWrite(_ block: @escaping @Sendable (SQLiteConnection) throws -> Void) async throws {
        try await writer.batchWrite(block)
    }

    /// 强制提交当前未刷盘的批次
    public func flush() async throws {
        try await writer.flush()
    }

    /// 受控 WAL Checkpoint
    public func checkpoint() async throws {
        try await writer.checkpoint()
    }

    // MARK: - Schema 初始化

    private static func initSchema(writer: DedicatedWriter) async throws {
        let schemaSQL: String
        if let bundleURL = Bundle.module.url(forResource: "schema", withExtension: "sql"),
           let content = try? String(contentsOf: bundleURL, encoding: .utf8) {
            schemaSQL = content
        } else {
            // 开发或测试环境回退读取相对路径
            let currentDir = FileManager.default.currentDirectoryPath
            let fallbackURL = URL(fileURLWithPath: currentDir).appendingPathComponent("Sources/GDrive/Storage/SQLite/schema.sql")
            if let content = try? String(contentsOf: fallbackURL, encoding: .utf8) {
                schemaSQL = content
            } else {
                throw NSError(domain: "StateStore", code: 1, userInfo: [NSLocalizedDescriptionKey: "未找到 schema.sql 文件"])
            }
        }

        try await writer.writeImmediate { conn in
            try conn.execute(schemaSQL)
            // 确保 store_meta 初始化
            let stmt = try conn.prepare("INSERT OR IGNORE INTO store_meta (singleton, schema_version, created_at, updated_at) VALUES (1, 1, ?, ?);")
            let now = Date().timeIntervalSince1970
            stmt.bindDouble(now, at: 1)
            stmt.bindDouble(now, at: 2)
            _ = try stmt.step()
        }
    }
}
