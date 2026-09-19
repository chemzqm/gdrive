import Foundation

/// SQLite Synchronize baseline storage engines (StateStore)
/// Follow the v1.md §10.2 Core Principles:
/// - Single full-time writer (DedicatedWriter)Handle all writing transactions and eliminate lock contention
/// - Concurrent read-only connection pools (ReaderPool)Handle scan checksum scheduling queries with zero wait
/// - WAL Mode, memory bounded, support group commit & Controlled checkpoint
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

        // Make sure the directory exists
        let directory = URL(fileURLWithPath: resolvedPath).deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)

        // 1. Initialize single writer and apply Schema
        let writer = try DedicatedWriter(path: resolvedPath, batchCapacity: batchCapacity, batchTimeoutMs: batchTimeoutMs)
        try await Self.initSchema(writer: writer)
        self.writer = writer

        // 2. Initialize concurrent read-only connection pools
        self.readerPool = ReaderPool(path: resolvedPath, maxConnections: maxReaders)
    }

    /// Get the underlying writer statistic metrics
    public func getWriterStats() async -> WriterStats {
        await writer.getStats()
    }

    /// Execute concurrent read-only queries
    public func read<T: Sendable>(_ block: @escaping @Sendable (SQLiteConnection) throws -> T) async throws -> T {
        try await readerPool.withReader(block)
    }

    /// Perform instant serial writes (automatic independent transaction submission, first file/Directory Creation Submit Now)
    @discardableResult
    public func write<T: Sendable>(_ block: @Sendable (SQLiteConnection) throws -> T) async throws -> T {
        try await writer.writeImmediate(block)
    }

    /// Group Commit Batch write: Concurrent tasks are automatically merged into 64 Strips or 5ms Batch submission, greatly reducing the number of disk disks
    public func batchWrite(_ block: @escaping @Sendable (SQLiteConnection) throws -> Void) async throws {
        try await writer.batchWrite(block)
    }

    /// Force submission of batches with currently unbrushed disks
    public func flush() async throws {
        try await writer.flush()
    }

    /// Controlled WAL Checkpoint
    public func checkpoint() async throws {
        try await writer.checkpoint()
    }

    // MARK: - Schema Initialization

    private static func initSchema(writer: DedicatedWriter) async throws {
        let schemaSQL: String
        if let bundleURL = Bundle.module.url(forResource: "schema", withExtension: "sql"),
           let content = try? String(contentsOf: bundleURL, encoding: .utf8) {
            schemaSQL = content
        } else {
            // Development or test environment fallback read relative path
            let currentDir = FileManager.default.currentDirectoryPath
            let fallbackURL = URL(fileURLWithPath: currentDir).appendingPathComponent("Sources/GDrive/Storage/SQLite/schema.sql")
            if let content = try? String(contentsOf: fallbackURL, encoding: .utf8) {
                schemaSQL = content
            } else {
                throw NSError(domain: "StateStore", code: 1, userInfo: [NSLocalizedDescriptionKey: "schema.sql was not found"])
            }
        }

        try await writer.writeImmediate { conn in
            try conn.execute(schemaSQL)
        }
    }
}
