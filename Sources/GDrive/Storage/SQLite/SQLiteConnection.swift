import Foundation
import SQLite3

/// Lightweight SQLite Connect wrapper, encapsulate libsqlite3 C Interface
public final class SQLiteConnection: @unchecked Sendable {
    private var db: OpaquePointer?

    public init(path: String, readonly: Bool = false) throws {
        var flags = readonly ? SQLITE_OPEN_READONLY : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)
        flags |= SQLITE_OPEN_NOMUTEX // Use single-threaded lockless mode, concurrently connected by the outer connection pool and Actor Scheduling

        var pointer: OpaquePointer?
        let rc = sqlite3_open_v2(path, &pointer, flags, nil)
        guard rc == SQLITE_OK, let pointer else {
            let msg = pointer.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "Failed to open database (code \(rc))"
            if let pointer { sqlite3_close(pointer) }
            throw NSError(domain: "SQLiteConnection", code: Int(rc), userInfo: [NSLocalizedDescriptionKey: msg])
        }
        self.db = pointer

        let functionRC = sqlite3_create_function_v2(pointer, "gdrive_name_key", 1,
            SQLITE_UTF8 | SQLITE_DETERMINISTIC, nil, { context, _, args in
                guard let value = args?[0], let bytes = sqlite3_value_text(value) else {
                    sqlite3_result_null(context); return
                }
                let key = RemoteNameMapping.key(String(cString: bytes))
                key.withCString { sqlite3_result_text(context, $0, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self)) }
            }, nil, nil, nil)
        guard functionRC == SQLITE_OK else {
            throw NSError(domain: "SQLiteConnection", code: Int(functionRC))
        }

        // Basic Performance and Stability Configuration
        if !readonly {
            try execute("PRAGMA journal_mode = WAL;")
            try execute("PRAGMA synchronous = NORMAL;")
        }
        try execute("PRAGMA foreign_keys = ON;")
        try execute("PRAGMA busy_timeout = 5000;")
        try execute("PRAGMA temp_store = MEMORY;")
        try execute("PRAGMA cache_size = -64000;") // 64MB Page Cache
    }

    private var statementCache: [String: SQLiteStatement] = [:]

    deinit {
        close()
    }

    public func close() {
        statementCache.removeAll()
        if let db {
            sqlite3_close_v2(db)
            self.db = nil
        }
    }

    /// Get or reuse precompiled statements (automatic reset),Significantly increase throughput for bulk operations
    public func cachedStatement(_ sql: String) throws -> SQLiteStatement {
        if let stmt = statementCache[sql] {
            stmt.reset()
            return stmt
        }
        let stmt = try prepare(sql)
        statementCache[sql] = stmt
        return stmt
    }

    public func execute(_ sql: String) throws {
        guard let db else { throw NSError(domain: "SQLiteConnection", code: 1, userInfo: [NSLocalizedDescriptionKey: "Database is closed"]) }
        var errMsg: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(db, sql, nil, nil, &errMsg)
        if rc != SQLITE_OK {
            let msg = errMsg.flatMap { String(cString: $0) } ?? "Execution SQL Failed"
            sqlite3_free(errMsg)
            throw NSError(domain: "SQLiteConnection", code: Int(rc), userInfo: [NSLocalizedDescriptionKey: "\(msg): \(sql)"])
        }
    }

    public func prepare(_ sql: String) throws -> SQLiteStatement {
        guard let db else { throw NSError(domain: "SQLiteConnection", code: 1, userInfo: [NSLocalizedDescriptionKey: "Database is closed"]) }
        var stmt: OpaquePointer?
        let rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
        guard rc == SQLITE_OK, let stmt else {
            let msg = String(cString: sqlite3_errmsg(db))
            throw NSError(domain: "SQLiteConnection", code: Int(rc), userInfo: [NSLocalizedDescriptionKey: "Prepare failed: \(msg) [\(sql)]"])
        }
        return SQLiteStatement(stmt: stmt, db: db)
    }

    /// Execute a block of code in a transaction
    public func transaction<T>(_ block: () throws -> T) throws -> T {
        try execute("BEGIN IMMEDIATE TRANSACTION;")
        do {
            let result = try block()
            try execute("COMMIT;")
            return result
        } catch {
            try? execute("ROLLBACK;")
            throw error
        }
    }

    public var lastInsertRowId: Int64 {
        guard let db else { return 0 }
        return sqlite3_last_insert_rowid(db)
    }

    public var changes: Int {
        guard let db else { return 0 }
        return Int(sqlite3_changes(db))
    }
}

/// Precompiled SQL Statement Wrapping
public final class SQLiteStatement: @unchecked Sendable {
    private let stmt: OpaquePointer
    private let db: OpaquePointer

    init(stmt: OpaquePointer, db: OpaquePointer) {
        self.stmt = stmt
        self.db = db
    }

    deinit {
        sqlite3_finalize(stmt)
    }

    public func reset() {
        sqlite3_reset(stmt)
        sqlite3_clear_bindings(stmt)
    }

    // MARK: - Bind

    public func bindNull(at index: Int32) {
        sqlite3_bind_null(stmt, index)
    }

    public func bindInt64(_ value: Int64?, at index: Int32) {
        if let value {
            sqlite3_bind_int64(stmt, index, value)
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    public func bindText(_ value: String?, at index: Int32) {
        if let value {
            sqlite3_bind_text(stmt, index, (value as NSString).utf8String, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    public func bindDouble(_ value: Double?, at index: Int32) {
        if let value {
            sqlite3_bind_double(stmt, index, value)
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    public func bindBlob(_ value: Data?, at index: Int32) {
        if let value {
            _ = value.withUnsafeBytes { buffer in
                sqlite3_bind_blob(stmt, index, buffer.baseAddress, Int32(value.count), unsafeBitCast(-1, to: sqlite3_destructor_type.self))
            }
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    // MARK: - Execute & Read

    public func step() throws -> Bool {
        let rc = sqlite3_step(stmt)
        if rc == SQLITE_ROW {
            return true
        } else if rc == SQLITE_DONE {
            return false
        } else {
            let msg = String(cString: sqlite3_errmsg(db))
            throw NSError(domain: "SQLiteStatement", code: Int(rc), userInfo: [NSLocalizedDescriptionKey: "Step failed: \(msg)"])
        }
    }

    public func columnInt64(at index: Int32) -> Int64? {
        if sqlite3_column_type(stmt, index) == SQLITE_NULL { return nil }
        return sqlite3_column_int64(stmt, index)
    }

    public func columnText(at index: Int32) -> String? {
        if sqlite3_column_type(stmt, index) == SQLITE_NULL { return nil }
        guard let cStr = sqlite3_column_text(stmt, index) else { return nil }
        return String(cString: cStr)
    }

    public func columnDouble(at index: Int32) -> Double? {
        if sqlite3_column_type(stmt, index) == SQLITE_NULL { return nil }
        return sqlite3_column_double(stmt, index)
    }

    public func columnBlob(at index: Int32) -> Data? {
        if sqlite3_column_type(stmt, index) == SQLITE_NULL { return nil }
        guard let bytes = sqlite3_column_blob(stmt, index) else { return nil }
        let count = Int(sqlite3_column_bytes(stmt, index))
        return Data(bytes: bytes, count: count)
    }
}
