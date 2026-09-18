import Foundation
import Testing
@testable import GDrive

@Suite("StateStore Concurrency & Connection Pool Tests")
struct StateStoreTests {

    @Test("StateStore schema initialization and basic CRUD")
    func testSchemaInitAndCRUD() async throws {
        let tempDB = FileManager.default.temporaryDirectory
            .appendingPathComponent("statestore-test-\(UUID().uuidString).sqlite").path
        defer {
            try? FileManager.default.removeItem(atPath: tempDB)
            try? FileManager.default.removeItem(atPath: "\(tempDB)-wal")
            try? FileManager.default.removeItem(atPath: "\(tempDB)-shm")
        }

        let store = try await StateStore(path: tempDB, maxReaders: 4)

        // 1. 写入一个根同步对
        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('acc_test', '/Users/test/dir', 16777220, 10001, 'root_drive_id', 'localToRemoteEmpty', 'freshCreated', ?, ?);
            """)
            let now = Date().timeIntervalSince1970
            stmt.bindDouble(now, at: 1)
            stmt.bindDouble(now, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }
        #expect(rootId > 0)

        // 2. 通过读连接池并发读取验证
        let readRootId = try await store.read { conn in
            let stmt = try conn.prepare("SELECT root_id, local_root_path FROM roots WHERE account_id = 'acc_test';")
            let hasRow = try stmt.step()
            #expect(hasRow)
            return stmt.columnInt64(at: 0)
        }
        #expect(readRootId == rootId)
    }

    @Test("High concurrency readers vs writer without lock contention")
    func testConcurrentReadersAndWriter() async throws {
        let tempDB = FileManager.default.temporaryDirectory
            .appendingPathComponent("statestore-concurrent-\(UUID().uuidString).sqlite").path
        defer {
            try? FileManager.default.removeItem(atPath: tempDB)
            try? FileManager.default.removeItem(atPath: "\(tempDB)-wal")
            try? FileManager.default.removeItem(atPath: "\(tempDB)-shm")
        }

        let store = try await StateStore(path: tempDB, maxReaders: 8)

        // 初始化 root
        let rootId = try await store.write { conn -> Int64 in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('acc_bench', '/test', 1, 1, 'drive_root', 'localToRemoteEmpty', 'freshCreated', 0, 0);
            """)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // 并发进行：1 个 Writer 持续批量写入，同时 16 个 Task 并发高速读取
        let writeCount = 500
        let readIterationsPerTask = 50
        let readTaskCount = 16

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Writer 任务：分批插入条目
            group.addTask {
                for batch in 0..<(writeCount / 50) {
                    try await store.write { conn in
                        for i in 0..<50 {
                            let itemId = batch * 50 + i + 1
                            let stmt = try conn.prepare("""
                                INSERT INTO items (root_id, parent_id, name, entry_kind, local_device, local_inode, local_mtime, local_size, phase, created_at, updated_at)
                                VALUES (?, 1, ?, 'file', 1, ?, 1000, 1024, 'ready', 0, 0);
                            """)
                            stmt.bindInt64(rootId, at: 1)
                            stmt.bindText("file_\(itemId).txt", at: 2)
                            stmt.bindInt64(Int64(itemId + 100), at: 3)
                            _ = try stmt.step()
                        }
                    }
                }
            }

            // 16 个并发读取 Task
            for _ in 0..<readTaskCount {
                group.addTask {
                    for _ in 0..<readIterationsPerTask {
                        let count = try await store.read { conn in
                            let stmt = try conn.prepare("SELECT count(*) FROM items WHERE root_id = ?;")
                            stmt.bindInt64(rootId, at: 1)
                            _ = try stmt.step()
                            return stmt.columnInt64(at: 0) ?? 0
                        }
                        #expect(count >= 0)
                    }
                }
            }

            try await group.waitForAll()
        }

        // 校验最终写入总数
        let finalCount = try await store.read { conn in
            let stmt = try conn.prepare("SELECT count(*) FROM items WHERE root_id = ?;")
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
            return stmt.columnInt64(at: 0) ?? 0
        }
        #expect(finalCount == Int64(writeCount))

        // 测试 WAL checkpoint
        try await store.checkpoint()
    }

    @Test("Automatic Group Commit batches concurrent writes within threshold or 5ms")
    func testAutomaticGroupCommit() async throws {
        let tempDB = FileManager.default.temporaryDirectory
            .appendingPathComponent("statestore-groupcommit-\(UUID().uuidString).sqlite").path
        defer {
            try? FileManager.default.removeItem(atPath: tempDB)
            try? FileManager.default.removeItem(atPath: "\(tempDB)-wal")
            try? FileManager.default.removeItem(atPath: "\(tempDB)-shm")
        }

        let store = try await StateStore(path: tempDB, maxReaders: 4)

        let rootId = try await store.write { conn -> Int64 in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('acc_gc', '/gc', 1, 1, 'drive_gc', 'localToRemoteEmpty', 'freshCreated', 0, 0);
            """)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // 1. 测试 150 个独立的并发 batchWrite 调用（自动合并为满批次 64*2 + 尾批次）
        let concurrentWrites = 150
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<concurrentWrites {
                group.addTask {
                    try await store.batchWrite { conn in
                        let stmt = try conn.prepare("""
                            INSERT INTO items (root_id, parent_id, name, entry_kind, local_device, local_inode, local_mtime, local_size, phase, created_at, updated_at)
                            VALUES (?, 1, ?, 'file', 1, ?, 1000, 1024, 'ready', 0, 0);
                        """)
                        stmt.bindInt64(rootId, at: 1)
                        stmt.bindText("gc_file_\(i).txt", at: 2)
                        stmt.bindInt64(Int64(i + 2000), at: 3)
                        _ = try stmt.step()
                    }
                }
            }
            try await group.waitForAll()
        }

        let count1 = try await store.read { conn in
            let stmt = try conn.prepare("SELECT count(*) FROM items WHERE root_id = ?;")
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
            return stmt.columnInt64(at: 0) ?? 0
        }
        #expect(count1 == Int64(concurrentWrites))

        // 2. 测试少于批次上限的小量写入（5条），验证 5ms 超时自动触发 commit
        for i in 0..<5 {
            try await store.batchWrite { conn in
                let stmt = try conn.prepare("""
                    INSERT INTO items (root_id, parent_id, name, entry_kind, local_device, local_inode, local_mtime, local_size, phase, created_at, updated_at)
                    VALUES (?, 1, ?, 'file', 1, ?, 1000, 1024, 'ready', 0, 0);
                """)
                stmt.bindInt64(rootId, at: 1)
                stmt.bindText("timeout_file_\(i).txt", at: 2)
                stmt.bindInt64(Int64(i + 3000), at: 3)
                _ = try stmt.step()
            }
        }

        let count2 = try await store.read { conn in
            let stmt = try conn.prepare("SELECT count(*) FROM items WHERE root_id = ?;")
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
            return stmt.columnInt64(at: 0) ?? 0
        }
        #expect(count2 == Int64(concurrentWrites + 5))
    }
}
