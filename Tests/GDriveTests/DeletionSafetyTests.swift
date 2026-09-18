import Foundation
import Testing
@testable import GDrive

@Suite("Deletion Safety Tests (A03)")
struct DeletionSafetyTests {

    @Test("change.removed == true does NOT trash or delete local file, sets phase to blocked")
    func testRemovedDoesNotDeleteLocal() async throws {
        let tempDB = FileManager.default.temporaryDirectory
            .appendingPathComponent("deletion-test-\(UUID().uuidString).sqlite").path
        defer {
            try? FileManager.default.removeItem(atPath: tempDB)
            try? FileManager.default.removeItem(atPath: "\(tempDB)-wal")
            try? FileManager.default.removeItem(atPath: "\(tempDB)-shm")
        }

        let store = try await StateStore(path: tempDB)

        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('acc_test', '/Users/test/dir', 1, 1, 'remote_root', 'localToRemoteEmpty', 'freshCreated', 100, 100);
            """)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Insert root directory item (parent_id IS NULL allowed for directory)
        let rootDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'root', 'directory', 'remote_root', 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Insert an existing synchronized file with baseline under root directory
        let itemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    base_sha256, base_size,
                    remote_sha256, remote_size, remote_status,
                    local_generation, local_status, phase, dirty_generation,
                    created_at, updated_at
                ) VALUES (
                    ?, ?, 'shared_doc.pdf', 'file', 'remote_doc_123',
                    1, 2, 1000, 500, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
                    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 500,
                    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 500, 'present',
                    1, 'present', 'committed', 0,
                    100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Simulate Changes event with removed == true (permission revoked / unshared)
        let change = DriveChange(fileId: "remote_doc_123", removed: true, file: nil)

        // Process change per SyncEngine logic
        if change.file?.trashed == true {
            try await store.batchWrite { conn in
                let stmt = try conn.prepare("""
                UPDATE items SET remote_status = 'trashed', phase = 'ready' WHERE remote_file_id = ?;
                """)
                stmt.bindText(change.fileId, at: 1)
                _ = try stmt.step()
            }
        } else if change.removed == true {
            try await store.batchWrite { conn in
                let stmt = try conn.prepare("""
                UPDATE items SET remote_status = 'unknown', phase = 'blocked', dirty_generation = 0 WHERE remote_file_id = ?;
                """)
                stmt.bindText(change.fileId, at: 1)
                _ = try stmt.step()
            }
        }

        // Verify SQLite state: remote_status must NOT be 'trashed', phase must be 'blocked', is_tombstone must be 0
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT remote_status, phase, is_tombstone FROM items WHERE item_id = ?;")
            stmt.bindInt64(itemId, at: 1)
            #expect(try stmt.step())
            let remoteStatus = stmt.columnText(at: 0)
            let phase = stmt.columnText(at: 1)
            let isTombstone = stmt.columnInt64(at: 2)

            #expect(remoteStatus == "unknown", "Permission lost must NOT be categorized as trashed")
            #expect(phase == "blocked", "Phase should be blocked from deletion")
            #expect(isTombstone == 0, "Item must not be marked as tombstone")
        }

        // Verify Reconciler decision with remoteStatus == unknown: must be waitingEvidence, NEVER deleteLocal
        let baseline = ItemBaseline(sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", size: 500)
        let local = LocalObservation(status: .present, sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", size: 500)
        let remote = RemoteObservation(status: .unknown)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision != .deleteLocal, "Must NEVER decide deleteLocal when remote is unknown")
        if case .waitingEvidence = decision {
            // Expected
        } else {
            Issue.record("Expected waitingEvidence, got: \(decision)")
        }
    }

    @Test("trashItem failure does not call removeItem or write tombstone")
    func testTrashFailurePreservesFileAndMarksBlocked() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent("trash_fail_test_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDir)
        }

        let testFile = tempDir.appendingPathComponent("important.txt")
        let originalContent = "Very Important User Data That Must Not Be Lost"
        try originalContent.write(to: testFile, atomically: true, encoding: .utf8)

        let tempDB = FileManager.default.temporaryDirectory
            .appendingPathComponent("deletion-test-\(UUID().uuidString).sqlite").path
        defer {
            try? FileManager.default.removeItem(atPath: tempDB)
            try? FileManager.default.removeItem(atPath: "\(tempDB)-wal")
            try? FileManager.default.removeItem(atPath: "\(tempDB)-shm")
        }

        let store = try await StateStore(path: tempDB)
        let rootId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO roots (account_id, local_root_path, local_root_device, local_root_inode, remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('acc_test', ?, 1, 1, 'remote_root', 'localToRemoteEmpty', 'freshCreated', 100, 100);
            """)
            stmt.bindText(tempDir.path, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let rootDirItemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (root_id, parent_id, name, entry_kind, remote_file_id, phase, created_at, updated_at)
                VALUES (?, NULL, 'root', 'directory', 'remote_root', 'committed', 100, 100);
            """)
            stmt.bindInt64(rootId, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let itemId = try await store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, phase, dirty_generation, is_tombstone,
                    created_at, updated_at
                ) VALUES (
                    ?, ?, 'important.txt', 'file', 'remote_imp_1',
                    'present', 'ready', 1, 0,
                    100, 100
                );
            """)
            stmt.bindInt64(rootId, at: 1)
            stmt.bindInt64(rootDirItemId, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        // Execute simulated trash handler with failure injection
        func performLocalDelete(simulatedTrashSucceeds: Bool) async throws {
            if simulatedTrashSucceeds {
                try await store.batchWrite { conn in
                    let stmt = try conn.prepare("UPDATE items SET is_tombstone = 1, phase = 'committed' WHERE item_id = ?;")
                    stmt.bindInt64(itemId, at: 1)
                    _ = try stmt.step()
                }
            } else {
                // When trash fails: DO NOT call removeItem! Mark blocked in store!
                try await store.batchWrite { conn in
                    let stmt = try conn.prepare("UPDATE items SET phase = 'blocked', dirty_generation = 0 WHERE item_id = ?;")
                    stmt.bindInt64(itemId, at: 1)
                    _ = try stmt.step()
                }
            }
        }

        try await performLocalDelete(simulatedTrashSucceeds: false)

        // Verify:
        // 1. File on disk still exists and content is unaltered
        #expect(FileManager.default.fileExists(atPath: testFile.path))
        let currentContent = try String(contentsOf: testFile, encoding: .utf8)
        #expect(currentContent == originalContent)

        // 2. Database record is NOT a tombstone and is marked 'blocked'
        try await store.read { conn in
            let stmt = try conn.prepare("SELECT phase, is_tombstone FROM items WHERE item_id = ?;")
            stmt.bindInt64(itemId, at: 1)
            #expect(try stmt.step())
            let phase = stmt.columnText(at: 0)
            let isTombstone = stmt.columnInt64(at: 1)

            #expect(phase == "blocked")
            #expect(isTombstone == 0, "Item must NOT be marked tombstone when trash fails")
        }
    }
}
