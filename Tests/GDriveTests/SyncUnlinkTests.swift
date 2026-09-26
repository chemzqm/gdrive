import Foundation
import Testing
@testable import GDrive

@Suite("Sync unlink")
struct SyncUnlinkTests {
    private struct Fixture {
        let directory: URL
        let local: URL
        let external: URL
        let store: StateStore
        let engine: SyncEngine
    }

    private func fixture(downloadName: String = "downloads") async throws -> Fixture {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "sync-unlink-\(UUID().uuidString)")
        let local = directory.appendingPathComponent("local")
        let external = directory.appendingPathComponent("external")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: external, withIntermediateDirectories: true)
        let authURL = external.appendingPathComponent("auth.json")
        let data = AuthData(
            clientId: "unlink-client", rootID: "root", accessToken: "token",
            expiresAt: Date().addingTimeInterval(3600))
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(data).write(to: authURL)
        let auth = try Auth(path: authURL.path)
        let store = try await StateStore(path: external.appendingPathComponent("state.sqlite").path)
        let engine = try await SyncEngine(
            auth: auth, store: store,
            downloadTemporaryDirectory: external.appendingPathComponent(downloadName, isDirectory: true),
            conflictDirectory: external.appendingPathComponent("conflicts", isDirectory: true))
        return Fixture(directory: directory, local: local, external: external, store: store, engine: engine)
    }

    private func seedRoot(
        _ fixture: Fixture, local: URL, remoteID: String, active: Bool = true
    ) async throws -> (rootID: Int64, itemID: Int64) {
        let identity = try #require(try LocalDirectoryIdentity.read(at: local))
        return try await fixture.store.write { conn in
            let root = try conn.prepare("""
                INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, is_active, created_at, updated_at)
                VALUES ('default', ?, ?, ?, ?, 'remoteToLocalEmpty', 'freshCreated', ?, 1, 1);
                """)
            defer { root.reset() }
            root.bindText(local.path, at: 1)
            root.bindInt64(identity.device, at: 2)
            root.bindInt64(identity.inode, at: 3)
            root.bindText(remoteID, at: 4)
            root.bindInt64(active ? 1 : 0, at: 5)
            _ = try root.step()
            let rootID = conn.lastInsertRowId
            let item = try conn.prepare("""
                INSERT INTO items(root_id, name, entry_kind, remote_file_id, local_status,
                    remote_status, phase, created_at, updated_at)
                VALUES (?, ?, 'directory', ?, 'present', 'present', 'committed', 1, 1);
                """)
            defer { item.reset() }
            item.bindInt64(rootID, at: 1)
            item.bindText(local.lastPathComponent, at: 2)
            item.bindText(remoteID, at: 3)
            _ = try item.step()
            return (rootID, conn.lastInsertRowId)
        }
    }

    private func count(_ store: StateStore, _ table: String, rootID: Int64) async throws -> Int64 {
        try await store.read { conn in
            let query = try conn.prepare("SELECT COUNT(*) FROM \(table) WHERE root_id = ?;")
            defer { query.reset() }
            query.bindInt64(rootID, at: 1)
            _ = try #require(try query.step())
            return try #require(query.columnInt64(at: 0))
        }
    }

    private func storagePaths(_ store: StateStore, rootID: Int64) async throws -> Set<String> {
        try await store.read { conn in
            let query = try conn.prepare(
                "SELECT path FROM root_storage_directories WHERE root_id = ?;")
            defer { query.reset() }
            query.bindInt64(rootID, at: 1)
            var paths = Set<String>()
            while try query.step() {
                if let path = query.columnText(at: 0) { paths.insert(path) }
            }
            return paths
        }
    }

    @Test("unlink deletes root state and owned artifacts while retaining normal and parent-removed files")
    private func deletesRootStateAndArtifacts() async throws {
        let test = try await fixture()
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let normal = test.local.appendingPathComponent("keep.txt")
        try Data("local".utf8).write(to: normal)
        let root = try await seedRoot(test, local: test.local, remoteID: "remote-a", active: false)
        let childID = try await test.store.write { conn in
            let child = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, created_at, updated_at)
                VALUES (?, ?, 'child.txt', 'file', 'child-a', 'present', 'present', 'blocked', 1, 1);
                """)
            defer { child.reset() }
            child.bindInt64(root.rootID, at: 1)
            child.bindInt64(root.itemID, at: 2)
            _ = try child.step()
            return conn.lastInsertRowId
        }
        let oldDownloads = test.external.appendingPathComponent("old-downloads/remote-a", isDirectory: true)
        let currentDownloads = test.external.appendingPathComponent("downloads/remote-a", isDirectory: true)
        let conflict = test.external.appendingPathComponent("conflicts/remote-a/conflict.txt")
        let parentRemoved = test.external.appendingPathComponent("parent_removed/1/preserved.txt")
        for url in [oldDownloads, currentDownloads, conflict.deletingLastPathComponent(), parentRemoved.deletingLastPathComponent()] {
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        }
        try Data("old".utf8).write(to: oldDownloads.appendingPathComponent("tmp"))
        try Data("current".utf8).write(to: currentDownloads.appendingPathComponent("tmp"))
        try Data("remote".utf8).write(to: conflict)
        try Data("preserved".utf8).write(to: parentRemoved)
        try await test.engine.registerSyncStorageDirectories(
            rootID: root.rootID, remoteRootID: "remote-a", downloadDirectory: oldDownloads)
        try await test.store.write { conn in
            let operation = try conn.prepare("""
                INSERT INTO operations(operation_id, root_id, item_id, operation_type, state, created_at, updated_at)
                VALUES ('unlink-op', ?, ?, 'uploadMultipart', 'ready', 1, 1);
                """)
            defer { operation.reset() }
            operation.bindInt64(root.rootID, at: 1)
            operation.bindInt64(childID, at: 2)
            _ = try operation.step()
            try conn.execute("INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at) VALUES (\(root.rootID), 'default', 'drive_changes', 'C0', 1);")
            try conn.execute("INSERT INTO remote_change_inbox(root_id, remote_id, payload) VALUES (\(root.rootID), 'child-a', '{}');")
            try conn.execute("INSERT INTO remote_directory_scans(root_id, remote_id, scan_id, state) VALUES (\(root.rootID), 'remote-a', 'scan', 'pending');")
            let syncConflict = try conn.prepare("""
                INSERT INTO sync_conflicts(conflict_id, root_id, item_id, remote_file_id, relative_path,
                    local_path, conflict_path, remote_sha256, remote_size, remote_status, created_at, updated_at)
                VALUES ('unlink-conflict', ?, ?, 'child-a', 'child.txt', ?, ?, ?, 6, 'present', 1, 1);
                """)
            defer { syncConflict.reset() }
            syncConflict.bindInt64(root.rootID, at: 1)
            syncConflict.bindInt64(childID, at: 2)
            syncConflict.bindText(test.local.appendingPathComponent("child.txt").path, at: 3)
            syncConflict.bindText(conflict.path, at: 4)
            syncConflict.bindText(String(repeating: "a", count: 64), at: 5)
            _ = try syncConflict.step()
            try conn.execute("""
                INSERT INTO sync_issues(issue_id, root_id, subject_key, relative_path, stage, category,
                    suggested_action, message, first_seen_at, last_seen_at)
                VALUES ('unlink-issue', \(root.rootID), 'child-a', 'child.txt', 'download', 'localIO',
                    'retry', 'failed', 1, 1);
                """)
            let localConflict = try conn.prepare("""
                INSERT INTO local_conflicts(conflict_id, root_id, original_path, stored_path, created_at)
                VALUES ('parent-unlink', ?, ?, ?, 1);
                """)
            defer { localConflict.reset() }
            localConflict.bindInt64(root.rootID, at: 1)
            localConflict.bindText(test.local.appendingPathComponent("old.txt").path, at: 2)
            localConflict.bindText(parentRemoved.path, at: 3)
            _ = try localConflict.step()
        }

        let otherLocal = test.directory.appendingPathComponent("other")
        try FileManager.default.createDirectory(at: otherLocal, withIntermediateDirectories: true)
        let other = try await seedRoot(test, local: otherLocal, remoteID: "remote-b")
        let otherStorage = test.external.appendingPathComponent("other-storage/remote-b")
        try FileManager.default.createDirectory(at: otherStorage, withIntermediateDirectories: true)
        let otherFile = otherStorage.appendingPathComponent("tmp")
        try Data("other".utf8).write(to: otherFile)
        try await test.engine.registerSyncStorageDirectories(
            rootID: other.rootID, remoteRootID: "remote-b", downloadDirectory: otherStorage)

        try await test.engine.unlink(localPath: test.local.path)

        #expect(try Data(contentsOf: normal) == Data("local".utf8))
        #expect(!FileManager.default.fileExists(atPath: oldDownloads.path))
        #expect(!FileManager.default.fileExists(atPath: currentDownloads.path))
        #expect(!FileManager.default.fileExists(atPath: conflict.path))
        #expect(try Data(contentsOf: parentRemoved) == Data("preserved".utf8))
        #expect(try Data(contentsOf: otherFile) == Data("other".utf8))
        for table in ["roots", "items", "operations", "cursors", "remote_change_inbox", "remote_directory_scans", "sync_conflicts", "sync_issues", "root_storage_directories"] {
            #expect(try await count(test.store, table, rootID: root.rootID) == 0)
        }
        #expect(try await count(test.store, "local_conflicts", rootID: root.rootID) == 0)
        #expect(try await count(test.store, "roots", rootID: other.rootID) == 1)
    }

    @Test("unlink is idempotent for missing bindings and removes orphan records without deleting their bytes")
    private func removesOrphanRecords() async throws {
        let test = try await fixture()
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let missing = test.directory.appendingPathComponent("missing-root")
        let preserved = test.external.appendingPathComponent("parent_removed/orphan.txt")
        try FileManager.default.createDirectory(at: preserved.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("orphan".utf8).write(to: preserved)
        try await test.store.write { conn in
            let record = try conn.prepare("""
                INSERT INTO local_conflicts(conflict_id, root_id, original_path, stored_path, created_at)
                VALUES ('orphan-unlink', 999, ?, ?, 1);
                """)
            defer { record.reset() }
            record.bindText(missing.appendingPathComponent("file.txt").path, at: 1)
            record.bindText(preserved.path, at: 2)
            _ = try record.step()
        }

        try await test.engine.unlink(localPath: missing.path)
        try await test.engine.unlink(localPath: missing.path)

        #expect(try Data(contentsOf: preserved) == Data("orphan".utf8))
        #expect(try await count(test.store, "local_conflicts", rootID: 999) == 0)
    }

    @Test("a filesystem cleanup failure retains the binding and succeeds after the target is repaired")
    private func retainsRecordsAfterFilesystemFailure() async throws {
        let test = try await fixture()
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let root = try await seedRoot(test, local: test.local, remoteID: "remote-failure")
        let broken = URL(fileURLWithPath: "/dev/null/unlink-storage")
        try await test.engine.registerSyncStorageDirectories(
            rootID: root.rootID, remoteRootID: "remote-failure", downloadDirectory: broken)

        await #expect(throws: POSIXError.self) {
            try await test.engine.unlink(localPath: test.local.path)
        }
        #expect(try await count(test.store, "roots", rootID: root.rootID) == 1)
        try await test.store.write { conn in
            let delete = try conn.prepare("DELETE FROM root_storage_directories WHERE root_id = ? AND path = ?;")
            defer { delete.reset() }
            delete.bindInt64(root.rootID, at: 1)
            delete.bindText(RootSyncCoordinator.normalizedPath(broken.path), at: 2)
            _ = try delete.step()
        }
        try await test.engine.unlink(localPath: test.local.path)
        #expect(try await count(test.store, "roots", rootID: root.rootID) == 0)
    }

    @Test("unlink retains locating rows when the final SQLite transaction fails")
    private func retainsRowsAfterSQLiteFailure() async throws {
        let test = try await fixture()
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let root = try await seedRoot(test, local: test.local, remoteID: "remote-sql")
        let storage = test.external.appendingPathComponent("staging/remote-sql")
        let preserved = test.external.appendingPathComponent("parent_removed/sql.txt")
        try FileManager.default.createDirectory(at: storage, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: preserved.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("staging".utf8).write(to: storage.appendingPathComponent("tmp"))
        try Data("preserved".utf8).write(to: preserved)
        try await test.engine.registerSyncStorageDirectories(
            rootID: root.rootID, remoteRootID: "remote-sql", downloadDirectory: storage)
        try await test.store.write { conn in
            let record = try conn.prepare("""
                INSERT INTO local_conflicts(conflict_id, root_id, original_path, stored_path, created_at)
                VALUES ('sql-unlink', ?, ?, ?, 1);
                """)
            defer { record.reset() }
            record.bindInt64(root.rootID, at: 1)
            record.bindText(test.local.appendingPathComponent("old.txt").path, at: 2)
            record.bindText(preserved.path, at: 3)
            _ = try record.step()
            try conn.execute("""
                CREATE TRIGGER fail_unlink_root BEFORE DELETE ON roots
                WHEN OLD.root_id = \(root.rootID)
                BEGIN SELECT RAISE(ABORT, 'injected unlink database failure'); END;
                """)
        }

        await #expect(throws: SQLiteError.Statement.self) {
            try await test.engine.unlink(localPath: test.local.path)
        }
        #expect(!FileManager.default.fileExists(atPath: storage.path))
        #expect(try await count(test.store, "roots", rootID: root.rootID) == 1)
        #expect(try await count(test.store, "local_conflicts", rootID: root.rootID) == 1)
        let expectedStoragePaths: Set<String> = [
            RootSyncCoordinator.normalizedPath(storage.path),
            RootSyncCoordinator.normalizedPath(
                test.external.appendingPathComponent("conflicts/remote-sql").path),
            RootSyncCoordinator.normalizedPath(
                test.external.appendingPathComponent("downloads/remote-sql").path)
        ]
        #expect(try await storagePaths(test.store, rootID: root.rootID) == expectedStoragePaths)
        try await test.store.write { try $0.execute("DROP TRIGGER fail_unlink_root;") }
        try await test.engine.unlink(localPath: test.local.path)
        #expect(try await count(test.store, "roots", rootID: root.rootID) == 0)
        #expect(try await count(test.store, "local_conflicts", rootID: root.rootID) == 0)
        #expect(try Data(contentsOf: preserved) == Data("preserved".utf8))
    }

    @Test("unlink protects unindexed parent-removed bytes from a storage overlap")
    private func protectsUnindexedParentRemovedBytes() async throws {
        let test = try await fixture()
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let root = try await seedRoot(test, local: test.local, remoteID: "remote-parent-removed")
        let parentRemoved = test.external.appendingPathComponent("parent_removed", isDirectory: true)
        let preserved = parentRemoved.appendingPathComponent("unindexed.txt")
        try FileManager.default.createDirectory(at: parentRemoved, withIntermediateDirectories: true)
        try Data("unindexed".utf8).write(to: preserved)
        try await test.engine.registerSyncStorageDirectories(
            rootID: root.rootID, remoteRootID: "remote-parent-removed",
            downloadDirectory: parentRemoved)

        await #expect(throws: SyncEngineError.self) {
            try await test.engine.unlink(localPath: test.local.path)
        }
        #expect(try Data(contentsOf: preserved) == Data("unindexed".utf8))
        #expect(try await count(test.store, "roots", rootID: root.rootID) == 1)
        #expect(try await count(test.store, "local_conflicts", rootID: root.rootID) == 0)
    }

    @Test("unlink removes a replaced storage symlink without following it")
    private func removesReplacedStorageSymlink() async throws {
        let test = try await fixture()
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let root = try await seedRoot(test, local: test.local, remoteID: "remote-link")
        let storage = test.external.appendingPathComponent("staging/remote-link")
        let unrelated = test.external.appendingPathComponent("unrelated")
        try FileManager.default.createDirectory(at: storage, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: unrelated, withIntermediateDirectories: true)
        let unrelatedFile = unrelated.appendingPathComponent("keep")
        try Data("unrelated".utf8).write(to: unrelatedFile)
        try await test.engine.registerSyncStorageDirectories(
            rootID: root.rootID, remoteRootID: "remote-link", downloadDirectory: storage)
        try FileManager.default.removeItem(at: storage)
        try FileManager.default.createSymbolicLink(at: storage, withDestinationURL: unrelated)

        try await test.engine.unlink(localPath: test.local.path)
        #expect(try Data(contentsOf: unrelatedFile) == Data("unrelated".utf8))
        #expect(!FileManager.default.fileExists(atPath: storage.path))
        #expect(try await count(test.store, "roots", rootID: root.rootID) == 0)
    }

    @Test("unlink does not follow a current staging symlink without storage history")
    private func doesNotFollowCurrentStagingSymlink() async throws {
        let test = try await fixture()
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let root = try await seedRoot(test, local: test.local, remoteID: "remote-current-link")
        let staging = test.external.appendingPathComponent("downloads/remote-current-link")
        let unrelated = test.external.appendingPathComponent("unrelated-current")
        try FileManager.default.createDirectory(at: unrelated, withIntermediateDirectories: true)
        let unrelatedFile = unrelated.appendingPathComponent("keep")
        try Data("unrelated".utf8).write(to: unrelatedFile)
        try FileManager.default.createDirectory(at: staging.deletingLastPathComponent(), withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(at: staging, withDestinationURL: unrelated)

        try await test.engine.unlink(localPath: test.local.path)

        #expect(try Data(contentsOf: unrelatedFile) == Data("unrelated".utf8))
        #expect(!FileManager.default.fileExists(atPath: staging.path))
        #expect(try await count(test.store, "roots", rootID: root.rootID) == 0)
    }

    @Test("unlink supports a symlinked configured staging base")
    private func supportsSymlinkedStagingBase() async throws {
        let test = try await fixture()
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let root = try await seedRoot(test, local: test.local, remoteID: "remote-base-link")
        let actualBase = test.external.appendingPathComponent("actual-downloads")
        let configuredBase = test.external.appendingPathComponent("configured-downloads")
        try FileManager.default.createDirectory(at: actualBase, withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(at: configuredBase, withDestinationURL: actualBase)
        let storage = actualBase.appendingPathComponent("remote-base-link")
        try FileManager.default.createDirectory(at: storage, withIntermediateDirectories: true)
        try Data("owned".utf8).write(to: storage.appendingPathComponent("tmp"))
        let engine = try await SyncEngine(
            auth: test.engine.auth, store: test.store,
            downloadTemporaryDirectory: configuredBase,
            conflictDirectory: test.external.appendingPathComponent("conflicts", isDirectory: true))

        try await engine.unlink(localPath: test.local.path)

        #expect(!FileManager.default.fileExists(atPath: storage.path))
        #expect(try await count(test.store, "roots", rootID: root.rootID) == 0)
    }

    @Test("unlink removes storage registered before an engine restart and configuration change")
    private func removesHistoricalStorageAfterRestart() async throws {
        let test = try await fixture(downloadName: "old-downloads")
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let root = try await seedRoot(test, local: test.local, remoteID: "remote-history")
        let oldStorage = test.external.appendingPathComponent("old-downloads/remote-history")
        try FileManager.default.createDirectory(at: oldStorage, withIntermediateDirectories: true)
        try Data("old".utf8).write(to: oldStorage.appendingPathComponent("tmp"))
        try await test.engine.registerSyncStorageDirectories(
            rootID: root.rootID, remoteRootID: "remote-history", downloadDirectory: oldStorage)
        let newStorage = test.external.appendingPathComponent("new-downloads/remote-history")
        try FileManager.default.createDirectory(at: newStorage, withIntermediateDirectories: true)
        try Data("new".utf8).write(to: newStorage.appendingPathComponent("tmp"))
        let restarted = try await SyncEngine(
            auth: test.engine.auth, store: test.store,
            downloadTemporaryDirectory: test.external.appendingPathComponent("new-downloads", isDirectory: true),
            conflictDirectory: test.external.appendingPathComponent("new-conflicts", isDirectory: true))

        try await restarted.unlink(localPath: test.local.path)

        #expect(!FileManager.default.fileExists(atPath: oldStorage.path))
        #expect(!FileManager.default.fileExists(atPath: newStorage.path))
        #expect(try await count(test.store, "roots", rootID: root.rootID) == 0)
    }

    @Test("unlink rejects a target that would recursively remove another root's recorded storage")
    private func rejectsOtherRootStorageOverlap() async throws {
        let test = try await fixture()
        defer { try? FileManager.default.removeItem(at: test.directory) }
        let first = try await seedRoot(test, local: test.local, remoteID: "remote-first")
        let secondLocal = test.directory.appendingPathComponent("second")
        try FileManager.default.createDirectory(at: secondLocal, withIntermediateDirectories: true)
        let second = try await seedRoot(test, local: secondLocal, remoteID: "remote-second")
        let firstStorage = test.external.appendingPathComponent("shared/remote-first")
        let secondStorage = firstStorage.appendingPathComponent("nested/remote-second")
        try FileManager.default.createDirectory(at: secondStorage, withIntermediateDirectories: true)
        try Data("must survive".utf8).write(to: secondStorage.appendingPathComponent("file"))
        try await test.engine.registerSyncStorageDirectories(
            rootID: first.rootID, remoteRootID: "remote-first", downloadDirectory: firstStorage)
        await #expect(throws: SyncEngineError.self) {
            try await test.engine.registerSyncStorageDirectories(
                rootID: second.rootID, remoteRootID: "remote-second", downloadDirectory: secondStorage)
        }
        try await test.store.write { conn in
            let insert = try conn.prepare("""
                INSERT INTO root_storage_directories(root_id, path) VALUES (?, ?);
                """)
            defer { insert.reset() }
            insert.bindInt64(second.rootID, at: 1)
            insert.bindText(RootSyncCoordinator.normalizedPath(secondStorage.path), at: 2)
            _ = try insert.step()
        }

        await #expect(throws: SyncEngineError.self) {
            try await test.engine.unlink(localPath: test.local.path)
        }
        #expect(try Data(contentsOf: secondStorage.appendingPathComponent("file")) == Data("must survive".utf8))
        #expect(try await count(test.store, "roots", rootID: first.rootID) == 1)
        #expect(try await count(test.store, "roots", rootID: second.rootID) == 1)
    }
}
