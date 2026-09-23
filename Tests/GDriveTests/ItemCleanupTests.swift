import Foundation
import os
import Testing
@testable import GDrive

private struct CleanupRequestState: Sendable {
    var trashed: [String] = []
    var trashAttempts: [String] = []
    var failNextTrash = false
}

private final class CleanupProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let context = TestHTTPContext<OSAllocatedUnfairLock<CleanupRequestState>>.value(for: request)!
        let statusCode = if request.httpMethod == "PATCH" {
            context.withLock { state in
                state.trashAttempts.append(request.url!.lastPathComponent)
                if state.failNextTrash {
                    state.failNextTrash = false
                    return 400
                } else {
                    state.trashed.append(request.url!.lastPathComponent)
                    return 204
                }
            }
        } else { 204 }
        let response = HTTPURLResponse(
            url: request.url!, statusCode: statusCode, httpVersion: nil, headerFields: nil)!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}

@Suite("Item cleanup")
struct ItemCleanupTests {
    private let context = TestHTTPContext(OSAllocatedUnfairLock(initialState: CleanupRequestState()))

    private struct Fixture {
        let directory: URL
        let localRoot: URL
        let conflictBase: URL
        let store: StateStore
        let engine: SyncEngine
        let rootID: Int64
        let rootItemID: Int64
    }

    private struct StoredLocalConflict {
        let rootID: Int64
        let originalPath: String
        let storedPath: String
    }

    private func storedLocalConflicts(_ store: StateStore) async throws -> [StoredLocalConflict] {
        try await store.read { conn in
            let stmt = try conn.prepare("""
                SELECT root_id, original_path, stored_path
                FROM local_conflicts ORDER BY original_path;
                """)
            defer { stmt.reset() }
            var result: [StoredLocalConflict] = []
            while try stmt.step(), let rootID = stmt.columnInt64(at: 0),
                  let original = stmt.columnText(at: 1),
                  let stored = stmt.columnText(at: 2) {
                result.append(.init(
                    rootID: rootID, originalPath: original, storedPath: stored))
            }
            return result
        }
    }

    private func fixture() async throws -> Fixture {
        context.value.withLock { $0 = CleanupRequestState() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("item-cleanup-\(UUID().uuidString)")
        let localRoot = directory.appendingPathComponent("local")
        let conflictBase = directory.appendingPathComponent("conflicts")
        try FileManager.default.createDirectory(at: localRoot, withIntermediateDirectories: true)
        let authURL = directory.appendingPathComponent("auth.json")
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        try encoder.encode(AuthData(
            clientId: "test", accessToken: "test",
            expiresAt: Date().addingTimeInterval(3600))).write(to: authURL)
        let configuration = URLSessionConfiguration.ephemeral
        context.configure(configuration)
        configuration.protocolClasses = [CleanupProtocol.self]
        let auth = try Auth(path: authURL.path)
        let client = DriveClient(
            auth: auth, session: URLSession(configuration: configuration), requestsPerSecond: nil)
        let store = try await StateStore(path: directory.appendingPathComponent("state.sqlite").path)
        let rootID = try await store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO roots(account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at)
                VALUES ('default', ?, 1, 1, 'remote-root', 'localToRemoteEmpty',
                    'existingKnown', 1, 1);
                """)
            stmt.bindText(localRoot.path, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }
        let rootItemID = try await store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, created_at, updated_at)
                VALUES (?, NULL, 'local', 'directory', 'remote-root', 'present', 'present',
                    'committed', 1, 1);
                """)
            stmt.bindInt64(rootID, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }
        let engine = try await SyncEngine(
            auth: auth, store: store, client: client,
            downloadTemporaryDirectory: directory.appendingPathComponent("remotes"),
            conflictDirectory: conflictBase)
        return Fixture(directory: directory, localRoot: localRoot, conflictBase: conflictBase,
            store: store, engine: engine, rootID: rootID, rootItemID: rootItemID)
    }

    @Test("Local directory deletion cancels subtree work and removes every related row")
    func localToRemoteDirectory() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        struct IDs { let directory: Int64; let file: Int64 }
        let ids = try await fixture.store.write { conn -> IDs in
            let directory = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'removed', 'directory', 'remote-directory', 'absent', 'present',
                    'ready', 1, 1, 1);
                """)
            directory.bindInt64(fixture.rootID, at: 1)
            directory.bindInt64(fixture.rootItemID, at: 2)
            _ = try directory.step()
            let directoryID = conn.lastInsertRowId
            let file = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'child.txt', 'file', 'remote-child', 'absent', 'present',
                    'ready', 1, 1, 1);
                """)
            file.bindInt64(fixture.rootID, at: 1)
            file.bindInt64(directoryID, at: 2)
            _ = try file.step()
            let fileID = conn.lastInsertRowId
            try conn.execute(
                "INSERT INTO operations(operation_id, root_id, item_id, operation_type, state, created_at, updated_at) VALUES ('pending-child', \(fixture.rootID), \(fileID), 'uploadMultipart', 'ready', 1, 1);")
            try conn.execute(
                "INSERT INTO remote_change_inbox(root_id, remote_id, payload) VALUES (\(fixture.rootID), 'remote-child', '{}');")
            try conn.execute(
                "INSERT INTO remote_directory_scans(root_id, remote_id, scan_id, state) VALUES (\(fixture.rootID), 'remote-directory', 'scan', 'pending');")
            return IDs(directory: directoryID, file: fileID)
        }
        let conflict = fixture.conflictBase
            .appendingPathComponent("remote-root/removed/child.txt")
        try FileManager.default.createDirectory(
            at: conflict.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("remote conflict".utf8).write(to: conflict)
        try await fixture.store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO sync_conflicts(conflict_id, root_id, item_id, remote_file_id,
                    relative_path, local_path, conflict_path, remote_sha256, remote_size,
                    remote_status, created_at, updated_at)
                VALUES ('conflict', ?, ?, 'remote-child', 'removed/child.txt', ?, ?, ?, 15,
                    'present', 1, 1);
                """)
            stmt.bindInt64(fixture.rootID, at: 1)
            stmt.bindInt64(ids.file, at: 2)
            stmt.bindText(fixture.localRoot.appendingPathComponent("removed/child.txt").path, at: 3)
            stmt.bindText(conflict.path, at: 4)
            stmt.bindText(SyncEngine.computeSha256(of: Data("remote conflict".utf8)), at: 5)
            _ = try stmt.step()
        }

        let cancelled = OSAllocatedUnfairLock(initialState: false)
        let registry = ItemTaskRegistry()
        _ = try await registry.start(itemIDs: [ids.file]) {
            while !Task.isCancelled { await Task.yield() }
            cancelled.withLock { $0 = true }
        }

        try await fixture.engine.cleanupLocalDeletionToRemote(
            itemID: ids.directory,
            expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
            taskRegistry: registry)

        #expect(cancelled.withLock { $0 })
        #expect(context.value.withLock { $0.trashed } == ["remote-directory"])
        #expect(!FileManager.default.fileExists(atPath: conflict.path))
        try await fixture.store.read { conn in
            for table in ["items", "operations", "sync_conflicts",
                          "remote_change_inbox", "remote_directory_scans"] {
                let stmt = try conn.prepare("SELECT COUNT(*) FROM \(table) WHERE " +
                    (table == "items" ? "item_id IN (\(ids.directory), \(ids.file))" :
                     table == "operations" ? "operation_id = 'pending-child'" :
                     table == "sync_conflicts" ? "conflict_id = 'conflict'" :
                     table == "remote_change_inbox" ? "remote_id = 'remote-child'" :
                     "remote_id = 'remote-directory'") + ";")
                #expect(try stmt.step())
                #expect(stmt.columnInt64(at: 0) == 0)
                stmt.reset()
            }
        }
    }

    @Test("Remote file deletion trashes the unchanged local file and deletes its row")
    func remoteToLocalFile() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let local = fixture.localRoot.appendingPathComponent("file.txt")
        let data = Data("baseline".utf8)
        try data.write(to: local)
        let version = try #require(try LocalFileVersion.read(at: local))
        let sha = SyncEngine.computeSha256(of: data)
        let itemID = try await fixture.store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'file.txt', 'file', 'remote-file', ?, ?, ?, ?, ?, 'present',
                    'trashed', 'ready', 1, 1, 1);
                """)
            stmt.bindInt64(fixture.rootID, at: 1)
            stmt.bindInt64(fixture.rootItemID, at: 2)
            stmt.bindInt64(version.device, at: 3)
            stmt.bindInt64(version.inode, at: 4)
            stmt.bindInt64(version.mtime, at: 5)
            stmt.bindInt64(version.size, at: 6)
            stmt.bindText(sha, at: 7)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        try await fixture.engine.cleanupRemoteDeletionToLocal(
            itemID: itemID,
            expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
            taskRegistry: ItemTaskRegistry())

        #expect(!FileManager.default.fileExists(atPath: local.path))
        let remaining = try await fixture.store.read { conn -> Int64 in
            let stmt = try conn.prepare("SELECT COUNT(*) FROM items WHERE item_id = \(itemID);")
            _ = try stmt.step()
            return stmt.columnInt64(at: 0) ?? -1
        }
        #expect(remaining == 0)
    }

    @Test("A file replaced during local trash becomes a resolvable remote deletion conflict")
    func remoteToLocalReplacementBecomesConflict() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let local = fixture.localRoot.appendingPathComponent("file.txt")
        let replacement = fixture.localRoot.appendingPathComponent("replacement.txt")
        let trashed = fixture.directory.appendingPathComponent("trashed.txt")
        let baseline = Data("baseline".utf8)
        let newData = Data("replacement".utf8)
        try baseline.write(to: local)
        let version = try #require(try LocalFileVersion.read(at: local))
        let oldSHA = SyncEngine.computeSha256(of: baseline)
        let newSHA = SyncEngine.computeSha256(of: newData)
        let itemID = try await fixture.store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    base_sha256, base_size, local_status, remote_status, phase,
                    dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'file.txt', 'file', 'remote-file', ?, ?, ?, ?, ?, ?, ?,
                    'present', 'trashed', 'ready', 1, 1, 1);
                """)
            stmt.bindInt64(fixture.rootID, at: 1)
            stmt.bindInt64(fixture.rootItemID, at: 2)
            stmt.bindInt64(version.device, at: 3)
            stmt.bindInt64(version.inode, at: 4)
            stmt.bindInt64(version.mtime, at: 5)
            stmt.bindInt64(version.size, at: 6)
            stmt.bindText(oldSHA, at: 7)
            stmt.bindText(oldSHA, at: 8)
            stmt.bindInt64(version.size, at: 9)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        let deleted = try await fixture.engine.cleanupRemoteDeletionToLocal(
            itemID: itemID,
            expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
            taskRegistry: ItemTaskRegistry(),
            trash: { url, result in
                try newData.write(to: replacement)
                _ = try FileManager.default.replaceItemAt(url, withItemAt: replacement)
                try FileManager.default.moveItem(at: url, to: trashed)
                result?.pointee = trashed as NSURL
            })

        #expect(!deleted)
        #expect(try Data(contentsOf: local) == newData)
        #expect(!FileManager.default.fileExists(atPath: trashed.path))
        let restoredVersion = try #require(try LocalFileVersion.read(at: local))
        let conflicts = try await fixture.engine.listConflicts(localPath: fixture.localRoot.path)
        let conflict = try #require(conflicts.first)
        #expect(conflicts.count == 1)
        #expect(conflict.localPath == local.path)
        #expect(conflict.conflictPath == nil)
        #expect(conflict.remoteSHA256 == oldSHA)
        #expect(conflict.remoteStatus == .trashed)
        try await fixture.store.read { conn in
            let stmt = try conn.prepare("""
                SELECT local_device, local_inode, local_size, local_sha256,
                    base_sha256, phase, local_status, dirty_generation,
                    (SELECT COUNT(*) FROM operations WHERE item_id = ?)
                FROM items WHERE item_id = ?;
                """)
            stmt.bindInt64(itemID, at: 1)
            stmt.bindInt64(itemID, at: 2)
            #expect(try stmt.step())
            #expect(stmt.columnInt64(at: 0) == restoredVersion.device)
            #expect(stmt.columnInt64(at: 1) == restoredVersion.inode)
            #expect(stmt.columnInt64(at: 2) == Int64(newData.count))
            #expect(stmt.columnText(at: 3) == newSHA)
            #expect(stmt.columnText(at: 4) == oldSHA)
            #expect(stmt.columnText(at: 5) == "blocked")
            #expect(stmt.columnText(at: 6) == "present")
            #expect(stmt.columnInt64(at: 7) == 0)
            #expect(stmt.columnInt64(at: 8) == 0)
        }

        try await fixture.engine.resolveConflict(id: conflict.id, resolution: .remote)
        #expect(!FileManager.default.fileExists(atPath: local.path))
        #expect(try await fixture.engine.listConflicts(localPath: fixture.localRoot.path).isEmpty)
    }

    @Test("Remote directory deletion preserves changed and new local files")
    func remoteToLocalDirectoryPreservesLocalFiles() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let directory = fixture.localRoot.appendingPathComponent("removed")
        let modified = directory.appendingPathComponent("modified.txt")
        let unchanged = directory.appendingPathComponent("unchanged.txt")
        let newFile = directory.appendingPathComponent("new.txt")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let directoryIdentity = try #require(try LocalDirectoryIdentity.read(at: directory))
        let baselineModified = Data("old".utf8)
        let observedModified = Data("new".utf8)
        let baselineUnchanged = Data("same".utf8)
        try observedModified.write(to: modified)
        try baselineUnchanged.write(to: unchanged)
        try Data("new local file".utf8).write(to: newFile)
        let directoryID = try await fixture.store.write { conn in
            let directoryStmt = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'removed', 'directory', 'removed-directory', ?, ?, 'present', 'trashed',
                    'ready', 1, 1, 1);
                """)
            directoryStmt.bindInt64(fixture.rootID, at: 1)
            directoryStmt.bindInt64(fixture.rootItemID, at: 2)
            directoryStmt.bindInt64(directoryIdentity.device, at: 3)
            directoryStmt.bindInt64(directoryIdentity.inode, at: 4)
            _ = try directoryStmt.step()
            let id = conn.lastInsertRowId
            let fileStmt = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    base_sha256, base_size, local_status, remote_status, phase,
                    created_at, updated_at)
                VALUES (?, ?, ?, 'file', ?, ?, ?, 'present', 'trashed', 'ready', 1, 1);
                """)
            for (name, remoteID, data) in [
                ("modified.txt", "modified", baselineModified),
                ("unchanged.txt", "unchanged", baselineUnchanged)
            ] {
                fileStmt.bindInt64(fixture.rootID, at: 1)
                fileStmt.bindInt64(id, at: 2)
                fileStmt.bindText(name, at: 3)
                fileStmt.bindText(remoteID, at: 4)
                fileStmt.bindText(SyncEngine.computeSha256(of: data), at: 5)
                fileStmt.bindInt64(Int64(data.count), at: 6)
                _ = try fileStmt.step()
                fileStmt.reset()
            }
            return id
        }

        try await fixture.engine.cleanupRemoteDeletionToLocal(
            itemID: directoryID,
            expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
            taskRegistry: ItemTaskRegistry())

        #expect(!FileManager.default.fileExists(atPath: directory.path))
        let changes = try await storedLocalConflicts(fixture.store)
        #expect(changes.count == 2)
        let byPath = Dictionary(uniqueKeysWithValues: changes.map { ($0.originalPath, $0) })
        let modifiedCopy = try #require(byPath[modified.path])
        let newCopy = try #require(byPath[newFile.path])
        #expect(modifiedCopy.rootID == fixture.rootID)
        #expect(URL(fileURLWithPath: modifiedCopy.storedPath)
            .lastPathComponent.hasSuffix("-modified.txt"))
        #expect(try Data(contentsOf: URL(fileURLWithPath: modifiedCopy.storedPath)) == observedModified)
        #expect(try Data(contentsOf: URL(fileURLWithPath: newCopy.storedPath)) == Data("new local file".utf8))
        #expect(byPath[unchanged.path] == nil)
    }

    @Test("A failed file move does not block directory cleanup")
    func failedLocalConflictMoveIsSkipped() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let directory = fixture.localRoot.appendingPathComponent("removed")
        let movable = directory.appendingPathComponent("movable.txt")
        let locked = directory.appendingPathComponent("locked")
        let blocked = locked.appendingPathComponent("blocked.txt")
        try FileManager.default.createDirectory(at: locked, withIntermediateDirectories: true)
        try Data("movable".utf8).write(to: movable)
        try Data("blocked".utf8).write(to: blocked)
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o555], ofItemAtPath: locked.path)
        defer { try? FileManager.default.setAttributes(
            [.posixPermissions: 0o755], ofItemAtPath: locked.path) }
        let identity = try #require(try LocalDirectoryIdentity.read(at: directory))
        let itemID = try await fixture.store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status, phase,
                    dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'removed', 'directory', 'remote-removed', ?, ?, 'present',
                    'trashed', 'ready', 1, 1, 1);
                """)
            stmt.bindInt64(fixture.rootID, at: 1)
            stmt.bindInt64(fixture.rootItemID, at: 2)
            stmt.bindInt64(identity.device, at: 3)
            stmt.bindInt64(identity.inode, at: 4)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }

        try await fixture.engine.cleanupRemoteDeletionToLocal(
            itemID: itemID,
            expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
            taskRegistry: ItemTaskRegistry())

        #expect(!FileManager.default.fileExists(atPath: directory.path))
        let saved = try await storedLocalConflicts(fixture.store)
        #expect(saved.count == 1)
        #expect(saved.first?.originalPath == movable.path)
        #expect(try Data(contentsOf: URL(fileURLWithPath: saved[0].storedPath)) == Data("movable".utf8))
    }

    @Test("A failed conflict batch leaves moved files available for manual recovery")
    func localConflictBatchFailure() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let directory = fixture.localRoot.appendingPathComponent("removed")
        let first = directory.appendingPathComponent("first.txt")
        let second = directory.appendingPathComponent("second.txt")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        try Data("first".utf8).write(to: first)
        try Data("second".utf8).write(to: second)
        let identity = try #require(try LocalDirectoryIdentity.read(at: directory))
        let itemID = try await fixture.store.write { conn in
            let stmt = try conn.prepare("""
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status, phase,
                    dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'removed', 'directory', 'remote-removed', ?, ?, 'present',
                    'trashed', 'ready', 1, 1, 1);
                """)
            stmt.bindInt64(fixture.rootID, at: 1)
            stmt.bindInt64(fixture.rootItemID, at: 2)
            stmt.bindInt64(identity.device, at: 3)
            stmt.bindInt64(identity.inode, at: 4)
            _ = try stmt.step()
            let id = conn.lastInsertRowId
            try conn.execute("""
                CREATE TRIGGER fail_conflict_commit BEFORE INSERT ON local_conflicts
                WHEN (SELECT COUNT(*) FROM local_conflicts) > 0
                BEGIN SELECT RAISE(ABORT, 'injected conflict commit failure'); END;
                """)
            return id
        }

        await #expect(throws: Error.self) {
            try await fixture.engine.cleanupRemoteDeletionToLocal(
                itemID: itemID,
                expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
                taskRegistry: ItemTaskRegistry())
        }
        #expect(FileManager.default.fileExists(atPath: directory.path))
        #expect(!FileManager.default.fileExists(atPath: first.path))
        #expect(!FileManager.default.fileExists(atPath: second.path))
        #expect(try await storedLocalConflicts(fixture.store).isEmpty)
        let parentRemoved = fixture.directory.appendingPathComponent("parent_removed")
            .appendingPathComponent(String(fixture.rootID))
        let storedFiles = try FileManager.default.contentsOfDirectory(
            at: parentRemoved, includingPropertiesForKeys: nil)
        #expect(storedFiles.count == 2)
        for (name, expected) in [("first.txt", "first"), ("second.txt", "second")] {
            let stored = try #require(storedFiles.first {
                $0.lastPathComponent.hasSuffix("-\(name)")
            })
            #expect(try Data(contentsOf: stored) == Data(expected.utf8))
        }
        try await fixture.store.write { try $0.execute("DROP TRIGGER fail_conflict_commit;") }

        #expect(try await fixture.engine.recoverPendingItemCleanups(
            rootID: fixture.rootID, taskRegistry: ItemTaskRegistry()) == 1)
        #expect(!FileManager.default.fileExists(atPath: directory.path))
        #expect(try await storedLocalConflicts(fixture.store).isEmpty)
        #expect(try FileManager.default.contentsOfDirectory(
            at: parentRemoved, includingPropertiesForKeys: nil).count == 2)
    }

    @Test("A child named like the sync root does not trash the sync root")
    func sameNamedChildDirectory() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let child = fixture.localRoot.appendingPathComponent(fixture.localRoot.lastPathComponent)
        let nested = child.appendingPathComponent("nested.txt")
        let sibling = fixture.localRoot.appendingPathComponent("nested.txt")
        try FileManager.default.createDirectory(at: child, withIntermediateDirectories: true)
        let childIdentity = try #require(try LocalDirectoryIdentity.read(at: child))
        try Data("child".utf8).write(to: nested)
        try Data("sibling".utf8).write(to: sibling)
        let nestedVersion = try #require(try LocalFileVersion.read(at: nested))
        struct IDs { let directory: Int64; let file: Int64 }
        let ids = try await fixture.store.write { conn -> IDs in
            let directory = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, ?, 'directory', 'same-named-child', ?, ?, 'present', 'trashed',
                    'ready', 1, 1, 1);
                """)
            directory.bindInt64(fixture.rootID, at: 1)
            directory.bindInt64(fixture.rootItemID, at: 2)
            directory.bindText(fixture.localRoot.lastPathComponent, at: 3)
            directory.bindInt64(childIdentity.device, at: 4)
            directory.bindInt64(childIdentity.inode, at: 5)
            _ = try directory.step()
            let directoryID = conn.lastInsertRowId
            let file = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'nested.txt', 'file', 'nested-child', ?, ?, ?, ?, ?, 'present',
                    'trashed', 'ready', 1, 1, 1);
                """)
            file.bindInt64(fixture.rootID, at: 1)
            file.bindInt64(directoryID, at: 2)
            file.bindInt64(nestedVersion.device, at: 3)
            file.bindInt64(nestedVersion.inode, at: 4)
            file.bindInt64(nestedVersion.mtime, at: 5)
            file.bindInt64(nestedVersion.size, at: 6)
            file.bindText(SyncEngine.computeSha256(of: Data("child".utf8)), at: 7)
            _ = try file.step()
            return IDs(directory: directoryID, file: conn.lastInsertRowId)
        }

        try await fixture.engine.cleanupRemoteDeletionToLocal(
            itemID: ids.file,
            expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
            taskRegistry: ItemTaskRegistry())

        #expect(FileManager.default.fileExists(atPath: fixture.localRoot.path))
        #expect(FileManager.default.fileExists(atPath: child.path))
        #expect(!FileManager.default.fileExists(atPath: nested.path))
        #expect(try Data(contentsOf: sibling) == Data("sibling".utf8))

        try await fixture.engine.cleanupRemoteDeletionToLocal(
            itemID: ids.directory,
            expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
            taskRegistry: ItemTaskRegistry())

        #expect(FileManager.default.fileExists(atPath: fixture.localRoot.path))
        #expect(!FileManager.default.fileExists(atPath: child.path))
        #expect(try Data(contentsOf: sibling) == Data("sibling".utf8))
    }

    @Test("Remote trash survives a database cleanup failure and resumes from its intent")
    func remoteTrashIntentRecovery() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let itemID = try await fixture.store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'gone.txt', 'file', 'remote-gone', 'absent', 'present',
                    'ready', 1, 1, 1);
                """)
            stmt.bindInt64(fixture.rootID, at: 1)
            stmt.bindInt64(fixture.rootItemID, at: 2)
            _ = try stmt.step()
            let id = conn.lastInsertRowId
            try conn.execute(
                "CREATE TRIGGER fail_cleanup BEFORE DELETE ON items WHEN OLD.item_id = \(id) " +
                    "BEGIN SELECT RAISE(ABORT, 'injected cleanup failure'); END;")
            return id
        }

        await #expect(throws: Error.self) {
            try await fixture.engine.cleanupLocalDeletionToRemote(
                itemID: itemID,
                expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
                taskRegistry: ItemTaskRegistry())
        }
        #expect(context.value.withLock { $0.trashed } == ["remote-gone"])
        try await fixture.store.read { conn in
            let stmt = try conn.prepare(
                "SELECT COUNT(*) FROM operations WHERE item_id = \(itemID) " +
                    "AND operation_type = 'trashRemote';")
            #expect(try stmt.step())
            #expect(stmt.columnInt64(at: 0) == 1)
        }

        try await fixture.store.write { try $0.execute("DROP TRIGGER fail_cleanup;") }
        #expect(try await fixture.engine.recoverPendingItemCleanups(
            rootID: fixture.rootID, taskRegistry: ItemTaskRegistry()) == 1)
        #expect(context.value.withLock { $0.trashed } == ["remote-gone", "remote-gone"])
        try await fixture.store.read { conn in
            for table in ["items", "operations"] {
                let stmt = try conn.prepare(
                    "SELECT COUNT(*) FROM \(table) WHERE item_id = \(itemID);")
                #expect(try stmt.step())
                #expect(stmt.columnInt64(at: 0) == 0)
            }
        }
    }

    @Test("Pending remote trash stops after the local item is restored")
    func pendingRemoteTrashStopsAfterLocalRestore() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let local = fixture.localRoot.appendingPathComponent("restored.txt")
        let itemID = try await fixture.store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'restored.txt', 'file', 'remote-restored', 'absent', 'present',
                    'ready', 1, 1, 1);
                """)
            stmt.bindInt64(fixture.rootID, at: 1)
            stmt.bindInt64(fixture.rootItemID, at: 2)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }
        context.value.withLock { $0.failNextTrash = true }

        await #expect(throws: Error.self) {
            try await fixture.engine.cleanupLocalDeletionToRemote(
                itemID: itemID,
                expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
                taskRegistry: ItemTaskRegistry())
        }
        try Data("restored".utf8).write(to: local)

        #expect(try await fixture.engine.recoverPendingItemCleanups(
            rootID: fixture.rootID, taskRegistry: ItemTaskRegistry()) == 1)
        #expect(context.value.withLock { $0.trashAttempts } == ["remote-restored"])
        try await fixture.store.read { conn in
            let item = try conn.prepare("SELECT COUNT(*) FROM items WHERE item_id = ?;")
            item.bindInt64(itemID, at: 1)
            #expect(try item.step())
            #expect(item.columnInt64(at: 0) == 1)
            let operation = try conn.prepare(
                "SELECT COUNT(*) FROM operations WHERE item_id = ? AND operation_type = 'trashRemote';")
            operation.bindInt64(itemID, at: 1)
            #expect(try operation.step())
            #expect(operation.columnInt64(at: 0) == 0)
        }
    }

    @Test("Local trash survives a database cleanup failure and resumes from its intent")
    func localTrashIntentRecovery() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let local = fixture.localRoot.appendingPathComponent("gone.txt")
        let data = Data("baseline".utf8)
        try data.write(to: local)
        let version = try #require(try LocalFileVersion.read(at: local))
        let itemID = try await fixture.store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_mtime, local_size, local_sha256,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'gone.txt', 'file', 'remote-gone', ?, ?, ?, ?, ?, 'present',
                    'trashed', 'ready', 1, 1, 1);
                """)
            stmt.bindInt64(fixture.rootID, at: 1)
            stmt.bindInt64(fixture.rootItemID, at: 2)
            stmt.bindInt64(version.device, at: 3)
            stmt.bindInt64(version.inode, at: 4)
            stmt.bindInt64(version.mtime, at: 5)
            stmt.bindInt64(version.size, at: 6)
            stmt.bindText(SyncEngine.computeSha256(of: data), at: 7)
            _ = try stmt.step()
            let id = conn.lastInsertRowId
            try conn.execute(
                "CREATE TRIGGER fail_cleanup BEFORE DELETE ON items WHEN OLD.item_id = \(id) " +
                    "BEGIN SELECT RAISE(ABORT, 'injected cleanup failure'); END;")
            return id
        }

        await #expect(throws: Error.self) {
            try await fixture.engine.cleanupRemoteDeletionToLocal(
                itemID: itemID,
                expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
                taskRegistry: ItemTaskRegistry())
        }
        #expect(!FileManager.default.fileExists(atPath: local.path))

        try await fixture.store.write { try $0.execute("DROP TRIGGER fail_cleanup;") }
        #expect(try await fixture.engine.recoverPendingItemCleanups(
            rootID: fixture.rootID, taskRegistry: ItemTaskRegistry()) == 1)
        try await fixture.store.read { conn in
            for table in ["items", "operations"] {
                let stmt = try conn.prepare(
                    "SELECT COUNT(*) FROM \(table) WHERE item_id = \(itemID);")
                #expect(try stmt.step())
                #expect(stmt.columnInt64(at: 0) == 0)
            }
        }
    }

    @Test("Cleanup replay does not trash a replacement directory")
    func cleanupReplayDoesNotTrashReplacementDirectory() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let local = fixture.localRoot.appendingPathComponent("replaced")
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        let originalIdentity = try #require(try LocalDirectoryIdentity.read(at: local))
        let itemID = try await fixture.store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_device, local_inode, local_status, remote_status, phase,
                    dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'replaced', 'directory', 'remote-replaced', ?, ?, 'present',
                    'trashed', 'ready', 1, 1, 1);
                """)
            stmt.bindInt64(fixture.rootID, at: 1)
            stmt.bindInt64(fixture.rootItemID, at: 2)
            stmt.bindInt64(originalIdentity.device, at: 3)
            stmt.bindInt64(originalIdentity.inode, at: 4)
            _ = try stmt.step()
            let id = conn.lastInsertRowId
            try conn.execute(
                "CREATE TRIGGER fail_cleanup BEFORE DELETE ON items WHEN OLD.item_id = \(id) " +
                    "BEGIN SELECT RAISE(ABORT, 'injected cleanup failure'); END;")
            return id
        }

        await #expect(throws: Error.self) {
            try await fixture.engine.cleanupRemoteDeletionToLocal(
                itemID: itemID,
                expected: ItemCleanupGenerations(local: 0, remote: 0, dirty: 1),
                taskRegistry: ItemTaskRegistry())
        }
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        let replacement = local.appendingPathComponent("new.txt")
        try Data("new".utf8).write(to: replacement)
        let replacementIdentity = try #require(try LocalDirectoryIdentity.read(at: local))
        #expect(replacementIdentity != originalIdentity)
        try await fixture.store.write { try $0.execute("DROP TRIGGER fail_cleanup;") }

        #expect(try await fixture.engine.recoverPendingItemCleanups(
            rootID: fixture.rootID, taskRegistry: ItemTaskRegistry()) == 1)
        #expect(try Data(contentsOf: replacement) == Data("new".utf8))
        try await fixture.store.read { conn in
            let item = try conn.prepare("SELECT COUNT(*) FROM items WHERE item_id = ?;")
            item.bindInt64(itemID, at: 1)
            #expect(try item.step())
            #expect(item.columnInt64(at: 0) == 1)
            let operation = try conn.prepare(
                "SELECT COUNT(*) FROM operations WHERE item_id = ? AND operation_type = 'deleteLocal';")
            operation.bindInt64(itemID, at: 1)
            #expect(try operation.step())
            #expect(operation.columnInt64(at: 0) == 0)
        }
    }
}
