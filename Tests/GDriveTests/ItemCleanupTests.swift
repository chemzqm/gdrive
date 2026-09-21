import Foundation
import os
import Testing
@testable import GDrive

private struct CleanupRequestState: Sendable {
    var trashed: [String] = []
}

private final class CleanupProtocol: URLProtocol, @unchecked Sendable {
    override static func canInit(with request: URLRequest) -> Bool { true }
    override static func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let context = TestHTTPContext<OSAllocatedUnfairLock<CleanupRequestState>>.value(for: request)!
        if request.httpMethod == "PATCH" {
            context.withLock { $0.trashed.append(request.url!.lastPathComponent) }
        }
        let response = HTTPURLResponse(
            url: request.url!, statusCode: 204, httpVersion: nil, headerFields: nil)!
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
                "INSERT INTO operations(operation_id, root_id, item_id, operation_type, state, created_at, updated_at) VALUES ('pending-child', \(fixture.rootID), \(fileID), 'download', 'ready', 1, 1);")
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

    @Test("Remote directory deletion records modified files at their Trash paths")
    func remoteToLocalDirectoryRecordsModifiedFiles() async throws {
        let fixture = try await fixture()
        var trashedDirectory: URL?
        defer {
            if let trashedDirectory { try? FileManager.default.removeItem(at: trashedDirectory) }
            try? FileManager.default.removeItem(at: fixture.directory)
        }
        let directory = fixture.localRoot.appendingPathComponent("removed")
        let modified = directory.appendingPathComponent("modified.txt")
        let unchanged = directory.appendingPathComponent("unchanged.txt")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let baselineModified = Data("old".utf8)
        let observedModified = Data("new".utf8)
        let baselineUnchanged = Data("same".utf8)
        try observedModified.write(to: modified)
        try baselineUnchanged.write(to: unchanged)
        let directoryID = try await fixture.store.write { conn in
            let directoryStmt = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, 'removed', 'directory', 'removed-directory', 'present', 'trashed',
                    'ready', 1, 1, 1);
                """)
            directoryStmt.bindInt64(fixture.rootID, at: 1)
            directoryStmt.bindInt64(fixture.rootItemID, at: 2)
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
        let changes = try await fixture.engine.listTrashedLocalChanges(
            localPath: fixture.localRoot.path)
        let change = try #require(changes.first)
        #expect(changes.count == 1)
        #expect(change.relativePath == "removed/modified.txt")
        #expect(change.originalPath == modified.path)
        #expect(change.baselineSHA256 == SyncEngine.computeSha256(of: baselineModified))
        #expect(change.observedSHA256 == SyncEngine.computeSha256(of: observedModified))
        let trashPath = try #require(change.trashPath)
        #expect(try Data(contentsOf: URL(fileURLWithPath: trashPath)) == observedModified)
        trashedDirectory = URL(fileURLWithPath: trashPath).deletingLastPathComponent()
    }

    @Test("A child named like the sync root does not trash the sync root")
    func sameNamedChildDirectory() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let child = fixture.localRoot.appendingPathComponent(fixture.localRoot.lastPathComponent)
        let nested = child.appendingPathComponent("nested.txt")
        let sibling = fixture.localRoot.appendingPathComponent("nested.txt")
        try FileManager.default.createDirectory(at: child, withIntermediateDirectories: true)
        try Data("child".utf8).write(to: nested)
        try Data("sibling".utf8).write(to: sibling)
        let nestedVersion = try #require(try LocalFileVersion.read(at: nested))
        struct IDs { let directory: Int64; let file: Int64 }
        let ids = try await fixture.store.write { conn -> IDs in
            let directory = try conn.prepare(
                """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id,
                    local_status, remote_status, phase, dirty_generation, created_at, updated_at)
                VALUES (?, ?, ?, 'directory', 'same-named-child', 'present', 'trashed',
                    'ready', 1, 1, 1);
                """)
            directory.bindInt64(fixture.rootID, at: 1)
            directory.bindInt64(fixture.rootItemID, at: 2)
            directory.bindText(fixture.localRoot.lastPathComponent, at: 3)
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
}
