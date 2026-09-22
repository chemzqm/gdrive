import Foundation
import Testing
@testable import GDrive

@Suite("Persistent sync issues")
struct SyncIssueTests {
    private struct Fixture {
        let directory: URL
        let store: StateStore
        let rootID: Int64
    }

    private func fixture() async throws -> Fixture {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("sync-issues-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let store = try await StateStore(
            path: directory.appendingPathComponent("state.sqlite").path)
        let rootID = try await store.write { conn in
            let stmt = try conn.prepare(
                """
                INSERT INTO roots(
                    account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state,
                    created_at, updated_at
                ) VALUES ('default', ?, 1, 1, 'root', 'localToRemoteEmpty',
                    'existingKnown', 1, 1);
                """)
            stmt.bindText(directory.appendingPathComponent("local").path, at: 1)
            _ = try stmt.step()
            return conn.lastInsertRowId
        }
        return Fixture(directory: directory, store: store, rootID: rootID)
    }

    @Test("Repeated failures upsert and pages survive reopening")
    func persistenceAndPagination() async throws {
        let fixture = try await fixture()
        let directory = fixture.directory
        let store = fixture.store
        let rootID = fixture.rootID
        defer { try? FileManager.default.removeItem(at: directory) }
        let first = SyncIssueSubject(
            itemID: 42, remoteFileID: "remote-42", relativePath: "a.txt")
        try await SyncIssueStore.record(
            store: store, rootID: rootID, subject: first, stage: .upload,
            error: DriveError.rateLimited429(retryAfter: 3))
        try await SyncIssueStore.record(
            store: store, rootID: rootID, subject: first, stage: .upload,
            error: DriveError.rateLimited429(retryAfter: 5))
        try await SyncIssueStore.record(
            store: store, rootID: rootID,
            subject: SyncIssueSubject(
                itemID: nil, remoteFileID: "remote-43", relativePath: "b.txt"),
            stage: .download,
            error: DriveError.checksumMismatch(expected: String(repeating: "a", count: 64),
                actual: String(repeating: "b", count: 64)))
        try await store.flush()

        let firstPage = try await SyncIssueStore.page(
            store: store, rootID: rootID, limit: 1, offset: 0)
        #expect(firstPage.totalCount == 2)
        #expect(firstPage.issues.count == 1)
        #expect(firstPage.nextOffset == 1)
        let secondPage = try await SyncIssueStore.page(
            store: store, rootID: rootID, limit: 1, offset: 1)
        #expect(secondPage.issues.count == 1)
        #expect(secondPage.nextOffset == nil)

        let issues = firstPage.issues + secondPage.issues
        let upload = try #require(issues.first { $0.stage == .upload })
        #expect(upload.itemId == 42)
        #expect(upload.category == .rateLimited)
        #expect(upload.suggestedAction == .retryLater)
        #expect(upload.occurrenceCount == 2)
        #expect(upload.retryAt != nil)
        let download = try #require(issues.first { $0.stage == .download })
        #expect(download.category == .integrityMismatch)
        #expect(download.suggestedAction == .retry)

        let reopened = try await StateStore(
            path: directory.appendingPathComponent("state.sqlite").path)
        #expect(try await SyncIssueStore.count(store: reopened, rootID: rootID) == 2)
        try await SyncIssueStore.clear(store: reopened, rootID: rootID)
        #expect(try await SyncIssueStore.count(store: reopened, rootID: rootID) == 0)
    }

    @Test("A malformed issue does not truncate later valid rows")
    func malformedRowDoesNotTruncatePage() async throws {
        let fixture = try await fixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try await SyncIssueStore.record(
            store: fixture.store, rootID: fixture.rootID,
            subject: SyncIssueSubject(
                itemID: nil, remoteFileID: "valid", relativePath: "valid.txt"),
            stage: .download,
            error: DriveError.checksumMismatch(expected: "expected", actual: "actual"))
        try await fixture.store.write { conn in
            try conn.execute("PRAGMA ignore_check_constraints = ON;")
            defer { try? conn.execute("PRAGMA ignore_check_constraints = OFF;") }
            try conn.execute(
                """
                INSERT INTO sync_issues(
                    issue_id, root_id, subject_key, relative_path, stage,
                    category, suggested_action, message, first_seen_at,
                    last_seen_at, occurrence_count
                ) VALUES (
                    'malformed', \(fixture.rootID), 'malformed', 'bad.txt',
                    'invalid-stage', 'unknown', 'retry', 'bad row',
                    9999999999, 9999999999, 1
                );
                """)
        }

        let page = try await SyncIssueStore.page(
            store: fixture.store, rootID: fixture.rootID, limit: 2, offset: 0)

        #expect(page.totalCount == 2)
        #expect(page.issues.map(\.remoteFileId) == ["valid"])
        #expect(page.nextOffset == nil)
    }
}
