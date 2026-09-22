import Foundation

public struct SyncIssue: Sendable, Equatable {
    public enum Stage: String, Sendable {
        case localScan, createDirectory, upload, download
        case pathUpdate, delete, conflictRefresh
    }

    public enum Category: String, Sendable {
        case network, rateLimited, permissionDenied, remoteMissing, remoteConflict
        case integrityMismatch, invalidResponse, safetyBlocked, localChanged, localIO
        case unsupportedFilesystem, staleState, unknown
    }

    public enum SuggestedAction: String, Sendable {
        case retry, retryLater, checkPermissions, inspectLocalFile
        case renameRemote, resolveManually
    }

    public let id: String
    public let itemId: Int64?
    public let remoteFileId: String?
    public let relativePath: String
    public let stage: Stage
    public let category: Category
    public let suggestedAction: SuggestedAction
    public let message: String
    public let retryAt: Date?
    public let firstSeenAt: Date
    public let lastSeenAt: Date
    public let occurrenceCount: Int
}

public struct SyncIssuePage: Sendable, Equatable {
    public let issues: [SyncIssue]
    public let totalCount: Int
    public let nextOffset: Int?
}

struct SyncIssueSubject: Sendable {
    let itemID: Int64?
    let remoteFileID: String?
    let relativePath: String

    var key: String {
        if let itemID { return "item:\(itemID)" }
        if let remoteFileID { return "remote:\(remoteFileID)" }
        return "path:\(relativePath)"
    }
}

struct LocalScanIssueError: Error, CustomStringConvertible, Sendable {
    let description: String
}

struct RemoteNameConflictIssueError: Error, CustomStringConvertible, Sendable {
    let description: String
}

struct LocalTypeReplacementDeferredError: Error, CustomStringConvertible, Sendable {
    let description: String
}

enum SyncIssueStore {
    private struct Classification {
        let category: SyncIssue.Category
        let action: SyncIssue.SuggestedAction
        let retryDelay: TimeInterval?
    }

    static func record(
        store: StateStore,
        rootID: Int64,
        subject: SyncIssueSubject,
        stage: SyncIssue.Stage,
        error: Error
    ) async throws {
        if error is CancellationError { return }
        let classification = classify(error)
        let now = Date().timeIntervalSince1970
        let retryAt = classification.retryDelay.map { now + $0 }
        try await store.batchWrite { conn in
            let stmt = try conn.cachedStatement(
                """
                INSERT INTO sync_issues(
                    issue_id, root_id, subject_key, item_id, remote_file_id,
                    relative_path, stage, category, suggested_action, message,
                    retry_at, first_seen_at, last_seen_at, occurrence_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(root_id, subject_key, stage) DO UPDATE SET
                    item_id = excluded.item_id,
                    remote_file_id = excluded.remote_file_id,
                    relative_path = excluded.relative_path,
                    category = excluded.category,
                    suggested_action = excluded.suggested_action,
                    message = excluded.message,
                    retry_at = excluded.retry_at,
                    last_seen_at = excluded.last_seen_at,
                    occurrence_count = sync_issues.occurrence_count + 1;
                """)
            defer { stmt.reset() }
            stmt.bindText(UUID().uuidString, at: 1)
            stmt.bindInt64(rootID, at: 2)
            stmt.bindText(subject.key, at: 3)
            stmt.bindInt64(subject.itemID, at: 4)
            stmt.bindText(subject.remoteFileID, at: 5)
            stmt.bindText(subject.relativePath, at: 6)
            stmt.bindText(stage.rawValue, at: 7)
            stmt.bindText(classification.category.rawValue, at: 8)
            stmt.bindText(classification.action.rawValue, at: 9)
            stmt.bindText(String(describing: error), at: 10)
            stmt.bindDouble(retryAt, at: 11)
            stmt.bindDouble(now, at: 12)
            stmt.bindDouble(now, at: 13)
            _ = try stmt.step()
        }
    }

    static func clear(store: StateStore, rootID: Int64) async throws {
        try await store.write { conn in
            let stmt = try conn.cachedStatement("DELETE FROM sync_issues WHERE root_id = ?;")
            defer { stmt.reset() }
            stmt.bindInt64(rootID, at: 1)
            _ = try stmt.step()
        }
    }

    static func count(store: StateStore, rootID: Int64) async throws -> Int {
        try await store.read { conn in
            let stmt = try conn.cachedStatement(
                "SELECT COUNT(*) FROM sync_issues WHERE root_id = ?;")
            defer { stmt.reset() }
            stmt.bindInt64(rootID, at: 1)
            return try stmt.step() ? Int(stmt.columnInt64(at: 0) ?? 0) : 0
        }
    }

    static func page(
        store: StateStore, rootID: Int64, limit: Int, offset: Int
    ) async throws -> SyncIssuePage {
        try await store.read { conn in
            let count = try conn.cachedStatement(
                "SELECT COUNT(*) FROM sync_issues WHERE root_id = ?;")
            defer { count.reset() }
            count.bindInt64(rootID, at: 1)
            let total = try count.step() ? Int(count.columnInt64(at: 0) ?? 0) : 0

            let query = try conn.cachedStatement(
                """
                SELECT issue_id, item_id, remote_file_id, relative_path, stage,
                    category, suggested_action, message, retry_at,
                    first_seen_at, last_seen_at, occurrence_count
                FROM sync_issues WHERE root_id = ?
                ORDER BY last_seen_at DESC, issue_id
                LIMIT ? OFFSET ?;
                """)
            defer { query.reset() }
            query.bindInt64(rootID, at: 1)
            query.bindInt64(Int64(limit), at: 2)
            query.bindInt64(Int64(offset), at: 3)
            var issues: [SyncIssue] = []
            while try query.step(),
                  let id = query.columnText(at: 0),
                  let path = query.columnText(at: 3),
                  let stageValue = query.columnText(at: 4),
                  let stage = SyncIssue.Stage(rawValue: stageValue),
                  let categoryValue = query.columnText(at: 5),
                  let category = SyncIssue.Category(rawValue: categoryValue),
                  let actionValue = query.columnText(at: 6),
                  let action = SyncIssue.SuggestedAction(rawValue: actionValue),
                  let message = query.columnText(at: 7),
                  let firstSeen = query.columnDouble(at: 9),
                  let lastSeen = query.columnDouble(at: 10) {
                issues.append(SyncIssue(
                    id: id, itemId: query.columnInt64(at: 1),
                    remoteFileId: query.columnText(at: 2), relativePath: path,
                    stage: stage, category: category, suggestedAction: action,
                    message: message,
                    retryAt: query.columnDouble(at: 8).map(Date.init(timeIntervalSince1970:)),
                    firstSeenAt: Date(timeIntervalSince1970: firstSeen),
                    lastSeenAt: Date(timeIntervalSince1970: lastSeen),
                    occurrenceCount: Int(query.columnInt64(at: 11) ?? 1)))
            }
            let consumed = offset + issues.count
            return SyncIssuePage(
                issues: issues, totalCount: total,
                nextOffset: consumed < total ? consumed : nil)
        }
    }

    private static func classify(_ error: Error) -> Classification {
        if error is LocalScanIssueError {
            return Classification(category: .localIO,
                action: .inspectLocalFile, retryDelay: nil)
        }
        if error is RemoteNameConflictIssueError {
            return Classification(category: .remoteConflict,
                action: .renameRemote, retryDelay: nil)
        }
        if error is LocalTypeReplacementDeferredError {
            return Classification(category: .staleState,
                action: .retry, retryDelay: nil)
        }
        if error is LocalFilePublicationRecoveryError {
            return Classification(category: .localIO,
                action: .inspectLocalFile, retryDelay: nil)
        }
        if let driveError = error as? DriveError {
            return classifyDriveError(driveError)
        }
        if error is URLError {
            return Classification(category: .network, action: .retryLater, retryDelay: nil)
        }
        if let syncError = error as? SyncEngineError {
            return classifySyncError(syncError)
        }
        if error is CocoaError || error is POSIXError {
            return Classification(category: .localIO,
                action: .inspectLocalFile, retryDelay: nil)
        }
        return Classification(category: .unknown, action: .retry, retryDelay: nil)
    }

    private static func classifyDriveError(_ error: DriveError) -> Classification {
        switch error {
        case .rateLimited429(let delay), .rateLimited403(_, let delay):
            return Classification(category: .rateLimited, action: .retryLater,
                retryDelay: delay)
        case .notFound:
            return Classification(category: .remoteMissing, action: .retry, retryDelay: nil)
        case .conflict:
            return Classification(category: .remoteConflict, action: .retry, retryDelay: nil)
        case .checksumMismatch, .sizeMismatch:
            return Classification(category: .integrityMismatch, action: .retry, retryDelay: nil)
        case .serverError(let status, _):
            if status == 401 || status == 403 {
                return Classification(category: .permissionDenied,
                    action: .checkPermissions, retryDelay: nil)
            }
            return Classification(category: .network, action: .retryLater, retryDelay: nil)
        case .invalidResponse:
            return Classification(category: .invalidResponse, action: .retry, retryDelay: nil)
        case .unsafeOverwrite:
            return Classification(category: .safetyBlocked,
                action: .resolveManually, retryDelay: nil)
        case .stableInputUnavailable:
            return Classification(category: .unsupportedFilesystem,
                action: .inspectLocalFile, retryDelay: nil)
        }
    }

    private static func classifySyncError(_ error: SyncEngineError) -> Classification {
        switch error {
        case .localFileModified:
            return Classification(category: .localChanged, action: .retry, retryDelay: nil)
        case .localFilePublicationFailed:
            return Classification(category: .localIO,
                action: .inspectLocalFile, retryDelay: nil)
        case .general(let message) where
            message.localizedCaseInsensitiveContains("stale")
                || message.localizedCaseInsensitiveContains("expired")
                || message.localizedCaseInsensitiveContains("unavailable"):
            return Classification(category: .staleState, action: .retry, retryDelay: nil)
        default:
            return Classification(category: .unknown, action: .retry, retryDelay: nil)
        }
    }
}

extension SyncEngine {
    public func listSyncIssues(
        localPath: String, limit: Int = 100, offset: Int = 0
    ) async throws -> SyncIssuePage {
        guard (1...1000).contains(limit), offset >= 0 else {
            throw SyncEngineError.general("Sync issue pagination requires limit 1...1000 and offset >= 0")
        }
        let normalized = Self.normalizedPath(localPath)
        let rootID = try await store.read { conn -> Int64 in
            let stmt = try conn.cachedStatement(
                "SELECT root_id FROM roots WHERE local_root_path = ? AND is_active = 1;")
            defer { stmt.reset() }
            stmt.bindText(normalized, at: 1)
            guard try stmt.step(), let rootID = stmt.columnInt64(at: 0) else {
                throw SyncEngineError.general(
                    "Directory has no active sync root: \(normalized)")
            }
            return rootID
        }
        return try await SyncIssueStore.page(
            store: store, rootID: rootID, limit: limit, offset: offset)
    }
}
