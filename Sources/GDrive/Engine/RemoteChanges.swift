import Darwin
import Foundation
import os

/// The inbox and enumeration checkpoints are the durable boundary of the Changes cursor.
/// Enumeration is deliberately a separate, bounded stage after ready file transfers.
struct RemoteChanges: Sendable {
    let store: StateStore
    let client: DriveClient
    let rootID: Int64
    let remoteRootID: String
    let rootURL: URL

    private enum Value { case text(String?), int(Int64?), double(Double) }
    private static func statement(_ conn: SQLiteConnection, _ sql: String, _ values: [Value]) throws -> SQLiteStatement {
        let queryStatement = try conn.cachedStatement(sql)
        for (index, value) in values.enumerated() {
            switch value {
            case .text(let text): queryStatement.bindText(text, at: Int32(index + 1))
            case .int(let int): queryStatement.bindInt64(int, at: Int32(index + 1))
            case .double(let double): queryStatement.bindDouble(double, at: Int32(index + 1))
            }
        }
        return queryStatement
    }
    private static func execute(_ conn: SQLiteConnection, _ sql: String, _ values: [Value]) throws {
        let queryStatement = try statement(conn, sql, values)
        defer { queryStatement.reset() }
        _ = try queryStatement.step()
    }

    static func initialBootstrapCursor(
        client: DriveClient, rootExists: Bool, initialToken: String?
    ) async throws -> String? {
        guard !rootExists else { return nil }
        if let initialToken { return initialToken }
        return try await client.getStartPageToken()
    }

    static func insertInitialCursor(
        conn: SQLiteConnection, rootID: Int64, token: String?, now: Double
    ) throws {
        guard let token else { return }
        try execute(
            conn,
            """
            INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at)
            VALUES (?, 'default', 'drive_changes', ?, ?);
            """,
            [.int(rootID), .text(token), .double(now)])
    }

    static func saveInitialCursor(store: StateStore, client: DriveClient, rootID: Int64, requireExisting: Bool = false, initialToken: String? = nil) async throws {
        let exists = try await store.read { conn in
            let queryStatement = try statement(conn, "SELECT 1 FROM cursors WHERE root_id = ? AND cursor_kind = 'drive_changes';", [.int(rootID)])
            defer { queryStatement.reset() }
            return try queryStatement.step()
        }
        // A bootstrap retry must never replace an earlier recovery boundary.
        guard !exists else { return }
        guard !requireExisting else {
            throw SyncEngineError.general("The existing sync root has no Changes cursor; run syncIncremental to rebuild remote observations")
        }
        let token: String
        if let initialToken { token = initialToken } else { token = try await client.getStartPageToken() }
        try await store.write { conn in
            try execute(conn, """
                INSERT OR IGNORE INTO cursors(root_id, account_id, cursor_kind, token_value, updated_at)
                VALUES (?, 'default', 'drive_changes', ?, strftime('%s','now'));
                """, [.int(rootID), .text(token)])
        }
    }

    func recoverBootstrapCursorIfNeeded(maxConcurrency: Int) async throws {
        let hasCursor = try await store.read { conn in
            let queryStatement = try Self.statement(
                conn,
                "SELECT 1 FROM cursors WHERE root_id = ? AND cursor_kind = 'drive_changes' AND is_valid = 1;",
                [.int(rootID)])
            defer { queryStatement.reset() }
            return try queryStatement.step()
        }
        let hasRebuildCheckpoint = try await store.read { conn in
            let queryStatement = try Self.statement(
                conn,
                "SELECT 1 FROM remote_directory_scans WHERE root_id = ? LIMIT 1;",
                [.int(rootID)])
            defer { queryStatement.reset() }
            return try queryStatement.step()
        }
        if hasCursor, !hasRebuildCheckpoint { return }
        if !hasCursor {
            _ = try await startRebuild()
        }
        try await consume()
        while try await enumeratePending(limit: maxConcurrency) {
            try await applyPending()
        }
        try await applyPending()
        guard try await pendingCount() == 0 else {
            throw SyncEngineError.general(
                "The missing Changes cursor recovery still has remote observations pending")
        }
        try await store.write { conn in
            try Self.execute(
                conn,
                "DELETE FROM remote_directory_scans WHERE root_id = ?;",
                [.int(rootID)])
        }
    }

    static func retainBootstrapObservation(store: StateStore, rootID: Int64, file: DriveFile) async throws {
        let change = DriveChange(fileId: file.id, removed: nil, file: file)
        guard let payload = String(bytes: try JSONEncoder().encode(change), encoding: .utf8) else {
            throw SyncEngineError.general("Unable to encode bootstrap remote observation: \(file.id)")
        }
        try await store.write { conn in
            try execute(conn, """
                INSERT INTO remote_change_inbox(root_id, remote_id, payload)
                VALUES (?, ?, ?)
                ON CONFLICT(root_id, remote_id) DO UPDATE SET
                    payload = excluded.payload, scan_id = NULL, attempted_at = 0;
                """, [.int(rootID), .text(file.id), .text(payload)])
        }
    }

    private func enqueue(_ conn: SQLiteConnection, change: DriveChange, scanID: String? = nil) throws {
        let payload = (String(bytes: try JSONEncoder().encode(change), encoding: .utf8) ?? "Invalid UTF-8 data")
        try Self.execute(conn, """
            INSERT INTO remote_change_inbox(root_id, remote_id, payload, scan_id) VALUES (?, ?, ?, ?)
            ON CONFLICT(root_id, remote_id) DO UPDATE SET payload = excluded.payload,
                scan_id = excluded.scan_id, attempted_at = 0;
            """, [.int(rootID), .text(change.fileId), .text(payload), .text(scanID)])
    }

    private func schedule(_ conn: SQLiteConnection, remoteID: String, scanID: String) throws {
        try Self.execute(conn, """
            INSERT INTO remote_directory_scans(root_id, remote_id, scan_id, state) VALUES (?, ?, ?, 'pending')
            ON CONFLICT(root_id, remote_id) DO UPDATE SET scan_id = excluded.scan_id, page_token = NULL, state = 'pending'
            WHERE remote_directory_scans.scan_id != excluded.scan_id;
            """, [.int(rootID), .text(remoteID), .text(scanID)])
    }

    private func startRebuild() async throws -> String {
        // C0 is captured BEFORE enumeration, so subsequent Changes cover races with listing.
        let token = try await client.getStartPageToken()
        let scanID = UUID().uuidString
        try await store.write { conn in
            try Self.execute(conn, """
                UPDATE items SET remote_status = 'unknown', phase = 'waitingEvidence',
                    remote_generation = remote_generation + 1, dirty_generation = dirty_generation + 1
                WHERE root_id = ? AND parent_id IS NOT NULL AND remote_file_id IS NOT NULL
                    AND remote_status != 'unknown';
                """, [.int(rootID)])
            // Old queued payloads may precede the lost cursor's gap. Re-probe their
            // identities instead of using them to revalidate stale observations.
            let queued = try Self.statement(conn, "SELECT remote_id FROM remote_change_inbox WHERE root_id = ?;", [.int(rootID)])
            var ids: [String] = []
            while try queued.step() { if let id = queued.columnText(at: 0) { ids.append(id) } }
            queued.reset()
            for id in ids { try enqueue(conn, change: DriveChange(fileId: id, removed: nil, file: nil)) }
            try Self.execute(conn, "DELETE FROM remote_directory_scans WHERE root_id = ?;", [.int(rootID)])
            try schedule(conn, remoteID: remoteRootID, scanID: scanID)
            try Self.execute(conn, """
                INSERT INTO cursors(root_id, account_id, cursor_kind, token_value, is_valid, updated_at)
                VALUES (?, 'default', 'drive_changes', ?, 1, strftime('%s','now'))
                ON CONFLICT(root_id, cursor_kind) DO UPDATE SET token_value = excluded.token_value,
                    is_valid = 1, updated_at = excluded.updated_at;
                """, [.int(rootID), .text(token)])
        }
        return token
    }

    func consume() async throws {
        let saved: String? = try await store.read { conn in
            let queryStatement = try Self.statement(conn, "SELECT token_value FROM cursors WHERE root_id = ? AND cursor_kind = 'drive_changes' AND is_valid = 1;", [.int(rootID)])
            defer { queryStatement.reset() }
            return try queryStatement.step() ? queryStatement.columnText(at: 0) : nil
        }
        var token: String
        if let saved { token = saved } else { token = try await startRebuild() }
        var rebuilt = saved == nil
        while true {
            try SyncRunControl.current?.checkCancellation()
            let page: DriveChangesPage
            do { page = try await client.listChanges(pageToken: token) } catch let error as DriveError {
                // Drive documents non-expiring tokens. Handle explicit token rejection,
                // rather than treating permission/network errors as an expired cursor.
                if case .serverError(let code, let message) = error,
                   code == 410 || (code == 400 && message.lowercased().contains("pagetoken")), !rebuilt {
                    token = try await startRebuild()
                    rebuilt = true
                    continue
                }
                throw error
            }
            guard let next = page.nextPageToken ?? page.newStartPageToken, !next.isEmpty,
                  page.nextPageToken == nil || next != token else {
                throw DriveError.invalidResponse(message: "Changes returned no valid refresh cursor")
            }
            let expectedToken = token
            try await store.write { conn in
                for change in page.changes {
                    if change.fileId == remoteRootID {
                        if change.removed == true || change.file?.trashed == true {
                            throw SyncEngineError.remoteRootLost(remoteId: remoteRootID, reason: change.file?.trashed == true ? "trashed" : "removed")
                        }
                        continue
                    }
                    try enqueue(conn, change: change)
                    // A cursor may move only with the observation AND its invalidation.
                    try Self.execute(conn, """
                        UPDATE items SET remote_status = 'unknown', phase = 'waitingEvidence',
                            remote_generation = remote_generation + 1, dirty_generation = dirty_generation + 1
                        WHERE root_id = ? AND remote_file_id = ?;
                        """, [.int(rootID), .text(change.fileId)])
                }
                try Self.execute(conn, """
                    UPDATE cursors SET token_value = ?, updated_at = strftime('%s','now')
                    WHERE root_id = ? AND cursor_kind = 'drive_changes' AND is_valid = 1 AND token_value = ?;
                    """, [.text(next), .int(rootID), .text(expectedToken)])
                guard conn.changes == 1 else { throw SyncEngineError.general("The Changes cursor was updated by another sync round") }
            }
            token = next
            if page.nextPageToken == nil { break }
        }
        try await seedUnobservedItems()
        try await applyPending()
    }

    private func seedUnobservedItems() async throws {
        // After complete enumeration, only unobserved known IDs need metadata probes.
        // This avoids a files.get per item during a full index rebuild.
        let ids: [String] = try await store.read { conn in
            let queryStatement = try Self.statement(conn, """
                WITH RECURSIVE excluded(item_id) AS (
                    SELECT item_id FROM items WHERE root_id = ? AND remote_scope_excluded = 1
                    UNION ALL SELECT i.item_id FROM items i JOIN excluded e ON i.parent_id = e.item_id
                ) SELECT remote_file_id FROM items WHERE root_id = ? AND phase = 'waitingEvidence'
                    AND remote_status = 'unknown' AND remote_file_id IS NOT NULL AND parent_id IS NOT NULL
                    AND item_id NOT IN excluded
                    AND remote_file_id NOT IN (SELECT remote_id FROM remote_change_inbox WHERE root_id = ?)
                    AND NOT EXISTS (SELECT 1 FROM remote_directory_scans WHERE root_id = ? AND state = 'pending')
                    LIMIT 64;
                """, Array(repeating: .int(rootID), count: 4))
            var ids: [String] = []
            while try queryStatement.step() { if let id = queryStatement.columnText(at: 0) { ids.append(id) } }
            queryStatement.reset()
            return ids
        }
        guard !ids.isEmpty else { return }
        try await store.write { conn in
            for id in ids { try enqueue(conn, change: DriveChange(fileId: id, removed: nil, file: nil)) }
        }
    }

    private struct Entry: Sendable {
        let change: DriveChange
        let scanID: String?
    }
    private enum Scope: Sendable { case inside, outside, unknown }
    private struct Resolved: Sendable {
        let entry: Entry
        let scope: Scope
    }

    private func applyPending() async throws {
        let started = Date().timeIntervalSince1970
        var budget = 64 // bounded metadata ancestry probes, shared across all batches
        var fetched: [String: DriveFile] = [:]
        var unavailable: Set<String> = []
        while true {
            let entries: [Entry] = try await store.read { conn in
                let queryStatement = try Self.statement(conn, """
                    SELECT payload, scan_id FROM remote_change_inbox WHERE root_id = ? AND attempted_at < ?
                    ORDER BY attempted_at, remote_id LIMIT 1000;
                    """, [.int(rootID), .text(String(started))])
                defer { queryStatement.reset() }
                var rows: [Entry] = []
                while try queryStatement.step() {
                    guard let json = queryStatement.columnText(at: 0) else { continue }
                    rows.append(Entry(change: try JSONDecoder().decode(DriveChange.self, from: Data(json.utf8)), scanID: queryStatement.columnText(at: 1)))
                }
                return rows
            }
            guard !entries.isEmpty else { break }
            let known: Set<String> = try await store.read { conn in
                let queryStatement = try Self.statement(conn, """
                    SELECT remote_file_id FROM items WHERE root_id = ? AND entry_kind = 'directory'
                        AND remote_status = 'present'
                        AND remote_scope_excluded = 0;
                    """, [.int(rootID)])
                defer { queryStatement.reset() }
                var result: Set<String> = [remoteRootID]
                while try queryStatement.step() { if let id = queryStatement.columnText(at: 0) { result.insert(id) } }
                return result
            }
            let byID = Dictionary(entries.map { ($0.change.fileId, $0) }, uniquingKeysWith: { _, latest in latest })
            var resolved: [Resolved] = []
            var visited: [String: Scope] = [:]
            var visiting: Set<String> = []
            func resolve(_ id: String) async throws -> Scope {
                if id == remoteRootID { return .inside }
                try SyncRunControl.current?.checkCancellation()
                try Task.checkCancellation()
                func cachedScope() -> Scope? {
                    if let value = visited[id] { return value }
                    if unavailable.contains(id) { return .unknown }
                    guard !visiting.contains(id), visiting.count < 128 else { return .unknown }
                    return nil
                }
                if let scope = cachedScope() { return scope }
                visiting.insert(id)
                defer { visiting.remove(id) }
                let queued = byID[id]
                if queued == nil && known.contains(id) { return .inside }
                var file = queued?.change.file ?? fetched[id]
                if queued?.change.removed == true || file?.trashed == true {
                    if let queued { resolved.append(Resolved(entry: queued, scope: .outside)) }
                    visited[id] = .outside
                    return .outside
                }
                func fetchMissingFile() async throws -> Scope? {
                    if file == nil {
                        guard budget > 0 else { return .unknown }
                        budget -= 1
                        do { file = try await client.getFile(remoteId: id) } catch {
                            try SyncRunControl.current?.checkCancellation()
                            try Task.checkCancellation()
                            unavailable.insert(id)
                            return .unknown // retained inbox, never inferred absent
                        }
                        fetched[id] = file
                    }
                    return nil
                }
                if let scope = try await fetchMissingFile() { return scope }
                guard let file else { return .unknown }
                let scope: Scope
                if let parent = file.parents?.first {
                    scope = try await resolve(parent)
                } else { scope = .outside }
                let entry = Entry(change: DriveChange(fileId: id, removed: false, file: file), scanID: queued?.scanID)
                if queued != nil || scope == .inside { resolved.append(Resolved(entry: entry, scope: scope)) }
                visited[id] = scope
                return scope
            }
            for entry in entries {
                try SyncRunControl.current?.checkCancellation()
                _ = try await resolve(entry.change.fileId)
            }
            // Preserve any budget-limited/failed probe which did not produce a resolution.
            let handled = Set(resolved.map { $0.entry.change.fileId })
            for entry in entries where !handled.contains(entry.change.fileId) {
                resolved.append(Resolved(entry: entry, scope: .unknown))
            }
            let batch = resolved
            try await applyResolvedBatch(batch, started: started)
            if entries.count < 1000 { break }
        }
    }

    private func applyResolvedBatch(_ batch: [Resolved], started: Double) async throws {
        // Persist the observation boundary before touching the local filesystem.
        try await retainResolvedBatch(batch, started: started)
        var groups: [[Resolved]] = []
        var files: [Resolved] = []
        for result in batch {
            if result.entry.change.file?.isDirectory == true {
                // Later paths must see this directory's committed name and parent.
                if !files.isEmpty {
                    groups.append(files)
                    files = []
                }
                groups.append([result])
            } else {
                files.append(result)
            }
        }
        if !files.isEmpty { groups.append(files) }

        for group in groups {
            let prepared = try await prepareResolvedBatch(group)
            let completed = try await executeLocalPathOperations(prepared)
            let committed = try await commitResolvedBatch(completed)
            if group[0].entry.change.file?.isDirectory == true,
               completed.count != 1 || !committed { break }
        }
    }

    private func retainResolvedBatch(_ batch: [Resolved], started: Double) async throws {
        try await store.write { conn in
            for result in batch {
                try enqueue(conn, change: result.entry.change, scanID: result.entry.scanID)
                try Self.execute(conn, "UPDATE remote_change_inbox SET attempted_at = ? WHERE root_id = ? AND remote_id = ?;",
                    [.text(String(started)), .int(rootID), .text(result.entry.change.fileId)])
            }
        }
    }

    private struct PreparedResult: Sendable {
        let result: Resolved
        let operation: LocalPathOperation?
    }

    private enum LocalPathPreparation: Sendable {
        case blocked
        case ready(LocalPathOperation?)
    }

    private func prepareResolvedBatch(_ batch: [Resolved]) async throws -> [PreparedResult] {
        try await store.read { conn in
            var prepared: [PreparedResult] = []
            for result in batch {
                do {
                    let conflict = try Self.statement(conn, """
                        SELECT 1 FROM sync_conflicts
                        WHERE root_id = ? AND remote_file_id = ?;
                        """, [.int(rootID), .text(result.entry.change.fileId)])
                    let isInitialConflict = try conflict.step()
                    conflict.reset()
                    guard !isInitialConflict else { continue }
                    guard case .ready(let operation) = try prepareLocalPathOperation(conn, result)
                    else { continue }
                    prepared.append(PreparedResult(result: result, operation: operation))
                } catch {
                    if DatabaseFailure.isSQLite(error) { throw error }
                }
            }
            return prepared
        }
    }

    private func executeLocalPathOperations(
        _ prepared: [PreparedResult]
    ) async throws -> [PreparedResult] {
        var executed: [PreparedResult] = []
        executed.reserveCapacity(prepared.count)
        for item in prepared {
            do {
                guard try item.operation?.execute() != false else { continue }
                executed.append(item)
            } catch {
                let destination: URL?
                switch item.operation {
                case .move(_, let url, _, _, _), .createDirectory(let url): destination = url
                case nil: destination = nil
                }
                let fallbackPath = item.result.entry.change.file?.name
                    ?? item.result.entry.change.fileId
                let rootPath = rootURL.standardizedFileURL.path
                let relativePath = destination.map {
                    let path = $0.standardizedFileURL.path
                    let prefix = rootPath + "/"
                    return path.hasPrefix(prefix)
                        ? String(path.dropFirst(prefix.count)) : fallbackPath
                } ?? fallbackPath
                try await SyncIssueStore.record(
                    store: store, rootID: rootID,
                    subject: SyncIssueSubject(
                        itemID: nil, remoteFileID: item.result.entry.change.fileId,
                        relativePath: relativePath),
                    stage: .pathUpdate, error: error)
            }
        }
        return executed
    }

    private func commitResolvedBatch(_ completed: [PreparedResult]) async throws -> Bool {
        try await store.write { conn in
            var allCommitted = true
            for item in completed {
                let result = item.result
                try conn.execute("SAVEPOINT remote_apply;")
                do {
                    if try apply(conn, result, executedPathOperation: item.operation) {
                        try Self.execute(conn, "DELETE FROM remote_change_inbox WHERE root_id = ? AND remote_id = ?;",
                            [.int(rootID), .text(result.entry.change.fileId)])
                    } else {
                        allCommitted = false
                    }
                    try conn.execute("RELEASE remote_apply;")
                } catch {
                    try conn.execute("ROLLBACK TO remote_apply;")
                    try conn.execute("RELEASE remote_apply;")
                    if DatabaseFailure.isSQLite(error) { throw error }
                    allCommitted = false
                }
            }
            return allCommitted
        }
    }

    private struct Item {
        let id: Int64
        let parentID: Int64?
        let name: String
        let isDirectory: Bool
        let device: Int64?
        let inode: Int64?
        let localStatus: String?
        let version: Int64?
    }
    private func item(_ conn: SQLiteConnection, remoteID: String) throws -> Item? {
        let queryStatement = try Self.statement(conn, """
            SELECT item_id, parent_id, name, entry_kind, local_device, local_inode,
                local_status, remote_version
            FROM items WHERE root_id = ? AND remote_file_id = ?;
            """, [.int(rootID), .text(remoteID)])
        defer { queryStatement.reset() }
        guard try queryStatement.step(), let id = queryStatement.columnInt64(at: 0), let name = queryStatement.columnText(at: 2) else { return nil }
        return Item(id: id, parentID: queryStatement.columnInt64(at: 1), name: name, isDirectory: queryStatement.columnText(at: 3) == "directory",
                    device: queryStatement.columnInt64(at: 4), inode: queryStatement.columnInt64(at: 5),
                    localStatus: queryStatement.columnText(at: 6), version: queryStatement.columnInt64(at: 7))
    }
    private func unlandedFileHasNoSource(_ item: Item, source: URL) throws -> Bool {
        guard !item.isDirectory, item.localStatus == "absent",
              item.device == nil, item.inode == nil else { return false }
        var sourceStat = stat()
        if lstat(source.path, &sourceStat) == 0 { return false }
        guard errno == ENOENT else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        return true
    }
    private func path(_ conn: SQLiteConnection, itemID: Int64) throws -> String {
        guard let path = try conn.itemRelativePath(itemID: itemID) else {
            throw SyncEngineError.general("Unable to resolve local path for remote observation")
        }
        return path
    }
    private func exclude(_ conn: SQLiteConnection, itemID: Int64) throws {
        try Self.execute(conn, "UPDATE items SET remote_scope_excluded = 1 WHERE root_id = ? AND item_id = ? AND remote_scope_excluded = 0;", [.int(rootID), .int(itemID)])
        try Self.execute(conn, """
            WITH RECURSIVE tree(item_id) AS (
                SELECT ? UNION ALL SELECT i.item_id FROM items i JOIN tree t ON i.parent_id = t.item_id
            ) UPDATE items SET remote_status = 'unknown', phase = 'waitingEvidence', dirty_generation = dirty_generation + 1
            WHERE item_id IN tree;
            """, [.int(itemID)])
    }

    private func prepareLocalPathOperation(
        _ conn: SQLiteConnection, _ result: Resolved
    ) throws -> LocalPathPreparation {
        let change = result.entry.change
        let existing = try item(conn, remoteID: change.fileId)
        if change.removed == true || change.file?.trashed == true {
            return .ready(nil)
        }
        guard result.scope == .inside else {
            return result.scope == .outside ? .ready(nil) : .blocked
        }
        guard let file = change.file, let parentRemote = file.parents?.first,
              let parent = try item(conn, remoteID: parentRemote), parent.isDirectory,
              !file.name.isEmpty, file.name != ".", file.name != "..",
              !file.name.contains("/"), !file.name.contains("\0") else { return .blocked }
        try RemoteNameMapping.validate(file.name)

        let collision = try Self.statement(conn, """
            SELECT remote_file_id, name FROM items INDEXED BY idx_items_local_name_key
            WHERE root_id = ? AND parent_id = ? AND gdrive_name_key(name) = gdrive_name_key(?);
            """, [.int(rootID), .int(parent.id), .text(file.name)])
        while try collision.step() {
            if collision.columnText(at: 0) == nil, collision.columnText(at: 1) != file.name {
                collision.reset()
                return .blocked
            }
            if let remote = collision.columnText(at: 0), remote != file.id {
                collision.reset()
                return .blocked
            }
        }
        collision.reset()

        if let existing, let version = existing.version,
           let incoming = file.versionNumber, incoming < version {
            return .ready(nil)
        }
        let parentPath = try path(conn, itemID: parent.id)
        let destination = rootURL
            .appendingPathComponent(parentPath)
            .appendingPathComponent(file.name)
        try RemoteNameMapping.validateDestination(destination, root: rootURL)
        let moved = existing.map { $0.parentID != parent.id || $0.name != file.name } ?? false
        if let existing, moved {
            let source = rootURL.appendingPathComponent(try path(conn, itemID: existing.id))
            if try unlandedFileHasNoSource(existing, source: source) { return .ready(nil) }
            return .ready(.move(
                source: source,
                destination: destination,
                kind: existing.isDirectory ? .directory : .file,
                device: existing.device,
                inode: existing.inode))
        }
        if file.isDirectory, existing == nil {
            return .ready(.createDirectory(destination))
        }
        return .ready(nil)
    }

    private func apply(
        _ conn: SQLiteConnection,
        _ result: Resolved,
        executedPathOperation: LocalPathOperation?
    ) throws -> Bool {
        let conflict = try Self.statement(conn, """
            SELECT 1 FROM sync_conflicts
            WHERE root_id = ? AND remote_file_id = ?;
            """, [.int(rootID), .text(result.entry.change.fileId)])
        let isInitialConflict = try conflict.step()
        conflict.reset()
        guard !isInitialConflict else { return false }
        guard case .ready(let currentPathOperation) = try prepareLocalPathOperation(conn, result),
              currentPathOperation == executedPathOperation else { return false }
        let change = result.entry.change
        let existing = try item(conn, remoteID: change.fileId) // identity BEFORE parent classification
        func applyRemovalOrExclusion() throws -> Bool? {
            if change.removed == true {
                if let existing { try exclude(conn, itemID: existing.id) }
                return true
            }
            if change.file?.trashed == true {
                try Self.execute(conn, """
                    UPDATE items SET remote_status = 'trashed', phase = 'ready', dirty_generation = dirty_generation + 1
                    WHERE root_id = ? AND remote_file_id = ?;
                    """, [.int(rootID), .text(change.fileId)])
                return true
            }
            guard result.scope == .inside else {
                if let existing { try exclude(conn, itemID: existing.id) }
                return result.scope == .outside
            }
            return nil
        }
        if let applied = try applyRemovalOrExclusion() { return applied }
        guard let file = change.file, let parentRemote = file.parents?.first,
              let parent = try item(conn, remoteID: parentRemote), parent.isDirectory,
              !file.name.isEmpty, file.name != ".", file.name != "..", !file.name.contains("/"), !file.name.contains("\0") else { return false }
        try RemoteNameMapping.validate(file.name)
        let parentPath = try path(conn, itemID: parent.id)
        let destination = rootURL.appendingPathComponent(parentPath).appendingPathComponent(file.name)
        try RemoteNameMapping.validateDestination(destination, root: rootURL)
        func findLocalCollision() throws -> (Bool, Int64?) {
            let collision = try Self.statement(conn, """
                SELECT item_id, remote_file_id, name FROM items INDEXED BY idx_items_local_name_key WHERE root_id = ? AND parent_id = ? AND gdrive_name_key(name) = gdrive_name_key(?);
                """, [.int(rootID), .int(parent.id), .text(file.name)])
            var localOnlyID: Int64?
            while try collision.step() {
                if collision.columnText(at: 1) == nil, collision.columnText(at: 2) != file.name { collision.reset(); return (false, nil) }
                if let remote = collision.columnText(at: 1), remote != file.id { collision.reset(); return (false, nil) }
                localOnlyID = collision.columnInt64(at: 0)
            }
            collision.reset()
            return (true, localOnlyID)
        }
        let (collisionAccepted, localOnlyID) = try findLocalCollision()
        guard collisionAccepted else { return false }
        if let existing, let version = existing.version, let incoming = file.versionNumber, incoming < version { return true }
        let moved = existing.map { $0.parentID != parent.id || $0.name != file.name } ?? false
        let id: Int64
        if let existing { id = existing.id } else if let localOnlyID { id = localOnlyID } else {
            try Self.execute(conn, """
                INSERT INTO items(root_id, parent_id, name, entry_kind, remote_file_id, local_status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'), strftime('%s','now'));
                """, [.int(rootID), .int(parent.id), .text(file.name), .text(file.isDirectory ? "directory" : "file"),
                        .text(file.id), .text(file.isDirectory ? "present" : "absent")])
            id = conn.lastInsertRowId
        }
        try Self.execute(conn, """
            UPDATE items SET parent_id = ?, name = ?, remote_file_id = ?, remote_name = ?, remote_parent_file_id = ?,
                remote_sha256 = ?, remote_size = ?, remote_version = ?, remote_status = 'present',
                remote_generation = remote_generation + 1, dirty_generation = dirty_generation + 1,
                phase = 'ready', updated_at = strftime('%s','now') WHERE item_id = ?;
            """, [.int(parent.id), .text(file.name), .text(file.id), .text(file.name), .text(parentRemote),
                    .text(file.sha256Checksum), .int(file.sizeBytes), .int(file.versionNumber), .int(id)])
        func recordLocalDirectory() throws -> Bool {
            if file.isDirectory {
                var isDirectory: ObjCBool = false
                if FileManager.default.fileExists(atPath: destination.path, isDirectory: &isDirectory) {
                    guard isDirectory.boolValue else { return false }
                    let attrs = try FileManager.default.attributesOfItem(atPath: destination.path)
                    try Self.execute(conn, "UPDATE items SET local_device = ?, local_inode = ?, local_status = 'present' WHERE item_id = ?;",
                        [.int((attrs[.systemNumber] as? NSNumber)?.int64Value),
                         .int((attrs[.systemFileNumber] as? NSNumber)?.int64Value), .int(id)])
                } else {
                    try Self.execute(conn, "UPDATE items SET local_status = 'absent' WHERE item_id = ?;", [.int(id)])
                }
            }
            return true
        }
        guard try recordLocalDirectory() else { return false }
        let wasExcluded = try Self.statement(conn, "SELECT 1 FROM items WHERE root_id = ? AND item_id = ? AND remote_scope_excluded = 1;", [.int(rootID), .int(id)])
        let returning = try wasExcluded.step()
        wasExcluded.reset()
        try Self.execute(conn, "UPDATE items SET remote_scope_excluded = 0 WHERE root_id = ? AND item_id = ? AND remote_scope_excluded = 1;", [.int(rootID), .int(id)])
        if file.isDirectory, existing == nil || moved || returning || result.entry.scanID != nil {
            try schedule(conn, remoteID: file.id, scanID: result.entry.scanID ?? UUID().uuidString)
        }
        return true
    }

    final class Gate: Sendable {
        let paths: Set<String>
        let identities: Set<LocalBaselineCache.Key>
        private let aliases = OSAllocatedUnfairLock(initialState: Set<String>())
        init(paths: Set<String>, identities: Set<LocalBaselineCache.Key>) {
            self.paths = paths
            self.identities = identities
        }
        // Scanner workers may emit a new child path before its renamed parent.
        // Only unmapped parents under an active gate need this identity fallback.
        func blocksLocalAncestors(_ directory: URL, root: URL) throws -> Bool {
            guard !paths.isEmpty else { return false }
            var current = directory
            let prefix = root.path + "/"
            while current.path.hasPrefix(prefix) {
                let attrs = try FileManager.default.attributesOfItem(atPath: current.path)
                if let device = (attrs[.systemNumber] as? NSNumber)?.int64Value,
                   let inode = (attrs[.systemFileNumber] as? NSNumber)?.int64Value,
                   identities.contains(LocalBaselineCache.Key(device: device, inode: inode)) {
                    addAlias(String(current.path.dropFirst(prefix.count)))
                    return true
                }
                current.deleteLastPathComponent()
            }
            return false
        }
        func addAlias(_ path: String) { aliases.withLock { _ = $0.insert(path) } }
        func blocks(_ path: String) -> Bool {
            if paths.isEmpty { return false }
            if paths.contains("") { return true }
            return aliases.withLock { aliases in
                var current = path
                while !current.isEmpty && current != "." {
                    if paths.contains(current) || aliases.contains(current) { return true }
                    current = (current as NSString).deletingLastPathComponent
                }
                return false
            }
        }
    }

    struct NameConflict: Sendable {
        let remoteID: String
        let relativePath: String
        let message: String
    }

    func nameConflicts() async throws -> [NameConflict] {
        try await store.read { conn in
            let query = try Self.statement(conn, """
                SELECT DISTINCT c.remote_id, json_extract(c.payload, '$.file.name'), p.item_id
                FROM remote_change_inbox c
                CROSS JOIN items p ON p.root_id = c.root_id
                    AND p.remote_file_id = json_extract(c.payload, '$.file.parents[0]')
                CROSS JOIN items i INDEXED BY idx_items_local_name_key
                    ON i.root_id = p.root_id AND i.parent_id = p.item_id
                    AND gdrive_name_key(i.name) = gdrive_name_key(json_extract(c.payload, '$.file.name'))
                WHERE c.root_id = ? AND (i.remote_file_id != c.remote_id
                    OR (i.remote_file_id IS NULL
                        AND i.name != json_extract(c.payload, '$.file.name')))
                ORDER BY c.remote_id;
                """, [.int(rootID)])
            defer { query.reset() }
            var result: [NameConflict] = []
            while try query.step(), let remoteID = query.columnText(at: 0),
                  let name = query.columnText(at: 1),
                  let parentID = query.columnInt64(at: 2) {
                let parent = try path(conn, itemID: parentID)
                let relativePath = parent.isEmpty ? name : "\(parent)/\(name)"
                result.append(NameConflict(
                    remoteID: remoteID, relativePath: relativePath,
                    message: "Remote name conflicts with an existing local name: \(relativePath)"))
            }
            return result
        }
    }

    func gate() async throws -> Gate {
        try await store.read { conn in
            let queryStatement = try Self.statement(conn, """
                SELECT item_id FROM items WHERE root_id = ? AND phase = 'waitingEvidence' AND remote_status = 'unknown'
                UNION SELECT item_id FROM items WHERE root_id = ? AND remote_scope_excluded = 1
                UNION SELECT i.item_id FROM remote_change_inbox c JOIN items i
                    ON i.root_id = c.root_id AND i.remote_file_id = c.remote_id WHERE c.root_id = ?
                UNION SELECT i.item_id FROM remote_change_inbox c CROSS JOIN items p
                    ON p.root_id = c.root_id AND p.remote_file_id = json_extract(c.payload, '$.file.parents[0]') CROSS JOIN items i INDEXED BY idx_items_local_name_key ON i.root_id = p.root_id AND i.parent_id = p.item_id
                    AND gdrive_name_key(i.name) = gdrive_name_key(json_extract(c.payload, '$.file.name')) WHERE c.root_id = ?
                UNION SELECT i.item_id FROM remote_directory_scans d JOIN items i
                    ON i.root_id = d.root_id AND i.remote_file_id = d.remote_id WHERE d.root_id = ? AND d.state = 'pending'
                UNION SELECT item_id FROM sync_conflicts WHERE root_id = ?;
                """, Array(repeating: .int(rootID), count: 6))
            var ids: Set<Int64> = []
            while try queryStatement.step() { if let id = queryStatement.columnInt64(at: 0) { ids.insert(id) } }
            queryStatement.reset()
            var paths: Set<String> = []
            var identities: Set<LocalBaselineCache.Key> = []
            for id in ids {
                paths.insert(try path(conn, itemID: id))
                let identity = try Self.statement(conn, "SELECT local_device, local_inode FROM items WHERE item_id = ?;", [.int(id)])
                if try identity.step(), let device = identity.columnInt64(at: 0), let inode = identity.columnInt64(at: 1) {
                    identities.insert(LocalBaselineCache.Key(device: device, inode: inode))
                }
                identity.reset()
            }
            return Gate(paths: paths, identities: identities)
        }
    }

    /// One wave, at most `limit` directory pages; siblings can progress concurrently.
    /// Children remain in the inbox for the next reconcile round, not a full-tree barrier.
    func enumeratePending(limit: Int) async throws -> Bool {
        struct Job: Sendable { let id: String; let scanID: String; let token: String? }
        let jobs: [Job] = try await store.read { conn in
            let queryStatement = try Self.statement(conn, """
                SELECT remote_id, scan_id, page_token FROM remote_directory_scans
                WHERE root_id = ? AND state = 'pending' LIMIT ?;
                """, [.int(rootID), .int(Int64(max(1, min(64, limit))))])
            defer { queryStatement.reset() }
            var rows: [Job] = []
            while try queryStatement.step() {
                if let id = queryStatement.columnText(at: 0), let scan = queryStatement.columnText(at: 1) { rows.append(Job(id: id, scanID: scan, token: queryStatement.columnText(at: 2))) }
            }
            return rows
        }
        guard !jobs.isEmpty else { return false }
        try SyncRunControl.current?.checkCancellation()
        try await withThrowingTaskGroup(of: Void.self) { group in
            for job in jobs {
                group.addTask {
                    let page: DriveClient.ChildrenPage
                    do {
                        page = try await client.listChildrenPage(parentId: job.id, pageToken: job.token)
                    } catch let error as DriveError {
                        if job.token != nil, case .serverError(let code, let message) = error,
                           code == 410 || (code == 400 && message.lowercased().contains("pagetoken")) {
                            // files.list continuation tokens can be rejected. Restart this
                            // directory only; already durable child observations are retained.
                            try await store.batchWrite { conn in
                                try Self.execute(conn, """
                                    UPDATE remote_directory_scans SET page_token = NULL
                                    WHERE root_id = ? AND remote_id = ? AND scan_id = ? AND page_token IS ? AND state = 'pending';
                                    """, [.int(rootID), .text(job.id), .text(job.scanID), .text(job.token)])
                            }
                        }
                        throw error
                    }
                    guard page.nextPageToken == nil || page.nextPageToken != job.token else {
                        throw DriveError.invalidResponse(message: "Catalog pagination cursor does not advance")
                    }
                    try await store.batchWrite { conn in
                        for file in page.files {
                            try enqueue(conn, change: DriveChange(fileId: file.id, removed: false, file: file), scanID: job.scanID)
                        }
                        try Self.execute(conn, """
                            UPDATE remote_directory_scans SET page_token = ?, state = ?
                            WHERE root_id = ? AND remote_id = ? AND scan_id = ? AND page_token IS ? AND state = 'pending';
                            """, [.text(page.nextPageToken), .text(page.nextPageToken == nil ? "complete" : "pending"),
                                    .int(rootID), .text(job.id), .text(job.scanID), .text(job.token)])
                        guard conn.changes == 1 else { throw SyncEngineError.general("The catalog patch task is stale") }
                    }
                }
            }
            try await group.waitForAll()
        }
        try SyncRunControl.current?.checkCancellation()
        return true
    }

    func pendingCount() async throws -> Int {
        try await store.read { conn in
            let queryStatement = try Self.statement(conn, """
                WITH RECURSIVE excluded(item_id) AS (
                    SELECT item_id FROM items WHERE root_id = ? AND remote_scope_excluded = 1
                    UNION ALL SELECT i.item_id FROM items i JOIN excluded e ON i.parent_id = e.item_id
                ) SELECT (SELECT count(*) FROM remote_change_inbox WHERE root_id = ?)
                     + (SELECT count(*) FROM remote_directory_scans WHERE root_id = ? AND state = 'pending')
                     + (SELECT count(*) FROM items WHERE root_id = ? AND remote_status = 'unknown' AND phase = 'waitingEvidence'
                          AND item_id NOT IN excluded);
                """, Array(repeating: .int(rootID), count: 4))
            defer { queryStatement.reset() }
            _ = try queryStatement.step()
            return Int(queryStatement.columnInt64(at: 0) ?? 0)
        }
    }
}
