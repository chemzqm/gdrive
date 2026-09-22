import Foundation
import os

enum FileDownloadExecutionResult: Sendable {
    case published(LocalFileVersion, verifiedSHA256: String)
    case destinationChanged(DriveClient.VerifiedDownload)
}

struct LocalFilePublicationRecoveryError: Error, LocalizedError, CustomStringConvertible, Sendable {
    let destinationPath: String
    let stagingURL: URL
    let sha256: String
    let size: Int64
    let reason: String

    var errorDescription: String? {
        "Local publication failed after writing began at \(destinationPath). "
            + "Verified recovery copy retained at \(stagingURL.path) "
            + "(SHA-256: \(sha256), size: \(size)): \(reason)"
    }

    var description: String { errorDescription ?? reason }
}

enum FileDownloadReceiptExpectation: Sendable {
    case bootstrap(rootID: Int64, parentItemID: Int64, file: DriveFile)
    case incremental(
        itemID: Int64,
        localGeneration: Int64,
        remoteGeneration: Int64,
        dirtyGeneration: Int64
    )
}

extension SyncEngine {
    func executeFileDownload(
        remoteID: String,
        expectedSHA256: String?,
        destination: URL,
        expectedDestination: LocalFileVersion?,
        temporaryDirectory: URL,
        expectedLocalSHA256: String? = nil,
        cached: DriveClient.VerifiedDownload? = nil,
        onProgress: (@Sendable (Int64) -> Void)? = nil,
        beforePublish: (@Sendable () async throws -> Void)? = nil
    ) async throws -> FileDownloadExecutionResult {
        let download: DriveClient.VerifiedDownload
        if let cached {
            download = cached
        } else {
            download = try await client.downloadVerifiedFile(
                remoteId: remoteID,
                expectedSha256: expectedSHA256,
                temporaryDirectory: temporaryDirectory,
                onProgress: onProgress
            )
        }

        do {
            try await beforePublish?()
            switch try filePublisher(
                download.url, destination, expectedDestination, download.sha256, expectedLocalSHA256) {
            case .published(let version):
                try? FileManager.default.removeItem(at: download.url)
                return .published(version, verifiedSHA256: download.sha256)
            case .destinationChanged:
                return .destinationChanged(download)
            case .failedAfterWriteStarted(let failure):
                throw LocalFilePublicationRecoveryError(
                    destinationPath: failure.destinationPath,
                    stagingURL: download.url,
                    sha256: download.sha256,
                    size: download.size,
                    reason: failure.reason)
            }
        } catch let error as LocalFilePublicationRecoveryError {
            throw error
        } catch {
            try? FileManager.default.removeItem(at: download.url)
            throw error
        }
    }
}

extension StateStore {
    func commitFileDownloadReceipt(
        expectation: FileDownloadReceiptExpectation,
        localURL: URL,
        published: LocalFileVersion,
        verifiedSHA256: String? = nil,
        now: Double = Date().timeIntervalSince1970
    ) async throws -> SyncReceiptResult {
        let applied = OSAllocatedUnfairLock(initialState: false)
        try await batchWrite { conn in
            guard (try? LocalFileVersion.read(at: localURL)) == published else { return }
            switch expectation {
            case .bootstrap(let rootID, let parentItemID, let file):
                guard let sha256 = verifiedSHA256 ?? file.sha256Checksum else { return }
                let statement = try conn.cachedStatement("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id,
                    local_mtime, local_ctime, local_size, local_sha256,
                    base_sha256, base_size,
                    remote_sha256, remote_size, remote_status,
                    local_generation, local_status, phase,
                    local_device, local_inode,
                    created_at, updated_at
                ) VALUES (
                    ?, ?, ?, 'file', ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, 'present',
                    1, 'present', 'committed',
                    ?, ?,
                    ?, ?
                )
                ON CONFLICT (root_id, parent_id, name) WHERE parent_id IS NOT NULL
                DO UPDATE SET
                    remote_file_id = excluded.remote_file_id,
                    local_mtime = excluded.local_mtime, local_ctime = excluded.local_ctime,
                    local_size = excluded.local_size,
                    local_sha256 = excluded.local_sha256,
                    base_sha256 = excluded.base_sha256, base_size = excluded.base_size,
                    remote_sha256 = excluded.remote_sha256, remote_size = excluded.remote_size,
                    remote_status = 'present', local_status = 'present', phase = 'committed',
                    local_device = excluded.local_device, local_inode = excluded.local_inode,
                    updated_at = excluded.updated_at
                WHERE items.remote_file_id = excluded.remote_file_id;
                """)
                statement.bindInt64(rootID, at: 1)
                statement.bindInt64(parentItemID, at: 2)
                statement.bindText(file.name, at: 3)
                statement.bindText(file.id, at: 4)
                statement.bindInt64(published.mtime, at: 5)
                statement.bindInt64(published.ctime, at: 6)
                statement.bindInt64(published.size, at: 7)
                statement.bindText(sha256, at: 8)
                statement.bindText(sha256, at: 9)
                statement.bindInt64(published.size, at: 10)
                statement.bindText(sha256, at: 11)
                statement.bindInt64(published.size, at: 12)
                statement.bindInt64(published.device, at: 13)
                statement.bindInt64(published.inode, at: 14)
                statement.bindDouble(now, at: 15)
                statement.bindDouble(now, at: 16)
                _ = try statement.step()
                statement.reset()
                guard conn.changes == 1 else { return }

                let inbox = try conn.cachedStatement(
                    "DELETE FROM remote_change_inbox WHERE root_id = ? AND remote_id = ?;")
                inbox.bindInt64(rootID, at: 1)
                inbox.bindText(file.id, at: 2)
                _ = try inbox.step()
                inbox.reset()

            case .incremental(
                let itemID,
                let localGeneration,
                let remoteGeneration,
                let dirtyGeneration
            ):
                let statement = try conn.cachedStatement("""
                UPDATE items SET
                    local_sha256 = remote_sha256,
                    local_size = remote_size,
                    local_mtime = ?,
                    local_ctime = ?,
                    local_device = ?,
                    local_inode = ?,
                    local_status = 'present',
                    remote_status = 'present',
                    base_sha256 = remote_sha256,
                    base_size = remote_size,
                    phase = 'committed',
                    dirty_generation = 0,
                    updated_at = ?
                WHERE item_id = ? AND local_generation = ?
                    AND remote_generation = ? AND dirty_generation = ?;
                """)
                statement.bindInt64(published.mtime, at: 1)
                statement.bindInt64(published.ctime, at: 2)
                statement.bindInt64(published.device, at: 3)
                statement.bindInt64(published.inode, at: 4)
                statement.bindDouble(now, at: 5)
                statement.bindInt64(itemID, at: 6)
                statement.bindInt64(localGeneration, at: 7)
                statement.bindInt64(remoteGeneration, at: 8)
                statement.bindInt64(dirtyGeneration, at: 9)
                _ = try statement.step()
                statement.reset()
                guard conn.changes == 1 else { return }
            }
            applied.withLock { $0 = true }
        }
        return applied.withLock { $0 } ? .applied : .stale
    }
}
