import Darwin
import Foundation
import CommonCrypto
import Logging
import DirectoryScanner
import os

// Internal SyncEngine implementation split by synchronization phase.
extension SyncEngine {
    /// Execute large files (> 8MB) Resume upload in chunks:
    /// - automatic check operations Whether there are unfinished sessions and breakpoints in the table
    /// - towards Google Drive The server detects the confirmed receipt offset (queryResumableOffset)
    /// - Using constant memory FileHandle Streaming slice reading (Default 8MB,must be 256KB an integer multiple of)
    /// - Each time a chunk is completed, the new offset persist to SQLite operations meter, power outage/Seamless transmission can be resumed after changing threads
    @discardableResult
    func performResumableUpload(
        rootId: Int64,
        itemId: Int64,
        fileURL: URL,
        fileSize: Int64,
        expectedSha256: String,
        remoteId: String,
        parentId: String,
        name: String,
        isUpdate: Bool,
        chunkSize: Int64 = 8 * 1024 * 1024 // 8MB chunked,256KB Integer multiple
    ) async throws -> DriveFile {
        if isUpdate { throw DriveError.unsafeOverwrite(fileId: remoteId) }
        let opId = "resumable_\(remoteId)"
        var sessionURL: URL?
        var currentOffset: Int64 = 0
        var completedFile: DriveFile?

        struct ExistingResumableOp {
            let sessionURL: URL
            let confirmedOffset: Int64
            let expectedSha256: String
            let totalBytes: Int64
        }

        // 1. Query whether there are outstanding breakpoint sessions
        let existingOp: ExistingResumableOp? = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT session_uri, confirmed_offset, expected_sha256, total_bytes
            FROM operations
            WHERE operation_id = ? AND state = 'inFlight';
            """)
            stmt.bindText(opId, at: 1)
            defer { stmt.reset() }
            if try stmt.step(), let uriStr = stmt.columnText(at: 0), let uri = URL(string: uriStr) {
                let off = stmt.columnInt64(at: 1) ?? 0
                let sha = stmt.columnText(at: 2) ?? ""
                let bytes = stmt.columnInt64(at: 3) ?? 0
                return ExistingResumableOp(sessionURL: uri, confirmedOffset: off, expectedSha256: sha, totalBytes: bytes)
            }
            return nil
        }

        func restoreExistingSession() async throws {
            if let existing = existingOp {
                // Key: Verify Breakpoint Session Expectations sha256 Whether the file size is strictly consistent with the current file!
                // If the file has been modified in the middle, the old session will be immediately invalidated and reset to prevent incorrect resumption of data from splicing dirty data in the cloud.
                let isSameFile = (existing.totalBytes == fileSize &&
                                  existing.expectedSha256.caseInsensitiveCompare(expectedSha256) == .orderedSame)
                if isSameFile {
                    // Detect the valid data actually received by the server from the cloud. offset
                    do {
                        switch try await client.queryResumableOffset(sessionURL: existing.sessionURL, totalBytes: fileSize) {
                        case .complete(let file):
                            completedFile = file
                            sessionURL = existing.sessionURL
                            currentOffset = fileSize
                        case .incomplete(let serverOffset):
                            guard serverOffset >= 0, serverOffset < fileSize else {
                                throw DriveError.invalidResponse(message: "Resumable upload offset is out of bounds: \(serverOffset)/\(fileSize)")
                            }
                            sessionURL = existing.sessionURL
                            currentOffset = serverOffset
                        case .expired:
                            sessionURL = nil
                            currentOffset = 0
                        }
                    } catch {
                        // Ad hoc network/Server-side failures must not discard sessions that may still be valid.
                        throw error
                    }
                } else {
                    // The file content or size has changed, the breakpoint will be invalidated, and a new upload will be initiated again.
                    sessionURL = nil
                    currentOffset = 0
                }
            }
        }
        try await restoreExistingSession()

        // 2. If there is no valid session, initiate a new Resumable Upload session and persist operation intent
        var activeSessionURL: URL
        if let sURL = sessionURL {
            activeSessionURL = sURL
        } else {
            if isUpdate {
                activeSessionURL = try await client.initiateResumableUpdate(remoteId: remoteId, totalBytes: fileSize)
            } else {
                activeSessionURL = try await client.initiateResumableUpload(name: name, parentId: parentId, remoteId: remoteId, totalBytes: fileSize)
            }
            currentOffset = 0

            let now = Date().timeIntervalSince1970
            let activeSessionURI = activeSessionURL.absoluteString
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                INSERT INTO operations (
                    operation_id, root_id, item_id, operation_type, state,
                    expected_sha256, target_remote_id, target_parent_remote_id,
                    session_uri, confirmed_offset, total_bytes, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, 'uploadResumable', 'inFlight',
                    ?, ?, ?,
                    ?, 0, ?, ?, ?
                )
                ON CONFLICT (operation_id) DO UPDATE SET
                    session_uri = excluded.session_uri,
                    expected_sha256 = excluded.expected_sha256,
                    confirmed_offset = 0,
                    total_bytes = excluded.total_bytes,
                    state = 'inFlight',
                    updated_at = excluded.updated_at;
                """)
                stmt.bindText(opId, at: 1)
                stmt.bindInt64(rootId, at: 2)
                stmt.bindInt64(itemId, at: 3)
                stmt.bindText(expectedSha256, at: 4)
                stmt.bindText(remoteId, at: 5)
                stmt.bindText(parentId, at: 6)
                stmt.bindText(activeSessionURI, at: 7)
                stmt.bindInt64(fileSize, at: 8)
                stmt.bindDouble(now, at: 9)
                stmt.bindDouble(now, at: 10)
                _ = try stmt.step()
                stmt.reset()
            }
        }

        // 3. Streaming chunked read and upload loop
        let fileHandle = try FileHandle(forReadingFrom: fileURL)
        defer { try? fileHandle.close() }

        // Get the initial file timestamp and size for detecting concurrent modifications in a chunked transfer loop
        let initialAttrs = try? FileManager.default.attributesOfItem(atPath: fileURL.path)
        let initialMtime = initialAttrs?[.modificationDate] as? Date

        var finalDriveFile: DriveFile? = completedFile
        var repeatedNoProgress = false

        func validateSourceFile() async throws {
            if let currentAttrs = try? FileManager.default.attributesOfItem(atPath: fileURL.path) {
                let currentDiskSize = (currentAttrs[.size] as? NSNumber)?.int64Value ?? fileSize
                let currentMtime = currentAttrs[.modificationDate] as? Date
                if currentDiskSize != fileSize || (initialMtime != nil && currentMtime != initialMtime) {
                    // If the file is modified midway, the upload will be terminated immediately and marked operation for failed,Prevent corrupted data from being spliced and sent to the cloud
                    let now = Date().timeIntervalSince1970
                    try await store.write { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE operations SET state = 'failed', updated_at = ? WHERE operation_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindText(opId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                    throw DriveError.fileModifiedDuringUpload(path: fileURL.path)
                }
            }
        }

        while currentOffset < fileSize {
            // Detect whether the file is modified concurrently in the middle of uploading chunks
            try await validateSourceFile()

            try fileHandle.seek(toOffset: UInt64(currentOffset))
            let bytesToRead = min(chunkSize, fileSize - currentOffset)
            guard let chunkData = try fileHandle.read(upToCount: Int(bytesToRead)), !chunkData.isEmpty else {
                throw CocoaError(.fileReadUnknown)
            }

            let result = try await client.uploadResumableChunk(
                sessionURL: activeSessionURL,
                chunkData: chunkData,
                offset: currentOffset,
                totalBytes: fileSize
            )

            let previousOffset = currentOffset
            func applyChunkResult() async throws {
                switch result {
                case .incomplete(let confirmedOffset):
                    guard confirmedOffset >= previousOffset,
                          confirmedOffset <= previousOffset + Int64(chunkData.count),
                          confirmedOffset < fileSize else {
                        throw DriveError.invalidResponse(message: "Resumable upload confirmed an invalid offset: \(confirmedOffset); sent range \(previousOffset)-\(previousOffset + Int64(chunkData.count) - 1)")
                    }
                    if confirmedOffset == previousOffset {
                        guard !repeatedNoProgress else {
                            throw DriveError.invalidResponse(message: "Resumable upload repeatedly acknowledged no new bytes")
                        }
                        repeatedNoProgress = true
                    } else {
                        repeatedNoProgress = false
                    }
                    currentOffset = confirmedOffset
                case .complete(let file):
                    currentOffset = fileSize
                    finalDriveFile = file
                case .expired:
                    activeSessionURL = isUpdate
                        ? try await client.initiateResumableUpdate(remoteId: remoteId, totalBytes: fileSize)
                        : try await client.initiateResumableUpload(name: name, parentId: parentId, remoteId: remoteId, totalBytes: fileSize)
                    currentOffset = 0
                    repeatedNoProgress = false
                }
            }
            try await applyChunkResult()
            let confirmedBytes = max(0, currentOffset - previousOffset)
            if confirmedBytes > 0 {
                self.monitor.reportUploadProgress(id: fileURL.path, additionalBytes: confirmedBytes)
            }

            // Each time a block is completed, it is immediately recorded in the database as confirmed offset with status
            let recordedOffset = currentOffset
            let now = Date().timeIntervalSince1970
            let activeSessionURI = activeSessionURL.absoluteString
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                UPDATE operations SET
                    session_uri = ?,
                    confirmed_offset = ?,
                    state = 'inFlight',
                    updated_at = ?
                WHERE operation_id = ?;
                """)
                stmt.bindText(activeSessionURI, at: 1)
                stmt.bindInt64(recordedOffset, at: 2)
                stmt.bindDouble(now, at: 3)
                stmt.bindText(opId, at: 4)
                _ = try stmt.step()
                stmt.reset()
            }

            if finalDriveFile != nil {
                break
            }
        }

        let file = try await (finalDriveFile != nil ? finalDriveFile! : client.getFile(remoteId: remoteId))
        guard file.sizeBytes == fileSize else {
            let now = Date().timeIntervalSince1970
            try await store.write { conn in
                let stmt = try conn.cachedStatement("UPDATE operations SET state = 'failed', updated_at = ? WHERE operation_id = ?;")
                stmt.bindDouble(now, at: 1)
                stmt.bindText(opId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
            throw DriveError.sizeMismatch(expected: fileSize, actual: file.sizeBytes)
        }
        guard let checksum = file.sha256Checksum,
              checksum.caseInsensitiveCompare(expectedSha256) == .orderedSame else {
            // The hash calculated by the final stitching in the cloud does not match the expected one (indicating tampering or data corruption during transmission)
            let now = Date().timeIntervalSince1970
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                UPDATE operations SET state = 'failed', updated_at = ? WHERE operation_id = ?;
                """)
                stmt.bindDouble(now, at: 1)
                stmt.bindText(opId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
            throw DriveError.checksumMismatch(expected: expectedSha256, actual: file.sha256Checksum)
        }
        let now = Date().timeIntervalSince1970
        try await store.write { conn in
            let stmt = try conn.cachedStatement("""
            UPDATE operations SET state = 'completed', confirmed_offset = ?, updated_at = ? WHERE operation_id = ?;
            """)
            stmt.bindInt64(fileSize, at: 1)
            stmt.bindDouble(now, at: 2)
            stmt.bindText(opId, at: 3)
            _ = try stmt.step()
            stmt.reset()
        }
        return file
    }

}
