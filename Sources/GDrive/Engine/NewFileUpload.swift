import Foundation
import os

enum SyncReceiptResult: Sendable, Equatable {
    case applied
    case stale
}

enum MultipartCreateRecovery: Sendable {
    case verifyExisting
    case updatePersistedRemoteID
}

struct FileUploadReceiptExpectation: Sendable {
    let itemID: Int64
    let localGeneration: Int64
    let remoteGeneration: Int64
    let dirtyGeneration: Int64
    let operationID: String?

    init(
        intent: DurableCreateIntent,
        localGeneration: Int64,
        remoteGeneration: Int64,
        dirtyGeneration: Int64
    ) {
        self.itemID = intent.itemID
        self.localGeneration = localGeneration
        self.remoteGeneration = remoteGeneration
        self.dirtyGeneration = dirtyGeneration
        self.operationID = intent.operationID
    }

    init(
        itemID: Int64,
        localGeneration: Int64,
        remoteGeneration: Int64,
        dirtyGeneration: Int64
    ) {
        self.itemID = itemID
        self.localGeneration = localGeneration
        self.remoteGeneration = remoteGeneration
        self.dirtyGeneration = dirtyGeneration
        self.operationID = nil
    }
}

extension SyncEngine {
    func prepareNewFileUpload(
        rootID: Int64,
        itemID: Int64?,
        parentItemID: Int64,
        name: String,
        parentRemoteID: String,
        candidateRemoteID: String? = nil,
        input: StableUploadInput,
        pendingIntent: DurableCreateIntent?
    ) async throws -> DurableCreateIntent {
        if let pendingIntent {
            guard pendingIntent.totalBytes == input.size,
                  let expectedSHA256 = pendingIntent.expectedSHA256,
                  expectedSHA256.caseInsensitiveCompare(input.sha256) == .orderedSame else {
                throw SyncEngineError.general(
                    "Unfinished file creation intent input has changed: \(name)"
                )
            }
            return pendingIntent
        }

        let remoteID: String
        if let candidateRemoteID {
            remoteID = candidateRemoteID
        } else {
            remoteID = try await idPool.nextId()
        }
        return try await DurableCreateIntentStore.prepareFileUpload(
            store: store,
            rootID: rootID,
            itemID: itemID,
            parentItemID: parentItemID,
            name: name,
            targetParentRemoteID: parentRemoteID,
            candidateRemoteID: remoteID,
            device: input.version.device,
            inode: input.version.inode,
            mtime: input.version.mtime,
            size: input.size,
            sha256: input.sha256,
            transport: input.data == nil ? .resumable : .multipart
        )
    }

    func performNewFileUpload(
        rootID: Int64,
        intent: DurableCreateIntent,
        input: StableUploadInput,
        name: String,
        multipartRecovery: MultipartCreateRecovery
    ) async throws -> DriveFile {
        if let content = input.data {
            do {
                return try await client.uploadMultipart(
                    name: name,
                    parentId: intent.targetParentRemoteID,
                    remoteId: intent.targetRemoteID,
                    content: content,
                    expectedSha256: input.sha256
                )
            } catch let error as DriveError {
                guard multipartRecovery == .updatePersistedRemoteID else { throw error }
                switch error {
                case .conflict:
                    break
                case .serverError(let code, _) where code == 400 || code == 409:
                    break
                default:
                    throw error
                }
                return try await client.updateMultipart(
                    remoteId: intent.targetRemoteID,
                    content: content,
                    expectedSha256: input.sha256
                )
            }
        }
        return try await performResumableUpload(
            rootId: rootID,
            itemId: intent.itemID,
            fileURL: input.fileURL,
            fileSize: input.size,
            expectedSha256: input.sha256,
            remoteId: intent.targetRemoteID,
            parentId: intent.targetParentRemoteID,
            name: name,
            operationID: intent.operationID
        )
    }

}

extension DurableCreateIntentStore {
    static func commitFileUploadReceipt(
        store: StateStore,
        expectation: FileUploadReceiptExpectation,
        uploadedFile: DriveFile,
        input: StableUploadInput,
        now: Double = Date().timeIntervalSince1970
    ) async throws -> SyncReceiptResult {
        try input.version.validate(at: input.sourceURL)
        let applied = OSAllocatedUnfairLock(initialState: false)
        try await store.batchWrite { conn in
            guard (try? LocalFileVersion.read(at: input.sourceURL)) == input.version else {
                return
            }
            let statement = try conn.cachedStatement("""
            UPDATE items SET
                remote_file_id = ?,
                local_device = ?, local_inode = ?, local_mtime = ?,
                local_size = ?, local_sha256 = ?, local_status = 'present',
                base_sha256 = ?, base_size = ?,
                remote_sha256 = ?, remote_size = ?, remote_status = 'present',
                phase = 'committed', dirty_generation = 0,
                updated_at = ?
            WHERE item_id = ? AND local_generation = ? AND remote_generation = ?
                AND dirty_generation = ?;
            """)
            statement.bindText(uploadedFile.id, at: 1)
            statement.bindInt64(input.version.device, at: 2)
            statement.bindInt64(input.version.inode, at: 3)
            statement.bindInt64(input.version.mtime, at: 4)
            statement.bindInt64(input.size, at: 5)
            statement.bindText(input.sha256, at: 6)
            statement.bindText(input.sha256, at: 7)
            statement.bindInt64(input.size, at: 8)
            statement.bindText(input.sha256, at: 9)
            statement.bindInt64(input.size, at: 10)
            statement.bindDouble(now, at: 11)
            statement.bindInt64(expectation.itemID, at: 12)
            statement.bindInt64(expectation.localGeneration, at: 13)
            statement.bindInt64(expectation.remoteGeneration, at: 14)
            statement.bindInt64(expectation.dirtyGeneration, at: 15)
            _ = try statement.step()
            statement.reset()
            guard conn.changes == 1 else { return }
            if let operationID = expectation.operationID {
                try completeOperation(conn: conn, operationID: operationID, now: now)
            }
            applied.withLock { $0 = true }
        }
        return applied.withLock { $0 } ? .applied : .stale
    }
}
