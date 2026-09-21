import Darwin
import Foundation

enum LocalFilePublication {
    struct Exchange {
        let source: URL
        let destination: URL
        let displaced: LocalFileVersion
        let published: LocalFileVersion

        func deleteDisplacedFile() throws {
            try FileManager.default.removeItem(at: source)
        }

        func rollback() throws {
            guard try LocalFileVersion.read(at: source) == displaced,
                  try LocalFileVersion.read(at: destination) == published else {
                throw SyncEngineError.general(
                    "Conflict publication could not be rolled back because a file changed")
            }
            guard renameatx_np(
                AT_FDCWD, source.path, AT_FDCWD, destination.path, UInt32(RENAME_SWAP)) == 0
            else {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
        }
    }

    /// Exchanges a caller-owned source with an existing destination. After a
    /// successful exchange the old destination is at `source`, so the caller
    /// can remove it after committing its database state or roll the exchange
    /// back if that commit fails.
    static func exchangeOwnedFile(
        _ source: URL, with destination: URL, expected: LocalFileVersion
    ) throws -> Exchange {
        guard try LocalFileVersion.read(at: destination) == expected,
              let incoming = try LocalFileVersion.read(at: source),
              incoming.device == expected.device else {
            throw DriveError.fileModifiedDuringUpload(path: destination.path)
        }
        guard renameatx_np(
            AT_FDCWD, source.path, AT_FDCWD, destination.path, UInt32(RENAME_SWAP)) == 0
        else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        guard let displaced = try LocalFileVersion.read(at: source),
              displaced.device == expected.device, displaced.inode == expected.inode,
              displaced.mtime == expected.mtime, displaced.size == expected.size,
              let published = try LocalFileVersion.read(at: destination),
              published.device == incoming.device, published.inode == incoming.inode,
              published.mtime == incoming.mtime, published.size == incoming.size else {
            throw SyncEngineError.general(
                "A file changed during conflict publication; both files were retained")
        }
        return Exchange(source: source, destination: destination,
                        displaced: displaced, published: published)
    }

    /// Atomic namespace publication. Never remove the destination first.
    /// A race at the final exchange is detected on the displaced inode and its
    /// bytes are preserved in the staging directory instead of being deleted.
    static func publish(_ temporaryURL: URL, to destination: URL, expected: LocalFileVersion?) throws -> LocalFileVersion {
        guard try LocalFileVersion.read(at: destination) == expected,
              let downloaded = try LocalFileVersion.read(at: temporaryURL) else {
            throw DriveError.fileModifiedDuringUpload(path: destination.path)
        }
        if let expected {
            // Use a dedicated recovery path: callers may clean their download
            // temporary path on failure, but must never delete displaced data.
            let recovery = temporaryURL.appendingPathExtension("local-conflict-\(UUID().uuidString)")
            try FileManager.default.moveItem(at: temporaryURL, to: recovery)
            guard renameatx_np(AT_FDCWD, recovery.path, AT_FDCWD, destination.path, UInt32(RENAME_SWAP)) == 0 else {
                let error = POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
                try? FileManager.default.removeItem(at: recovery) // only our downloaded bytes
                throw error
            }
            let displaced = try LocalFileVersion.read(at: recovery)
            // rename changes ctime itself; identity, mtime and size remain comparable.
            guard displaced?.device == expected.device, displaced?.inode == expected.inode,
                  displaced?.mtime == expected.mtime, displaced?.size == expected.size else {
                throw SyncEngineError.general("The local file changed during publication; the original content was retained: \(recovery.path)")
            }
            // Keep the previous version recoverable (also preserves open writers).
            // If Trash is unavailable, leave the recovery file outside the sync tree.
            var trashURL: NSURL?
            _ = try? FileManager.default.trashItem(at: recovery, resultingItemURL: &trashURL)
        } else {
            guard renameatx_np(AT_FDCWD, temporaryURL.path, AT_FDCWD, destination.path, UInt32(RENAME_EXCL)) == 0 else {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
        }
        guard let published = try LocalFileVersion.read(at: destination),
              published.device == downloaded.device, published.inode == downloaded.inode,
              published.mtime == downloaded.mtime, published.size == downloaded.size else {
            throw DriveError.fileModifiedDuringUpload(path: destination.path)
        }
        return published
    }
}
