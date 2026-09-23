import Darwin
import Foundation

enum LocalDeletionSafety {
    enum Result {
        case missing
        case missingEvidence
        case trashed
        case trashedWithoutURL
        case changed(StableLocalFileDigest)
        case restoreFailed(trashURL: URL, reason: String)
    }

    static func trashFileIfUnchanged(
        at url: URL,
        expectedDevice: Int64?,
        expectedInode: Int64?,
        expectedSize: Int64?,
        expectedSHA256: String?,
        trash: (URL, AutoreleasingUnsafeMutablePointer<NSURL?>?) throws -> Void = {
            try FileManager.default.trashItem(at: $0, resultingItemURL: $1)
        }
    ) throws -> Result {
        guard let expectedDevice, let expectedInode,
              let expectedSize, let expectedSHA256 else { return .missingEvidence }
        guard try LocalFileVersion.read(at: url) != nil else { return .missing }
        let observed = try StableLocalFileDigest.capture(at: url)
        guard observed.version.device == expectedDevice,
              observed.version.inode == expectedInode,
              observed.fileSize == expectedSize,
              observed.sha256Hex.caseInsensitiveCompare(expectedSHA256) == .orderedSame else {
            return .changed(observed)
        }

        var trashURL: NSURL?
        try trash(url, &trashURL)
        guard let trashURL else { return .trashedWithoutURL }
        let movedURL = trashURL as URL
        let moved = try StableLocalFileDigest.capture(at: movedURL)
        if moved.sha256Hex.caseInsensitiveCompare(expectedSHA256) == .orderedSame {
            return .trashed
        }

        try moved.version.validate(at: movedURL)
        guard renameatx_np(
            AT_FDCWD, movedURL.path, AT_FDCWD, url.path, UInt32(RENAME_EXCL)) == 0 else {
            let error = POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            return .restoreFailed(trashURL: movedURL, reason: String(describing: error))
        }
        let restored = try StableLocalFileDigest.capture(at: url)
        return .changed(restored)
    }
}
