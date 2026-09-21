import Foundation

enum LocalDeletionSafety {
    static func trashFileIfUnchanged(
        at url: URL,
        expectedDevice: Int64?,
        expectedInode: Int64?,
        expectedMtime: Int64?,
        expectedSize: Int64?,
        expectedSHA256: String?,
        trash: (URL, AutoreleasingUnsafeMutablePointer<NSURL?>?) throws -> Void = {
            try FileManager.default.trashItem(at: $0, resultingItemURL: $1)
        }
    ) throws -> Bool {
        guard let expectedDevice, let expectedInode, let expectedMtime,
              let expectedSize, let expectedSHA256 else { return false }
        guard let current = try LocalFileVersion.read(at: url) else { return true }
        guard current.device == expectedDevice, current.inode == expectedInode,
              current.mtime == expectedMtime, current.size == expectedSize,
              try SyncEngine.computeFileSha256(at: url).sha256Hex
                .caseInsensitiveCompare(expectedSHA256) == .orderedSame else { return false }

        var trashURL: NSURL?
        try trash(url, &trashURL)
        return true
    }
}
