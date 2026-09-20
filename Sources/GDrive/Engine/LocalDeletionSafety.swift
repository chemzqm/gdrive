import Darwin
import Foundation

enum LocalDeletionSafety {
    static func trashDirectoryIfEmpty(
        at url: URL,
        contents: (String) throws -> [String] = FileManager.default.contentsOfDirectory,
        trash: (URL, AutoreleasingUnsafeMutablePointer<NSURL?>?) throws -> Void = {
            try FileManager.default.trashItem(at: $0, resultingItemURL: $1)
        }
    ) throws -> Bool {
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: url.path, isDirectory: &isDirectory) else {
            return true
        }
        guard isDirectory.boolValue, try contents(url.path).isEmpty else { return false }
        var trashURL: NSURL?
        try trash(url, &trashURL)
        return true
    }

    static func trashFileIfUnchanged(
        at url: URL,
        expectedDevice: Int64?,
        expectedInode: Int64?,
        expectedMtime: Int64?,
        expectedSize: Int64?,
        expectedSHA256: String?,
        afterIsolation: (() throws -> Void)? = nil,
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

        let quarantine = url.deletingLastPathComponent().appendingPathComponent(
            ".gdrive-delete-\(UUID().uuidString)")
        guard rename(url.path, quarantine.path) == 0 else {
            if errno == ENOENT { return true }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        do {
            try afterIsolation?()
            let quarantined = try LocalFileVersion.read(at: quarantine)
            let digest = try quarantined.map { _ in
                try SyncEngine.computeFileSha256(at: quarantine)
            }
            let matches = quarantined?.device == expectedDevice
                && quarantined?.inode == expectedInode
                && quarantined?.mtime == expectedMtime
                && quarantined?.size == expectedSize
                && digest?.sha256Hex.caseInsensitiveCompare(expectedSHA256) == .orderedSame
            guard matches else {
                try restore(quarantine: quarantine, original: url)
                return false
            }

            var trashURL: NSURL?
            try trash(quarantine, &trashURL)
            // A writer may have recreated the path after the atomic isolation. Keep that
            // newer object visible and retain the database deletion intent for rescanning.
            return try LocalFileVersion.read(at: url) == nil
        } catch {
            try? restore(quarantine: quarantine, original: url)
            throw error
        }
    }

    private static func restore(quarantine: URL, original: URL) throws {
        guard !FileManager.default.fileExists(atPath: original.path) else { return }
        guard renamex_np(quarantine.path, original.path, UInt32(RENAME_EXCL)) == 0 else {
            if errno == EEXIST { return }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
    }
}
