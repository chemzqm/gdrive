import Darwin
import Foundation

/// Cheap identity/version evidence. Checks metadata only; SHA-256 still decides
/// whether content needs a transfer.
struct LocalFileVersion: Sendable, Equatable {
    let device: Int64
    let inode: Int64
    let size: Int64
    let mtime: Int64
    let ctime: Int64

    static func read(at url: URL) throws -> LocalFileVersion? {
        var value = stat()
        guard lstat(url.path, &value) == 0 else {
            if errno == ENOENT { return nil }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        return try fromStat(value)
    }

    static func read(fileDescriptor: Int32) throws -> LocalFileVersion {
        var value = stat()
        guard fstat(fileDescriptor, &value) == 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        return try fromStat(value)
    }

    private static func fromStat(_ value: stat) throws -> LocalFileVersion {
        guard value.st_mode & S_IFMT == S_IFREG else {
            throw CocoaError(.fileReadUnsupportedScheme)
        }
        return LocalFileVersion(
            device: Int64(value.st_dev), inode: Int64(value.st_ino), size: value.st_size,
            mtime: Int64(value.st_mtimespec.tv_sec) * 1_000_000_000 + Int64(value.st_mtimespec.tv_nsec),
            ctime: Int64(value.st_ctimespec.tv_sec) * 1_000_000_000 + Int64(value.st_ctimespec.tv_nsec)
        )
    }

    func validate(at url: URL) throws {
        guard try Self.read(at: url) == self else {
            throw SyncEngineError.localFileModified(path: url.path)
        }
    }
}

struct StableLocalFileDigest: Sendable, Equatable {
    let sha256Hex: String
    let fileSize: Int64
    let version: LocalFileVersion

    static func capture(
        at url: URL,
        afterHash: @Sendable () throws -> Void = {}
    ) throws -> StableLocalFileDigest {
        guard let version = try LocalFileVersion.read(at: url) else {
            throw CocoaError(.fileNoSuchFile)
        }
        let digest = try SyncEngine.computeFileSha256(at: url)
        try afterHash()
        guard digest.fileSize == version.size else {
            throw SyncEngineError.localFileModified(path: url.path)
        }
        try version.validate(at: url)
        return StableLocalFileDigest(
            sha256Hex: digest.sha256Hex,
            fileSize: digest.fileSize,
            version: version)
    }
}

/// A transfer owns either immutable bytes or a copy-on-write filesystem clone.
/// The clone path is never the mutable user path, and is removed on all exits.
final class StableUploadInput: Sendable {
    let sourceURL: URL
    let version: LocalFileVersion
    let data: Data?
    let fileURL: URL
    let sha256: String
    let size: Int64
    private let temporaryDirectory: URL?

    private init(sourceURL: URL, version: LocalFileVersion, data: Data?, fileURL: URL,
                 sha256: String, size: Int64, temporaryDirectory: URL?) {
        self.sourceURL = sourceURL
        self.version = version
        self.data = data
        self.fileURL = fileURL
        self.sha256 = sha256
        self.size = size
        self.temporaryDirectory = temporaryDirectory
    }

    static func capture(at url: URL) throws -> StableUploadInput {
        guard let version = try LocalFileVersion.read(at: url) else {
            throw CocoaError(.fileNoSuchFile)
        }
        if version.size <= 8 * 1024 * 1024 {
            let data = try Data(contentsOf: url)
            try version.validate(at: url)
            return StableUploadInput(sourceURL: url, version: version, data: data, fileURL: url,
                                     sha256: SyncEngine.computeSha256(of: data), size: Int64(data.count), temporaryDirectory: nil)
        }

        let directory = try FileManager.default.url(for: .itemReplacementDirectory, in: .userDomainMask,
                                                     appropriateFor: url, create: true)
        let clone = directory.appendingPathComponent("upload")
        do {
            // No full-copy fallback: that would add an entire disk read/write
            // before each large upload. Unsupported filesystems fail explicitly.
            guard clonefile(url.path, clone.path, UInt32(CLONE_NOFOLLOW)) == 0 else {
                if errno == ENOTSUP || errno == EXDEV {
                    throw DriveError.stableInputUnavailable(path: url.path)
                }
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            try version.validate(at: url)
            let digest = try SyncEngine.computeFileSha256(at: clone)
            return StableUploadInput(sourceURL: url, version: version, data: nil, fileURL: clone,
                                     sha256: digest.sha256Hex, size: digest.fileSize, temporaryDirectory: directory)
        } catch {
            try? FileManager.default.removeItem(at: directory)
            throw error
        }
    }

    deinit {
        if let temporaryDirectory { try? FileManager.default.removeItem(at: temporaryDirectory) }
    }
}
