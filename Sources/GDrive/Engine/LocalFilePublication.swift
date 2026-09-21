import Darwin
import Foundation

enum LocalFilePublication {
    /// Writes source bytes through the destination path so filesystem watchers
    /// observe a content change. The caller retains ownership of `source`.
    static func publish(
        _ source: URL,
        to destination: URL,
        expected: LocalFileVersion?,
        expectedSHA256: String
    ) throws -> LocalFileVersion {
        guard let sourceVersion = try LocalFileVersion.read(at: source),
              try LocalFileVersion.read(at: destination) == expected else {
            throw DriveError.fileModifiedDuringUpload(path: destination.path)
        }

        let descriptor = try openDestination(destination, expected: expected)
        try writeContents(of: source, version: sourceVersion, to: descriptor)
        try sourceVersion.validate(at: source)

        guard let beforeHash = try LocalFileVersion.read(at: destination) else {
            throw DriveError.fileModifiedDuringUpload(path: destination.path)
        }
        let digest = try SyncEngine.computeFileSha256(at: destination)
        guard digest.sha256Hex.caseInsensitiveCompare(expectedSHA256) == .orderedSame,
              digest.fileSize == sourceVersion.size,
              try LocalFileVersion.read(at: destination) == beforeHash else {
            throw DriveError.fileModifiedDuringUpload(path: destination.path)
        }
        return beforeHash
    }

    private static func openDestination(
        _ destination: URL, expected: LocalFileVersion?
    ) throws -> Int32 {
        guard let expected else {
            let descriptor = open(
                destination.path,
                O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                mode_t(0o666))
            guard descriptor >= 0 else { throw posixError() }
            return descriptor
        }
        let descriptor = open(destination.path, O_RDWR | O_NOFOLLOW | O_CLOEXEC)
        guard descriptor >= 0 else { throw posixError() }
        do {
            guard try LocalFileVersion.read(fileDescriptor: descriptor) == expected,
                  try LocalFileVersion.read(at: destination) == expected else {
                throw DriveError.fileModifiedDuringUpload(path: destination.path)
            }
            return descriptor
        } catch {
            close(descriptor)
            throw error
        }
    }

    private static func writeContents(
        of source: URL, version: LocalFileVersion, to descriptor: Int32
    ) throws {
        defer { close(descriptor) }
        let input = open(source.path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC)
        guard input >= 0 else { throw posixError() }
        defer { close(input) }
        guard try LocalFileVersion.read(fileDescriptor: input) == version else {
            throw DriveError.fileModifiedDuringUpload(path: source.path)
        }
        guard ftruncate(descriptor, 0) == 0 else { throw posixError() }
        guard fcopyfile(input, descriptor, nil, copyfile_flags_t(COPYFILE_DATA)) == 0 else {
            throw posixError()
        }
        guard fsync(descriptor) == 0 else { throw posixError() }
        guard try LocalFileVersion.read(fileDescriptor: input) == version else {
            throw DriveError.fileModifiedDuringUpload(path: source.path)
        }
    }

    private static func posixError() -> POSIXError {
        POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
}
