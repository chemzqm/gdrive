import Darwin
import Foundation

enum LocalFilePublication {
    private static let atomicRenameThreshold: Int64 = 8 * 1024 * 1024

    enum WriteStage: CaseIterable, Sendable, Equatable {
        case afterTruncate
        case duringCopy
        case fsync
    }

    struct Failure: Sendable, Equatable {
        let destinationPath: String
        let reason: String
    }

    enum Result: Sendable, Equatable {
        case published(LocalFileVersion)
        case destinationChanged
        case failedAfterWriteStarted(Failure)
    }

    /// Small or cross-filesystem sources are written through the destination
    /// path. Large same-filesystem sources use atomic namespace publication.
    static func publish(
        _ source: URL,
        to destination: URL,
        expected: LocalFileVersion?,
        expectedSHA256: String,
        writeHook: (@Sendable (WriteStage, Int32, Int32) throws -> Void)? = nil
    ) throws -> Result {
        guard let sourceVersion = try LocalFileVersion.read(at: source) else {
            throw CocoaError(.fileNoSuchFile)
        }
        guard try LocalFileVersion.read(at: destination) == expected else {
            return .destinationChanged
        }

        if sourceVersion.size > atomicRenameThreshold,
           sourceVersion.device == publicationDevice(destination: destination, expected: expected) {
            return try publishByRename(
                source, to: destination, expected: expected)
        }

        guard let descriptor = try openDestination(destination, expected: expected) else {
            return .destinationChanged
        }
        do {
            try writeContents(
                of: source, version: sourceVersion, to: descriptor, writeHook: writeHook)
            guard try LocalFileVersion.read(at: source) == sourceVersion else {
                throw SyncEngineError.localFilePublicationFailed(path: source.path)
            }

            guard let beforeHash = try LocalFileVersion.read(at: destination) else {
                throw SyncEngineError.localFilePublicationFailed(path: destination.path)
            }
            let digest = try SyncEngine.computeFileSha256(at: destination)
            guard digest.sha256Hex.caseInsensitiveCompare(expectedSHA256) == .orderedSame else {
                throw DriveError.checksumMismatch(expected: expectedSHA256, actual: digest.sha256Hex)
            }
            guard digest.fileSize == sourceVersion.size else {
                throw DriveError.sizeMismatch(expected: sourceVersion.size, actual: digest.fileSize)
            }
            guard try LocalFileVersion.read(at: destination) == beforeHash else {
                throw SyncEngineError.localFilePublicationFailed(path: destination.path)
            }
            return .published(beforeHash)
        } catch {
            return .failedAfterWriteStarted(Failure(
                destinationPath: destination.path,
                reason: String(describing: error)))
        }
    }

    private static func publicationDevice(
        destination: URL, expected: LocalFileVersion?
    ) -> Int64? {
        if let expected { return expected.device }
        var value = stat()
        guard lstat(destination.deletingLastPathComponent().path, &value) == 0 else { return nil }
        return Int64(value.st_dev)
    }

    private static func publishByRename(
        _ source: URL,
        to destination: URL,
        expected: LocalFileVersion?
    ) throws -> Result {
        if expected != nil {
            guard renameatx_np(
                AT_FDCWD, source.path, AT_FDCWD, destination.path, UInt32(RENAME_SWAP)) == 0
            else {
                if errno == ENOENT { return .destinationChanged }
                throw posixError()
            }
            try FileManager.default.removeItem(at: source)
            guard let published = try LocalFileVersion.read(at: destination) else {
                throw SyncEngineError.localFilePublicationFailed(path: destination.path)
            }
            return .published(published)
        }

        guard renameatx_np(
            AT_FDCWD, source.path, AT_FDCWD, destination.path, UInt32(RENAME_EXCL)) == 0
        else {
            if errno == EEXIST { return .destinationChanged }
            throw posixError()
        }
        guard let published = try LocalFileVersion.read(at: destination) else {
            throw SyncEngineError.localFilePublicationFailed(path: destination.path)
        }
        return .published(published)
    }

    private static func openDestination(
        _ destination: URL, expected: LocalFileVersion?
    ) throws -> Int32? {
        guard let expected else {
            let descriptor = open(
                destination.path,
                O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                mode_t(0o666))
            guard descriptor >= 0 else {
                if errno == EEXIST { return nil }
                throw posixError()
            }
            return descriptor
        }
        let descriptor = open(destination.path, O_RDWR | O_NOFOLLOW | O_CLOEXEC)
        guard descriptor >= 0 else {
            if errno == ENOENT { return nil }
            throw posixError()
        }
        do {
            guard try LocalFileVersion.read(fileDescriptor: descriptor) == expected,
                  try LocalFileVersion.read(at: destination) == expected else {
                close(descriptor)
                return nil
            }
            return descriptor
        } catch {
            close(descriptor)
            throw error
        }
    }

    private static func writeContents(
        of source: URL,
        version: LocalFileVersion,
        to descriptor: Int32,
        writeHook: (@Sendable (WriteStage, Int32, Int32) throws -> Void)?
    ) throws {
        defer { close(descriptor) }
        let input = open(source.path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC)
        guard input >= 0 else { throw posixError() }
        defer { close(input) }
        guard try LocalFileVersion.read(fileDescriptor: input) == version else {
            throw SyncEngineError.localFilePublicationFailed(path: source.path)
        }
        guard ftruncate(descriptor, 0) == 0 else { throw posixError() }
        try writeHook?(.afterTruncate, input, descriptor)
        try writeHook?(.duringCopy, input, descriptor)
        guard fcopyfile(input, descriptor, nil, copyfile_flags_t(COPYFILE_DATA)) == 0 else {
            throw posixError()
        }
        try writeHook?(.fsync, input, descriptor)
        guard fsync(descriptor) == 0 else { throw posixError() }
        guard try LocalFileVersion.read(fileDescriptor: input) == version else {
            throw SyncEngineError.localFilePublicationFailed(path: source.path)
        }
    }

    private static func posixError() -> POSIXError {
        POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
}
