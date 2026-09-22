import Darwin
import Foundation

enum LocalFilePublication {
    private static let atomicRenameThreshold: Int64 = 8 * 1024 * 1024

    enum WriteStage: CaseIterable, Sendable, Equatable {
        case afterTruncate
        case duringCopy
        case fsync
    }

    enum RenameStage: Sendable {
        case beforeSwap
        case afterSwap
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
        expectedLocalSHA256: String? = nil,
        renameHook: (@Sendable (RenameStage) throws -> Void)? = nil,
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
                source, to: destination, expected: expected,
                expectedLocalSHA256: expectedLocalSHA256, hook: renameHook)
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
        expected: LocalFileVersion?,
        expectedLocalSHA256: String?,
        hook: (@Sendable (RenameStage) throws -> Void)?
    ) throws -> Result {
        if let expected {
            return try replaceByRename(source, destination: destination, expected: expected,
                                       expectedLocalSHA256: expectedLocalSHA256, hook: hook)
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

    private static func replaceByRename(
        _ source: URL, destination: URL, expected: LocalFileVersion,
        expectedLocalSHA256: String?, hook: (@Sendable (RenameStage) throws -> Void)?
    ) throws -> Result {
        let localSHA256: String
        if let expectedLocalSHA256 {
            localSHA256 = expectedLocalSHA256
        } else {
            let digest = try StableLocalFileDigest.capture(at: destination)
            guard digest.version == expected else { return .destinationChanged }
            localSHA256 = digest.sha256Hex
        }
        try hook?(.beforeSwap)
        guard renameatx_np(
            AT_FDCWD, source.path, AT_FDCWD, destination.path, UInt32(RENAME_SWAP)) == 0
        else {
            if errno == ENOENT { return .destinationChanged }
            throw posixError()
        }
        let displacedSHA256: String
        do {
            try hook?(.afterSwap)
            displacedSHA256 = try SyncEngine.computeFileSha256(at: source).sha256Hex
        } catch {
            try rollback(source, destination: destination)
            throw error
        }
        guard displacedSHA256.caseInsensitiveCompare(localSHA256) == .orderedSame else {
            try rollback(source, destination: destination)
            return .destinationChanged
        }
        try FileManager.default.removeItem(at: source)
        guard let published = try LocalFileVersion.read(at: destination) else {
            throw SyncEngineError.localFilePublicationFailed(path: destination.path)
        }
        return .published(published)
    }

    /// A failed rollback remains a failed publication even if the recovery attempt succeeds.
    /// Callers retain their existing temporary-file cleanup policy on errors.
    private static func rollback(
        _ source: URL, destination: URL
    ) throws {
        do {
            guard renameatx_np(
                AT_FDCWD, source.path, AT_FDCWD, destination.path, UInt32(RENAME_SWAP)) == 0
            else { throw posixError() }
        } catch {
            let rollbackError = error
            var recoveryError: (any Error)?
            do {
                if renameatx_np(
                    AT_FDCWD, source.path, AT_FDCWD, destination.path, UInt32(RENAME_SWAP)) != 0 {
                    // If the destination vanished, restore without overwriting a new arrival.
                    guard errno == ENOENT else { throw posixError() }
                    guard renameatx_np(
                        AT_FDCWD, source.path, AT_FDCWD, destination.path, UInt32(RENAME_EXCL)) == 0
                    else { throw posixError() }
                }
            } catch {
                recoveryError = error
            }
            throw SyncEngineError.general(
                "Download publication rollback failed at \(destination.path): \(rollbackError). "
                    + (recoveryError.map { "Local restore also failed: \($0)" } ?? "Local file restored."))
        }
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
