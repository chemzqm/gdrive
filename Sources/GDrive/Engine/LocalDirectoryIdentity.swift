import Darwin
import Foundation

struct LocalDirectoryIdentity: Sendable, Equatable {
    let device: Int64
    let inode: Int64

    static func read(at url: URL) throws -> LocalDirectoryIdentity? {
        var value = stat()
        guard lstat(url.path, &value) == 0 else {
            if errno == ENOENT { return nil }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        guard value.st_mode & S_IFMT == S_IFDIR else { return nil }
        return LocalDirectoryIdentity(device: Int64(value.st_dev), inode: Int64(value.st_ino))
    }

    static func require(
        at url: URL,
        or error: @autoclosure () -> any Error
    ) throws -> LocalDirectoryIdentity {
        guard let identity = try read(at: url) else { throw error() }
        return identity
    }
}
