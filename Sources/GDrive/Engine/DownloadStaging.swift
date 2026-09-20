import Darwin
import Foundation

enum DownloadStaging {
    /// Atomically removes only an empty directory; never removes recovery files.
    static func removeIfEmpty(_ directory: URL) throws {
        guard directory.withUnsafeFileSystemRepresentation({ path in
            guard let path else { return false }
            return rmdir(path) == 0
        }) else {
            let code = errno
            if code == ENOENT || code == ENOTEMPTY || code == EEXIST { return }
            throw POSIXError(POSIXErrorCode(rawValue: code) ?? .EIO)
        }
    }

    private static func resolved(_ url: URL) -> URL {
        var ancestor = url.standardizedFileURL
        var missing: [String] = []
        while ancestor.path != "/", !FileManager.default.fileExists(atPath: ancestor.path) {
            missing.append(ancestor.lastPathComponent)
            ancestor.deleteLastPathComponent()
        }
        var result = ancestor.resolvingSymlinksInPath().standardizedFileURL
        for name in missing.reversed() { result.appendPathComponent(name) }
        return result
    }

    static func directory(base: URL, remoteRootID: String, excluding roots: [URL]) throws -> URL {
        guard base.isFileURL, !remoteRootID.isEmpty,
              remoteRootID != ".", remoteRootID != "..",
              !remoteRootID.contains("/"), !remoteRootID.contains("\0") else {
            throw SyncEngineError.general("Invalid download temporary directory or remote root ID")
        }
        let directory = base.appendingPathComponent(remoteRootID, isDirectory: true)
        let resolvedDirectory = resolved(directory)
        for root in roots {
            let path = resolved(root).path
            let prefix = path.hasSuffix("/") ? path : path + "/"
            guard resolvedDirectory.path != path, !resolvedDirectory.path.hasPrefix(prefix) else {
                throw SyncEngineError.general("Download temp directory cannot be in sync directory: \(directory.path)")
            }
        }
        return resolvedDirectory
    }

    /// Atomic publication requires both directories to be on the same filesystem.
    static func prepare(_ directory: URL, destination: URL) throws {
        guard directory.isFileURL else { throw SyncEngineError.general("Download temporary directory must be local file path") }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        var staging = stat()
        var target = stat()
        guard stat(directory.path, &staging) == 0,
              stat(destination.deletingLastPathComponent().path, &target) == 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        guard staging.st_dev == target.st_dev else {
            throw SyncEngineError.general("The download temporary directory must be on the same file system as the target, please configure downloadTemporaryDirectory: \(directory.path)")
        }
    }
}

extension SyncEngine {
    func cleanupDownloadStagingDirectory(_ directory: URL) {
        do {
            try DownloadStaging.removeIfEmpty(directory)
        } catch {
            logger.warning("Failed to remove empty download staging directory \(directory.path): \(error)")
        }
    }

    func downloadStagingDirectory(remoteRootID: String, localRoot: URL) async throws -> URL {
        let roots = try await store.read { conn in
            let query = try conn.cachedStatement("SELECT local_root_path FROM roots WHERE is_active = 1;")
            defer { query.reset() }
            var roots = [localRoot]
            while try query.step() {
                if let path = query.columnText(at: 0) {
                    roots.append(URL(fileURLWithPath: (path as NSString).expandingTildeInPath))
                }
            }
            return roots
        }
        return try DownloadStaging.directory(base: downloadTemporaryDirectory,
            remoteRootID: remoteRootID, excluding: roots)
    }
}
