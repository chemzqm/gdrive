import Foundation
import Testing
@testable import GDrive

/// Recursively removes one test root's default download directory, like `rm -rf`.
/// Call after all transfers finish; never remove the shared remotes base directory.
func removeTestDownloadDirectory(remoteRootID: String, sourceLocation: SourceLocation = #_sourceLocation) {
    do {
        let directory = try DownloadStaging.directory(
            base: DriveClient.defaultDownloadTemporaryDirectory,
            remoteRootID: remoteRootID, excluding: [])
        do {
            try FileManager.default.removeItem(at: directory)
        } catch CocoaError.fileNoSuchFile {
            // An absent directory is already clean, including tests without downloads.
        }
    } catch {
        Issue.record("Failed to remove test download directory for \(remoteRootID): \(error)", sourceLocation: sourceLocation)
    }
}
