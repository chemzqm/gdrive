import Foundation
import Testing
@testable import GDrive

private struct RemoteTestBodyAndCleanupError: Error, CustomStringConvertible {
    let remoteID: String
    let bodyError: Error
    let cleanupError: Error

    var description: String {
        "Test failed and remote cleanup also failed for \(remoteID). " +
            "Test error: \(bodyError). Cleanup error: \(cleanupError)"
    }
}

/// Runs a live Drive test body and synchronously trashes its allocated remote directory afterward.
/// The ID may be registered before creation, so a lost create response is also recoverable.
func withRemoteTestDirectoryCleanup<T>(
    client: DriveClient,
    remoteID: String,
    operation: () async throws -> T
) async throws -> T {
    let result: Result<T, Error>
    do {
        result = .success(try await operation())
    } catch {
        result = .failure(error)
    }

    do {
        try await removeTestRemoteDirectory(client: client, remoteID: remoteID)
    } catch {
        if case .failure(let bodyError) = result {
            throw RemoteTestBodyAndCleanupError(
                remoteID: remoteID, bodyError: bodyError, cleanupError: error)
        }
        throw error
    }
    return try result.get()
}

func removeTestRemoteDirectory(client: DriveClient, remoteID: String) async throws {
    do {
        try await client.trash(remoteId: remoteID)
    } catch DriveError.notFound {
        // The create request may have failed before Drive created the allocated ID.
    } catch DriveError.serverError(let statusCode, _) where statusCode == 404 {
        // `trash` reports an absent Drive item as a generic HTTP error.
    }
}

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
