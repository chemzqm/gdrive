import Foundation
import Testing
@testable import GDrive

@Suite("Download staging configuration")
struct DownloadStagingTests {
    @Test("Sync cleanup removes empty directories but preserves recovery files and subdirectories")
    func emptyDirectoryCleanup() throws {
        let base = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: base) }
        let staging = base.appendingPathComponent("remote-root")
        try FileManager.default.createDirectory(at: staging, withIntermediateDirectories: true)
        let recovery = staging.appendingPathComponent(".tmp_recovery")
        let data = Data("preserve recovery".utf8)
        try data.write(to: recovery)
        try DownloadStaging.removeIfEmpty(staging)
        #expect(try Data(contentsOf: recovery) == data)
        try FileManager.default.removeItem(at: recovery)
        let child = staging.appendingPathComponent("child")
        try FileManager.default.createDirectory(at: child, withIntermediateDirectories: false)
        try DownloadStaging.removeIfEmpty(staging)
        #expect(FileManager.default.fileExists(atPath: child.path))
        try FileManager.default.removeItem(at: child)
        try DownloadStaging.removeIfEmpty(staging)
        #expect(!FileManager.default.fileExists(atPath: staging.path))
        #expect(FileManager.default.fileExists(atPath: base.path))
        try DownloadStaging.removeIfEmpty(staging)
    }

    @Test("Test cleanup recursively removes nonempty staging and tolerates missing directories")
    func recursiveCleanup() throws {
        let rootID = "cleanup-test-\(UUID().uuidString)"
        let directory = DriveClient.defaultDownloadTemporaryDirectory.appendingPathComponent(rootID)
        defer { removeTestDownloadDirectory(remoteRootID: rootID) }
        let nested = directory.appendingPathComponent("nested/child")
        try FileManager.default.createDirectory(at: nested, withIntermediateDirectories: true)
        try Data("unfinished download".utf8).write(to: nested.appendingPathComponent(".tmp_partial"))
        removeTestDownloadDirectory(remoteRootID: rootID)
        #expect(!FileManager.default.fileExists(atPath: directory.path))
        removeTestDownloadDirectory(remoteRootID: rootID)
    }

    @Test("Default directory and remote roots have separate staging paths")
    func roots() throws {
        let expected = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".gdrive/remotes")
        #expect(DriveClient.defaultDownloadTemporaryDirectory.path == expected.path)
        let base = FileManager.default.temporaryDirectory.appendingPathComponent("staging-\(UUID().uuidString)")
        let first = try DownloadStaging.directory(base: base, remoteRootID: "remote-root-A", excluding: [])
        let second = try DownloadStaging.directory(base: base, remoteRootID: "remote-root-B", excluding: [])
        #expect(first.lastPathComponent == "remote-root-A")
        #expect(second.lastPathComponent == "remote-root-B")
        #expect(first != second)
        #expect(!FileManager.default.fileExists(atPath: base.path))
    }

    @Test("Reject traversal IDs", arguments: ["", ".", "..", "../root", "a/b", "bad\0id"])
    func invalidID(_ id: String) {
        #expect(throws: (any Error).self) {
            try DownloadStaging.directory(base: FileManager.default.temporaryDirectory, remoteRootID: id, excluding: [])
        }
    }

    @Test("Reject staging inside either sync root, including missing descendants through a symlink")
    func overlappingRoots() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("staging-roots-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let root = directory.appendingPathComponent("sync")
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let alias = directory.appendingPathComponent("alias")
        try FileManager.default.createSymbolicLink(at: alias, withDestinationURL: root)
        for base in [root, root.appendingPathComponent("missing/child"), alias.appendingPathComponent("missing/child")] {
            #expect(throws: (any Error).self) {
                try DownloadStaging.directory(base: base, remoteRootID: "remote", excluding: [directory.appendingPathComponent("other"), root])
            }
        }
        let sibling = try DownloadStaging.directory(base: directory.appendingPathComponent("sync-other"), remoteRootID: "remote", excluding: [root])
        #expect(sibling.lastPathComponent == "remote")
        #expect(!FileManager.default.fileExists(atPath: root.appendingPathComponent("missing").path))
    }
}
