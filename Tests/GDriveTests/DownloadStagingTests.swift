import Foundation
import Testing
@testable import GDrive

@Suite("Download staging configuration")
struct DownloadStagingTests {
    @Test("Default directory and remote roots have separate staging paths")
    func roots() throws {
        let expected = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".gdrive")
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
