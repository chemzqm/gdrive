import Foundation
import Testing
@testable import GDrive

@Suite("Directory deletion inventory")
struct DirectoryDeletionInventoryTests {
    @Test("Fresh traversal finds changed and newly arrived files")
    func findsChangedAndNewFiles() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("directory-inventory-\(UUID().uuidString)")
        let nested = directory.appendingPathComponent("nested")
        try FileManager.default.createDirectory(at: nested, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let changed = directory.appendingPathComponent("changed.txt")
        let unchanged = directory.appendingPathComponent("unchanged.txt")
        let arrived = nested.appendingPathComponent("arrived.txt")
        let hidden = nested.appendingPathComponent(".hidden.txt")
        try Data("new value".utf8).write(to: changed)
        try Data("same value".utf8).write(to: unchanged)
        try Data("arrived after the scan".utf8).write(to: arrived)
        try Data("hidden new file".utf8).write(to: hidden)

        let candidates = try await DirectoryDeletionInventory.inspect(
            directory: directory,
            baselines: [
                "changed.txt": SyncEngine.computeSha256(of: Data("old value".utf8)),
                "unchanged.txt": SyncEngine.computeSha256(of: Data("same value".utf8))
            ])

        #expect(candidates.map(\.relativePath).sorted() == [
            "changed.txt", "nested/.hidden.txt", "nested/arrived.txt"
        ])
        #expect(FileManager.default.fileExists(atPath: changed.path))
        #expect(FileManager.default.fileExists(atPath: arrived.path))
    }

    @Test("A file that cannot be hashed is ignored and left untouched")
    func ignoresHashFailure() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("directory-inventory-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let unreadable = directory.appendingPathComponent("unreadable.txt")
        try Data("contents".utf8).write(to: unreadable)

        let candidates = try await DirectoryDeletionInventory.inspect(
            directory: directory, baselines: [:], hash: { _ in
                throw CocoaError(.fileReadNoPermission)
            })

        #expect(candidates.isEmpty)
        #expect(FileManager.default.fileExists(atPath: unreadable.path))
    }
}
