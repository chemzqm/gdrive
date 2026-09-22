import Foundation
import Testing
@testable import GDrive

@Suite("Local path operation safety")
struct PathOperationTests {
    @Test("A planned move does not move a replacement source")
    func replacementSourceIsRejected() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "path-operation-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let source = directory.appendingPathComponent("source.txt")
        let displaced = directory.appendingPathComponent("displaced.txt")
        let destination = directory.appendingPathComponent("destination.txt")
        try Data("original".utf8).write(to: source)
        let expected = try #require(try LocalFileVersion.read(at: source))
        let operation = LocalPathOperation.move(
            source: source, destination: destination,
            kind: .file,
            device: expected.device, inode: expected.inode)

        try FileManager.default.moveItem(at: source, to: displaced)
        try Data("replacement".utf8).write(to: source)

        #expect(try operation.execute() == false)
        #expect(try Data(contentsOf: source) == Data("replacement".utf8))
        #expect(!FileManager.default.fileExists(atPath: destination.path))
    }

    @Test("A symlink cannot stand in for the planned source identity")
    func sourceSymlinkIsRejectedWithoutFollowingIt() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "symlink-path-operation-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let source = directory.appendingPathComponent("source.txt")
        let displaced = directory.appendingPathComponent("displaced.txt")
        let destination = directory.appendingPathComponent("destination.txt")
        try Data("original".utf8).write(to: source)
        let expected = try #require(try LocalFileVersion.read(at: source))
        let operation = LocalPathOperation.move(
            source: source, destination: destination, kind: .file,
            device: expected.device, inode: expected.inode)

        try FileManager.default.moveItem(at: source, to: displaced)
        try FileManager.default.createSymbolicLink(
            at: source, withDestinationURL: displaced)

        #expect(try operation.execute() == false)
        #expect(try FileManager.default.destinationOfSymbolicLink(atPath: source.path)
            == displaced.path)
        #expect(!FileManager.default.fileExists(atPath: destination.path))
    }

    @Test("A completed move is recognized by destination identity on replay")
    func completedMoveIsReplayable() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "replay-path-operation-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let source = directory.appendingPathComponent("source.txt")
        let destination = directory.appendingPathComponent("nested/destination.txt")
        try Data("original".utf8).write(to: source)
        let expected = try #require(try LocalFileVersion.read(at: source))
        let operation = LocalPathOperation.move(
            source: source, destination: destination, kind: .file,
            device: expected.device, inode: expected.inode)

        #expect(try operation.execute())
        #expect(try operation.execute())
        #expect(try LocalFileVersion.read(at: destination)?.inode == expected.inode)
    }

    @Test("A source replacement after validation is rolled back")
    func replacementAfterValidationIsRolledBack() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "path-operation-race-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let source = directory.appendingPathComponent("source.txt")
        let displaced = directory.appendingPathComponent("displaced.txt")
        let replacement = directory.appendingPathComponent("replacement.txt")
        let destination = directory.appendingPathComponent("nested/destination.txt")
        try Data("original".utf8).write(to: source)
        try Data("replacement".utf8).write(to: replacement)
        let expected = try #require(try LocalFileVersion.read(at: source))
        let operation = LocalPathOperation.move(
            source: source, destination: destination, kind: .file,
            device: expected.device, inode: expected.inode)

        let moved = try operation.execute {
            try FileManager.default.moveItem(at: source, to: displaced)
            try FileManager.default.moveItem(at: replacement, to: source)
        }

        #expect(moved == false)
        #expect(try Data(contentsOf: source) == Data("replacement".utf8))
        #expect(try Data(contentsOf: displaced) == Data("original".utf8))
        #expect(!FileManager.default.fileExists(atPath: destination.path))
    }

    @Test("A move with neither its source nor its replay destination stays pending")
    func missingSourceAndDestinationIsRejected() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "missing-path-operation-\(UUID().uuidString)")
        let operation = LocalPathOperation.move(
            source: directory.appendingPathComponent("source.txt"),
            destination: directory.appendingPathComponent("destination.txt"),
            kind: .file,
            device: 1, inode: 2)

        #expect(try operation.execute() == false)
    }
}
