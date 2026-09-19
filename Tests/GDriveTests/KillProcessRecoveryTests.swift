import Darwin
import Foundation
import Testing
@testable import GDrive

@Suite("SQLite kill-process recovery", .serialized)
struct KillProcessRecoveryTests {
    private static let helperName = "GDriveKillProcessTestHelper"

    @Test("Acknowledged commits survive SIGKILL", arguments: ["immediate", "batch"])
    func acknowledgedCommitsSurviveSIGKILL(mode: String) throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("gdrive-kill-process-\(mode)-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let databasePath = directory.appendingPathComponent("state.sqlite").path
        let helperURL = try Self.helperURL()
        try Self.runToCompletion(helperURL, arguments: ["initialize", databasePath])

        var acknowledged: [String] = []
        for index in 0..<10 {
            let operationID = "\(mode)-\(index)-\(UUID().uuidString)"
            let process = try Self.startHelper(
                helperURL,
                arguments: ["commit-and-wait", databasePath, operationID, mode]
            )
            let output = try Self.readAcknowledgement(from: process)
            #expect(output == "COMMITTED \(operationID)\n")
            try Self.killAndWait(process)
            acknowledged.append(operationID)

            let walAttributes = try FileManager.default.attributesOfItem(atPath: "\(databasePath)-wal")
            let walSize = try #require((walAttributes[.size] as? NSNumber)?.int64Value)
            #expect(walSize > 32)

            try Self.verify(databasePath: databasePath, committed: acknowledged)
        }
    }

    @Test("An uncommitted transaction is rolled back after SIGKILL")
    func uncommittedTransactionIsRolledBack() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("gdrive-kill-process-uncommitted-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let databasePath = directory.appendingPathComponent("state.sqlite").path
        let helperURL = try Self.helperURL()
        try Self.runToCompletion(helperURL, arguments: ["initialize", databasePath])

        let operationID = "uncommitted-\(UUID().uuidString)"
        let process = try Self.startHelper(
            helperURL,
            arguments: ["uncommitted-and-wait", databasePath, operationID]
        )
        let output = try Self.readAcknowledgement(from: process)
        #expect(output == "UNCOMMITTED \(operationID)\n")
        try Self.killAndWait(process)

        let connection = try SQLiteConnection(path: databasePath, readonly: true)
        #expect(try Self.operationCount(connection: connection, operationID: operationID) == 0)
        try Self.verifyIntegrity(connection)
    }

    private static func helperURL() throws -> URL {
        var startingPoints = [
            Bundle.main.bundleURL,
            Bundle.main.executableURL,
            URL(fileURLWithPath: CommandLine.arguments[0])
        ].compactMap { $0 }
        for index in 0..<_dyld_image_count() {
            if let name = _dyld_get_image_name(index) {
                startingPoints.append(URL(fileURLWithPath: String(cString: name)))
            }
        }

        for startingPoint in startingPoints {
            var directory = startingPoint.hasDirectoryPath
                ? startingPoint
                : startingPoint.deletingLastPathComponent()
            for _ in 0..<6 {
                let candidate = directory.appendingPathComponent(helperName)
                if FileManager.default.isExecutableFile(atPath: candidate.path) {
                    return candidate
                }
                directory.deleteLastPathComponent()
            }
        }
        throw CocoaError(.fileNoSuchFile, userInfo: [
            NSFilePathErrorKey: "Could not locate \(helperName) beside a loaded test image"
        ])
    }

    private static func startHelper(_ helperURL: URL, arguments: [String]) throws -> Process {
        let process = Process()
        process.executableURL = helperURL
        process.arguments = arguments
        process.standardOutput = Pipe()
        process.standardError = Pipe()
        try process.run()
        return process
    }

    private static func runToCompletion(_ helperURL: URL, arguments: [String]) throws {
        let process = try startHelper(helperURL, arguments: arguments)
        process.waitUntilExit()
        let stderr = (process.standardError as? Pipe)?.fileHandleForReading.readDataToEndOfFile() ?? Data()
        #expect(
            process.terminationReason == .exit && process.terminationStatus == 0,
            "Helper failed: \(String(decoding: stderr, as: UTF8.self))"
        )
    }

    private static func readAcknowledgement(from process: Process) throws -> String {
        let timeout = DispatchWorkItem {
            if process.isRunning {
                _ = Darwin.kill(process.processIdentifier, SIGKILL)
            }
        }
        DispatchQueue.global().asyncAfter(deadline: .now() + 10, execute: timeout)
        defer { timeout.cancel() }

        let pipe = try #require(process.standardOutput as? Pipe)
        let data = pipe.fileHandleForReading.availableData
        guard !data.isEmpty else {
            process.waitUntilExit()
            let stderr = (process.standardError as? Pipe)?.fileHandleForReading.readDataToEndOfFile() ?? Data()
            throw CocoaError(.fileReadUnknown, userInfo: [
                NSLocalizedDescriptionKey: "Helper exited before acknowledgement: \(String(decoding: stderr, as: UTF8.self))"
            ])
        }
        return String(decoding: data, as: UTF8.self)
    }

    private static func killAndWait(_ process: Process) throws {
        let result = Darwin.kill(process.processIdentifier, SIGKILL)
        #expect(result == 0)
        process.waitUntilExit()
        #expect(process.terminationReason == .uncaughtSignal)
        #expect(process.terminationStatus == SIGKILL)
    }

    private static func verify(databasePath: String, committed operationIDs: [String]) throws {
        let connection = try SQLiteConnection(path: databasePath, readonly: true)
        for operationID in operationIDs {
            #expect(try operationCount(connection: connection, operationID: operationID) == 1)
        }
        try verifyIntegrity(connection)
    }

    private static func operationCount(
        connection: SQLiteConnection,
        operationID: String
    ) throws -> Int64 {
        let statement = try connection.prepare("SELECT count(*) FROM operations WHERE operation_id = ?;")
        statement.bindText(operationID, at: 1)
        _ = try statement.step()
        return statement.columnInt64(at: 0) ?? -1
    }

    private static func verifyIntegrity(_ connection: SQLiteConnection) throws {
        let quickCheck = try connection.prepare("PRAGMA quick_check;")
        #expect(try quickCheck.step())
        #expect(quickCheck.columnText(at: 0) == "ok")

        let foreignKeyCheck = try connection.prepare("PRAGMA foreign_key_check;")
        #expect(try foreignKeyCheck.step() == false)
    }
}
