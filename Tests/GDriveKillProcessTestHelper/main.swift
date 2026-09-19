import Foundation
import GDrive

private enum HelperError: Error {
    case invalidArguments
    case missingFixture
    case invalidMode(String)
}

@main
struct GDriveKillProcessTestHelper {
    static func main() async throws {
        guard CommandLine.arguments.count >= 3 else {
            throw HelperError.invalidArguments
        }

        let command = CommandLine.arguments[1]
        let databasePath = CommandLine.arguments[2]

        switch command {
        case "initialize":
            try await initialize(databasePath: databasePath)
            acknowledge("INITIALIZED")
        case "commit-and-wait":
            guard CommandLine.arguments.count == 5 else {
                throw HelperError.invalidArguments
            }
            try await commitAndWait(
                databasePath: databasePath,
                operationID: CommandLine.arguments[3],
                mode: CommandLine.arguments[4]
            )
        case "uncommitted-and-wait":
            guard CommandLine.arguments.count == 4 else {
                throw HelperError.invalidArguments
            }
            try uncommittedAndWait(
                databasePath: databasePath,
                operationID: CommandLine.arguments[3]
            )
        default:
            throw HelperError.invalidMode(command)
        }
    }

    private static func initialize(databasePath: String) async throws {
        let store = try await StateStore(path: databasePath)
        try await store.write { connection in
            try connection.execute("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES (
                'kill-process-test', '/kill-process-test', 1, 1,
                'kill-process-remote-root', 'localToRemoteEmpty', 'freshCreated', 1, 1
            );
            """)
            try connection.execute("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id,
                local_status, remote_status, phase, created_at, updated_at
            ) VALUES (
                last_insert_rowid(), NULL, 'root', 'directory', 'kill-process-remote-root',
                'present', 'present', 'ready', 1, 1
            );
            """)
        }
        // Establish a quiet baseline. The crash cases deliberately do not checkpoint.
        try await store.checkpoint()
    }

    private static func commitAndWait(
        databasePath: String,
        operationID: String,
        mode: String
    ) async throws {
        let store = try await StateStore(path: databasePath)
        let write: @Sendable (SQLiteConnection) throws -> Void = { connection in
            try insertOperation(connection: connection, operationID: operationID)
        }

        switch mode {
        case "immediate":
            try await store.write(write)
        case "batch":
            try await store.batchWrite(write)
        default:
            throw HelperError.invalidMode(mode)
        }

        acknowledge("COMMITTED \(operationID)")
        await waitToBeKilled()
    }

    private static func uncommittedAndWait(
        databasePath: String,
        operationID: String
    ) throws {
        let connection = try SQLiteConnection(path: databasePath)
        try connection.execute("BEGIN IMMEDIATE TRANSACTION;")
        try insertOperation(connection: connection, operationID: operationID)
        acknowledge("UNCOMMITTED \(operationID)")
        waitToBeKilledSynchronously()
    }

    private static func insertOperation(
        connection: SQLiteConnection,
        operationID: String
    ) throws {
        let fixture = try connection.prepare("""
        SELECT roots.root_id, items.item_id
        FROM roots
        JOIN items ON items.root_id = roots.root_id
        WHERE roots.account_id = 'kill-process-test' AND items.parent_id IS NULL;
        """)
        guard try fixture.step(),
              let rootID = fixture.columnInt64(at: 0),
              let itemID = fixture.columnInt64(at: 1) else {
            throw HelperError.missingFixture
        }

        let operation = try connection.prepare("""
        INSERT INTO operations (
            operation_id, root_id, item_id, operation_type, state,
            target_remote_id, created_at, updated_at
        ) VALUES (?, ?, ?, 'uploadMultipart', 'inFlight', ?, 1, 1);
        """)
        operation.bindText(operationID, at: 1)
        operation.bindInt64(rootID, at: 2)
        operation.bindInt64(itemID, at: 3)
        operation.bindText("remote-\(operationID)", at: 4)
        _ = try operation.step()
    }

    private static func acknowledge(_ message: String) {
        FileHandle.standardOutput.write(Data("\(message)\n".utf8))
    }

    private static func waitToBeKilled() async {
        while true {
            try? await Task.sleep(for: .seconds(60))
        }
    }

    private static func waitToBeKilledSynchronously() -> Never {
        while true {
            Thread.sleep(forTimeInterval: 60)
        }
    }
}
