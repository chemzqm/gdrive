import Foundation

/// Per-sync-invocation soft cancellation.  It owns only request tasks so the
/// surrounding scan, metadata and receipt work can drain normally.
final class SyncRunControl: Sendable {
    @TaskLocal static var current: SyncRunControl?

    private let transfers = TaskLifecycle()

    var isCancelled: Bool { transfers.isCancelled }

    func cancel() {
        transfers.cancelAll()
    }

    func checkCancellation() throws {
        if isCancelled { throw CancellationError() }
    }

    static func withTransfer<Success: Sendable>(
        _ operation: @escaping @Sendable () async throws -> Success
    ) async throws -> Success {
        guard let control = current else { return try await operation() }
        try control.checkCancellation()
        let task = control.transfers.startThrowing(operation)
        return try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {
            task.cancel()
        }
    }
}
