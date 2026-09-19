import Foundation

/// Directory Dependency Tracking & Asynchronous Wakeup Scheduler
/// Follow the v1.md §4.1 And §4.3 Specification:
/// - Eliminate full catalog pre-creation waiting
/// - As soon as the parent directory is confirmed at the remote end, all children (files and subdirectories) waiting for the parent directory are immediately broadcasted to wake up.
public actor DirectoryTracker {
    public enum DirectoryTrackerError: Error, LocalizedError, Sendable {
        case parentDirectoryFailed(parentRelPath: String, reason: String)
        case cancelled

        public var errorDescription: String? {
            switch self {
            case .parentDirectoryFailed(let parentRelPath, let reason):
                return "Parent Directory [\(parentRelPath)] Failed to create: \(reason)"
            case .cancelled:
                return "Waiting for parent directory has been canceled"
            }
        }
    }

    private enum State {
        case ready(remoteId: String)
        case failed(error: Error)
    }

    /// Directory Terminal State Mapping Table: Local Relative Path -> Status (Ready or Failed)
    private var states: [String: State] = [:]

    /// Suspended coroutine queue waiting for a parent directory to be ready: local relative path -> (Waiters ID -> Continuations)
    private var waiters: [String: [UInt64: CheckedContinuation<String, Error>]] = [:]
    private var nextWaiterId: UInt64 = 0

    public init(remoteRootId: String) {
        // Root directory (relative path is "")Confirmed ready at startup
        self.states[""] = .ready(remoteId: remoteRootId)
    }

    /// Check that the specified parent directory is ready, and if so, return to it directly remoteId
    public func getReadyParentId(for parentRelPath: String) -> String? {
        if case .ready(let id) = states[parentRelPath] {
            return id
        }
        return nil
    }

    /// Wait for the specified parent directory to be ready (return immediately if it is ready; suspend the current coroutine if it is not ready, wake up instantly after successful creation; throw an error if it fails or cancels)
    public func awaitParentReady(parentRelPath: String) async throws -> String {
        if Task.isCancelled {
            throw CancellationError()
        }

        if let state = states[parentRelPath] {
            switch state {
            case .ready(let remoteId):
                return remoteId
            case .failed(let error):
                throw error
            }
        }

        let waiterId = nextWaiterId
        nextWaiterId &+= 1

        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                waiters[parentRelPath, default: [:]][waiterId] = continuation
            }
        } onCancel: {
            Task { [self] in
                await self.cancelWaiter(parentRelPath: parentRelPath, id: waiterId)
            }
        }
    }

    private func cancelWaiter(parentRelPath: String, id: UInt64) {
        if let continuation = waiters[parentRelPath]?.removeValue(forKey: id) {
            if waiters[parentRelPath]?.isEmpty == true {
                waiters.removeValue(forKey: parentRelPath)
            }
            continuation.resume(throwing: CancellationError())
        }
    }

    /// Marks that a directory has been created remotely successfully and broadcasts a wakeup call to all children waiting for the directory
    public func markDirectoryReady(relPath: String, remoteId: String) {
        guard states[relPath] == nil else { return }
        states[relPath] = .ready(remoteId: remoteId)

        if let pending = waiters.removeValue(forKey: relPath) {
            for (_, continuation) in pending {
                continuation.resume(returning: remoteId)
            }
        }
    }

    /// Flag a directory creation failure and broadcast notification to all children waiting for the directory to throw an error
    public func markDirectoryFailed(relPath: String, error: Error) {
        guard states[relPath] == nil else { return }
        let failureError: Error
        if let trackerError = error as? DirectoryTrackerError {
            failureError = trackerError
        } else if error is CancellationError {
            failureError = DirectoryTrackerError.cancelled
        } else {
            failureError = DirectoryTrackerError.parentDirectoryFailed(
                parentRelPath: relPath,
                reason: String(describing: error)
            )
        }
        states[relPath] = .failed(error: failureError)

        if let pending = waiters.removeValue(forKey: relPath) {
            for (_, continuation) in pending {
                continuation.resume(throwing: failureError)
            }
        }
    }

    /// Cancel all pending processes
    public func cancelAll() {
        for (relPath, dict) in waiters {
            states[relPath] = .failed(error: DirectoryTrackerError.cancelled)
            for (_, continuation) in dict {
                continuation.resume(throwing: DirectoryTrackerError.cancelled)
            }
        }
        waiters.removeAll()
    }
}
