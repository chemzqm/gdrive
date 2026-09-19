import Foundation

/// 目录依赖跟踪与异步唤醒调度器
/// 遵循 v1.md §4.1 与 §4.3 规范：
/// - 消除全量目录预先创建等待
/// - 父目录在远端确认创建的瞬间，立即广播唤醒所有等待该父目录的子项（文件与子目录）
public actor DirectoryTracker {
    public enum DirectoryTrackerError: Error, LocalizedError, Sendable {
        case parentDirectoryFailed(parentRelPath: String, reason: String)
        case cancelled

        public var errorDescription: String? {
            switch self {
            case .parentDirectoryFailed(let parentRelPath, let reason):
                return "父目录 [\(parentRelPath)] 创建失败: \(reason)"
            case .cancelled:
                return "等待父目录已被取消"
            }
        }
    }

    private enum State {
        case ready(remoteId: String)
        case failed(error: Error)
    }

    /// 目录终态映射表：本地相对路径 -> 状态（已就绪或失败）
    private var states: [String: State] = [:]

    /// 正在等待某个父目录就绪的挂起协程队列：本地相对路径 -> (等待者 ID -> 续体)
    private var waiters: [String: [UInt64: CheckedContinuation<String, Error>]] = [:]
    private var nextWaiterId: UInt64 = 0

    public init(remoteRootId: String) {
        // 根目录（相对路径为 ""）在启动时即为已确认就绪
        self.states[""] = .ready(remoteId: remoteRootId)
    }

    /// 检查指定父目录是否已就绪，若已就绪直接返回其 remoteId
    public func getReadyParentId(for parentRelPath: String) -> String? {
        if case .ready(let id) = states[parentRelPath] {
            return id
        }
        return nil
    }

    /// 等待指定父目录就绪（若已就绪立即返回；若尚未就绪则挂起当前协程，待创建成功后瞬时唤醒；若失败或取消则抛出错误）
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

    /// 标记某个目录已在远端创建成功，并广播唤醒所有正在等待该目录的子项
    public func markDirectoryReady(relPath: String, remoteId: String) {
        guard states[relPath] == nil else { return }
        states[relPath] = .ready(remoteId: remoteId)

        if let pending = waiters.removeValue(forKey: relPath) {
            for (_, continuation) in pending {
                continuation.resume(returning: remoteId)
            }
        }
    }

    /// 标记某个目录创建失败，并广播通知所有正在等待该目录的子项抛出错误
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

    /// 取消所有正在等待的协程
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
