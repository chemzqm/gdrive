import Foundation

/// 目录依赖跟踪与异步唤醒调度器
/// 遵循 v1.md §4.1 与 §4.3 规范：
/// - 消除全量目录预先创建等待
/// - 父目录在远端确认创建的瞬间，立即广播唤醒所有等待该父目录的子项（文件与子目录）
public actor DirectoryTracker {
    /// 已在远端确认创建就绪的目录映射表：本地相对路径 -> 远端目录 ID
    private var confirmedDirs: [String: String] = [:]

    /// 正在等待某个父目录就绪的挂起协程队列
    private var waiters: [String: [CheckedContinuation<String, Never>]] = [:]

    public init(remoteRootId: String) {
        // 根目录（相对路径为 ""）在启动时即为已确认就绪
        self.confirmedDirs[""] = remoteRootId
    }

    /// 检查指定父目录是否已就绪，若已就绪直接返回其 remoteId
    public func getReadyParentId(for parentRelPath: String) -> String? {
        confirmedDirs[parentRelPath]
    }

    /// 等待指定父目录就绪（若已就绪立即返回；若尚未就绪则挂起当前协程，待创建成功后瞬时唤醒）
    public func awaitParentReady(parentRelPath: String) async -> String {
        if let existingId = confirmedDirs[parentRelPath] {
            return existingId
        }

        return await withCheckedContinuation { continuation in
            waiters[parentRelPath, default: []].append(continuation)
        }
    }

    /// 标记某个目录已在远端创建成功，并广播唤醒所有正在等待该目录的子项
    public func markDirectoryReady(relPath: String, remoteId: String) {
        confirmedDirs[relPath] = remoteId

        if let pending = waiters.removeValue(forKey: relPath) {
            for continuation in pending {
                continuation.resume(returning: remoteId)
            }
        }
    }
}
