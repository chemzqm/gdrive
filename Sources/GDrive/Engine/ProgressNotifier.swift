import Foundation

/// 同步进度模型
public struct SyncProgress: Sendable, CustomStringConvertible {
    /// 已完成处理（上传/下载）的文件数量
    public let completedFiles: Int
    /// 当前已发现的需处理（上传/下载）的文件总数（边扫描边累加）
    public let totalDiscoveredFiles: Int
    /// 已完成的字节数
    public let completedBytes: Int64
    /// 当前已发现的需处理文件总字节数
    public let totalDiscoveredBytes: Int64

    public init(
        completedFiles: Int,
        totalDiscoveredFiles: Int,
        completedBytes: Int64 = 0,
        totalDiscoveredBytes: Int64 = 0
    ) {
        self.completedFiles = completedFiles
        self.totalDiscoveredFiles = totalDiscoveredFiles
        self.completedBytes = completedBytes
        self.totalDiscoveredBytes = totalDiscoveredBytes
    }

    public var percentage: Double {
        guard totalDiscoveredFiles > 0 else { return 0.0 }
        return min(1.0, max(0.0, Double(completedFiles) / Double(totalDiscoveredFiles)))
    }

    public var description: String {
        "SyncProgress(completed: \(completedFiles)/\(totalDiscoveredFiles), bytes: \(completedBytes)/\(totalDiscoveredBytes))"
    }
}

/// 线程安全的进度通知器（支持 500ms 防抖/节流合并）
public final class ProgressNotifier: @unchecked Sendable {
    private let onProgress: (@Sendable (SyncProgress) -> Void)?
    private let debounceInterval: TimeInterval
    private var lock = os_unfair_lock()

    private var completedFiles: Int = 0
    private var totalDiscoveredFiles: Int = 0
    private var completedBytes: Int64 = 0
    private var totalDiscoveredBytes: Int64 = 0

    private var lastNotifiedTime: DispatchTime = .now()
    private var scheduledTask: Task<Void, Never>?
    private var hasPendingNotification = false

    public init(interval: TimeInterval = 0.5, onProgress: (@Sendable (SyncProgress) -> Void)?) {
        self.debounceInterval = interval
        self.onProgress = onProgress
    }

    /// 累加新发现的待传输文件
    public func addDiscovered(files: Int = 1, bytes: Int64 = 0) {
        guard onProgress != nil else { return }
        os_unfair_lock_lock(&lock)
        totalDiscoveredFiles += files
        totalDiscoveredBytes += bytes
        hasPendingNotification = true
        os_unfair_lock_unlock(&lock)
        triggerUpdate()
    }

    /// 累加已完成传输的文件
    public func addCompleted(files: Int = 1, bytes: Int64 = 0) {
        guard onProgress != nil else { return }
        os_unfair_lock_lock(&lock)
        completedFiles += files
        completedBytes += bytes
        hasPendingNotification = true
        os_unfair_lock_unlock(&lock)
        triggerUpdate()
    }

    /// 获取当前最新进度快照
    public func currentProgress() -> SyncProgress {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }
        return SyncProgress(
            completedFiles: completedFiles,
            totalDiscoveredFiles: totalDiscoveredFiles,
            completedBytes: completedBytes,
            totalDiscoveredBytes: totalDiscoveredBytes
        )
    }

    /// 触发或调度延迟通知
    private func triggerUpdate() {
        guard let onProgress = self.onProgress else { return }

        os_unfair_lock_lock(&lock)
        let now = DispatchTime.now()
        let elapsed = Double(now.uptimeNanoseconds - lastNotifiedTime.uptimeNanoseconds) / 1_000_000_000

        if elapsed >= debounceInterval {
            // 已超过 500ms，立即触发通知
            scheduledTask?.cancel()
            scheduledTask = nil
            lastNotifiedTime = now
            hasPendingNotification = false
            let progress = SyncProgress(
                completedFiles: completedFiles,
                totalDiscoveredFiles: totalDiscoveredFiles,
                completedBytes: completedBytes,
                totalDiscoveredBytes: totalDiscoveredBytes
            )
            os_unfair_lock_unlock(&lock)
            onProgress(progress)
        } else {
            // 未到 500ms，若无在途延迟任务则调度延迟触发 (Trailing edge)
            if scheduledTask == nil {
                let remainingDelay = debounceInterval - elapsed
                scheduledTask = Task { [weak self] in
                    try? await Task.sleep(nanoseconds: UInt64(remainingDelay * 1_000_000_000))
                    guard !Task.isCancelled, let self = self else { return }
                    self.fireTrailing()
                }
            }
            os_unfair_lock_unlock(&lock)
        }
    }

    private func fireTrailing() {
        guard let onProgress = self.onProgress else { return }
        os_unfair_lock_lock(&lock)
        scheduledTask = nil
        guard hasPendingNotification else {
            os_unfair_lock_unlock(&lock)
            return
        }
        lastNotifiedTime = .now()
        hasPendingNotification = false
        let progress = SyncProgress(
            completedFiles: completedFiles,
            totalDiscoveredFiles: totalDiscoveredFiles,
            completedBytes: completedBytes,
            totalDiscoveredBytes: totalDiscoveredBytes
        )
        os_unfair_lock_unlock(&lock)
        onProgress(progress)
    }

    /// 完成全部同步，强刷最终 100% 进度
    public func finish() {
        guard let onProgress = self.onProgress else { return }
        os_unfair_lock_lock(&lock)
        scheduledTask?.cancel()
        scheduledTask = nil
        let progress = SyncProgress(
            completedFiles: completedFiles,
            totalDiscoveredFiles: totalDiscoveredFiles,
            completedBytes: completedBytes,
            totalDiscoveredBytes: totalDiscoveredBytes
        )
        os_unfair_lock_unlock(&lock)
        onProgress(progress)
    }
}
