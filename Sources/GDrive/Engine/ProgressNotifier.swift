import Foundation

/// Synchronization Progress Model
public struct SyncProgress: Sendable, CustomStringConvertible {
    /// Processed (Uploaded/Files downloaded)
    public let completedFiles: Int
    /// Currently found needs to be processed (upload/Total number of files downloaded (accumulated with scan streaming)
    public let totalDiscoveredFiles: Int
    /// Bytes Completed
    public let completedBytes: Int64
    /// Total number of bytes of files currently found to be processed
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

/// Thread-Safe High-Performance Progress Notifier (Background 500ms Independent sampling and decoupling distribution, zero blocking of worker threads)
public final class ProgressNotifier: @unchecked Sendable {
    private let onProgress: (@Sendable (SyncProgress) -> Void)?
    private let interval: TimeInterval
    private var lock = os_unfair_lock()

    private var completedFiles: Int = 0
    private var totalDiscoveredFiles: Int = 0
    private var completedBytes: Int64 = 0
    private var totalDiscoveredBytes: Int64 = 0

    private var lastNotifiedCompleted: Int = -1
    private var lastNotifiedDiscovered: Int = -1
    private var tickerTask: Task<Void, Never>?

    public init(interval: TimeInterval = 0.5, onProgress: (@Sendable (SyncProgress) -> Void)?) {
        self.onProgress = onProgress
        self.interval = interval
        startTicker()
    }

    init(
        interval: TimeInterval,
        startsTicker: Bool,
        onProgress: (@Sendable (SyncProgress) -> Void)?
    ) {
        self.interval = interval
        self.onProgress = onProgress
        if startsTicker { startTicker() }
    }

    private func startTicker() {
        guard onProgress != nil else { return }

        // Launch Exclusive Independent Background Sampling Ticker,External callbacks and scans/Network pipeline is completely physically isolated
        self.tickerTask = Task { [weak self] in
            guard let self = self else { return }
            let nanoseconds = UInt64(interval * 1_000_000_000)
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: nanoseconds)
                guard !Task.isCancelled else { break }
                self.notifyIfChanged()
            }
        }
    }

    /// Accumulate newly discovered files to be transferred (pure memory lightweight operation, zero system calls, time-consuming ~2ns,Non-blocking scanning threads)
    @inline(__always)
    public func addDiscovered(files: Int = 1, bytes: Int64 = 0) {
        guard onProgress != nil else { return }
        os_unfair_lock_lock(&lock)
        totalDiscoveredFiles += files
        totalDiscoveredBytes += bytes
        os_unfair_lock_unlock(&lock)
    }

    /// Accumulate files that have finished transferring (pure memory lightweight operation, zero system calls, time consuming ~2ns,Do not block upload threads)
    @inline(__always)
    public func addCompleted(files: Int = 1, bytes: Int64 = 0) {
        guard onProgress != nil else { return }
        os_unfair_lock_lock(&lock)
        completedFiles += files
        completedBytes += bytes
        os_unfair_lock_unlock(&lock)
    }

    /// Take a snapshot of your current progress
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

    /// Review and distribute changes (back office only Ticker triggered, the worker thread never executes an external closure)
    func notifyIfChanged() {
        guard let onProgress = self.onProgress else { return }
        os_unfair_lock_lock(&lock)
        guard totalDiscoveredFiles != lastNotifiedDiscovered || completedFiles != lastNotifiedCompleted else {
            os_unfair_lock_unlock(&lock)
            return
        }
        lastNotifiedDiscovered = totalDiscoveredFiles
        lastNotifiedCompleted = completedFiles
        let progress = SyncProgress(
            completedFiles: completedFiles,
            totalDiscoveredFiles: totalDiscoveredFiles,
            completedBytes: completedBytes,
            totalDiscoveredBytes: totalDiscoveredBytes
        )
        os_unfair_lock_unlock(&lock)

        onProgress(progress)
    }

    /// Synchronize All, Stop Background Polling and Force Brush Final 100% Progress
    public func finish() {
        guard let onProgress = self.onProgress else { return }
        tickerTask?.cancel()
        tickerTask = nil

        os_unfair_lock_lock(&lock)
        let progress = SyncProgress(
            completedFiles: completedFiles,
            totalDiscoveredFiles: totalDiscoveredFiles,
            completedBytes: completedBytes,
            totalDiscoveredBytes: totalDiscoveredBytes
        )
        lastNotifiedDiscovered = totalDiscoveredFiles
        lastNotifiedCompleted = completedFiles
        os_unfair_lock_unlock(&lock)

        onProgress(progress)
    }
}
