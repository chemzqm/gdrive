import Foundation

/// File item model in transit
public struct TransferItem: Sendable, Identifiable {
    public var id: String { fileId }
    public let fileId: String
    public let name: String
    public let totalBytes: Int64
    public let transferredBytes: Int64
    public let startedAt: Date

    public init(fileId: String, name: String, totalBytes: Int64, transferredBytes: Int64, startedAt: Date) {
        self.fileId = fileId
        self.name = name
        self.totalBytes = totalBytes
        self.transferredBytes = transferredBytes
        self.startedAt = startedAt
    }

    public var progress: Double {
        guard totalBytes > 0 else { return 0 }
        return min(1.0, max(0.0, Double(transferredBytes) / Double(totalBytes)))
    }
}

/// File item model in queue for transfer
public struct QueuedTransferItem: Sendable, Identifiable {
    public var id: String { fileId }
    public let fileId: String
    public let name: String
    public let totalBytes: Int64
    public let queuedAt: Date

    public init(fileId: String, name: String, totalBytes: Int64, queuedAt: Date) {
        self.fileId = fileId
        self.name = name
        self.totalBytes = totalBytes
        self.queuedAt = queuedAt
    }
}

/// Real-time in-memory transfer snapshots (per 500ms Auto Refresh)
public struct TransferSnapshot: Sendable {
    public let activeUploads: [TransferItem]
    public let activeDownloads: [TransferItem]
    public let queuedUploads: [QueuedTransferItem]
    public let queuedDownloads: [QueuedTransferItem]
    public let uploadSpeedBytesPerSecond: Double
    public let downloadSpeedBytesPerSecond: Double
    public let refreshedAt: Date

    public init(
        activeUploads: [TransferItem] = [],
        activeDownloads: [TransferItem] = [],
        queuedUploads: [QueuedTransferItem] = [],
        queuedDownloads: [QueuedTransferItem] = [],
        uploadSpeedBytesPerSecond: Double = 0.0,
        downloadSpeedBytesPerSecond: Double = 0.0,
        refreshedAt: Date = Date()
    ) {
        self.activeUploads = activeUploads
        self.activeDownloads = activeDownloads
        self.queuedUploads = queuedUploads
        self.queuedDownloads = queuedDownloads
        self.uploadSpeedBytesPerSecond = uploadSpeedBytesPerSecond
        self.downloadSpeedBytesPerSecond = downloadSpeedBytesPerSecond
        self.refreshedAt = refreshedAt
    }
}

/// Thread-safe real-time transmission monitoring and rate sampler
public final class TransferMonitor: @unchecked Sendable {
    private let lock = NSLock()

    private var queuedUploads = [String: QueuedTransferItem]()
    private var queuedDownloads = [String: QueuedTransferItem]()

    private var activeUploads = [String: (name: String, totalBytes: Int64, transferredBytes: Int64, startedAt: Date)]()
    private var activeDownloads = [String: (name: String, totalBytes: Int64, transferredBytes: Int64, startedAt: Date)]()

    private var totalBytesUploaded: Int64 = 0
    private var totalBytesDownloaded: Int64 = 0

    private var lastSampleTime: Date = Date()
    private var lastSampleBytesUploaded: Int64 = 0
    private var lastSampleBytesDownloaded: Int64 = 0

    private var cachedSnapshot: TransferSnapshot

    private var timerTask: Task<Void, Never>?

    public init() {
        self.cachedSnapshot = TransferSnapshot()
        self.startTimer()
    }

    deinit {
        timerTask?.cancel()
    }

    private func startTimer() {
        timerTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 500_000_000) // 500ms
                guard let self else { break }
                self.refreshSnapshot()
            }
        }
    }

    // MARK: - Upload Queues & Active Management

    public func enqueueUpload(id: String, name: String, totalBytes: Int64) {
        lock.lock()
        defer { lock.unlock() }
        queuedUploads[id] = QueuedTransferItem(fileId: id, name: name, totalBytes: totalBytes, queuedAt: Date())
    }

    public func startUpload(id: String, name: String, totalBytes: Int64) {
        lock.lock()
        defer { lock.unlock() }
        queuedUploads.removeValue(forKey: id)
        activeUploads[id] = (name: name, totalBytes: totalBytes, transferredBytes: 0, startedAt: Date())
    }

    public func reportUploadProgress(id: String, additionalBytes: Int64) {
        guard additionalBytes > 0 else { return }
        lock.lock()
        defer { lock.unlock() }
        totalBytesUploaded += additionalBytes
        if var current = activeUploads[id] {
            current.transferredBytes += additionalBytes
            activeUploads[id] = current
        }
    }

    public func finishUpload(id: String) {
        lock.lock()
        defer { lock.unlock() }
        queuedUploads.removeValue(forKey: id)
        activeUploads.removeValue(forKey: id)
    }

    // MARK: - Download Queues & Active Management

    public func enqueueDownload(id: String, name: String, totalBytes: Int64) {
        lock.lock()
        defer { lock.unlock() }
        queuedDownloads[id] = QueuedTransferItem(fileId: id, name: name, totalBytes: totalBytes, queuedAt: Date())
    }

    public func startDownload(id: String, name: String, totalBytes: Int64) {
        lock.lock()
        defer { lock.unlock() }
        queuedDownloads.removeValue(forKey: id)
        activeDownloads[id] = (name: name, totalBytes: totalBytes, transferredBytes: 0, startedAt: Date())
    }

    public func reportDownloadProgress(id: String, additionalBytes: Int64) {
        guard additionalBytes > 0 else { return }
        lock.lock()
        defer { lock.unlock() }
        totalBytesDownloaded += additionalBytes
        if var current = activeDownloads[id] {
            current.transferredBytes += additionalBytes
            activeDownloads[id] = current
        }
    }

    public func finishDownload(id: String) {
        lock.lock()
        defer { lock.unlock() }
        queuedDownloads.removeValue(forKey: id)
        activeDownloads.removeValue(forKey: id)
    }

    // MARK: - Snapshot sampling and external reads

    /// Internal every 500ms Refresh the latest snapshot once (calculate the sliding second rate)
    public func refreshSnapshot() {
        lock.lock()
        defer { lock.unlock() }

        let now = Date()
        let interval = now.timeIntervalSince(lastSampleTime)

        let uploadSpeed: Double
        let downloadSpeed: Double

        if interval > 0.1 {
            let deltaUp = totalBytesUploaded - lastSampleBytesUploaded
            let deltaDown = totalBytesDownloaded - lastSampleBytesDownloaded
            uploadSpeed = max(0.0, Double(deltaUp) / interval)
            downloadSpeed = max(0.0, Double(deltaDown) / interval)
            lastSampleTime = now
            lastSampleBytesUploaded = totalBytesUploaded
            lastSampleBytesDownloaded = totalBytesDownloaded
        } else {
            uploadSpeed = cachedSnapshot.uploadSpeedBytesPerSecond
            downloadSpeed = cachedSnapshot.downloadSpeedBytesPerSecond
        }

        let curActiveUploads = activeUploads.map { k, v in
            TransferItem(fileId: k, name: v.name, totalBytes: v.totalBytes, transferredBytes: v.transferredBytes, startedAt: v.startedAt)
        }.sorted { $0.startedAt < $1.startedAt }

        let curActiveDownloads = activeDownloads.map { k, v in
            TransferItem(fileId: k, name: v.name, totalBytes: v.totalBytes, transferredBytes: v.transferredBytes, startedAt: v.startedAt)
        }.sorted { $0.startedAt < $1.startedAt }

        let curQueuedUploads = Array(queuedUploads.values).sorted { $0.queuedAt < $1.queuedAt }
        let curQueuedDownloads = Array(queuedDownloads.values).sorted { $0.queuedAt < $1.queuedAt }

        cachedSnapshot = TransferSnapshot(
            activeUploads: curActiveUploads,
            activeDownloads: curActiveDownloads,
            queuedUploads: curQueuedUploads,
            queuedDownloads: curQueuedDownloads,
            uploadSpeedBytesPerSecond: uploadSpeed,
            downloadSpeedBytesPerSecond: downloadSpeed,
            refreshedAt: now
        )
    }

    /// External direct sync for memory snapshots, no network/No Disks I/O,Time consuming 0ms
    public func getSnapshot() -> TransferSnapshot {
        lock.lock()
        defer { lock.unlock() }
        return cachedSnapshot
    }
}
