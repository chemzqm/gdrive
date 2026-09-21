import Darwin
import Foundation

/// 高性能无共享并发目录扫描器
public struct DirectoryScanner: Sendable {
    public init() {}

    /// 执行扫描并通过批次流式消费（高性能推荐接口）
    /// - Parameters:
    ///   - request: 扫描请求
    ///   - consume: 串行调用的批次消费闭包
    /// - Returns: 扫描执行摘要
    public func scan(
        _ request: ScanRequest,
        consume: @Sendable (ScanBatch) async throws -> Void
    ) async throws -> ScanSummary {
        let startTime = DispatchTime.now().uptimeNanoseconds
        let options = request.options
        try options.validate()
        // Compile before acquiring the root handle so invalid filters cannot leak it.
        let filterPlan = try FilterPlan(filters: request.filters)
        if Task.isCancelled { throw ScanError.cancelled }

        // 尝试提高文件描述符限制，避免大深度并发扫描耗尽 fd
        DirectoryReader.raiseFDLimit()

        // 解析并规范化根路径
        let resolvedRoot: String
        if request.root.hasPrefix("/") {
            resolvedRoot = request.root
        } else {
            let cwd = FileManager.default.currentDirectoryPath
            if request.root == "." || request.root.isEmpty {
                resolvedRoot = cwd
            } else {
                resolvedRoot = (cwd as NSString).appendingPathComponent(request.root)
            }
        }

        // 打开根目录
        let (rootDir, rootIdentity) = try DirectoryReader.openRoot(path: resolvedRoot)
        let scheduler = ScanScheduler(workers: options.workers)
        let channel = BatchChannel(capacity: options.workers * 2)

        // 注入根目录任务
        let initialPrefix = options.pathPrefix ?? []
        let rootWork = ScanScheduler.DirectoryWork(dir: rootDir, prefix: initialPrefix, depth: 0)
        guard case .enqueued = scheduler.enqueue(rootWork) else {
            closedir(rootDir)
            throw ScanError.cancelled
        }

        // 创建专用工作线程与协同组
        let group = DispatchGroup()
        var workers: [ScanWorker] = []
        workers.reserveCapacity(options.workers)

        for i in 0..<options.workers {
            let worker = ScanWorker(
                workerId: i,
                scheduler: scheduler,
                channel: channel,
                options: options,
                filterPlan: filterPlan
            )
            workers.append(worker)

            group.enter()
            let thread = Thread {
                worker.run()
                group.leave()
            }
            thread.qualityOfService = .userInitiated
            thread.start()
        }

        // 所有工作线程退出后通知关闭通道
        group.notify(queue: .global(qos: .userInitiated)) {
            channel.close()
        }

        // 取消必须同时唤醒结果通道与等待目录任务的 worker。
        await withTaskCancellationHandler(operation: {
            do {
                while let batch = try await channel.next() {
                    if Task.isCancelled { throw ScanError.cancelled }
                    try await consume(batch)
                }
            } catch {
                channel.fail(error)
                scheduler.stop()
            }
            // 异步等待所有工作线程完全退出
            await withCheckedContinuation { continuation in
                group.notify(queue: .global(qos: .userInitiated)) {
                    continuation.resume()
                }
            }
        }, onCancel: {
            channel.cancel()
            scheduler.stop()
        })

        scheduler.drainAndCloseAll()

        if let err = channel.failure() {
            throw err
        }
        if Task.isCancelled {
            throw ScanError.cancelled
        }

        try DirectoryReader.validateRootIdentity(path: resolvedRoot, identity: rootIdentity)

        let endTime = DispatchTime.now().uptimeNanoseconds
        let elapsed = endTime - startTime

        // 汇总统计指标
        var totalFiles = 0
        var totalDirs = 0
        var totalPruned = 0
        var totalEmitted = 0

        for worker in workers {
            let s = worker.stats
            totalFiles += s.fileCount
            totalDirs += s.directoryCount
            totalPruned += s.prunedCount
            totalEmitted += s.emittedCount
        }

        return ScanSummary(
            rootIdentity: rootIdentity,
            fileCount: totalFiles,
            directoryCount: totalDirs,
            prunedCount: totalPruned,
            totalEmitted: totalEmitted,
            elapsedNanoseconds: elapsed
        )
    }

    /// 便捷同步回调接口：逐项处理每个文件与目录
    /// - Parameters:
    ///   - request: 扫描请求
    ///   - onEntry: 每个文件或目录的自定义操作闭包（抛出错误时中断扫描）
    /// - Returns: 扫描执行摘要
    @discardableResult
    public func scanEach(
        _ request: ScanRequest,
        onEntry: @Sendable (ScanEntry) throws -> Void
    ) async throws -> ScanSummary {
        try await scan(request) { batch in
            try batch.forEach { entry in
                try onEntry(entry)
            }
        }
    }

    /// 便捷异步回调接口：逐项异步处理每个文件与目录
    @discardableResult
    public func scanEachAsync(
        _ request: ScanRequest,
        onEntry: @Sendable (ScanEntry) async throws -> Void
    ) async throws -> ScanSummary {
        try await scan(request) { batch in
            for i in 0..<batch.count {
                let entry = ScanEntry(
                    type: batch.type(at: i),
                    relativePath: batch.relativePath(at: i),
                    metadata: batch.metadata(at: i)
                )
                try await onEntry(entry)
            }
        }
    }

    /// 高级静态遍历辅助函数
    @discardableResult
    public static func walk(
        _ root: String,
        options: ScanOptions = .init(),
        filters: [FilterRule] = [],
        onEntry: @Sendable (ScanEntry) throws -> Void
    ) async throws -> ScanSummary {
        let scanner = DirectoryScanner()
        let req = ScanRequest(root: root, filters: filters, options: options)
        return try await scanner.scanEach(req, onEntry: onEntry)
    }
}
