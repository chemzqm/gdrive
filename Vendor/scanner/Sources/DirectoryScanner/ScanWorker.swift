import Darwin
import Foundation

/// 扫描统计计数器（单个工作线程累加，避免全局锁竞争）
public struct WorkerStats: Sendable {
    public var fileCount: Int = 0
    public var directoryCount: Int = 0
    public var prunedCount: Int = 0
    public var emittedCount: Int = 0

    public init() {}
}

/// 专用 I/O 扫描工作线程实现
public final class ScanWorker: @unchecked Sendable {
    private struct LocalDFSEntry {
        let dir: UnsafeMutablePointer<DIR>
        let prefixLength: Int
        let depth: Int
    }

    public let workerId: Int
    private let scheduler: ScanScheduler
    private let channel: BatchChannel
    private let options: ScanOptions
    private let filterPlan: FilterPlan

    public private(set) var stats = WorkerStats()

    // 线程局部预分配缓冲区，避免频繁堆分配
    private var pathData: [UInt8]
    private var records: [ScanBatch.Record]
    private var currentPrefix: [UInt8]
    private var dfsStack: [LocalDFSEntry]

    private let nameOffset = MemoryLayout<dirent>.offset(of: \dirent.d_name)!

    public init(
        workerId: Int,
        scheduler: ScanScheduler,
        channel: BatchChannel,
        options: ScanOptions,
        filterPlan: FilterPlan
    ) {
        self.workerId = workerId
        self.scheduler = scheduler
        self.channel = channel
        self.options = options
        self.filterPlan = filterPlan

        self.pathData = []
        self.pathData.reserveCapacity(min(options.batchBytesLimit, 256 * 1024))
        self.records = []
        self.records.reserveCapacity(min(options.batchCapacity, 1024))
        self.currentPrefix = []
        self.dfsStack = []
    }

    /// 工作线程执行主入口
    public func run() {
        var wasActive = false
        do {
            while let work = scheduler.acquireWork(wasActive: wasActive) {
                wasActive = true
                try processWork(initialWork: work)
            }
            try flushBatch()
        } catch {
            channel.fail(error)
            scheduler.stop()
        }
    }

    /// 处理获取到的目录任务（结合就绪队列移交与本地深度优先搜索）
    private func processWork(initialWork: ScanScheduler.DirectoryWork) throws {
        var currentDir: UnsafeMutablePointer<DIR>? = initialWork.dir
        var currentDepth = initialWork.depth
        currentPrefix = initialWork.prefix
        var isCurrentDirClosed = false

        defer {
            if !isCurrentDirClosed, let dir = currentDir {
                closedir(dir)
            }
            for entry in dfsStack {
                closedir(entry.dir)
            }
            dfsStack.removeAll(keepingCapacity: true)
        }

        while let activeDir = currentDir {
            if scheduler.shouldAbort() { return }
            let parentFD = dirfd(activeDir)
            var doveIntoChild = false
            var entriesSinceStopCheck = 0

            while true {
                if entriesSinceStopCheck == 256 {
                    if scheduler.shouldAbort() { return }
                    entriesSinceStopCheck = 0
                }
                errno = 0
                guard let entry = readdir(activeDir) else {
                    if errno != 0 {
                        let err = errno
                        throw ScanError.ioError(err, String(cString: strerror(err)))
                    }
                    break
                }
                entriesSinceStopCheck += 1
                let namlen = Int(entry.pointee.d_namlen)
                let rawEntry = UnsafeRawPointer(entry)
                let u8Ptr = rawEntry.advanced(by: nameOffset).assumingMemoryBound(to: UInt8.self)
                let ccharPtr = rawEntry.advanced(by: nameOffset).assumingMemoryBound(to: CChar.self)

                // 1. 跳过 "." 和 ".."
                if namlen == 1 && u8Ptr[0] == 0x2E {
                    continue
                }
                if namlen == 2 && u8Ptr[0] == 0x2E && u8Ptr[1] == 0x2E {
                    continue
                }

                // 2. 隐藏文件/目录处理
                if !options.includeHidden && u8Ptr[0] == 0x2E {
                    continue
                }

                // 3. 解析条目类型
                let dType = entry.pointee.d_type
                let entryType: EntryType
                if dType == UInt8(DT_REG) {
                    entryType = .file
                } else if dType == UInt8(DT_DIR) {
                    entryType = .directory
                } else if dType == UInt8(DT_LNK) {
                    entryType = .symbolicLink
                } else {
                    entryType = try DirectoryReader.resolveType(dType: dType, parentFD: parentFD, namePtr: ccharPtr)
                }

                // 4. 根据类型进行过滤与派发
                if entryType == .directory {
                    // 目录全匹配排除检查（剪枝）
                    if filterPlan.shouldExcludeDirectory(namePtr: u8Ptr, length: namlen) {
                        stats.prunedCount += 1
                        continue
                    }

                    // 若 emission 为 .all，输出目录自身
                    if options.emission == .all {
                        try emitRecord(
                            namePtr: u8Ptr,
                            namlen: namlen,
                            type: .directory,
                            parentFD: parentFD,
                            ccharPtr: ccharPtr
                        )
                        stats.directoryCount += 1
                    }

                    // 深度限制检查
                    if currentDepth >= options.maxDepth {
                        throw ScanError.maxDepthExceeded(currentDepth + 1)
                    }

                    // 打开子目录
                    let childDir = try DirectoryReader.openChild(parentFD: parentFD, namePtr: ccharPtr)

                    // 构造子目录路径前缀
                    var childPrefix = currentPrefix
                    childPrefix.append(contentsOf: UnsafeBufferPointer(start: u8Ptr, count: namlen))
                    childPrefix.append(0x2F) // '/'

                    let childWork = ScanScheduler.DirectoryWork(
                        dir: childDir,
                        prefix: childPrefix,
                        depth: currentDepth + 1
                    )

                    // 尝试移交至共享就绪队列
                    if options.emission == .all {
                        switch scheduler.tryReserve() {
                        case .reserved:
                            do {
                                try flushBatch()
                                scheduler.commitReserved(childWork)
                                continue
                            } catch {
                                scheduler.cancelReservation()
                                closedir(childDir)
                                throw error
                            }
                        case .stopped:
                            closedir(childDir)
                            return
                        case .full:
                            break
                        }
                    } else {
                        switch scheduler.enqueue(childWork) {
                        case .enqueued:
                            continue
                        case .stopped:
                            closedir(childDir)
                            return
                        case .full:
                            break
                        }
                    }

                    // 共享队列已满：当前 worker 立即执行本地 DFS
                    dfsStack.append(LocalDFSEntry(
                        dir: activeDir,
                        prefixLength: currentPrefix.count,
                        depth: currentDepth
                    ))

                    currentDir = childDir
                    currentDepth += 1
                    currentPrefix = childPrefix
                    doveIntoChild = true
                    break // 跳出当前 readdir 循环，开始扫描子目录

                } else if entryType == .file {
                    // 文件排除检查
                    if filterPlan.shouldExcludeFile(namePtr: u8Ptr, length: namlen) {
                        continue
                    }
                    // 文件包含检查
                    if !filterPlan.shouldIncludeFile(namePtr: u8Ptr, length: namlen) {
                        continue
                    }

                    try emitRecord(
                        namePtr: u8Ptr,
                        namlen: namlen,
                        type: .file,
                        parentFD: parentFD,
                        ccharPtr: ccharPtr
                    )
                    stats.fileCount += 1

                } else {
                    // 符号链接或其他类型
                    if options.emission == .all {
                        if !filterPlan.shouldExcludeFile(namePtr: u8Ptr, length: namlen) &&
                           filterPlan.shouldIncludeFile(namePtr: u8Ptr, length: namlen) {
                            try emitRecord(
                                namePtr: u8Ptr,
                                namlen: namlen,
                                type: entryType,
                                parentFD: parentFD,
                                ccharPtr: ccharPtr
                            )
                            stats.fileCount += 1
                        }
                    }
                }
            }

            if doveIntoChild {
                continue
            }

            // 当前目录读取结束（EOF）
            closedir(activeDir)
            isCurrentDirClosed = true

            if let parent = dfsStack.popLast() {
                currentDir = parent.dir
                currentDepth = parent.depth
                currentPrefix.removeSubrange(parent.prefixLength..<currentPrefix.count)
                isCurrentDirClosed = false
            } else {
                currentDir = nil
                break
            }
        }
    }

    /// 向当前批次写入一条记录
    private func emitRecord(
        namePtr: UnsafePointer<UInt8>,
        namlen: Int,
        type: EntryType,
        parentFD: Int32,
        ccharPtr: UnsafePointer<CChar>
    ) throws {
        // The delimiter is part of the byte budget, even though it is not part
        // of the public path range.  Use checked arithmetic before mutating the
        // batch so a mutated public option cannot overflow Int/UInt32 here.
        guard currentPrefix.count <= Int.max - namlen else {
            throw ScanError.invalidOptions("batchBytesLimit")
        }
        let pathLength = currentPrefix.count + namlen
        guard pathLength < Int.max else {
            throw ScanError.invalidOptions("batchBytesLimit")
        }
        let entryBytes = pathLength + 1
        guard entryBytes <= options.batchBytesLimit else {
            throw ScanError.invalidOptions("batchBytesLimit")
        }

        if !records.isEmpty {
            guard pathData.count <= options.batchBytesLimit else {
                throw ScanError.invalidOptions("batchBytesLimit")
            }
            if entryBytes > options.batchBytesLimit - pathData.count {
                try flushBatch()
            }
        }

        guard pathData.count <= Int(UInt32.max) - entryBytes else {
            throw ScanError.invalidOptions("batchBytesLimit")
        }
        let offset = UInt32(pathData.count)

        pathData.append(contentsOf: currentPrefix)
        pathData.append(contentsOf: UnsafeBufferPointer(start: namePtr, count: namlen))
        let length = UInt32(pathData.count) - offset
        pathData.append(options.delimiter)

        let metadata: FileMetadata?
        if options.mode == .basic {
            metadata = try DirectoryReader.readMetadata(parentFD: parentFD, namePtr: ccharPtr, type: type)
        } else {
            metadata = nil
        }

        records.append(ScanBatch.Record(offset: offset, length: length, type: type, metadata: metadata))
        stats.emittedCount += 1

        if records.count >= options.batchCapacity || pathData.count >= options.batchBytesLimit {
            try flushBatch()
        }
    }

    /// 刷新当前批次并提交至通道
    private func flushBatch() throws {
        guard !records.isEmpty else { return }

        let batch = ScanBatch(pathData: pathData, records: records)
        if scheduler.shouldAbort() {
            throw channel.failure() ?? ScanError.cancelled
        }
        guard channel.send(batch) else {
            throw channel.failure() ?? ScanError.cancelled
        }
        pathData.removeAll(keepingCapacity: true)
        records.removeAll(keepingCapacity: true)
    }
}
