import Darwin
import Foundation

/// 扫描模式
public enum ScanMode: Sendable, Hashable {
    /// 仅相对路径与节点类型，不读取元数据（最高性能，与 rg --files 同口径）
    case paths
    /// 相对路径、类型、身份、mtime、ctime 与普通文件大小
    case basic
}

/// 输出范围
public enum Emission: Sendable, Hashable {
    /// 输出全部保留节点（包括目录、文件、符号链接等）
    case all
    /// 仅输出普通文件（目录仍参与递归但不在结果中单独输出）
    case regularFiles
}

/// 节点类型
public enum EntryType: UInt8, Sendable, CustomStringConvertible {
    case file = 1
    case directory = 2
    case symbolicLink = 3
    case other = 4

    public var description: String {
        switch self {
        case .file: return "file"
        case .directory: return "directory"
        case .symbolicLink: return "symbolicLink"
        case .other: return "other"
        }
    }
}

/// 文件身份标识（设备号 + inode）
public struct FileIdentity: Sendable, Hashable, CustomStringConvertible {
    public let device: dev_t
    public let inode: ino_t

    public init(device: dev_t, inode: ino_t) {
        self.device = device
        self.inode = inode
    }

    public var description: String {
        "dev:\(device),ino:\(inode)"
    }
}

/// 文件高精度时间戳
public struct FileTimestamp: Sendable, Hashable, Comparable {
    public let seconds: Int64
    public let nanoseconds: Int32

    public init(seconds: Int64, nanoseconds: Int32) {
        self.seconds = seconds
        self.nanoseconds = nanoseconds
    }

    public static func < (lhs: FileTimestamp, rhs: FileTimestamp) -> Bool {
        if lhs.seconds != rhs.seconds {
            return lhs.seconds < rhs.seconds
        }
        return lhs.nanoseconds < rhs.nanoseconds
    }
}

/// 文件元数据
public struct FileMetadata: Sendable {
    public let identity: FileIdentity
    public let modificationTime: FileTimestamp
    public let changeTime: FileTimestamp
    public let fileSize: Int64?

    public init(
        identity: FileIdentity,
        modificationTime: FileTimestamp,
        changeTime: FileTimestamp,
        fileSize: Int64?
    ) {
        self.identity = identity
        self.modificationTime = modificationTime
        self.changeTime = changeTime
        self.fileSize = fileSize
    }
}

/// 过滤规则：按单层名称全匹配，* 匹配零个或多个字符。
/// 大小写敏感，其他符号按字面匹配；不支持路径和转义。
public enum FilterRule: Sendable, Hashable {
    /// 目录名称全匹配排除（命中目录时直接剪枝）
    case excludeDirectory(String)
    /// 文件名称全匹配排除（仅排除非目录节点）
    case excludeFile(String)
    /// 节点名称全匹配排除（文件/目录均匹配；命中目录同时剪枝）
    case excludeName(String)
    /// 文件名称全匹配包含（筛选非目录节点）
    case includeFile(String)
}

/// 扫描选项
public struct ScanOptions: Sendable {
    /// 扫描模式
    public var mode: ScanMode
    /// 输出范围
    public var emission: Emission
    /// 是否包含隐藏文件与目录（点文件）
    public var includeHidden: Bool
    /// 并行工作线程数
    public var workers: Int
    /// 单批次最大记录数
    public var batchCapacity: Int
    /// 单批次最大路径字节预算
    public var batchBytesLimit: Int
    /// 最大递归深度（根目录为 0）
    public var maxDepth: Int
    /// 批次内部路径分隔符（默认换行符 0x0A，可选 NUL 0x00）
    public var delimiter: UInt8
    /// 可选的输出路径前缀（如 "./" 或自定义目录名前缀）
    public var pathPrefix: [UInt8]?

    public init(
        mode: ScanMode = .paths,
        emission: Emission = .all,
        includeHidden: Bool = true,
        workers: Int = min(8, max(1, ProcessInfo.processInfo.activeProcessorCount)),
        batchCapacity: Int = 1024,
        batchBytesLimit: Int = 256 * 1024,
        maxDepth: Int = 128,
        delimiter: UInt8 = 0x0A,
        pathPrefix: [UInt8]? = nil
    ) {
        self.mode = mode
        self.emission = emission
        self.includeHidden = includeHidden
        self.workers = max(1, min(64, workers))
        self.batchCapacity = max(1, batchCapacity)
        self.batchBytesLimit = max(4096, batchBytesLimit)
        self.maxDepth = max(1, maxDepth)
        self.delimiter = delimiter
        self.pathPrefix = pathPrefix
    }

    /// 验证可变公开成员在请求进入扫描器后的最终取值。
    /// 初始化器继续为兼容性进行钳制；调用入口不能接受之后被改成
    /// 非法值的预算，以免数组预留、乘法或线程创建发生溢出/崩溃。
    func validate() throws {
        guard (1...64).contains(workers) else {
            throw ScanError.invalidOptions("workers")
        }
        guard batchCapacity > 0,
              batchCapacity <= Int.max / MemoryLayout<ScanBatch.Record>.stride else {
            throw ScanError.invalidOptions("batchCapacity")
        }
        // ScanBatch stores ranges as UInt32 and appends one delimiter after a path.
        guard batchBytesLimit > 0,
              batchBytesLimit < Int(UInt32.max),
              (pathPrefix?.count ?? 0) < Int(UInt32.max) else {
            throw ScanError.invalidOptions("batchBytesLimit")
        }
        guard maxDepth > 0 else {
            throw ScanError.invalidOptions("maxDepth")
        }
    }
}

/// 扫描请求
public struct ScanRequest: Sendable {
    public var root: String
    public var filters: [FilterRule]
    public var options: ScanOptions

    public init(
        root: String,
        filters: [FilterRule] = [],
        options: ScanOptions = .init()
    ) {
        self.root = root
        self.filters = filters
        self.options = options
    }
}

/// 扫描结果条目（用于单条目回调操作）
public struct ScanEntry: Sendable {
    public let type: EntryType
    public let relativePath: String
    public let metadata: FileMetadata?

    public init(type: EntryType, relativePath: String, metadata: FileMetadata? = nil) {
        self.type = type
        self.relativePath = relativePath
        self.metadata = metadata
    }
}

/// 扫描执行摘要
public struct ScanSummary: Sendable {
    public let rootIdentity: FileIdentity
    public let fileCount: Int
    public let directoryCount: Int
    public let prunedCount: Int
    public let totalEmitted: Int
    public let elapsedNanoseconds: UInt64

    public init(
        rootIdentity: FileIdentity,
        fileCount: Int,
        directoryCount: Int,
        prunedCount: Int,
        totalEmitted: Int,
        elapsedNanoseconds: UInt64
    ) {
        self.rootIdentity = rootIdentity
        self.fileCount = fileCount
        self.directoryCount = directoryCount
        self.prunedCount = prunedCount
        self.totalEmitted = totalEmitted
        self.elapsedNanoseconds = elapsedNanoseconds
    }
}

/// 扫描错误类型
public enum ScanError: Error, Sendable, CustomStringConvertible {
    case rootNotFound(String)
    case rootNotDirectory(String)
    case permissionDenied(String)
    case maxDepthExceeded(Int)
    case invalidFilter(String)
    case invalidOptions(String)
    case ioError(Int32, String)
    case changedDuringScan(String)
    case cancelled

    public var description: String {
        switch self {
        case .rootNotFound(let p): return "Root path not found: \(p)"
        case .rootNotDirectory(let p): return "Root path is not a directory: \(p)"
        case .permissionDenied(let p): return "Permission denied: \(p)"
        case .maxDepthExceeded(let d): return "Maximum directory depth exceeded: \(d)"
        case .invalidFilter(let f): return "Invalid filter rule: \(f)"
        case .invalidOptions(let option): return "Invalid scan option: \(option)"
        case .ioError(let code, let msg): return "I/O error (\(code)): \(msg)"
        case .changedDuringScan(let p): return "Directory modified during scan: \(p)"
        case .cancelled: return "Scan was cancelled"
        }
    }
}
