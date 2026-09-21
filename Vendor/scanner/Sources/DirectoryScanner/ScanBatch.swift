import Foundation

/// 拥有连续 UTF-8 路径存储与记录数组的不可变批次
public struct ScanBatch: Sendable {
    public struct Record: Sendable {
        public let offset: UInt32
        public let length: UInt32
        public let type: EntryType
        public let metadata: FileMetadata?

        @inlinable
        public init(offset: UInt32, length: UInt32, type: EntryType, metadata: FileMetadata? = nil) {
            self.offset = offset
            self.length = length
            self.type = type
            self.metadata = metadata
        }
    }

    /// 连续 UTF-8 路径数据存储
    public let pathData: [UInt8]
    /// 记录元数据数组
    public let records: [Record]

    public init(pathData: [UInt8], records: [Record]) {
        self.pathData = pathData
        self.records = records
    }

    /// 本批次记录总数
    @inlinable
    public var count: Int {
        records.count
    }

    /// 获取指定索引的节点类型
    @inlinable
    public func type(at index: Int) -> EntryType {
        records[index].type
    }

    /// 获取指定索引的元数据（`.paths` 模式下为 nil）
    @inlinable
    public func metadata(at index: Int) -> FileMetadata? {
        records[index].metadata
    }

    /// 在同步闭包内借用只读路径字节，零内存分配
    @inlinable
    public func withUTF8Path<R>(
        at index: Int,
        _ body: (UnsafeBufferPointer<UInt8>) throws -> R
    ) rethrows -> R {
        let record = records[index]
        return try pathData.withUnsafeBufferPointer { ptr in
            let slice = UnsafeBufferPointer(
                start: ptr.baseAddress! + Int(record.offset),
                count: Int(record.length)
            )
            return try body(slice)
        }
    }

    /// 显式将指定记录路径转换为 Swift String
    @inlinable
    public func relativePath(at index: Int) -> String {
        let record = records[index]
        return pathData.withUnsafeBufferPointer { ptr in
            let base = ptr.baseAddress! + Int(record.offset)
            return String(decoding: UnsafeBufferPointer(start: base, count: Int(record.length)), as: UTF8.self)
        }
    }

    /// 遍历批次中的每个条目，构造 ScanEntry 并传递给回调
    @inlinable
    public func forEach(_ body: (ScanEntry) throws -> Void) rethrows {
        for i in 0..<records.count {
            let entry = ScanEntry(
                type: records[i].type,
                relativePath: relativePath(at: i),
                metadata: records[i].metadata
            )
            try body(entry)
        }
    }

    /// 零拷贝遍历批次中的每个条目（直接借用 UTF-8 缓冲区）
    @inlinable
    public func withEachUTF8Path(
        _ body: (UnsafeBufferPointer<UInt8>, EntryType, FileMetadata?) throws -> Void
    ) rethrows {
        try pathData.withUnsafeBufferPointer { ptr in
            guard let base = ptr.baseAddress else { return }
            for record in records {
                let slice = UnsafeBufferPointer(
                    start: base + Int(record.offset),
                    count: Int(record.length)
                )
                try body(slice, record.type, record.metadata)
            }
        }
    }

    /// 直接访问底层连续字节缓冲区（用于整批写入 stdout/pipe 等高性能 I/O 场景）
    @inlinable
    public func withRawData<R>(_ body: (UnsafeBufferPointer<UInt8>) throws -> R) rethrows -> R {
        try pathData.withUnsafeBufferPointer(body)
    }
}
