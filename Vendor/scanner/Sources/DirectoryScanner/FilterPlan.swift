import Foundation
import CNameMatcher

/// 预编译的名称过滤计划（支持 * 通配符）
public struct FilterPlan: Sendable {
    /// C 规则在构建后不可变，指针由该对象独占管理，可安全跨 worker 共享。
    private final class BytePattern: @unchecked Sendable {
        let pointer: OpaquePointer

        init(_ name: String) {
            let bytes = Array(name.utf8)
            guard let pointer = bytes.withUnsafeBufferPointer({
                scanner_name_pattern_create($0.baseAddress!, $0.count)
            }) else { fatalError("Unable to allocate name filter") }
            self.pointer = pointer
        }

        deinit { scanner_name_pattern_destroy(pointer) }
    }

    private struct RuleItem: Sendable {
        let pattern: BytePattern
        let normalized: BytePattern?
        let wildcard: Bool

        init(_ name: String) {
            pattern = BytePattern(name)
            wildcard = name.utf8.contains(42)
            normalized = name.utf8.contains { $0 >= 128 }
                ? BytePattern(name.decomposedStringWithCanonicalMapping) : nil
        }
    }

    /// 是否存在任何过滤规则
    public let hasFilters: Bool
    /// 是否存在包含规则
    public let hasIncludes: Bool

    /// 目录排除规则列表（来自 excludeDirectory 和 excludeName）
    private let directoryExcludes: [RuleItem]
    /// 目录排除字符串集合（用于 Unicode 规范等价匹配）
    private let directoryExcludeStrings: Set<String>

    /// 文件排除规则列表（来自 excludeFile 和 excludeName）
    private let fileExcludes: [RuleItem]
    /// 文件排除字符串集合
    private let fileExcludeStrings: Set<String>

    /// 文件包含规则列表（来自 includeFile）
    private let fileIncludes: [RuleItem]
    /// 文件包含字符串集合
    private let fileIncludeStrings: Set<String>

    public init(filters: [FilterRule]) throws {
        if filters.isEmpty {
            self.hasFilters = false
            self.hasIncludes = false
            self.directoryExcludes = []
            self.directoryExcludeStrings = []
            self.fileExcludes = []
            self.fileExcludeStrings = []
            self.fileIncludes = []
            self.fileIncludeStrings = []
            return
        }

        self.hasFilters = true
        var dirEx: [RuleItem] = []
        var dirExStr: Set<String> = []
        var fileEx: [RuleItem] = []
        var fileExStr: Set<String> = []
        var fileInc: [RuleItem] = []
        var fileIncStr: Set<String> = []

        for filter in filters {
            let name: String
            switch filter {
            case .excludeDirectory(let s): name = s
            case .excludeFile(let s): name = s
            case .excludeName(let s): name = s
            case .includeFile(let s): name = s
            }

            // 规则有效性校验
            if name.isEmpty || name == "." || name == ".." || name.contains("/") || name.contains("\0") {
                throw ScanError.invalidFilter(name)
            }

            let item = RuleItem(name)
            let wildcard = item.wildcard

            switch filter {
            case .excludeDirectory:
                dirEx.append(item)
                if !wildcard { dirExStr.insert(name) }
            case .excludeFile:
                fileEx.append(item)
                if !wildcard { fileExStr.insert(name) }
            case .excludeName:
                dirEx.append(item)
                if !wildcard { dirExStr.insert(name) }
                fileEx.append(item)
                if !wildcard { fileExStr.insert(name) }
            case .includeFile:
                fileInc.append(item)
                if !wildcard { fileIncStr.insert(name) }
            }
        }

        self.directoryExcludes = dirEx
        self.directoryExcludeStrings = dirExStr
        self.fileExcludes = fileEx
        self.fileExcludeStrings = fileExStr
        self.fileIncludes = fileInc
        self.fileIncludeStrings = fileIncStr
        self.hasIncludes = !fileInc.isEmpty
    }

    /// 检查是否排除该目录名称（如果匹配则直接剪枝）
    public func shouldExcludeDirectory(namePtr: UnsafePointer<UInt8>, length: Int) -> Bool {
        guard hasFilters, !directoryExcludes.isEmpty else { return false }
        return matches(
            namePtr: namePtr,
            length: length,
            items: directoryExcludes,
            stringSet: directoryExcludeStrings
        )
    }

    /// 检查是否排除该文件名称
    public func shouldExcludeFile(namePtr: UnsafePointer<UInt8>, length: Int) -> Bool {
        guard hasFilters, !fileExcludes.isEmpty else { return false }
        return matches(
            namePtr: namePtr,
            length: length,
            items: fileExcludes,
            stringSet: fileExcludeStrings
        )
    }

    /// 检查该文件名称是否包含在 include 规则中
    public func shouldIncludeFile(namePtr: UnsafePointer<UInt8>, length: Int) -> Bool {
        guard hasIncludes else { return true }
        return matches(
            namePtr: namePtr,
            length: length,
            items: fileIncludes,
            stringSet: fileIncludeStrings
        )
    }

    /// 针对单层 basename 的高性能比对（先连续内存字节快路径，未命中或非 ASCII 时走 Unicode 等价慢路径）
    private func matches(
        namePtr: UnsafePointer<UInt8>,
        length: Int,
        items: [RuleItem],
        stringSet: Set<String>
    ) -> Bool {
        // 1. ASCII 字节快速比对
        for i in 0..<items.count {
            let item = items[i]
            if scanner_name_pattern_matches(item.pattern.pointer, namePtr, length) { return true }
            if let normalized = item.normalized,
               scanner_name_pattern_matches(normalized.pointer, namePtr, length) { return true }
        }

        // 非 ASCII 名称才需要 Unicode 规范等价慢路径。
        if scanner_name_is_ascii(namePtr, length) { return false }

        // 2. Unicode 规范等价慢路径（处理不同 NFD/NFC 等价表示）
        let str = String(decoding: UnsafeBufferPointer(start: namePtr, count: length), as: UTF8.self)
        if stringSet.contains(str) { return true }
        guard items.contains(where: { $0.wildcard }) else { return false }
        let normalized = Array(str.decomposedStringWithCanonicalMapping.utf8)
        return normalized.withUnsafeBufferPointer { buffer in
            guard let base = buffer.baseAddress else { return false }
            return items.contains { $0.wildcard && scanner_name_pattern_matches(($0.normalized ?? $0.pattern).pointer, base, buffer.count) }
        }
    }
}
