import Foundation

/// 本地基线缓存项，用于极速识别未改变的文件
public struct CachedItemMetadata: Sendable {
    public let itemId: Int64
    public let parentId: Int64?
    public let name: String
    public let remoteFileId: String?
    public let mtime: Int64
    public let size: Int64
    public let baseSha256: String?

    public init(
        itemId: Int64,
        parentId: Int64?,
        name: String,
        remoteFileId: String?,
        mtime: Int64,
        size: Int64,
        baseSha256: String?
    ) {
        self.itemId = itemId
        self.parentId = parentId
        self.name = name
        self.remoteFileId = remoteFileId
        self.mtime = mtime
        self.size = size
        self.baseSha256 = baseSha256
    }
}

/// 快速变更检测器（FastChangeDetector）
/// 遵循 v1.md §6.2 规范：
/// 利用 scanner `.basic` 模式获取的 FileMetadata (device + inode) 以及 mtime + size 与基线缓存比对
/// 全部匹配时，该文件以极高概率未被修改，直接跳过内容读取与 SHA-256 计算（跳过率 > 95%）
public final class LocalBaselineCache: @unchecked Sendable {
    public struct Key: Hashable, Sendable {
        public let device: Int64
        public let inode: Int64

        public init(device: Int64, inode: Int64) {
            self.device = device
            self.inode = inode
        }
    }

    private var cache: [Key: CachedItemMetadata] = [:]
    private var lock = os_unfair_lock()

    public init() {}

    /// 从 SQLite 加载指定 rootId 的全部有效文件基线元数据
    public static func load(store: StateStore, rootId: Int64) async throws -> LocalBaselineCache {
        let detector = LocalBaselineCache()

        try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT item_id, parent_id, name, remote_file_id, local_device, local_inode, local_mtime, local_size, base_sha256
            FROM items
            WHERE root_id = ?
              AND entry_kind = 'file'
              AND is_tombstone = 0
              AND local_inode IS NOT NULL
              AND phase = 'committed'
              AND base_sha256 IS NOT NULL
              AND dirty_generation = 0
              AND remote_status = 'present'
              AND remote_file_id IS NOT NULL;
            """)
            stmt.bindInt64(rootId, at: 1)

            while try stmt.step() {
                guard let itemId = stmt.columnInt64(at: 0),
                      let name = stmt.columnText(at: 2),
                      let dev = stmt.columnInt64(at: 4),
                      let ino = stmt.columnInt64(at: 5),
                      let mtime = stmt.columnInt64(at: 6),
                      let size = stmt.columnInt64(at: 7) else {
                    continue
                }

                let parentId = stmt.columnInt64(at: 1)
                let remoteId = stmt.columnText(at: 3)
                let sha256 = stmt.columnText(at: 8)

                let meta = CachedItemMetadata(
                    itemId: itemId,
                    parentId: parentId,
                    name: name,
                    remoteFileId: remoteId,
                    mtime: mtime,
                    size: size,
                    baseSha256: sha256
                )
                detector.cache[Key(device: dev, inode: ino)] = meta
            }
            stmt.reset()
        }

        return detector
    }

    /// 检查文件是否完全未变
    /// - Returns: 若未变返回既有 CachedItemMetadata，若已变更或为新文件则返回 nil
    public func lookupUnchanged(device: Int64, inode: Int64, mtime: Int64, size: Int64) -> CachedItemMetadata? {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }

        guard let cached = cache[Key(device: device, inode: inode)] else {
            return nil
        }

        if cached.mtime == mtime && cached.size == size {
            return cached
        }

        return nil
    }

    /// 当前缓存条目总数
    public var count: Int {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }
        return cache.count
    }
}
