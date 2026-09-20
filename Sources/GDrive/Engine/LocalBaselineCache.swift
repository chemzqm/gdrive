import Foundation

/// Local baseline cache entry for identifying unchanged files at breakneck speed
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

/// Rapid Change Detector (FastChangeDetector)
/// Follow the v1.md §6.2 Specification:
/// Use scanner `.basic` Mode Acquired FileMetadata (device + inode) and mtime + size Compare to baseline cache
/// When all match, the file is unmodified with a very high probability, skipping content reading and SHA-256 Calculated (Skip Rate > 95%)
public final class LocalBaselineCache: @unchecked Sendable {
    public struct Key: Hashable, Sendable {
        public let device: Int64
        public let inode: Int64

        public init(device: Int64, inode: Int64) {
            self.device = device
            self.inode = inode
        }
    }

    private var cache: [Key: [CachedItemMetadata]] = [:]
    private var lock = os_unfair_lock()

    public init() {}

    /// From SQLite Load Assignments rootId All valid document baseline metadata
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
                detector.cache[Key(device: dev, inode: ino), default: []].append(meta)
            }
            stmt.reset()
        }

        return detector
    }

    /// Check if the file is completely unchanged
    /// - Returns: If it does not change to return to the existing CachedItemMetadata,Go back if changed or as a new file nil
    public func lookupUnchanged(device: Int64, inode: Int64, mtime: Int64, size: Int64) -> CachedItemMetadata? {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }

        guard let cached = cache[Key(device: device, inode: inode)]?.first else {
            return nil
        }

        if cached.mtime == mtime && cached.size == size {
            return cached
        }

        return nil
    }

    func lookupUnchanged(
        device: Int64, inode: Int64, mtime: Int64, size: Int64,
        parentId: Int64, name: String
    ) -> CachedItemMetadata? {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }
        return cache[Key(device: device, inode: inode)]?.first {
            $0.parentId == parentId && $0.name == name && $0.mtime == mtime && $0.size == size
        }
    }

    /// Total number of current cache entries
    public var count: Int {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }
        return cache.values.reduce(0) { $0 + $1.count }
    }
}
