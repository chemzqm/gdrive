import Darwin
import Foundation
import CommonCrypto
import Logging
import DirectoryScanner

/// 同步进度统计指标
public struct SyncStats: Sendable {
    public var filesScanned: Int = 0
    public var filesSkipped: Int = 0
    public var directoriesCreated: Int = 0
    public var filesUploaded: Int = 0
    public var bytesUploaded: Int64 = 0
    public var filesDownloaded: Int = 0
    public var bytesDownloaded: Int64 = 0
    public var filesDeleted: Int = 0
    public var conflictsResolved: Int = 0
    public var filesFailed: Int = 0
    public var elapsedSeconds: Double = 0
}

/// SyncEngine 异常类型定义
public enum SyncEngineError: Error, LocalizedError, CustomStringConvertible, Sendable, Equatable {
    case localRootNotFound(path: String)
    case remoteRootLost(remoteId: String, reason: String)
    case rootNotConfigured(remoteId: String)
    case invalidDirectory(path: String)
    case general(String)

    public var errorDescription: String? {
        description
    }

    public var description: String {
        switch self {
        case .localRootNotFound(let path):
            return "本地同步根目录已不存在或不是有效目录: \(path)，已终止同步以保护云端文件不被扩散删除"
        case .remoteRootLost(let remoteId, let reason):
            return "远端同步根目录已被移除或移入回收站 (\(reason)): \(remoteId)，已终止同步以保护本地文件不被扩散删除"
        case .rootNotConfigured(let remoteId):
            return "未找到对应的同步根，请先执行初始化同步: \(remoteId)"
        case .invalidDirectory(let path):
            return "路径不是有效目录: \(path)"
        case .general(let msg):
            return msg
        }
    }
}

/// GDrive 核心同步引擎
/// 遵循 v1.md 与 AGENTS.md 规范：
/// - 仅支持本地目录到远程空目录同步 (localToRemoteEmpty) 和远程目录到本地空目录同步 (remoteToLocalEmpty)
/// - 以 SQLite 为唯一同步基线，以文件 SHA-256 为唯一判定依据
/// - 边扫描、边建目录、边上传的流式极速管道，首文件零延迟发出
/// - 大于 8MB 文件分块断点续传，≤ 8MB 文件 Multipart 一步上传
/// - 预分配 ID 内存池，0 网络延迟出 ID
public final class SyncEngine: Sendable {
    public let auth: Auth
    public let store: StateStore
    public let client: DriveClient
    public let idPool: IDPool
    private let logger = Logger(label: "gdrive.engine")

    public init(
        auth: Auth,
        store: StateStore? = nil,
        client: DriveClient? = nil,
        idPool: IDPool? = nil
    ) async throws {
        self.auth = auth
        let effectiveClient = client ?? DriveClient(auth: auth)
        self.client = effectiveClient
        if let store {
            self.store = store
        } else {
            self.store = try await StateStore()
        }
        self.idPool = idPool ?? IDPool(api: effectiveClient)
    }

    /// 实时传输与速率监控器
    public let monitor = TransferMonitor()

    /// 获取当前正在传输、队列中等待以及实时滑动速度（每秒字节）的内存快照（每 500ms 自动刷新）
    public var transferStatus: TransferSnapshot {
        monitor.getSnapshot()
    }

    /// 外部直接调用获取当前传输状态快照
    public func getTransferStatus() -> TransferSnapshot {
        monitor.getSnapshot()
    }

    // MARK: - 统一同步入口 (自动状态探测与方向分流)

    /// 统一双向同步入口：
    /// - 自动检测该目录对在本地 SQLite 中是否已有同步基线
    /// - 若已有基线：自动执行极速双向增量同步 (syncIncremental)
    /// - 若首次同步：自动向云端与本地发起状态探测：
    ///   - 本地有内容且云端为空：自动执行 localToRemoteEmpty 流式全量上传
    ///   - 云端有内容且本地为空：自动执行 remoteToLocalEmpty 流式全量下载
    ///   - 双端均为空：注册初始空基线
    ///   - 双端均非空：抛出明确异常保护，杜绝无基线盲合并导致的数据覆盖
    @discardableResult
    public func sync(
        localPath: String,
        remoteFolderId: String,
        concurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath

        // 1. 检查 SQLite 是否已存在处于激活状态的同步根
        let existingRootId: Int64? = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT root_id FROM roots
            WHERE local_root_path = ? AND remote_root_id = ? AND is_active = 1;
            """)
            stmt.bindText(resolvedLocalPath, at: 1)
            stmt.bindText(remoteFolderId, at: 2)
            defer { stmt.reset() }
            if try stmt.step() {
                return stmt.columnInt64(at: 0)
            }
            return nil
        }

        if existingRootId != nil {
            logger.info("[Sync] 已有共同同步基线，自动执行增量双向同步: \(resolvedLocalPath) <-> \(remoteFolderId)")
            return try await syncIncremental(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxConcurrency: concurrency, onProgress: onProgress)
        }

        // 2. 首次同步：自动探测本地与远端目录真实状态
        logger.info("[Sync] 未建立基线，开始探测本地与云端目录状态...")

        // 探测远端目录：验证存在、是否为目录、是否包含非回收站子项
        let remoteFile = try await client.getFile(remoteId: remoteFolderId)
        guard remoteFile.trashed != true else {
            throw SyncEngineError.remoteRootLost(remoteId: remoteFolderId, reason: "trashed")
        }
        guard remoteFile.isDirectory else {
            throw NSError(domain: "SyncEngine", code: 101, userInfo: [NSLocalizedDescriptionKey: "远端目标不是有效目录: \(remoteFolderId)"])
        }
        let remoteChildren = try await client.listChildren(parentId: remoteFolderId)
        let isRemoteEmpty = remoteChildren.isEmpty

        // 探测本地目录：是否包含非隐藏文件
        var isDir: ObjCBool = false
        let localExists = FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir)
        var isLocalEmpty = true
        if localExists && isDir.boolValue {
            let localContents = (try? FileManager.default.contentsOfDirectory(atPath: resolvedLocalPath)) ?? []
            let realFiles = localContents.filter { !$0.hasPrefix(".") }
            isLocalEmpty = realFiles.isEmpty
        } else if localExists && !isDir.boolValue {
            throw NSError(domain: "SyncEngine", code: 102, userInfo: [NSLocalizedDescriptionKey: "本地路径已存在但不是目录: \(resolvedLocalPath)"])
        }

        // 3. 根据探测结果安全分流
        if !isLocalEmpty && isRemoteEmpty {
            logger.info("[Sync] 检测到【本地包含文件，云端为空目录】，自动启动 localToRemoteEmpty 初始化上传")
            return try await syncLocalToRemoteEmpty(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxUploadConcurrency: concurrency, onProgress: onProgress)
        } else if isLocalEmpty && !isRemoteEmpty {
            logger.info("[Sync] 检测到【云端包含文件，本地为空目录】，自动启动 remoteToLocalEmpty 初始化下载")
            return try await syncRemoteToLocalEmpty(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxDownloadConcurrency: concurrency, onProgress: onProgress)
        } else if isLocalEmpty && isRemoteEmpty {
            logger.info("[Sync] 检测到【本地与云端均为空目录】，建立初始空基线")
            let now = Date().timeIntervalSince1970
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                INSERT INTO roots (
                    account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
                ) VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'freshCreated', ?, ?)
                ON CONFLICT (account_id, remote_root_id) DO UPDATE SET updated_at = excluded.updated_at;
                """)
                stmt.bindText(resolvedLocalPath, at: 1)
                stmt.bindText(remoteFolderId, at: 2)
                stmt.bindDouble(now, at: 3)
                stmt.bindDouble(now, at: 4)
                _ = try stmt.step()
                stmt.reset()
            }
            return SyncStats()
        } else {
            // 双端皆非空
            throw NSError(domain: "SyncEngine", code: 103, userInfo: [
                NSLocalizedDescriptionKey: "双向同步初始基线要求其中一端必须为空目录。当前检测到本地路径(\(resolvedLocalPath))与远端文件夹(\(remoteFolderId))均包含已有文件。为避免盲合并导致数据覆盖或大规模冲突，请指定空目录进行首次初始化。"
            ])
        }
    }

    // MARK: - 模式 1：本地目录 -> 远端空目录极速上传 (localToRemoteEmpty)

    /// 将本地非空目录流式全量同步至远端空目录
    @discardableResult
    public func syncLocalToRemoteEmpty(
        localPath: String,
        remoteRootId: String,
        maxUploadConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)

        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)

        // 验证本地根目录
        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir), isDir.boolValue else {
            throw NSError(domain: "SyncEngine", code: 1, userInfo: [NSLocalizedDescriptionKey: "本地路径不存在或不是目录: \(resolvedLocalPath)"])
        }

        // 验证远端根目录存在
        let remoteRoot = try await client.getFile(remoteId: remoteRootId)
        guard remoteRoot.isDirectory else {
            throw NSError(domain: "SyncEngine", code: 2, userInfo: [NSLocalizedDescriptionKey: "远端目标不是目录: \(remoteRootId)"])
        }

        // 仅在首次初始化未建立根基线时校验远端是否为空目录
        let rootExists: Bool = try await store.read { conn in
            let stmt = try conn.cachedStatement("SELECT 1 FROM roots WHERE remote_root_id = ? AND is_active = 1;")
            stmt.bindText(remoteRootId, at: 1)
            defer { stmt.reset() }
            return try stmt.step()
        }
        if !rootExists {
            let existingChildren = try await client.listChildren(parentId: remoteRootId)
            guard existingChildren.isEmpty else {
                throw NSError(domain: "SyncEngine", code: 20, userInfo: [NSLocalizedDescriptionKey: "远端目标不是空目录，无法执行 localToRemoteEmpty: \(remoteRootId)"])
            }
        }

        let now = Date().timeIntervalSince1970

        // 1. 注册或获取 Root 记录与根目录项
        let (rootId, rootItemId): (Int64, Int64) = try await store.write { conn in
            let rootStmt = try conn.cachedStatement("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', ?, 1, 1, ?, 'localToRemoteEmpty', 'freshCreated', ?, ?)
            ON CONFLICT (account_id, remote_root_id) DO UPDATE SET updated_at = excluded.updated_at;
            """)
            rootStmt.bindText(resolvedLocalPath, at: 1)
            rootStmt.bindText(remoteRootId, at: 2)
            rootStmt.bindDouble(now, at: 3)
            rootStmt.bindDouble(now, at: 4)
            _ = try rootStmt.step()
            rootStmt.reset()

            let rootQuery = try conn.cachedStatement("SELECT root_id FROM roots WHERE account_id = 'default' AND remote_root_id = ?;")
            rootQuery.bindText(remoteRootId, at: 1)
            guard try rootQuery.step(), let rId = rootQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 3, userInfo: [NSLocalizedDescriptionKey: "无法获取 root_id"])
            }
            rootQuery.reset()

            let itemStmt = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (?, NULL, ?, 'directory', ?, ?, ?)
            ON CONFLICT (root_id) WHERE parent_id IS NULL AND is_tombstone = 0 DO UPDATE SET updated_at = excluded.updated_at;
            """)
            itemStmt.bindInt64(rId, at: 1)
            itemStmt.bindText(rootURL.lastPathComponent, at: 2)
            itemStmt.bindText(remoteRootId, at: 3)
            itemStmt.bindDouble(now, at: 4)
            itemStmt.bindDouble(now, at: 5)
            _ = try itemStmt.step()
            itemStmt.reset()

            let itemQuery = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL AND is_tombstone = 0;")
            itemQuery.bindInt64(rId, at: 1)
            guard try itemQuery.step(), let rItemId = itemQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 4, userInfo: [NSLocalizedDescriptionKey: "无法获取 root item_id"])
            }
            itemQuery.reset()

            return (rId, rItemId)
        }

        // 获取并持久化远端 Changes 起始 token C0 (§3.3)
        if let startPageToken = try? await client.getStartPageToken() {
            try? await store.write { conn in
                let cursorStmt = try conn.cachedStatement("""
                INSERT INTO cursors (
                    root_id, account_id, cursor_kind, token_value, updated_at
                ) VALUES (?, 'default', 'drive_changes', ?, ?)
                ON CONFLICT (root_id, cursor_kind) DO UPDATE SET token_value = excluded.token_value, updated_at = excluded.updated_at;
                """)
                cursorStmt.bindInt64(rootId, at: 1)
                cursorStmt.bindText(startPageToken, at: 2)
                cursorStmt.bindDouble(now, at: 3)
                _ = try cursorStmt.step()
                cursorStmt.reset()
            }
        }

        // 2. 初始化目录异步唤醒调度器（根目录预设为已就绪）
        let directoryTracker = DirectoryTracker(remoteRootId: remoteRootId)

        // 记录本地目录相对路径与其数据库 itemId
        final class LocalDirMap: @unchecked Sendable {
            private var map: [String: Int64] = [:]
            private var lock = os_unfair_lock()

            init(rootItemId: Int64) {
                map[""] = rootItemId
            }

            func set(_ path: String, id: Int64) {
                os_unfair_lock_lock(&lock)
                map[path] = id
                os_unfair_lock_unlock(&lock)
            }

            func get(_ path: String) -> Int64? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return map[path]
            }
        }
        let localDirMap = LocalDirMap(rootItemId: rootItemId)

        // 3. 设置有界并发上传流水线（严格上限 64 并发）
        let effectiveConcurrency = max(1, min(64, maxUploadConcurrency))
        let uploadSemaphore = AsyncSemaphore(count: effectiveConcurrency)
        let uploadGroup = DispatchGroup()

        // 加载快速变更比对基线缓存 (§6.2)
        let baselineCache = try await LocalBaselineCache.load(store: store, rootId: rootId)

        final class ProgressTracker: @unchecked Sendable {
            private var _filesUploaded = 0
            private var _filesSkipped = 0
            private var _filesFailed = 0
            private var _bytesUploaded: Int64 = 0
            private var _dirsCreated = 0
            private var lock = os_unfair_lock()

            var filesUploaded: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _filesUploaded
            }

            var filesSkipped: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _filesSkipped
            }

            var filesFailed: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _filesFailed
            }

            var bytesUploaded: Int64 {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _bytesUploaded
            }

            var dirsCreated: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return _dirsCreated
            }

            func recordSuccess(bytes: Int64) {
                os_unfair_lock_lock(&lock)
                _filesUploaded += 1
                _bytesUploaded += bytes
                os_unfair_lock_unlock(&lock)
            }

            func recordFailure() {
                os_unfair_lock_lock(&lock)
                _filesFailed += 1
                os_unfair_lock_unlock(&lock)
            }

            func recordSkipped() {
                os_unfair_lock_lock(&lock)
                _filesSkipped += 1
                os_unfair_lock_unlock(&lock)
            }

            func recordDirCreated() {
                os_unfair_lock_lock(&lock)
                _dirsCreated += 1
                os_unfair_lock_unlock(&lock)
            }
        }
        let progress = ProgressTracker()

        // 4. 启动 DirectoryScanner 并发扫描
        let staticPrefix = resolvedLocalPath.hasSuffix("/") ? resolvedLocalPath : resolvedLocalPath + "/"
        let prefixBytes = Array(staticPrefix.utf8)

        let scanOptions = ScanOptions(
            mode: .basic,
            emission: .all,
            includeHidden: true,
            workers: min(6, ProcessInfo.processInfo.activeProcessorCount),
            batchCapacity: 512,
            delimiter: 0x00,
            pathPrefix: prefixBytes
        )
        let scanFilters: [FilterRule] = [.excludeDirectory(".git")]
        let request = ScanRequest(root: resolvedLocalPath, filters: scanFilters, options: scanOptions)
        let scanner = DirectoryScanner()

        _ = try await scanner.scan(request) { batch in
            batch.withRawData { rawBuf in
                guard let basePtr = rawBuf.baseAddress else { return }

                for idx in 0..<batch.count {
                    let record = batch.records[idx]
                    let rawPtr = UnsafeRawPointer(basePtr + Int(record.offset))
                    let cPath = rawPtr.assumingMemoryBound(to: CChar.self)
                    let fullPath = String(cString: cPath)

                    let relPath: String
                    if fullPath.hasPrefix(staticPrefix) {
                        relPath = String(fullPath.dropFirst(staticPrefix.count))
                    } else {
                        relPath = (fullPath as NSString).lastPathComponent
                    }

                    if relPath.isEmpty || relPath == ".git" || relPath.hasPrefix(".git/") { continue }

                    let parentRel = (relPath as NSString).deletingLastPathComponent
                    let name = (relPath as NSString).lastPathComponent

                    if record.type == .directory {
                        // 目录处理：排队等待其父目录就绪后立即在远端创建
                        uploadGroup.enter()
                        Task {
                            defer { uploadGroup.leave() }
                            var createIntent: DurableCreateIntent?
                            do {
                                let candidateRemoteId = try await self.idPool.nextId()
                                let remoteParentId = await directoryTracker.awaitParentReady(parentRelPath: parentRel)
                                let grandParentItemId = localDirMap.get(parentRel) ?? rootItemId
                                let intent = try await DurableCreateIntentStore.prepareDirectory(
                                    store: self.store,
                                    rootID: rootId,
                                    parentItemID: grandParentItemId,
                                    name: name,
                                    targetParentRemoteID: remoteParentId,
                                    candidateRemoteID: candidateRemoteId,
                                    device: Int64(record.metadata?.identity.device ?? 1),
                                    inode: Int64(record.metadata?.identity.inode ?? 0)
                                )
                                createIntent = intent

                                // Intent 的 group-commit 已确认后，才允许发出远端创建请求。
                                _ = try await self.client.createDirectory(
                                    name: name,
                                    parentId: intent.targetParentRemoteID,
                                    remoteId: intent.targetRemoteID
                                )

                                try await self.store.batchWrite { conn in
                                    let ts = Date().timeIntervalSince1970
                                    let stmt = try conn.cachedStatement("""
                                    UPDATE items SET
                                        remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                                    WHERE item_id = ?;
                                    """)
                                    stmt.bindDouble(ts, at: 1)
                                    stmt.bindInt64(intent.itemID, at: 2)
                                    _ = try stmt.step()
                                    stmt.reset()
                                    try DurableCreateIntentStore.completeOperation(conn: conn, operationID: intent.operationID, now: ts)
                                }
                                localDirMap.set(relPath, id: intent.itemID)

                                // 广播唤醒等待该目录的全部子项
                                await directoryTracker.markDirectoryReady(relPath: relPath, remoteId: intent.targetRemoteID)
                                progress.recordDirCreated()
                            } catch {
                                if let createIntent {
                                    await DurableCreateIntentStore.markUnknownOutcome(
                                        store: self.store,
                                        operationID: createIntent.operationID,
                                        error: error
                                    )
                                }
                                self.logger.error("创建远端目录失败 [\(relPath)]: \(error)")
                            }
                        }
                    } else if record.type == .file {
                        let dev = Int64(record.metadata?.identity.device ?? 1)
                        let ino = Int64(record.metadata?.identity.inode ?? 0)
                        let mtime = (record.metadata?.modificationTime.seconds ?? 0) * 1_000_000_000 + Int64(record.metadata?.modificationTime.nanoseconds ?? 0)
                        let fileSize = record.metadata?.fileSize ?? 0

                        // 快速变更检测 (§6.2)：dev + inode + mtime + size 匹配即跳过内容读取和哈希计算
                        if let _ = baselineCache.lookupUnchanged(device: dev, inode: ino, mtime: mtime, size: fileSize) {
                            progress.recordSkipped()
                            continue
                        }

                        // 文件处理：等待其直接父目录就绪后立即上传
                        notifier.addDiscovered(files: 1, bytes: Int64(fileSize))
                        self.monitor.enqueueUpload(id: fullPath, name: name, totalBytes: Int64(record.metadata?.fileSize ?? 0))
                        uploadGroup.enter()
                        Task {
                            var createIntent: DurableCreateIntent?
                            // 先等待直接父目录在 Google Drive 远端就绪，避免空占并发上传槽位
                            let remoteParentId = await directoryTracker.awaitParentReady(parentRelPath: parentRel)

                            await uploadSemaphore.wait()
                            defer {
                                self.monitor.finishUpload(id: fullPath)
                                uploadSemaphore.signal()
                                uploadGroup.leave()
                                notifier.addCompleted(files: 1, bytes: Int64(fileSize))
                            }

                            do {
                                let fileURL = URL(fileURLWithPath: fullPath)
                                let limit8MB: Int64 = 8 * 1024 * 1024
                                let metaSize = Int64(record.metadata?.fileSize ?? 0)

                                let sha256Hex: String
                                let fileSize: Int64
                                let smallContent: Data?

                                if metaSize <= limit8MB {
                                    // 小文件 (≤ 8MB)：单次磁盘读取装入内存，并在内存计算 SHA-256（彻底杜绝二次读盘）
                                    let data = try Data(contentsOf: fileURL)
                                    let actualSize = Int64(data.count)
                                    if actualSize <= limit8MB {
                                        fileSize = actualSize
                                        sha256Hex = SyncEngine.computeSha256(of: data)
                                        smallContent = data
                                    } else {
                                        let (s, sz) = try SyncEngine.computeFileSha256(at: fileURL)
                                        sha256Hex = s
                                        fileSize = sz
                                        smallContent = nil
                                    }
                                } else {
                                    let (s, sz) = try SyncEngine.computeFileSha256(at: fileURL)
                                    sha256Hex = s
                                    fileSize = sz
                                    smallContent = nil
                                }

                                self.monitor.startUpload(id: fullPath, name: name, totalBytes: fileSize)

                                let dev = Int64(record.metadata?.identity.device ?? 1)
                                let ino = Int64(record.metadata?.identity.inode ?? 0)
                                let mtime = (record.metadata?.modificationTime.seconds ?? 0) * 1_000_000_000 + Int64(record.metadata?.modificationTime.nanoseconds ?? 0)

                                let parentDirItemId = localDirMap.get(parentRel) ?? rootItemId

                                // 根据文件大小执行上传：≤ 8MB 走 Multipart，> 8MB 走 Resumable
                                if fileSize <= limit8MB, let content = smallContent {
                                    let committedTarget: (itemID: Int64, remoteID: String)? = try await self.store.read { conn in
                                        let stmt = try conn.cachedStatement("""
                                        SELECT item_id, remote_file_id
                                        FROM items
                                        WHERE root_id = ? AND parent_id = ? AND name = ?
                                          AND is_tombstone = 0 AND phase = 'committed'
                                          AND remote_status = 'present' AND remote_file_id IS NOT NULL;
                                        """)
                                        stmt.bindInt64(rootId, at: 1)
                                        stmt.bindInt64(parentDirItemId, at: 2)
                                        stmt.bindText(name, at: 3)
                                        defer { stmt.reset() }
                                        guard try stmt.step(),
                                              let itemID = stmt.columnInt64(at: 0),
                                              let remoteID = stmt.columnText(at: 1) else { return nil }
                                        return (itemID, remoteID)
                                    }

                                    let uploadedFile: DriveFile
                                    let committedItemID: Int64
                                    if let committedTarget {
                                        // 已提交对象是内容更新，不创建新的 Drive 对象或 create intent。
                                        uploadedFile = try await self.client.updateMultipart(
                                            remoteId: committedTarget.remoteID,
                                            content: content,
                                            expectedSha256: sha256Hex
                                        )
                                        committedItemID = committedTarget.itemID
                                    } else {
                                        let candidateRemoteID = try await self.idPool.nextId()
                                        let intent = try await DurableCreateIntentStore.prepareMultipartUpload(
                                            store: self.store,
                                            rootID: rootId,
                                            parentItemID: parentDirItemId,
                                            name: name,
                                            targetParentRemoteID: remoteParentId,
                                            candidateRemoteID: candidateRemoteID,
                                            device: dev,
                                            inode: ino,
                                            mtime: mtime,
                                            size: fileSize,
                                            sha256: sha256Hex
                                        )
                                        createIntent = intent
                                        uploadedFile = try await self.client.uploadMultipart(
                                            name: name,
                                            parentId: intent.targetParentRemoteID,
                                            remoteId: intent.targetRemoteID,
                                            content: content,
                                            expectedSha256: sha256Hex
                                        )
                                        committedItemID = intent.itemID
                                    }
                                    self.monitor.reportUploadProgress(id: fullPath, additionalBytes: fileSize)

                                    // 上传成功后，通过 batchWrite（Group Commit，合并 256 项或 5ms 刷盘）写入基线
                                    let completedCreateIntent = createIntent
                                    try await self.store.batchWrite { conn in
                                        let itemStmt = try conn.cachedStatement("""
                                        UPDATE items SET
                                            remote_file_id = ?,
                                            local_device = ?, local_inode = ?, local_mtime = ?,
                                            local_size = ?, local_sha256 = ?,
                                            base_sha256 = ?, base_size = ?,
                                            remote_sha256 = ?, remote_size = ?, remote_status = 'present',
                                            local_status = 'present', phase = 'committed', dirty_generation = 0,
                                            updated_at = ?
                                        WHERE item_id = ?;
                                        """)
                                        itemStmt.bindText(uploadedFile.id, at: 1)
                                        itemStmt.bindInt64(dev, at: 2)
                                        itemStmt.bindInt64(ino, at: 3)
                                        itemStmt.bindInt64(mtime, at: 4)
                                        itemStmt.bindInt64(fileSize, at: 5)
                                        itemStmt.bindText(sha256Hex, at: 6)
                                        itemStmt.bindText(sha256Hex, at: 7)
                                        itemStmt.bindInt64(fileSize, at: 8)
                                        itemStmt.bindText(sha256Hex, at: 9)
                                        itemStmt.bindInt64(fileSize, at: 10)
                                        let ts = Date().timeIntervalSince1970
                                        itemStmt.bindDouble(ts, at: 11)
                                        itemStmt.bindInt64(committedItemID, at: 12)
                                        _ = try itemStmt.step()
                                        itemStmt.reset()
                                        if let createIntent = completedCreateIntent {
                                            try DurableCreateIntentStore.completeOperation(
                                                conn: conn,
                                                operationID: createIntent.operationID,
                                                now: ts
                                            )
                                        }
                                    }
                                } else {
                                    // 大文件 (> 8MB)：上传前单次写入 inFlight 状态，获取 itemId 以支持断点分块续传
                                    let remoteFileId = try await self.idPool.nextId()
                                    let currentItemId: Int64 = try await self.store.write { conn in
                                        let itemStmt = try conn.cachedStatement("""
                                        INSERT INTO items (
                                            root_id, parent_id, name, entry_kind, remote_file_id,
                                            local_device, local_inode, local_mtime, local_size, local_sha256,
                                            local_generation, local_status, phase, dirty_generation,
                                            created_at, updated_at
                                        ) VALUES (
                                            ?, ?, ?, 'file', ?,
                                            ?, ?, ?, ?, ?,
                                            1, 'present', 'inFlight', 1,
                                            ?, ?
                                        )
                                        ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                                        DO UPDATE SET
                                            remote_file_id = excluded.remote_file_id,
                                            local_device = excluded.local_device,
                                            local_inode = excluded.local_inode,
                                            local_mtime = excluded.local_mtime,
                                            local_size = excluded.local_size,
                                            local_sha256 = excluded.local_sha256,
                                            phase = excluded.phase,
                                            dirty_generation = excluded.dirty_generation,
                                            updated_at = excluded.updated_at;
                                        """)
                                        itemStmt.bindInt64(rootId, at: 1)
                                        itemStmt.bindInt64(parentDirItemId, at: 2)
                                        itemStmt.bindText(name, at: 3)
                                        itemStmt.bindText(remoteFileId, at: 4)
                                        itemStmt.bindInt64(dev, at: 5)
                                        itemStmt.bindInt64(ino, at: 6)
                                        itemStmt.bindInt64(mtime, at: 7)
                                        itemStmt.bindInt64(fileSize, at: 8)
                                        itemStmt.bindText(sha256Hex, at: 9)
                                        let ts = Date().timeIntervalSince1970
                                        itemStmt.bindDouble(ts, at: 10)
                                        itemStmt.bindDouble(ts, at: 11)
                                        _ = try itemStmt.step()
                                        itemStmt.reset()

                                        let qStmt = try conn.cachedStatement("""
                                        SELECT item_id FROM items
                                        WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;
                                        """)
                                        qStmt.bindInt64(rootId, at: 1)
                                        qStmt.bindInt64(parentDirItemId, at: 2)
                                        qStmt.bindText(name, at: 3)
                                        defer { qStmt.reset() }
                                        if try qStmt.step(), let id = qStmt.columnInt64(at: 0) {
                                            return id
                                        }
                                        return 0
                                    }

                                    // 大文件 (> 8MB) Resumable 8MB 流式分块断点续传（每块落盘 offset）
                                    _ = try await self.performResumableUpload(
                                        rootId: rootId,
                                        itemId: currentItemId,
                                        fileURL: fileURL,
                                        fileSize: fileSize,
                                        expectedSha256: sha256Hex,
                                        remoteId: remoteFileId,
                                        parentId: remoteParentId,
                                        name: name,
                                        isUpdate: false
                                    )

                                    // 提交共同基线 B
                                    try await self.store.batchWrite { conn in
                                        let updateStmt = try conn.cachedStatement("""
                                        UPDATE items SET
                                            base_sha256 = ?,
                                            base_size = ?,
                                            remote_sha256 = ?,
                                            remote_size = ?,
                                            remote_status = 'present',
                                            phase = 'committed',
                                            dirty_generation = 0,
                                            updated_at = ?
                                        WHERE root_id = ? AND remote_file_id = ?;
                                        """)
                                        updateStmt.bindText(sha256Hex, at: 1)
                                        updateStmt.bindInt64(fileSize, at: 2)
                                        updateStmt.bindText(sha256Hex, at: 3)
                                        updateStmt.bindInt64(fileSize, at: 4)
                                        updateStmt.bindDouble(Date().timeIntervalSince1970, at: 5)
                                        updateStmt.bindInt64(rootId, at: 6)
                                        updateStmt.bindText(remoteFileId, at: 7)
                                        _ = try updateStmt.step()
                                        updateStmt.reset()
                                    }
                                }

                                progress.recordSuccess(bytes: fileSize)
                            } catch {
                                if let createIntent {
                                    await DurableCreateIntentStore.markUnknownOutcome(
                                        store: self.store,
                                        operationID: createIntent.operationID,
                                        error: error
                                    )
                                }
                                progress.recordFailure()
                                self.logger.error("上传文件失败 [\(relPath)]: \(error)")
                            }
                        }
                    }
                }
            }
        }

        // 5. 等待所有并发目录创建与上传完成
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            uploadGroup.notify(queue: .global(qos: .userInitiated)) {
                cont.resume()
            }
        }

        // 6. 强制将缓冲区写入磁盘并执行 WAL checkpoint
        try await store.flush()
        try await store.checkpoint()

        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        stats.directoriesCreated = progress.dirsCreated
        stats.filesUploaded = progress.filesUploaded
        stats.filesSkipped = progress.filesSkipped
        stats.filesFailed = progress.filesFailed
        stats.bytesUploaded = progress.bytesUploaded
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }

    // MARK: - 模式 2：远端目录 -> 本地空目录极速下载 (remoteToLocalEmpty)

    /// 将远端非空目录流式全量下载同步至本地空目录
    @discardableResult
    public func syncRemoteToLocalEmpty(
        localPath: String,
        remoteRootId: String,
        maxDownloadConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)

        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)

        // 验证远端根目录存在
        let remoteRoot = try await client.getFile(remoteId: remoteRootId)
        guard remoteRoot.isDirectory else {
            throw NSError(domain: "SyncEngine", code: 10, userInfo: [NSLocalizedDescriptionKey: "远端目标不是有效目录: \(remoteRootId)"])
        }

        // 确保本地目录存在且为空
        var isDir: ObjCBool = false
        if FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir) {
            guard isDir.boolValue else {
                throw NSError(domain: "SyncEngine", code: 11, userInfo: [NSLocalizedDescriptionKey: "本地路径已存在且不是目录: \(resolvedLocalPath)"])
            }
            let contents = try FileManager.default.contentsOfDirectory(atPath: resolvedLocalPath)
            guard contents.isEmpty else {
                throw NSError(domain: "SyncEngine", code: 12, userInfo: [NSLocalizedDescriptionKey: "本地目标目录必须为空: \(resolvedLocalPath)"])
            }
        } else {
            try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        }

        let now = Date().timeIntervalSince1970

        // 1. 注册或获取 Root 记录与根目录项
        let (rootId, rootItemId): (Int64, Int64) = try await store.write { conn in
            let rootStmt = try conn.cachedStatement("""
            INSERT INTO roots (
                account_id, local_root_path, local_root_device, local_root_inode,
                remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
            ) VALUES ('default', ?, 1, 1, ?, 'remoteToLocalEmpty', 'freshCreated', ?, ?)
            ON CONFLICT (account_id, remote_root_id) DO UPDATE SET updated_at = excluded.updated_at;
            """)
            rootStmt.bindText(resolvedLocalPath, at: 1)
            rootStmt.bindText(remoteRootId, at: 2)
            rootStmt.bindDouble(now, at: 3)
            rootStmt.bindDouble(now, at: 4)
            _ = try rootStmt.step()
            rootStmt.reset()

            let rootQuery = try conn.cachedStatement("SELECT root_id FROM roots WHERE account_id = 'default' AND remote_root_id = ?;")
            rootQuery.bindText(remoteRootId, at: 1)
            guard try rootQuery.step(), let rId = rootQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 13, userInfo: [NSLocalizedDescriptionKey: "无法获取 root_id"])
            }
            rootQuery.reset()

            let itemStmt = try conn.cachedStatement("""
            INSERT INTO items (
                root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
            ) VALUES (?, NULL, ?, 'directory', ?, ?, ?)
            ON CONFLICT (root_id) WHERE parent_id IS NULL AND is_tombstone = 0 DO UPDATE SET updated_at = excluded.updated_at;
            """)
            itemStmt.bindInt64(rId, at: 1)
            itemStmt.bindText(rootURL.lastPathComponent, at: 2)
            itemStmt.bindText(remoteRootId, at: 3)
            itemStmt.bindDouble(now, at: 4)
            itemStmt.bindDouble(now, at: 5)
            _ = try itemStmt.step()
            itemStmt.reset()

            let itemQuery = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL AND is_tombstone = 0;")
            itemQuery.bindInt64(rId, at: 1)
            guard try itemQuery.step(), let rItemId = itemQuery.columnInt64(at: 0) else {
                throw NSError(domain: "SyncEngine", code: 14, userInfo: [NSLocalizedDescriptionKey: "无法获取 root item_id"])
            }
            itemQuery.reset()

            return (rId, rItemId)
        }

        // 获取并持久化远端 Changes 起始 token C0 (§3.3)
        if let startPageToken = try? await client.getStartPageToken() {
            try? await store.write { conn in
                let cursorStmt = try conn.cachedStatement("""
                INSERT INTO cursors (
                    root_id, account_id, cursor_kind, token_value, updated_at
                ) VALUES (?, 'default', 'drive_changes', ?, ?)
                ON CONFLICT (root_id, cursor_kind) DO UPDATE SET token_value = excluded.token_value, updated_at = excluded.updated_at;
                """)
                cursorStmt.bindInt64(rootId, at: 1)
                cursorStmt.bindText(startPageToken, at: 2)
                cursorStmt.bindDouble(now, at: 3)
                _ = try cursorStmt.step()
                cursorStmt.reset()
            }
        }

        let effectiveDownloadConcurrency = max(1, min(64, maxDownloadConcurrency))
        let downloadSemaphore = AsyncSemaphore(count: effectiveDownloadConcurrency)
        let downloadGroup = DispatchGroup()

        final class DownloadTracker: @unchecked Sendable {
            var filesDownloaded = 0
            var bytesDownloaded: Int64 = 0
            var dirsCreated = 0
        }
        let progress = DownloadTracker()

        // 2. 递归枚举远端文件与流式下载
        func traverseRemote(parentRemoteId: String, currentLocalURL: URL, parentItemId: Int64) async throws {
            let children = try await self.client.listChildren(parentId: parentRemoteId)

            for item in children {
                let itemLocalURL = currentLocalURL.appendingPathComponent(item.name)

                if item.isDirectory {
                    // 创建本地目录
                    try FileManager.default.createDirectory(at: itemLocalURL, withIntermediateDirectories: true)
                    progress.dirsCreated += 1

                    // 写入 SQLite
                    let dirItemId: Int64 = try await self.store.write { conn in
                        let stmt = try conn.cachedStatement("""
                        INSERT INTO items (
                            root_id, parent_id, name, entry_kind, remote_file_id,
                            phase, created_at, updated_at
                        ) VALUES (?, ?, ?, 'directory', ?, 'committed', ?, ?)
                        ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                        DO UPDATE SET remote_file_id = excluded.remote_file_id, updated_at = excluded.updated_at;
                        """)
                        stmt.bindInt64(rootId, at: 1)
                        stmt.bindInt64(parentItemId, at: 2)
                        stmt.bindText(item.name, at: 3)
                        stmt.bindText(item.id, at: 4)
                        let ts = Date().timeIntervalSince1970
                        stmt.bindDouble(ts, at: 5)
                        stmt.bindDouble(ts, at: 6)
                        _ = try stmt.step()
                        stmt.reset()

                        let qStmt = try conn.cachedStatement("""
                        SELECT item_id FROM items WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;
                        """)
                        qStmt.bindInt64(rootId, at: 1)
                        qStmt.bindInt64(parentItemId, at: 2)
                        qStmt.bindText(item.name, at: 3)
                        guard try qStmt.step(), let dId = qStmt.columnInt64(at: 0) else {
                            throw NSError(domain: "SyncEngine", code: 15, userInfo: [NSLocalizedDescriptionKey: "无法获取 dir item_id"])
                        }
                        qStmt.reset()
                        return dId
                    }

                    // 递归下一层
                    try await traverseRemote(parentRemoteId: item.id, currentLocalURL: itemLocalURL, parentItemId: dirItemId)
                } else {
                    // 文件：加入并发下载队列
                    let downloadBytes = item.sizeBytes ?? 0
                    notifier.addDiscovered(files: 1, bytes: downloadBytes)
                    self.monitor.enqueueDownload(id: item.id, name: item.name, totalBytes: downloadBytes)
                    downloadGroup.enter()
                    Task {
                        await downloadSemaphore.wait()
                        defer {
                            self.monitor.finishDownload(id: item.id)
                            downloadSemaphore.signal()
                            downloadGroup.leave()
                            notifier.addCompleted(files: 1, bytes: downloadBytes)
                        }

                        do {
                            self.monitor.startDownload(id: item.id, name: item.name, totalBytes: item.sizeBytes ?? 0)
                            // 流式下载并核验 SHA-256
                            try await self.client.downloadFile(
                                remoteId: item.id,
                                destinationURL: itemLocalURL,
                                expectedSha256: item.sha256Checksum,
                                onProgress: { delta in
                                    self.monitor.reportDownloadProgress(id: item.id, additionalBytes: delta)
                                }
                            )

                            // 获取本地落盘后的元数据
                            let attrs = try FileManager.default.attributesOfItem(atPath: itemLocalURL.path)
                            let fileSize = (attrs[.size] as? Int64) ?? item.sizeBytes ?? 0
                            let mtime = ((attrs[.modificationDate] as? Date)?.timeIntervalSince1970 ?? now) * 1_000_000_000

                            // 写入 SQLite 基线 B
                            try await self.store.batchWrite { conn in
                                let stmt = try conn.cachedStatement("""
                                INSERT INTO items (
                                    root_id, parent_id, name, entry_kind, remote_file_id,
                                    local_mtime, local_size, local_sha256,
                                    base_sha256, base_size,
                                    remote_sha256, remote_size, remote_status,
                                    local_generation, local_status, phase,
                                    created_at, updated_at
                                ) VALUES (
                                    ?, ?, ?, 'file', ?,
                                    ?, ?, ?,
                                    ?, ?,
                                    ?, ?, 'present',
                                    1, 'present', 'committed',
                                    ?, ?
                                )
                                ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                                DO UPDATE SET
                                    remote_file_id = excluded.remote_file_id,
                                    local_mtime = excluded.local_mtime,
                                    local_size = excluded.local_size,
                                    local_sha256 = excluded.local_sha256,
                                    base_sha256 = excluded.base_sha256,
                                    base_size = excluded.base_size,
                                    remote_sha256 = excluded.remote_sha256,
                                    remote_size = excluded.remote_size,
                                    remote_status = 'present',
                                    local_generation = excluded.local_generation,
                                    local_status = excluded.local_status,
                                    phase = excluded.phase,
                                    updated_at = excluded.updated_at;
                                """)
                                stmt.bindInt64(rootId, at: 1)
                                stmt.bindInt64(parentItemId, at: 2)
                                stmt.bindText(item.name, at: 3)
                                stmt.bindText(item.id, at: 4)
                                stmt.bindInt64(Int64(mtime), at: 5)
                                stmt.bindInt64(fileSize, at: 6)
                                stmt.bindText(item.sha256Checksum, at: 7)
                                stmt.bindText(item.sha256Checksum, at: 8)
                                stmt.bindInt64(fileSize, at: 9)
                                stmt.bindText(item.sha256Checksum, at: 10)
                                stmt.bindInt64(fileSize, at: 11)
                                let ts = Date().timeIntervalSince1970
                                stmt.bindDouble(ts, at: 12)
                                stmt.bindDouble(ts, at: 13)
                                _ = try stmt.step()
                                stmt.reset()
                            }

                            progress.filesDownloaded += 1
                            progress.bytesDownloaded += fileSize
                        } catch {
                            self.logger.error("下载文件失败 [\(item.name)]: \(error)")
                        }
                    }
                }
            }
        }

        // 开始递归列举与下载
        try await traverseRemote(parentRemoteId: remoteRootId, currentLocalURL: rootURL, parentItemId: rootItemId)

        // 等待所有在途下载任务完成
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            downloadGroup.notify(queue: .global(qos: .userInitiated)) {
                cont.resume()
            }
        }

        try await store.flush()
        try await store.checkpoint()

        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        stats.directoriesCreated = progress.dirsCreated
        stats.filesDownloaded = progress.filesDownloaded
        stats.bytesDownloaded = progress.bytesDownloaded
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }

    /// 采用 1MB 恒定内存流式计算文件的 SHA-256 与字节长度
    static func computeFileSha256(at url: URL) throws -> (sha256Hex: String, fileSize: Int64) {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var ctx = CC_SHA256_CTX()
        CC_SHA256_Init(&ctx)
        var totalSize: Int64 = 0
        let bufferSize = 1024 * 1024 // 1MB 流式分片，避免大文件占用内存
        while let chunk = try handle.read(upToCount: bufferSize), !chunk.isEmpty {
            totalSize += Int64(chunk.count)
            _ = chunk.withUnsafeBytes { ptr in
                CC_SHA256_Update(&ctx, ptr.baseAddress, CC_LONG(chunk.count))
            }
        }
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        CC_SHA256_Final(&digest, &ctx)
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return (hex, totalSize)
    }

    /// 在内存中极速计算 Data 的 SHA-256 字符串（小文件专用，单次耗时 ~10us）
    static func computeSha256(of data: Data) -> String {
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        data.withUnsafeBytes { ptr in
            _ = CC_SHA256(ptr.baseAddress, CC_LONG(data.count), &digest)
        }
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    /// 执行大文件 (> 8MB) 分块断点续传：
    /// - 自动检查 operations 表中是否存在未完成的会话与断点
    /// - 向 Google Drive 服务端探测已确认接收的 offset (queryResumableOffset)
    /// - 采用恒定内存的 FileHandle 流式切片读取 (默认 8MB，必须是 256KB 的整数倍)
    /// - 每完成一个分块，立即将新 offset 持久化至 SQLite operations 表，断电/换线程后可无缝续传
    @discardableResult
    private func performResumableUpload(
        rootId: Int64,
        itemId: Int64,
        fileURL: URL,
        fileSize: Int64,
        expectedSha256: String,
        remoteId: String,
        parentId: String,
        name: String,
        isUpdate: Bool,
        chunkSize: Int64 = 8 * 1024 * 1024 // 8MB 分块，256KB 整数倍
    ) async throws -> DriveFile {
        let opId = "resumable_\(remoteId)"
        var sessionURL: URL? = nil
        var currentOffset: Int64 = 0

        struct ExistingResumableOp {
            let sessionURL: URL
            let confirmedOffset: Int64
            let expectedSha256: String
            let totalBytes: Int64
        }

        // 1. 查询是否存在未完成的断点会话
        let existingOp: ExistingResumableOp? = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT session_uri, confirmed_offset, expected_sha256, total_bytes
            FROM operations
            WHERE operation_id = ? AND state = 'inFlight';
            """)
            stmt.bindText(opId, at: 1)
            defer { stmt.reset() }
            if try stmt.step(), let uriStr = stmt.columnText(at: 0), let uri = URL(string: uriStr) {
                let off = stmt.columnInt64(at: 1) ?? 0
                let sha = stmt.columnText(at: 2) ?? ""
                let bytes = stmt.columnInt64(at: 3) ?? 0
                return ExistingResumableOp(sessionURL: uri, confirmedOffset: off, expectedSha256: sha, totalBytes: bytes)
            }
            return nil
        }

        if let existing = existingOp {
            // 关键：核验断点会话的预期 sha256 与文件大小是否与当前文件严格一致！
            // 若文件在中途已被修改，旧会话立即作废并重置，防止错误续传导致云端拼接脏数据
            let isSameFile = (existing.totalBytes == fileSize &&
                              existing.expectedSha256.caseInsensitiveCompare(expectedSha256) == .orderedSame)
            if isSameFile {
                // 向云端探测服务端实际已接收的有效 offset
                do {
                    let serverOffset = try await client.queryResumableOffset(sessionURL: existing.sessionURL, totalBytes: fileSize)
                    if serverOffset >= fileSize {
                        // 云端已全部接收完毕
                        let now = Date().timeIntervalSince1970
                        try await store.write { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE operations SET state = 'completed', confirmed_offset = ?, updated_at = ? WHERE operation_id = ?;
                            """)
                            stmt.bindInt64(fileSize, at: 1)
                            stmt.bindDouble(now, at: 2)
                            stmt.bindText(opId, at: 3)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        return try await client.getFile(remoteId: remoteId)
                    }
                    sessionURL = existing.sessionURL
                    currentOffset = serverOffset
                } catch {
                    // 会话已过期或失效，重新发起新会话
                    sessionURL = nil
                    currentOffset = 0
                }
            } else {
                // 文件内容或大小已发生变化，断点作废，重新发起全新上传
                sessionURL = nil
                currentOffset = 0
            }
        }

        // 2. 若无有效会话，发起新 Resumable 上传会话并持久化操作意图
        let activeSessionURL: URL
        if let sURL = sessionURL {
            activeSessionURL = sURL
        } else {
            if isUpdate {
                activeSessionURL = try await client.initiateResumableUpdate(remoteId: remoteId, totalBytes: fileSize)
            } else {
                activeSessionURL = try await client.initiateResumableUpload(name: name, parentId: parentId, remoteId: remoteId, totalBytes: fileSize)
            }
            currentOffset = 0

            let now = Date().timeIntervalSince1970
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                INSERT INTO operations (
                    operation_id, root_id, item_id, operation_type, state,
                    expected_sha256, target_remote_id, target_parent_remote_id,
                    session_uri, confirmed_offset, total_bytes, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, 'uploadResumable', 'inFlight',
                    ?, ?, ?,
                    ?, 0, ?, ?, ?
                )
                ON CONFLICT (operation_id) DO UPDATE SET
                    session_uri = excluded.session_uri,
                    expected_sha256 = excluded.expected_sha256,
                    confirmed_offset = 0,
                    total_bytes = excluded.total_bytes,
                    state = 'inFlight',
                    updated_at = excluded.updated_at;
                """)
                stmt.bindText(opId, at: 1)
                stmt.bindInt64(rootId, at: 2)
                stmt.bindInt64(itemId, at: 3)
                stmt.bindText(expectedSha256, at: 4)
                stmt.bindText(remoteId, at: 5)
                stmt.bindText(parentId, at: 6)
                stmt.bindText(activeSessionURL.absoluteString, at: 7)
                stmt.bindInt64(fileSize, at: 8)
                stmt.bindDouble(now, at: 9)
                stmt.bindDouble(now, at: 10)
                _ = try stmt.step()
                stmt.reset()
            }
        }

        // 3. 流式分块读取与上传循环
        let fileHandle = try FileHandle(forReadingFrom: fileURL)
        defer { try? fileHandle.close() }

        // 获取初始文件时间戳与大小，用于在分块传输循环中检测并发修改
        let initialAttrs = try? FileManager.default.attributesOfItem(atPath: fileURL.path)
        let initialMtime = initialAttrs?[.modificationDate] as? Date

        var finalDriveFile: DriveFile? = nil

        while currentOffset < fileSize {
            // 检测文件是否在上传分块中途被并发修改
            if let currentAttrs = try? FileManager.default.attributesOfItem(atPath: fileURL.path) {
                let currentDiskSize = (currentAttrs[.size] as? NSNumber)?.int64Value ?? fileSize
                let currentMtime = currentAttrs[.modificationDate] as? Date
                if currentDiskSize != fileSize || (initialMtime != nil && currentMtime != initialMtime) {
                    // 文件在中途被修改，立即终止上传并标记 operation 为 failed，防止将损坏数据拼接发往云端
                    let now = Date().timeIntervalSince1970
                    try? await store.write { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE operations SET state = 'failed', updated_at = ? WHERE operation_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindText(opId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                    throw DriveError.fileModifiedDuringUpload(path: fileURL.path)
                }
            }

            try fileHandle.seek(toOffset: UInt64(currentOffset))
            let bytesToRead = min(chunkSize, fileSize - currentOffset)
            guard let chunkData = try fileHandle.read(upToCount: Int(bytesToRead)), !chunkData.isEmpty else {
                throw CocoaError(.fileReadUnknown)
            }

            let result = try await client.uploadResumableChunk(
                sessionURL: activeSessionURL,
                chunkData: chunkData,
                offset: currentOffset,
                totalBytes: fileSize
            )

            currentOffset += Int64(chunkData.count)
            self.monitor.reportUploadProgress(id: fileURL.path, additionalBytes: Int64(chunkData.count))

            // 每完成一个块，立即在数据库记录已确认 offset 与状态
            let isComplete = (currentOffset >= fileSize)
            let recordedOffset = currentOffset
            let now = Date().timeIntervalSince1970
            try await store.write { conn in
                let stmt = try conn.cachedStatement("""
                UPDATE operations SET
                    confirmed_offset = ?,
                    state = ?,
                    updated_at = ?
                WHERE operation_id = ?;
                """)
                stmt.bindInt64(recordedOffset, at: 1)
                stmt.bindText(isComplete ? "completed" : "inFlight", at: 2)
                stmt.bindDouble(now, at: 3)
                stmt.bindText(opId, at: 4)
                _ = try stmt.step()
                stmt.reset()
            }

            if let res = result {
                finalDriveFile = res
                break
            }
        }

        let file = try await (finalDriveFile != nil ? finalDriveFile! : client.getFile(remoteId: remoteId))
        if let checksum = file.sha256Checksum, checksum.caseInsensitiveCompare(expectedSha256) != .orderedSame {
            // 云端最终拼接计算的哈希与预期不匹配（说明在传输过程中发生篡改或数据损坏）
            let now = Date().timeIntervalSince1970
            try? await store.write { conn in
                let stmt = try conn.cachedStatement("""
                UPDATE operations SET state = 'failed', updated_at = ? WHERE operation_id = ?;
                """)
                stmt.bindDouble(now, at: 1)
                stmt.bindText(opId, at: 2)
                _ = try stmt.step()
                stmt.reset()
            }
            throw DriveError.checksumMismatch(expected: expectedSha256, actual: checksum)
        }
        return file
    }

    // MARK: - 模式 3：已有根目录增量双向同步 (syncIncremental)

    /// 对已有同步根执行双向增量同步
    /// 结合本地 DirectoryScanner 快速扫描与 Google Drive Changes 增量变更，由 Reconciler 驱动三方决策
    @discardableResult
    public func syncIncremental(
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath

        // 查找 rootId 与 rootItemId
        let rootInfo: (rootId: Int64, rootItemId: Int64)? = try await store.read { conn in
            let stmt = try conn.cachedStatement("SELECT root_id FROM roots WHERE account_id = 'default' AND remote_root_id = ?;")
            stmt.bindText(remoteRootId, at: 1)
            guard try stmt.step(), let rId = stmt.columnInt64(at: 0) else {
                stmt.reset()
                return nil
            }
            stmt.reset()

            let itemStmt = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id IS NULL AND is_tombstone = 0;")
            itemStmt.bindInt64(rId, at: 1)
            guard try itemStmt.step(), let rItemId = itemStmt.columnInt64(at: 0) else {
                itemStmt.reset()
                return nil
            }
            itemStmt.reset()
            return (rId, rItemId)
        }

        guard let rootInfo else {
            throw NSError(domain: "SyncEngine", code: 20, userInfo: [NSLocalizedDescriptionKey: "未找到对应的同步根，请先执行初始化同步: \(remoteRootId)"])
        }

        return try await syncIncremental(
            rootId: rootInfo.rootId,
            rootItemId: rootInfo.rootItemId,
            localPath: resolvedLocalPath,
            remoteRootId: remoteRootId,
            maxConcurrency: maxConcurrency,
            onProgress: onProgress
        )
    }

    /// 执行双向增量同步
    @discardableResult
    public func syncIncremental(
        rootId: Int64,
        rootItemId: Int64,
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int = 64,
        onProgress: (@Sendable (SyncProgress) -> Void)? = nil
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()
        let notifier = ProgressNotifier(interval: 0.5, onProgress: onProgress)
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)
        let now = Date().timeIntervalSince1970

        // -------------------------------------------------------------
        // 0. 根目录防扩散安全校验 (§7.2, §9.1)
        // -------------------------------------------------------------
        // A. 本地根目录校验：若本地根目录消失，严禁做删除扩散，立即报错终止
        var isDir: ObjCBool = false
        let localExists = FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir)
        guard localExists && isDir.boolValue else {
            logger.error("[Sync] 本地同步根目录已不存在或不是有效目录: \(resolvedLocalPath)，终止同步以保护云端文件")
            throw SyncEngineError.localRootNotFound(path: resolvedLocalPath)
        }

        // B. 远端根目录校验：若远端根目录被移入回收站或彻底删除，严禁做删除扩散，立即报错终止
        let remoteRoot: DriveFile
        do {
            remoteRoot = try await client.getFile(remoteId: remoteRootId)
        } catch let error as DriveError {
            switch error {
            case .notFound:
                logger.error("[Sync] 远端同步根目录不存在 (404): \(remoteRootId)，终止同步以保护本地文件")
                throw SyncEngineError.remoteRootLost(remoteId: remoteRootId, reason: "notFound")
            default:
                throw error
            }
        } catch {
            let nsError = error as NSError
            if (nsError.domain == "SyncEngine" || nsError.domain == "DriveError") && nsError.code == 404 {
                logger.error("[Sync] 远端同步根目录不存在 (404): \(remoteRootId)，终止同步以保护本地文件")
                throw SyncEngineError.remoteRootLost(remoteId: remoteRootId, reason: "notFound")
            }
            throw error
        }

        if remoteRoot.trashed == true {
            logger.error("[Sync] 远端同步根目录已被移入回收站 (trashed): \(remoteRootId)，终止同步以保护本地文件")
            throw SyncEngineError.remoteRootLost(remoteId: remoteRootId, reason: "trashed")
        }
        guard remoteRoot.isDirectory else {
            logger.error("[Sync] 远端同步根目录不是有效目录: \(remoteRootId)，终止同步以保护本地文件")
            throw SyncEngineError.remoteRootLost(remoteId: remoteRootId, reason: "notDirectory")
        }

        // -------------------------------------------------------------
        // 1. 构建目录拓扑映射 (在内存中快速维护，O(1) 路径与父项解析)
        // -------------------------------------------------------------
        final class DirectoryContext: @unchecked Sendable {
            private var dirPaths: [Int64: String] = [:]         // item_id -> relPath
            private var dirRemoteIds: [Int64: String] = [:]     // item_id -> remote_file_id
            private var dirIdByRemote: [String: Int64] = [:]    // remote_file_id -> item_id
            private var dirIdByRelPath: [String: Int64] = [:]   // relPath -> item_id
            private var lock = os_unfair_lock()

            init(rootItemId: Int64, remoteRootId: String) {
                dirPaths[rootItemId] = ""
                dirRemoteIds[rootItemId] = remoteRootId
                dirIdByRemote[remoteRootId] = rootItemId
                dirIdByRelPath[""] = rootItemId
            }

            func register(itemId: Int64, parentItemId: Int64, name: String, remoteId: String) {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                let parentPath = dirPaths[parentItemId] ?? ""
                let relPath = parentPath.isEmpty ? name : "\(parentPath)/\(name)"
                dirPaths[itemId] = relPath
                dirRemoteIds[itemId] = remoteId
                dirIdByRemote[remoteId] = itemId
                dirIdByRelPath[relPath] = itemId
            }

            func getRelPath(for itemId: Int64) -> String? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return dirPaths[itemId]
            }

            func getRemoteId(for itemId: Int64) -> String? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return dirRemoteIds[itemId]
            }

            func getItemId(byRemote remoteId: String) -> Int64? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return dirIdByRemote[remoteId]
            }

            func getItemId(byRelPath path: String) -> Int64? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return dirIdByRelPath[path]
            }
        }

        let dirContext = DirectoryContext(rootItemId: rootItemId, remoteRootId: remoteRootId)

        // 从 SQLite 加载已知的所有目录
        try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT item_id, parent_id, name, remote_file_id
            FROM items
            WHERE root_id = ? AND entry_kind = 'directory' AND is_tombstone = 0 AND parent_id IS NOT NULL;
            """)
            stmt.bindInt64(rootId, at: 1)
            var rawDirs: [(id: Int64, parentId: Int64, name: String, remoteId: String)] = []
            while try stmt.step() {
                if let id = stmt.columnInt64(at: 0),
                   let parentId = stmt.columnInt64(at: 1),
                   let name = stmt.columnText(at: 2),
                   let rId = stmt.columnText(at: 3) {
                    rawDirs.append((id, parentId, name, rId))
                }
            }
            stmt.reset()

            // 按照拓扑顺序注册进 dirContext
            var registered = Set<Int64>([rootItemId])
            var remaining = rawDirs
            while !remaining.isEmpty {
                let countBefore = remaining.count
                remaining.removeAll { dir in
                    if registered.contains(dir.parentId) {
                        dirContext.register(itemId: dir.id, parentItemId: dir.parentId, name: dir.name, remoteId: dir.remoteId)
                        registered.insert(dir.id)
                        return true
                    }
                    return false
                }
                if remaining.count == countBefore {
                    // 若存在孤立项则按根目录兜底
                    for dir in remaining {
                        dirContext.register(itemId: dir.id, parentItemId: rootItemId, name: dir.name, remoteId: dir.remoteId)
                    }
                    break
                }
            }
        }

        // -------------------------------------------------------------
        // 2. 消费 Google Drive Changes 增量变更 feed (§9.4)
        // -------------------------------------------------------------
        var changeToken: String? = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT token_value FROM cursors WHERE root_id = ? AND cursor_kind = 'drive_changes' AND is_valid = 1;
            """)
            stmt.bindInt64(rootId, at: 1)
            defer { stmt.reset() }
            if try stmt.step(), let token = stmt.columnText(at: 0) {
                return token
            }
            return nil
        }

        if changeToken == nil {
            changeToken = try? await client.getStartPageToken()
        }

        if let currentToken = changeToken {
            var activeToken = currentToken
            var hasMorePages = true
            var latestNewStartToken: String? = nil

            while hasMorePages {
                do {
                    let page = try await client.listChanges(pageToken: activeToken)
                    for change in page.changes {
                        let fileId = change.fileId
                        if fileId == remoteRootId {
                            if change.file?.trashed == true || change.removed == true {
                                let reason = change.removed == true ? "removed" : "trashed"
                                self.logger.error("增量变更检测到远端同步根目录被移除或移入回收站 (\(reason)): \(fileId)，终止同步以保护本地文件")
                                throw SyncEngineError.remoteRootLost(remoteId: fileId, reason: reason)
                            }
                            // 远端根目录自身的普通元数据更新无需作为子项处理
                            continue
                        }
                        if change.file?.trashed == true {
                            // 远端明确移入回收站
                            try await store.batchWrite { conn in
                                let stmt = try conn.cachedStatement("""
                                UPDATE items SET
                                    remote_status = 'trashed',
                                    remote_generation = remote_generation + 1,
                                    dirty_generation = dirty_generation + 1,
                                    phase = 'ready',
                                    updated_at = ?
                                WHERE root_id = ? AND remote_file_id = ? AND is_tombstone = 0;
                                """)
                                stmt.bindDouble(now, at: 1)
                                stmt.bindInt64(rootId, at: 2)
                                stmt.bindText(fileId, at: 3)
                                _ = try stmt.step()
                                stmt.reset()
                            }
                        } else if change.removed == true {
                            // 远端项失去访问权限或已被移除 (removed != trashed)
                            // 遵循官方契约：removed 包括失去访问权限，绝不能单凭此字段判定文件已被用户删除而触发本地误删
                            // 标记为 remote_status = 'unknown', phase = 'blocked'，绝不触发本地删除
                            self.logger.info("远端项访问权限失效或被移除 (removed=true) [\(fileId)]，保护本地文件副本不予删除")
                            try await store.batchWrite { conn in
                                let stmt = try conn.cachedStatement("""
                                UPDATE items SET
                                    remote_status = 'unknown',
                                    phase = 'blocked',
                                    dirty_generation = 0,
                                    updated_at = ?
                                WHERE root_id = ? AND remote_file_id = ? AND is_tombstone = 0;
                                """)
                                stmt.bindDouble(now, at: 1)
                                stmt.bindInt64(rootId, at: 2)
                                stmt.bindText(fileId, at: 3)
                                _ = try stmt.step()
                                stmt.reset()
                            }
                        } else if let file = change.file {
                            // 检查该远端项的父目录是否属于本同步树
                            let parentRemoteId = file.parents?.first ?? ""
                            guard let parentItemId = dirContext.getItemId(byRemote: parentRemoteId) else {
                                continue
                            }

                            // 检查该 remote_file_id 是否已在本地数据库中存在
                            let existingItem: (itemId: Int64, parentId: Int64, name: String, isDir: Bool)? = try await store.read { conn in
                                let stmt = try conn.cachedStatement("""
                                SELECT item_id, parent_id, name, entry_kind
                                FROM items
                                WHERE root_id = ? AND remote_file_id = ? AND is_tombstone = 0;
                                """)
                                stmt.bindInt64(rootId, at: 1)
                                stmt.bindText(file.id, at: 2)
                                defer { stmt.reset() }
                                if try stmt.step(),
                                   let iId = stmt.columnInt64(at: 0),
                                   let pId = stmt.columnInt64(at: 1),
                                   let nm = stmt.columnText(at: 2),
                                   let kind = stmt.columnText(at: 3) {
                                    return (iId, pId, nm, kind == "directory")
                                }
                                return nil
                            }

                            if let existing = existingItem {
                                // 远端已存在对象：检查是否发生重命名或移动
                                let isRenamedOrMoved = (existing.name != file.name || existing.parentId != parentItemId)
                                if isRenamedOrMoved {
                                    let oldParentRel = dirContext.getRelPath(for: existing.parentId) ?? ""
                                    let oldRel = oldParentRel.isEmpty ? existing.name : "\(oldParentRel)/\(existing.name)"
                                    let oldURL = rootURL.appendingPathComponent(oldRel)

                                    let newParentRel = dirContext.getRelPath(for: parentItemId) ?? ""
                                    let newRel = newParentRel.isEmpty ? file.name : "\(newParentRel)/\(file.name)"
                                    let newURL = rootURL.appendingPathComponent(newRel)

                                    if FileManager.default.fileExists(atPath: oldURL.path) {
                                        try? FileManager.default.createDirectory(at: newURL.deletingLastPathComponent(), withIntermediateDirectories: true)
                                        try? FileManager.default.moveItem(at: oldURL, to: newURL)
                                    }

                                    if existing.isDir {
                                        dirContext.register(itemId: existing.itemId, parentItemId: parentItemId, name: file.name, remoteId: file.id)
                                    }
                                }

                                try await store.batchWrite { conn in
                                    let stmt = try conn.cachedStatement("""
                                    UPDATE items SET
                                        parent_id = ?,
                                        name = ?,
                                        remote_name = ?,
                                        remote_sha256 = ?,
                                        remote_size = ?,
                                        remote_status = 'present',
                                        remote_generation = remote_generation + 1,
                                        dirty_generation = dirty_generation + 1,
                                        phase = 'ready',
                                        updated_at = ?
                                    WHERE item_id = ?;
                                    """)
                                    stmt.bindInt64(parentItemId, at: 1)
                                    stmt.bindText(file.name, at: 2)
                                    stmt.bindText(file.name, at: 3)
                                    stmt.bindText(file.sha256Checksum, at: 4)
                                    stmt.bindInt64(file.sizeBytes, at: 5)
                                    stmt.bindDouble(now, at: 6)
                                    stmt.bindInt64(existing.itemId, at: 7)
                                    _ = try stmt.step()
                                    stmt.reset()
                                }
                            } else if file.isDirectory {
                                // 远端全新目录
                                let parentRelPath = dirContext.getRelPath(for: parentItemId) ?? ""
                                let relPath = parentRelPath.isEmpty ? file.name : "\(parentRelPath)/\(file.name)"
                                let localDirURL = rootURL.appendingPathComponent(relPath)
                                try FileManager.default.createDirectory(at: localDirURL, withIntermediateDirectories: true)

                                let dItemId: Int64 = try await store.write { conn in
                                    let stmt = try conn.cachedStatement("""
                                    INSERT INTO items (
                                        root_id, parent_id, name, entry_kind, remote_file_id,
                                        phase, created_at, updated_at
                                    ) VALUES (?, ?, ?, 'directory', ?, 'committed', ?, ?)
                                    ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                                    DO UPDATE SET remote_file_id = excluded.remote_file_id, updated_at = excluded.updated_at;
                                    """)
                                    stmt.bindInt64(rootId, at: 1)
                                    stmt.bindInt64(parentItemId, at: 2)
                                    stmt.bindText(file.name, at: 3)
                                    stmt.bindText(file.id, at: 4)
                                    stmt.bindDouble(now, at: 5)
                                    stmt.bindDouble(now, at: 6)
                                    _ = try stmt.step()
                                    stmt.reset()

                                    let q = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;")
                                    q.bindInt64(rootId, at: 1)
                                    q.bindInt64(parentItemId, at: 2)
                                    q.bindText(file.name, at: 3)
                                    guard try q.step(), let id = q.columnInt64(at: 0) else {
                                        throw NSError(domain: "SyncEngine", code: 21, userInfo: nil)
                                    }
                                    q.reset()
                                    return id
                                }
                                dirContext.register(itemId: dItemId, parentItemId: parentItemId, name: file.name, remoteId: file.id)
                            } else {
                                // 远端全新文件
                                try await store.batchWrite { conn in
                                    let stmt = try conn.cachedStatement("""
                                    INSERT INTO items (
                                        root_id, parent_id, name, entry_kind, remote_file_id,
                                        remote_name, remote_sha256, remote_size, remote_status,
                                        local_status,
                                        remote_generation, phase, dirty_generation,
                                        created_at, updated_at
                                    ) VALUES (
                                        ?, ?, ?, 'file', ?,
                                        ?, ?, ?, 'present',
                                        'absent',
                                        1, 'ready', 1,
                                        ?, ?
                                    )
                                    ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                                    DO UPDATE SET
                                        remote_file_id = excluded.remote_file_id,
                                        remote_name = excluded.remote_name,
                                        remote_sha256 = excluded.remote_sha256,
                                        remote_size = excluded.remote_size,
                                        remote_status = 'present',
                                        remote_generation = items.remote_generation + 1,
                                        dirty_generation = items.dirty_generation + 1,
                                        phase = 'ready',
                                        updated_at = excluded.updated_at;
                                    """)
                                    stmt.bindInt64(rootId, at: 1)
                                    stmt.bindInt64(parentItemId, at: 2)
                                    stmt.bindText(file.name, at: 3)
                                    stmt.bindText(file.id, at: 4)
                                    stmt.bindText(file.name, at: 5)
                                    stmt.bindText(file.sha256Checksum, at: 6)
                                    stmt.bindInt64(file.sizeBytes, at: 7)
                                    stmt.bindDouble(now, at: 8)
                                    stmt.bindDouble(now, at: 9)
                                    _ = try stmt.step()
                                    stmt.reset()
                                }
                            }
                        }
                    }

                    if let next = page.nextPageToken {
                        activeToken = next
                    } else {
                        hasMorePages = false
                        latestNewStartToken = page.newStartPageToken
                    }
                } catch let error as SyncEngineError {
                    throw error
                } catch {
                    self.logger.warning("获取 Changes 失败: \(error)")
                    hasMorePages = false
                }
            }

            // 更新 Changes 游标
            if let finalToken = latestNewStartToken {
                try await store.write { conn in
                    let stmt = try conn.cachedStatement("""
                    INSERT INTO cursors (root_id, account_id, cursor_kind, token_value, updated_at)
                    VALUES (?, 'default', 'drive_changes', ?, ?)
                    ON CONFLICT (root_id, cursor_kind) DO UPDATE SET token_value = excluded.token_value, updated_at = excluded.updated_at;
                    """)
                    stmt.bindInt64(rootId, at: 1)
                    stmt.bindText(finalToken, at: 2)
                    stmt.bindDouble(now, at: 3)
                    _ = try stmt.step()
                    stmt.reset()
                }
            }
        }

        // -------------------------------------------------------------
        // 3. 本地 DirectoryScanner 扫描与快速变更检测 (§6.2)
        // -------------------------------------------------------------
        let baselineCache = try await LocalBaselineCache.load(store: store, rootId: rootId)
        final class SeenItemsTracker: @unchecked Sendable {
            private var seen = Set<String>()
            private var lock = os_unfair_lock()

            func markSeen(parentId: Int64, name: String) {
                os_unfair_lock_lock(&lock)
                seen.insert("\(parentId):\(name)")
                os_unfair_lock_unlock(&lock)
            }

            func contains(parentId: Int64, name: String) -> Bool {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return seen.contains("\(parentId):\(name)")
            }
        }
        let seenTracker = SeenItemsTracker()
        let seenDirTracker = SeenItemsTracker()

        final class ScanProgress: @unchecked Sendable {
            var scanned = 0
            var skipped = 0
            var dirsCreated = 0
            private var lock = os_unfair_lock()

            func incScanned() {
                os_unfair_lock_lock(&lock)
                scanned += 1
                os_unfair_lock_unlock(&lock)
            }

            func incSkipped() {
                os_unfair_lock_lock(&lock)
                skipped += 1
                os_unfair_lock_unlock(&lock)
            }

            func incDirs() {
                os_unfair_lock_lock(&lock)
                dirsCreated += 1
                os_unfair_lock_unlock(&lock)
            }
        }
        let scanProgress = ScanProgress()

        let staticPrefix = resolvedLocalPath.hasSuffix("/") ? resolvedLocalPath : resolvedLocalPath + "/"
        let prefixBytes = Array(staticPrefix.utf8)

        let scanOptions = ScanOptions(
            mode: .basic,
            emission: .all,
            includeHidden: true,
            workers: min(6, ProcessInfo.processInfo.activeProcessorCount),
            batchCapacity: 512,
            delimiter: 0x00,
            pathPrefix: prefixBytes
        )
        let scanFilters: [FilterRule] = [.excludeDirectory(".git")]
        let request = ScanRequest(root: resolvedLocalPath, filters: scanFilters, options: scanOptions)
        let scanner = DirectoryScanner()

        struct DiscoveredRecord {
            let type: EntryType
            let fullPath: String
            let dev: Int64
            let ino: Int64
            let mtime: Int64
            let fileSize: Int64
        }

        _ = try await scanner.scan(request) { batch in
            var itemsInBatch: [DiscoveredRecord] = []
            batch.withRawData { rawBuf in
                guard let basePtr = rawBuf.baseAddress else { return }

                for idx in 0..<batch.count {
                    let record = batch.records[idx]
                    let rawPtr = UnsafeRawPointer(basePtr + Int(record.offset))
                    let cPath = rawPtr.assumingMemoryBound(to: CChar.self)
                    let fullPath = String(cString: cPath)

                    let dev = Int64(record.metadata?.identity.device ?? 1)
                    let ino = Int64(record.metadata?.identity.inode ?? 0)
                    let mtime = (record.metadata?.modificationTime.seconds ?? 0) * 1_000_000_000 + Int64(record.metadata?.modificationTime.nanoseconds ?? 0)
                    let fileSize = record.metadata?.fileSize ?? 0

                    itemsInBatch.append(DiscoveredRecord(
                        type: record.type,
                        fullPath: fullPath,
                        dev: dev,
                        ino: ino,
                        mtime: mtime,
                        fileSize: fileSize
                    ))
                }
            }

            for record in itemsInBatch {
                let fullPath = record.fullPath
                let relPath: String
                if fullPath.hasPrefix(staticPrefix) {
                    relPath = String(fullPath.dropFirst(staticPrefix.count))
                } else {
                    relPath = (fullPath as NSString).lastPathComponent
                }

                if relPath.isEmpty || relPath == ".git" || relPath.hasPrefix(".git/") { continue }

                let name = (relPath as NSString).lastPathComponent
                let parentRel = (relPath as NSString).deletingLastPathComponent
                let parentRelNormalized = (parentRel == "." || parentRel.isEmpty) ? "" : parentRel

                if record.type == .directory {
                    let parentItemId = dirContext.getItemId(byRelPath: parentRelNormalized) ?? rootItemId
                    let dev = record.dev
                    let ino = record.ino

                    // 检查本地目录是否发生重命名或移动 (按 dev + ino 查找)
                    let existingDir: (itemId: Int64, parentId: Int64, name: String, remoteId: String?)? = try await self.store.read { conn in
                        let stmt = try conn.cachedStatement("""
                        SELECT item_id, parent_id, name, remote_file_id
                        FROM items
                        WHERE root_id = ? AND entry_kind = 'directory' AND local_device = ? AND local_inode = ? AND is_tombstone = 0;
                        """)
                        stmt.bindInt64(rootId, at: 1)
                        stmt.bindInt64(dev, at: 2)
                        stmt.bindInt64(ino, at: 3)
                        defer { stmt.reset() }
                        if try stmt.step(),
                           let iId = stmt.columnInt64(at: 0),
                           let pId = stmt.columnInt64(at: 1),
                           let nm = stmt.columnText(at: 2) {
                            let rId = stmt.columnText(at: 3)
                            return (iId, pId, nm, rId)
                        }
                        return nil
                    }

                    if let existing = existingDir, (existing.name != name || existing.parentId != parentItemId) {
                        // 本地目录发生重命名或移动
                        seenDirTracker.markSeen(parentId: existing.parentId, name: existing.name)
                        seenDirTracker.markSeen(parentId: parentItemId, name: name)
                        if let rId = existing.remoteId {
                            let oldPRemote = dirContext.getRemoteId(for: existing.parentId)
                            let newPRemote = dirContext.getRemoteId(for: parentItemId)
                            let addP = (parentItemId != existing.parentId) ? newPRemote : nil
                            let remP = (parentItemId != existing.parentId) ? oldPRemote : nil
                            _ = try? await self.client.updateMetadata(remoteId: rId, newName: name, addParentId: addP, removeParentId: remP)
                        }
                        try await self.store.write { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET name = ?, parent_id = ?, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindText(name, at: 1)
                            stmt.bindInt64(parentItemId, at: 2)
                            stmt.bindDouble(now, at: 3)
                            stmt.bindInt64(existing.itemId, at: 4)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        dirContext.register(itemId: existing.itemId, parentItemId: parentItemId, name: name, remoteId: existing.remoteId ?? "")
                    } else if dirContext.getItemId(byRelPath: relPath) == nil {
                        // 新建本地目录
                        let remoteParentId = dirContext.getRemoteId(for: parentItemId) ?? remoteRootId
                        let candidateRemoteID = try await self.idPool.nextId()
                        var intent: DurableCreateIntent?
                        do {
                            let prepared = try await DurableCreateIntentStore.prepareDirectory(
                                store: self.store,
                                rootID: rootId,
                                parentItemID: parentItemId,
                                name: name,
                                targetParentRemoteID: remoteParentId,
                                candidateRemoteID: candidateRemoteID,
                                device: dev,
                                inode: ino
                            )
                            intent = prepared
                            _ = try await self.client.createDirectory(
                                name: name,
                                parentId: prepared.targetParentRemoteID,
                                remoteId: prepared.targetRemoteID
                            )
                            try await self.store.batchWrite { conn in
                                let ts = Date().timeIntervalSince1970
                                let stmt = try conn.cachedStatement("""
                                UPDATE items SET
                                    remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                                WHERE item_id = ?;
                                """)
                                stmt.bindDouble(ts, at: 1)
                                stmt.bindInt64(prepared.itemID, at: 2)
                                _ = try stmt.step()
                                stmt.reset()
                                try DurableCreateIntentStore.completeOperation(conn: conn, operationID: prepared.operationID, now: ts)
                            }
                            dirContext.register(
                                itemId: prepared.itemID,
                                parentItemId: parentItemId,
                                name: name,
                                remoteId: prepared.targetRemoteID
                            )
                            seenDirTracker.markSeen(parentId: parentItemId, name: name)
                            scanProgress.incDirs()
                        } catch {
                            if let intent {
                                await DurableCreateIntentStore.markUnknownOutcome(
                                    store: self.store,
                                    operationID: intent.operationID,
                                    error: error
                                )
                            }
                            self.logger.error("创建远端目录失败 [\(relPath)]: \(error)")
                        }
                    } else {
                        seenDirTracker.markSeen(parentId: parentItemId, name: name)
                    }
                } else if record.type == .file {
                    scanProgress.incScanned()
                    let parentItemId = dirContext.getItemId(byRelPath: parentRelNormalized) ?? rootItemId
                    let dev = record.dev
                    let ino = record.ino
                    let mtime = record.mtime
                    let fileSize = record.fileSize

                    // 检查本地文件是否发生重命名或移动 (按 dev + ino 查找)
                    let existingFile: (itemId: Int64, parentId: Int64, name: String, remoteId: String?)? = try await self.store.read { conn in
                        let stmt = try conn.cachedStatement("""
                        SELECT item_id, parent_id, name, remote_file_id
                        FROM items
                        WHERE root_id = ? AND entry_kind = 'file' AND local_device = ? AND local_inode = ? AND is_tombstone = 0;
                        """)
                        stmt.bindInt64(rootId, at: 1)
                        stmt.bindInt64(dev, at: 2)
                        stmt.bindInt64(ino, at: 3)
                        defer { stmt.reset() }
                        if try stmt.step(),
                           let iId = stmt.columnInt64(at: 0),
                           let pId = stmt.columnInt64(at: 1),
                           let nm = stmt.columnText(at: 2) {
                            let rId = stmt.columnText(at: 3)
                            return (iId, pId, nm, rId)
                        }
                        return nil
                    }

                    if let existing = existingFile, (existing.name != name || existing.parentId != parentItemId) {
                        // 本地文件发生重命名或移动
                        seenTracker.markSeen(parentId: existing.parentId, name: existing.name)
                        seenTracker.markSeen(parentId: parentItemId, name: name)

                        if let rId = existing.remoteId {
                            let oldPRemote = dirContext.getRemoteId(for: existing.parentId)
                            let newPRemote = dirContext.getRemoteId(for: parentItemId)
                            let addP = (parentItemId != existing.parentId) ? newPRemote : nil
                            let remP = (parentItemId != existing.parentId) ? oldPRemote : nil
                            _ = try? await self.client.updateMetadata(remoteId: rId, newName: name, addParentId: addP, removeParentId: remP)
                        }

                        try await self.store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET
                                name = ?,
                                parent_id = ?,
                                local_mtime = ?,
                                local_size = ?,
                                updated_at = ?
                            WHERE item_id = ?;
                            """)
                            stmt.bindText(name, at: 1)
                            stmt.bindInt64(parentItemId, at: 2)
                            stmt.bindInt64(mtime, at: 3)
                            stmt.bindInt64(fileSize, at: 4)
                            stmt.bindDouble(now, at: 5)
                            stmt.bindInt64(existing.itemId, at: 6)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        continue
                    }

                    seenTracker.markSeen(parentId: parentItemId, name: name)

                    // 快速变更比对 (§6.2)
                    if let _ = baselineCache.lookupUnchanged(device: dev, inode: ino, mtime: mtime, size: fileSize) {
                        scanProgress.incSkipped()
                        continue
                    }

                    // 文件有修改或为新文件：计算 SHA-256
                    let fileURL = URL(fileURLWithPath: fullPath)
                    guard let fileData = try? Data(contentsOf: fileURL) else { continue }
                    var ctx = CC_SHA256_CTX()
                    CC_SHA256_Init(&ctx)
                    _ = fileData.withUnsafeBytes { CC_SHA256_Update(&ctx, $0.baseAddress, CC_LONG(fileData.count)) }
                    var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
                    CC_SHA256_Final(&digest, &ctx)
                    let sha256Hex = digest.map { String(format: "%02x", $0) }.joined()

                    try await self.store.batchWrite { conn in
                        let stmt = try conn.cachedStatement("""
                        INSERT INTO items (
                            root_id, parent_id, name, entry_kind,
                            local_device, local_inode, local_mtime, local_size, local_sha256,
                            remote_status,
                            local_generation, local_status, phase, dirty_generation,
                            created_at, updated_at
                        ) VALUES (
                            ?, ?, ?, 'file',
                            ?, ?, ?, ?, ?,
                            'absent',
                            1, 'present', 'ready', 1,
                            ?, ?
                        )
                        ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                        DO UPDATE SET
                            local_device = excluded.local_device,
                            local_inode = excluded.local_inode,
                            local_mtime = excluded.local_mtime,
                            local_size = excluded.local_size,
                            local_sha256 = excluded.local_sha256,
                            local_status = 'present',
                            local_generation = items.local_generation + 1,
                            dirty_generation = items.dirty_generation + 1,
                            phase = 'ready',
                            updated_at = excluded.updated_at;
                        """)
                        stmt.bindInt64(rootId, at: 1)
                        stmt.bindInt64(parentItemId, at: 2)
                        stmt.bindText(name, at: 3)
                        stmt.bindInt64(dev, at: 4)
                        stmt.bindInt64(ino, at: 5)
                        stmt.bindInt64(mtime, at: 6)
                        stmt.bindInt64(fileSize, at: 7)
                        stmt.bindText(sha256Hex, at: 8)
                        stmt.bindDouble(now, at: 9)
                        stmt.bindDouble(now, at: 10)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                }
            }
        }

        // 识别本地已删除的文件与目录 (§9.1)
        // 再次核验本地根目录是否存在，避免在扫描期间本地目录被移除导致全部误判 absent 扩散删除
        var isStillDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isStillDir), isStillDir.boolValue else {
            logger.error("[Sync] 本地同步根目录在扫描期间消失: \(resolvedLocalPath)，终止同步以保护云端文件")
            throw SyncEngineError.localRootNotFound(path: resolvedLocalPath)
        }

        try await store.write { conn in
            // 1. 文件删除检测
            let stmt = try conn.cachedStatement("""
            SELECT item_id, parent_id, name
            FROM items
            WHERE root_id = ? AND entry_kind = 'file' AND local_status = 'present' AND is_tombstone = 0;
            """)
            stmt.bindInt64(rootId, at: 1)
            var deletedIds: [Int64] = []
            while try stmt.step() {
                if let iId = stmt.columnInt64(at: 0),
                   let pId = stmt.columnInt64(at: 1),
                   let name = stmt.columnText(at: 2) {
                    if !seenTracker.contains(parentId: pId, name: name) {
                        deletedIds.append(iId)
                    }
                }
            }
            stmt.reset()

            // 2. 目录删除检测（排除根目录项）
            let dirStmt = try conn.cachedStatement("""
            SELECT item_id, parent_id, name
            FROM items
            WHERE root_id = ? AND entry_kind = 'directory' AND parent_id IS NOT NULL AND local_status = 'present' AND is_tombstone = 0;
            """)
            dirStmt.bindInt64(rootId, at: 1)
            while try dirStmt.step() {
                if let iId = dirStmt.columnInt64(at: 0),
                   let pId = dirStmt.columnInt64(at: 1),
                   let name = dirStmt.columnText(at: 2) {
                    if !seenDirTracker.contains(parentId: pId, name: name) {
                        deletedIds.append(iId)
                    }
                }
            }
            dirStmt.reset()

            for dId in deletedIds {
                let u = try conn.cachedStatement("""
                UPDATE items SET
                    local_status = 'absent',
                    local_generation = local_generation + 1,
                    dirty_generation = dirty_generation + 1,
                    phase = 'ready',
                    updated_at = ?
                WHERE item_id = ?;
                """)
                u.bindDouble(now, at: 1)
                u.bindInt64(dId, at: 2)
                _ = try u.step()
                u.reset()
            }
        }

        // 刷新所有未提交的 Changes 及扫描批次至磁盘，确保对后续查询立即可见
        try await store.flush()

        // -------------------------------------------------------------
        // 4. Reconciler 决策与执行 (§9.1)
        // -------------------------------------------------------------
        struct DirtyRecord: Sendable {
            let itemId: Int64
            let parentId: Int64
            let name: String
            let remoteFileId: String?
            let entryKind: String
            let baseline: ItemBaseline?
            let local: LocalObservation?
            let remote: RemoteObservation?
            let pendingCreate: DurableCreateIntent?
        }

        let dirtyItems: [DirtyRecord] = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT items.item_id, items.parent_id, items.name, items.remote_file_id, items.entry_kind,
                   items.base_sha256, items.base_size,
                   items.local_sha256, items.local_size, items.local_status,
                   items.remote_sha256, items.remote_size, items.remote_status,
                   op.operation_id, op.target_remote_id, op.target_parent_remote_id,
                   op.expected_local_generation, op.expected_sha256, op.total_bytes
            FROM items
            LEFT JOIN operations op ON op.operation_id = (
                SELECT candidate.operation_id
                FROM operations candidate
                WHERE candidate.item_id = items.item_id
                  AND candidate.operation_type IN ('createDirectory', 'uploadMultipart')
                  AND candidate.state IN ('ready', 'inFlight', 'verify', 'unknownOutcome')
                ORDER BY candidate.created_at DESC
                LIMIT 1
            )
            WHERE items.root_id = ? AND items.dirty_generation > 0 AND items.is_tombstone = 0;
            """)
            stmt.bindInt64(rootId, at: 1)
            var records: [DirtyRecord] = []
            while try stmt.step() {
                guard let iId = stmt.columnInt64(at: 0),
                      let pId = stmt.columnInt64(at: 1),
                      let name = stmt.columnText(at: 2) else { continue }
                let rFileId = stmt.columnText(at: 3)
                let kind = stmt.columnText(at: 4) ?? "file"

                let bSha = stmt.columnText(at: 5)
                let bSize = stmt.columnInt64(at: 6)
                let baseline: ItemBaseline? = (bSha != nil) ? ItemBaseline(sha256: bSha, size: bSize) : nil

                let lSha = stmt.columnText(at: 7)
                let lSize = stmt.columnInt64(at: 8)
                let lStatusStr = stmt.columnText(at: 9) ?? "unknown"
                let lStatus = LocalObservation.Status(rawValue: lStatusStr) ?? .unknown
                let local = LocalObservation(status: lStatus, sha256: lSha, size: lSize)

                let rSha = stmt.columnText(at: 10)
                let rSize = stmt.columnInt64(at: 11)
                let rStatusStr = stmt.columnText(at: 12) ?? "unknown"
                let rStatus = RemoteObservation.Status(rawValue: rStatusStr) ?? .unknown
                let remote = RemoteObservation(status: rStatus, sha256: rSha, size: rSize)

                let pendingCreate: DurableCreateIntent?
                if let operationID = stmt.columnText(at: 13),
                   let targetRemoteID = stmt.columnText(at: 14),
                   let targetParentRemoteID = stmt.columnText(at: 15) {
                    pendingCreate = DurableCreateIntent(
                        operationID: operationID,
                        itemID: iId,
                        targetRemoteID: targetRemoteID,
                        targetParentRemoteID: targetParentRemoteID,
                        expectedLocalGeneration: stmt.columnInt64(at: 16) ?? 0,
                        expectedSHA256: stmt.columnText(at: 17),
                        totalBytes: stmt.columnInt64(at: 18)
                    )
                } else {
                    pendingCreate = nil
                }

                records.append(DirtyRecord(
                    itemId: iId, parentId: pId, name: name, remoteFileId: rFileId,
                    entryKind: kind, baseline: baseline, local: local, remote: remote,
                    pendingCreate: pendingCreate
                ))
            }
            stmt.reset()
            return records
        }

        let effectiveSyncConcurrency = max(1, min(64, maxConcurrency))
        let syncSemaphore = AsyncSemaphore(count: effectiveSyncConcurrency)
        let syncGroup = DispatchGroup()

        final class ActionTracker: @unchecked Sendable {
            var uploaded = 0
            var bytesUp: Int64 = 0
            var downloaded = 0
            var bytesDown: Int64 = 0
            var deleted = 0
            var conflicts = 0
        }
        let actionTracker = ActionTracker()

        // -------------------------------------------------------------
        // 5A. 阶段一：先对所有文件项执行 Reconciler 裁决与调度执行 (§9.1, A04)
        // 确保所有文件级上传、下载、修改保留与删除动作彻底完成，作为目录删除的屏障
        // -------------------------------------------------------------
        let fileItems = dirtyItems.filter { $0.entryKind == "file" }
        let dirItems = dirtyItems.filter { $0.entryKind == "directory" }

        // 恢复上次在请求前已持久化、但尚未提交完成回执的目录创建。
        // createDirectory 使用相同预生成 ID 重试；若服务端上次已成功，DriveClient 会在 409 后核验同一对象。
        let pendingDirectoryCreates = dirItems.compactMap { item -> (DirtyRecord, DurableCreateIntent)? in
            guard let intent = item.pendingCreate else { return nil }
            return (item, intent)
        }.sorted { lhs, rhs in
            let left = dirContext.getRelPath(for: lhs.0.itemId) ?? lhs.0.name
            let right = dirContext.getRelPath(for: rhs.0.itemId) ?? rhs.0.name
            return left.split(separator: "/").count < right.split(separator: "/").count
        }

        for (item, intent) in pendingDirectoryCreates {
            do {
                _ = try await client.createDirectory(
                    name: item.name,
                    parentId: intent.targetParentRemoteID,
                    remoteId: intent.targetRemoteID
                )
                try await store.batchWrite { conn in
                    let ts = Date().timeIntervalSince1970
                    let stmt = try conn.cachedStatement("""
                    UPDATE items SET
                        remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ?
                    WHERE item_id = ?;
                    """)
                    stmt.bindDouble(ts, at: 1)
                    stmt.bindInt64(intent.itemID, at: 2)
                    _ = try stmt.step()
                    stmt.reset()
                    try DurableCreateIntentStore.completeOperation(conn: conn, operationID: intent.operationID, now: ts)
                }
            } catch {
                await DurableCreateIntentStore.markUnknownOutcome(
                    store: store,
                    operationID: intent.operationID,
                    error: error
                )
                logger.error("恢复远端目录创建失败 [\(item.name)]: \(error)")
            }
        }

        for item in fileItems {
            let decision: ReconcileDecision
            if item.pendingCreate != nil {
                decision = .upload(reason: "恢复已持久化的小文件创建意图")
            } else {
                decision = Reconciler.decide(baseline: item.baseline, local: item.local, remote: item.remote)
            }

            switch decision {
            case .upload, .keepModified(preferLocal: true):
                let upBytes = item.local?.size ?? 0
                notifier.addDiscovered(files: 1, bytes: upBytes)
                self.monitor.enqueueUpload(id: item.name, name: item.name, totalBytes: upBytes)
                syncGroup.enter()
                Task {
                    var createIntent = item.pendingCreate
                    await syncSemaphore.wait()
                    defer {
                        self.monitor.finishUpload(id: item.name)
                        syncSemaphore.signal()
                        syncGroup.leave()
                        notifier.addCompleted(files: 1, bytes: upBytes)
                    }

                    do {
                        let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                        let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                        let localFileURL = rootURL.appendingPathComponent(relPath)
                        let remoteParentId = dirContext.getRemoteId(for: item.parentId) ?? remoteRootId

                        // 若远端父目录此前被移入回收站，在上传子文件前自动恢复父目录
                        if let parentRId = dirContext.getRemoteId(for: item.parentId) {
                            let isParentTrashed: Bool = (try? await self.store.read { conn in
                                let stmt = try conn.cachedStatement("SELECT remote_status FROM items WHERE item_id = ?;")
                                stmt.bindInt64(item.parentId, at: 1)
                                defer { stmt.reset() }
                                if try stmt.step(), let st = stmt.columnText(at: 0) {
                                    return st == "trashed"
                                }
                                return false
                            }) ?? false

                            if isParentTrashed {
                                try? await self.client.untrash(remoteId: parentRId)
                                try? await self.store.write { conn in
                                    let stmt = try conn.cachedStatement("UPDATE items SET remote_status = 'present', updated_at = ? WHERE item_id = ?;")
                                    stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                                    stmt.bindInt64(item.parentId, at: 2)
                                    _ = try stmt.step()
                                }
                            }
                        }

                        let fSize: Int64
                        let sha256Hex: String
                        if let s = item.local?.sha256, let sz = item.local?.size {
                            sha256Hex = s
                            fSize = sz
                        } else {
                            let res = try SyncEngine.computeFileSha256(at: localFileURL)
                            sha256Hex = res.sha256Hex
                            fSize = res.fileSize
                        }
                        self.monitor.startUpload(id: item.name, name: item.name, totalBytes: fSize)

                        let fileData = try Data(contentsOf: localFileURL)
                        let attrs = try FileManager.default.attributesOfItem(atPath: localFileURL.path)
                        let mtime = ((attrs[.modificationDate] as? Date)?.timeIntervalSince1970 ?? Date().timeIntervalSince1970) * 1_000_000_000
                        let dev = (attrs[.systemNumber] as? NSNumber)?.int64Value ?? 0
                        let ino = (attrs[.systemFileNumber] as? NSNumber)?.int64Value ?? 0
                        let uploadedFile: DriveFile
                        if let pending = createIntent {
                            guard let expectedSHA256 = pending.expectedSHA256,
                                  expectedSHA256.caseInsensitiveCompare(sha256Hex) == .orderedSame,
                                  pending.totalBytes == fSize else {
                                throw SyncEngineError.general("未完成的小文件创建意图输入已变化: \(item.name)")
                            }
                            uploadedFile = try await self.client.uploadMultipart(
                                name: item.name,
                                parentId: pending.targetParentRemoteID,
                                remoteId: pending.targetRemoteID,
                                content: fileData,
                                expectedSha256: sha256Hex
                            )
                        } else if let existingRemoteId = item.remoteFileId {
                            uploadedFile = try await self.client.updateMultipart(
                                remoteId: existingRemoteId,
                                content: fileData,
                                expectedSha256: sha256Hex
                            )
                        } else {
                            let newRemoteId = try await self.idPool.nextId()
                            let prepared = try await DurableCreateIntentStore.prepareMultipartUpload(
                                store: self.store,
                                rootID: rootId,
                                itemID: item.itemId,
                                parentItemID: item.parentId,
                                name: item.name,
                                targetParentRemoteID: remoteParentId,
                                candidateRemoteID: newRemoteId,
                                device: dev,
                                inode: ino,
                                mtime: Int64(mtime),
                                size: fSize,
                                sha256: sha256Hex
                            )
                            createIntent = prepared
                            uploadedFile = try await self.client.uploadMultipart(
                                name: item.name,
                                parentId: prepared.targetParentRemoteID,
                                remoteId: prepared.targetRemoteID,
                                content: fileData,
                                expectedSha256: sha256Hex
                            )
                        }

                        let completedCreateIntent = createIntent
                        try await self.store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET
                                remote_file_id = ?,
                                local_device = ?,
                                local_inode = ?,
                                local_mtime = ?,
                                local_size = ?,
                                base_sha256 = ?,
                                base_size = ?,
                                remote_sha256 = ?,
                                remote_size = ?,
                                remote_status = 'present',
                                phase = 'committed',
                                dirty_generation = 0,
                                updated_at = ?
                            WHERE item_id = ?;
                            """)
                            stmt.bindText(uploadedFile.id, at: 1)
                            stmt.bindInt64(dev, at: 2)
                            stmt.bindInt64(ino, at: 3)
                            stmt.bindInt64(Int64(mtime), at: 4)
                            stmt.bindInt64(fSize, at: 5)
                            stmt.bindText(sha256Hex, at: 6)
                            stmt.bindInt64(fSize, at: 7)
                            stmt.bindText(sha256Hex, at: 8)
                            stmt.bindInt64(fSize, at: 9)
                            stmt.bindDouble(now, at: 10)
                            stmt.bindInt64(item.itemId, at: 11)
                            _ = try stmt.step()
                            stmt.reset()
                            if let createIntent = completedCreateIntent {
                                try DurableCreateIntentStore.completeOperation(
                                    conn: conn,
                                    operationID: createIntent.operationID,
                                    now: now
                                )
                            }
                        }

                        actionTracker.uploaded += 1
                        actionTracker.bytesUp += fSize
                    } catch {
                        if let createIntent {
                            await DurableCreateIntentStore.markUnknownOutcome(
                                store: self.store,
                                operationID: createIntent.operationID,
                                error: error
                            )
                        }
                        self.logger.error("增量上传失败 [\(item.name)]: \(error)")
                    }
                }

            case .download, .keepModified(preferLocal: false):
                let downId = item.remoteFileId ?? item.name
                let downSize = item.remote?.size ?? 0
                notifier.addDiscovered(files: 1, bytes: downSize)
                self.monitor.enqueueDownload(id: downId, name: item.name, totalBytes: downSize)
                syncGroup.enter()
                Task {
                    await syncSemaphore.wait()
                    defer {
                        self.monitor.finishDownload(id: downId)
                        syncSemaphore.signal()
                        syncGroup.leave()
                        notifier.addCompleted(files: 1, bytes: downSize)
                    }

                    do {
                        guard let remoteFileId = item.remoteFileId else { return }
                        let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                        let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                        let localFileURL = rootURL.appendingPathComponent(relPath)

                        // 确保本地目标父目录存在
                        try? FileManager.default.createDirectory(at: localFileURL.deletingLastPathComponent(), withIntermediateDirectories: true)

                        self.monitor.startDownload(id: downId, name: item.name, totalBytes: downSize)
                        try await self.client.downloadFile(
                            remoteId: remoteFileId,
                            destinationURL: localFileURL,
                            expectedSha256: item.remote?.sha256,
                            onProgress: { delta in
                                self.monitor.reportDownloadProgress(id: downId, additionalBytes: delta)
                            }
                        )

                        let attrs = try FileManager.default.attributesOfItem(atPath: localFileURL.path)
                        let fSize = (attrs[.size] as? Int64) ?? item.remote?.size ?? 0
                        let mtime = ((attrs[.modificationDate] as? Date)?.timeIntervalSince1970 ?? Date().timeIntervalSince1970) * 1_000_000_000

                        try await self.store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET
                                local_sha256 = remote_sha256,
                                local_size = remote_size,
                                local_mtime = ?,
                                local_status = 'present',
                                remote_status = 'present',
                                base_sha256 = remote_sha256,
                                base_size = remote_size,
                                phase = 'committed',
                                dirty_generation = 0,
                                updated_at = ?
                            WHERE item_id = ?;
                            """)
                            stmt.bindInt64(Int64(mtime), at: 1)
                            stmt.bindDouble(Date().timeIntervalSince1970, at: 2)
                            stmt.bindInt64(item.itemId, at: 3)
                            _ = try stmt.step()
                            stmt.reset()
                        }

                        actionTracker.downloaded += 1
                        actionTracker.bytesDown += fSize
                    } catch {
                        self.logger.error("增量下载失败 [\(item.name)]: \(error)")
                    }
                }

            case .matchUpdateBaseline(let sha, let size):
                try await store.batchWrite { conn in
                    let stmt = try conn.cachedStatement("""
                    UPDATE items SET
                        base_sha256 = ?,
                        base_size = ?,
                        remote_status = 'present',
                        local_status = 'present',
                        phase = 'committed',
                        dirty_generation = 0,
                        updated_at = ?
                    WHERE item_id = ?;
                    """)
                    stmt.bindText(sha, at: 1)
                    stmt.bindInt64(size, at: 2)
                    stmt.bindDouble(now, at: 3)
                    stmt.bindInt64(item.itemId, at: 4)
                    _ = try stmt.step()
                    stmt.reset()
                }

            case .trashRemote:
                syncGroup.enter()
                Task {
                    await syncSemaphore.wait()
                    defer {
                        syncSemaphore.signal()
                        syncGroup.leave()
                    }
                    if let rId = item.remoteFileId {
                        try? await self.client.trash(remoteId: rId)
                    }
                    try? await self.store.batchWrite { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                        """)
                        stmt.bindDouble(Date().timeIntervalSince1970, at: 1)
                        stmt.bindInt64(item.itemId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                    actionTracker.deleted += 1
                }

            case .deleteLocal:
                let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                let localFileURL = rootURL.appendingPathComponent(relPath)
                var trashSucceeded = true

                if FileManager.default.fileExists(atPath: localFileURL.path) {
                    var trashURL: NSURL?
                    do {
                        try FileManager.default.trashItem(at: localFileURL, resultingItemURL: &trashURL)
                    } catch {
                        trashSucceeded = false
                        self.logger.warning("无法将本地文件移入废纸篓 [\(relPath)]: \(error)，保留本地文件并标记为 blocked，绝不执行永久删除")
                    }
                }

                if trashSucceeded {
                    try await store.batchWrite { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindInt64(item.itemId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                    actionTracker.deleted += 1
                } else {
                    try await store.batchWrite { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE items SET phase = 'blocked', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindInt64(item.itemId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                }

            case .conflict(let winner, let conflictId):
                syncGroup.enter()
                Task {
                    await syncSemaphore.wait()
                    defer {
                        syncSemaphore.signal()
                        syncGroup.leave()
                    }
                    let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                    let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                    let originalURL = rootURL.appendingPathComponent(relPath)

                    let ext = originalURL.pathExtension
                    let base = (item.name as NSString).deletingPathExtension
                    let conflictName = ext.isEmpty ? "\(base) (Conflict \(conflictId))" : "\(base) (Conflict \(conflictId)).\(ext)"
                    let conflictURL = originalURL.deletingLastPathComponent().appendingPathComponent(conflictName)

                    do {
                        if winner == .remote, let rId = item.remoteFileId {
                            try FileManager.default.moveItem(at: originalURL, to: conflictURL)
                            try await self.client.downloadFile(remoteId: rId, destinationURL: originalURL, expectedSha256: item.remote?.sha256)
                        } else if winner == .local, let rId = item.remoteFileId {
                            try await self.client.downloadFile(remoteId: rId, destinationURL: conflictURL, expectedSha256: item.remote?.sha256)
                        }

                        try await self.store.write { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET conflict_id = ?, conflict_winner = ?, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindText(conflictId, at: 1)
                            stmt.bindText(winner.rawValue, at: 2)
                            stmt.bindDouble(now, at: 3)
                            stmt.bindInt64(item.itemId, at: 4)
                            _ = try stmt.step()
                            stmt.reset()
                        }

                        actionTracker.conflicts += 1
                    } catch {
                        self.logger.error("处理文件冲突失败 [\(item.name)]: \(error)")
                    }
                }

            case .unchanged:
                try await store.batchWrite { conn in
                    let stmt = try conn.cachedStatement("UPDATE items SET dirty_generation = 0 WHERE item_id = ?;")
                    stmt.bindInt64(item.itemId, at: 1)
                    _ = try stmt.step()
                    stmt.reset()
                }

            case .waitingEvidence:
                break
            }
        }

        // 等待所有文件级上传、下载、修改保留与删除动作彻底完成并落库
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            syncGroup.notify(queue: .global(qos: .userInitiated)) {
                cont.resume()
            }
        }
        try await store.flush()

        // -------------------------------------------------------------
        // 5B. 阶段二：后代冲突屏障与受控自底向上目录处理 (A04)
        // 依赖所有后代裁决结果，任何保留、新增、待上传/下载或冲突都会阻断目录删除
        // -------------------------------------------------------------
        // 1. 对于非删除状态的普通目录，直接提交并清除 dirty_generation
        for item in dirItems {
            let isCandidate = (item.local?.status == .absent && item.remote?.status == .present) ||
                              (item.remote?.status == .trashed && item.local?.status == .present) ||
                              (item.local?.status == .absent && item.remote?.status == .trashed)
            if !isCandidate {
                if item.local?.status != .unknown && item.remote?.status != .unknown {
                    try await store.batchWrite { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE items SET phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindInt64(item.itemId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                }
            } else if item.local?.status == .absent && item.remote?.status == .trashed {
                // 两端皆已删除
                try await store.batchWrite { conn in
                    let stmt = try conn.cachedStatement("""
                    UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                    """)
                    stmt.bindDouble(now, at: 1)
                    stmt.bindInt64(item.itemId, at: 2)
                    _ = try stmt.step()
                    stmt.reset()
                }
                actionTracker.deleted += 1
            }
        }

        // 2. 对于存在单侧删除意图的目录，按树深度倒序（自底向上，叶子目录优先）进行屏障核验
        let dirCandidates = dirItems.filter {
            ($0.local?.status == .absent && $0.remote?.status == .present) ||
            ($0.remote?.status == .trashed && $0.local?.status == .present)
        }.sorted { a, b in
            let pathA = dirContext.getRelPath(for: a.itemId) ?? ""
            let pathB = dirContext.getRelPath(for: b.itemId) ?? ""
            let depthA = pathA.isEmpty ? 0 : pathA.split(separator: "/").count
            let depthB = pathB.isEmpty ? 0 : pathB.split(separator: "/").count
            return depthA > depthB
        }

        for dirItem in dirCandidates {
            let parentRel = dirContext.getRelPath(for: dirItem.parentId) ?? ""
            let relPath = parentRel.isEmpty ? dirItem.name : "\(parentRel)/\(dirItem.name)"
            let localDirURL = rootURL.appendingPathComponent(relPath)

            // 递归查询当前目录的所有后代状态
            let barrier = try await store.read { conn in
                let stmt = try conn.cachedStatement("""
                WITH RECURSIVE subtree AS (
                    SELECT item_id, entry_kind, local_status, remote_status, phase, is_tombstone
                    FROM items
                    WHERE parent_id = ? AND root_id = ?
                    UNION ALL
                    SELECT i.item_id, i.entry_kind, i.local_status, i.remote_status, i.phase, i.is_tombstone
                    FROM items i
                    JOIN subtree s ON i.parent_id = s.item_id
                    WHERE i.root_id = ?
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN is_tombstone = 0 AND local_status = 'present' THEN 1 ELSE 0 END) AS local_present,
                    SUM(CASE WHEN is_tombstone = 0 AND remote_status = 'present' THEN 1 ELSE 0 END) AS remote_present,
                    SUM(CASE WHEN is_tombstone = 0 AND (phase IN ('waitingEvidence', 'conflict', 'blocked') OR local_status = 'unknown' OR remote_status = 'unknown') THEN 1 ELSE 0 END) AS pending_count
                FROM subtree;
                """)
                stmt.bindInt64(dirItem.itemId, at: 1)
                stmt.bindInt64(rootId, at: 2)
                stmt.bindInt64(rootId, at: 3)
                defer { stmt.reset() }
                if try stmt.step() {
                    let total = Int(stmt.columnInt64(at: 0) ?? 0)
                    let localPresent = Int(stmt.columnInt64(at: 1) ?? 0)
                    let remotePresent = Int(stmt.columnInt64(at: 2) ?? 0)
                    let pending = Int(stmt.columnInt64(at: 3) ?? 0)
                    return (total: total, localPresent: localPresent, remotePresent: remotePresent, pending: pending)
                }
                return (total: 0, localPresent: 0, remotePresent: 0, pending: 0)
            }

            if dirItem.local?.status == .absent && dirItem.remote?.status == .present {
                // 本地删除了目录，但远端目录仍在 (原意图: trashRemote)
                // 屏障检查：若后代中存在任何需保留的远端文件/本地下载文件/冲突/未决项，绝对禁止删除远端目录
                if barrier.remotePresent > 0 || barrier.localPresent > 0 || barrier.pending > 0 {
                    self.logger.info("后代屏障生效：远端目录 [\(relPath)] 包含需保留或新增的后代文件 (remotePresent: \(barrier.remotePresent), localPresent: \(barrier.localPresent))，阻止远端目录删除并恢复本地目录")
                    try? FileManager.default.createDirectory(at: localDirURL, withIntermediateDirectories: true)
                    try await store.batchWrite { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE items SET local_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindInt64(dirItem.itemId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                } else {
                    // 后代全部已安全删除，向云端发送 trashRemote
                    do {
                        if let rId = dirItem.remoteFileId {
                            try await self.client.trash(remoteId: rId)
                        }
                        try await store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        actionTracker.deleted += 1
                    } catch {
                        self.logger.error("远端目录删除失败 [\(relPath)]: \(error)")
                    }
                }
            } else if dirItem.remote?.status == .trashed && dirItem.local?.status == .present {
                // 远端删除了目录，但本地目录仍在 (原意图: deleteLocal)
                // 屏障检查：若后代中存在本地新增、修改或冲突文件，绝对禁止删除本地目录
                if barrier.localPresent > 0 || barrier.remotePresent > 0 || barrier.pending > 0 {
                    self.logger.info("后代屏障生效：本地目录 [\(relPath)] 包含本地新增或修改的后代文件 (localPresent: \(barrier.localPresent))，阻止本地目录删除扩散")
                    if let rId = dirItem.remoteFileId {
                        try? await self.client.untrash(remoteId: rId)
                    }
                    try await store.batchWrite { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE items SET remote_status = 'present', phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindInt64(dirItem.itemId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                } else {
                    // 后代全部已清理，核实本地目录为空后安全移入废纸篓
                    var trashSucceeded = true
                    if FileManager.default.fileExists(atPath: localDirURL.path) {
                        let contents = (try? FileManager.default.contentsOfDirectory(atPath: localDirURL.path)) ?? []
                        let nonHidden = contents.filter { !$0.hasPrefix(".") }
                        if nonHidden.isEmpty {
                            var trashURL: NSURL?
                            do {
                                try FileManager.default.trashItem(at: localDirURL, resultingItemURL: &trashURL)
                            } catch {
                                trashSucceeded = false
                                self.logger.warning("无法将本地目录移入废纸篓 [\(relPath)]: \(error)，保留本地目录并标记为 blocked")
                            }
                        } else {
                            trashSucceeded = false
                            self.logger.warning("本地目录 [\(relPath)] 非空，阻止删除并标记为 blocked")
                        }
                    }

                    if trashSucceeded {
                        try await store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET is_tombstone = 1, phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        actionTracker.deleted += 1
                    } else {
                        try await store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET phase = 'blocked', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                            """)
                            stmt.bindDouble(now, at: 1)
                            stmt.bindInt64(dirItem.itemId, at: 2)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                    }
                }
            }
        }

        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            syncGroup.notify(queue: .global(qos: .userInitiated)) {
                cont.resume()
            }
        }

        try await store.flush()
        try await store.checkpoint()

        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        stats.directoriesCreated = scanProgress.dirsCreated
        stats.filesScanned = scanProgress.scanned
        stats.filesSkipped = scanProgress.skipped
        stats.filesUploaded = actionTracker.uploaded
        stats.bytesUploaded = actionTracker.bytesUp
        stats.filesDownloaded = actionTracker.downloaded
        stats.bytesDownloaded = actionTracker.bytesDown
        stats.filesDeleted = actionTracker.deleted
        stats.conflictsResolved = actionTracker.conflicts
        stats.elapsedSeconds = elapsed
        notifier.finish()

        return stats
    }
}
