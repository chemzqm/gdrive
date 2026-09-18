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
    public var elapsedSeconds: Double = 0
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
        concurrency: Int = 16
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
            return try await syncIncremental(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxConcurrency: concurrency)
        }

        // 2. 首次同步：自动探测本地与远端目录真实状态
        logger.info("[Sync] 未建立基线，开始探测本地与云端目录状态...")

        // 探测远端目录：验证存在、是否为目录、是否包含非回收站子项
        let remoteFile = try await client.getFile(remoteId: remoteFolderId)
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
            return try await syncLocalToRemoteEmpty(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxUploadConcurrency: concurrency)
        } else if isLocalEmpty && !isRemoteEmpty {
            logger.info("[Sync] 检测到【云端包含文件，本地为空目录】，自动启动 remoteToLocalEmpty 初始化下载")
            return try await syncRemoteToLocalEmpty(localPath: resolvedLocalPath, remoteRootId: remoteFolderId, maxDownloadConcurrency: concurrency)
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
        maxUploadConcurrency: Int = 16
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()

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

        // 3. 设置有界并发上传流水线
        let uploadSemaphore = AsyncSemaphore(count: maxUploadConcurrency)
        let uploadGroup = DispatchGroup()

        // 加载快速变更比对基线缓存 (§6.2)
        let baselineCache = try await LocalBaselineCache.load(store: store, rootId: rootId)

        final class ProgressTracker: @unchecked Sendable {
            var filesUploaded = 0
            var filesSkipped = 0
            var bytesUploaded: Int64 = 0
            var dirsCreated = 0
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
        let request = ScanRequest(root: resolvedLocalPath, filters: [], options: scanOptions)
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

                    if relPath.isEmpty { continue }

                    let parentRel = (relPath as NSString).deletingLastPathComponent
                    let name = (relPath as NSString).lastPathComponent

                    if record.type == .directory {
                        // 目录处理：排队等待其父目录就绪后立即在远端创建
                        uploadGroup.enter()
                        Task {
                            defer { uploadGroup.leave() }
                            do {
                                let remoteId = try await self.idPool.nextId()
                                let remoteParentId = await directoryTracker.awaitParentReady(parentRelPath: parentRel)

                                // 远端创建目录
                                _ = try await self.client.createDirectory(name: name, parentId: remoteParentId, remoteId: remoteId)

                                // 记录数据库项
                                let grandParentItemId = localDirMap.get(parentRel) ?? rootItemId
                                let dirItemId: Int64 = try await self.store.write { conn in
                                    let dirDev = Int64(record.metadata?.identity.device ?? 1)
                                    let dirIno = Int64(record.metadata?.identity.inode ?? 0)
                                    let stmt = try conn.cachedStatement("""
                                    INSERT INTO items (
                                        root_id, parent_id, name, entry_kind, remote_file_id,
                                        local_device, local_inode, local_status,
                                        phase, created_at, updated_at
                                    ) VALUES (?, ?, ?, 'directory', ?, ?, ?, 'present', 'committed', ?, ?)
                                    ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                                    DO UPDATE SET remote_file_id = excluded.remote_file_id, local_device = excluded.local_device, local_inode = excluded.local_inode, local_status = 'present', updated_at = excluded.updated_at;
                                    """)
                                    stmt.bindInt64(rootId, at: 1)
                                    stmt.bindInt64(grandParentItemId, at: 2)
                                    stmt.bindText(name, at: 3)
                                    stmt.bindText(remoteId, at: 4)
                                    stmt.bindInt64(dirDev, at: 5)
                                    stmt.bindInt64(dirIno, at: 6)
                                    let ts = Date().timeIntervalSince1970
                                    stmt.bindDouble(ts, at: 7)
                                    stmt.bindDouble(ts, at: 8)
                                    _ = try stmt.step()
                                    stmt.reset()

                                    let qStmt = try conn.cachedStatement("""
                                    SELECT item_id FROM items WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;
                                    """)
                                    qStmt.bindInt64(rootId, at: 1)
                                    qStmt.bindInt64(grandParentItemId, at: 2)
                                    qStmt.bindText(name, at: 3)
                                    guard try qStmt.step(), let dId = qStmt.columnInt64(at: 0) else {
                                        throw NSError(domain: "SyncEngine", code: 5, userInfo: [NSLocalizedDescriptionKey: "无法获取 dir item_id"])
                                    }
                                    qStmt.reset()
                                    return dId
                                }
                                localDirMap.set(relPath, id: dirItemId)

                                // 广播唤醒等待该目录的全部子项
                                await directoryTracker.markDirectoryReady(relPath: relPath, remoteId: remoteId)
                                progress.dirsCreated += 1
                            } catch {
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
                            progress.filesSkipped += 1
                            continue
                        }

                        // 文件处理：等待其直接父目录就绪后立即上传
                        self.monitor.enqueueUpload(id: fullPath, name: name, totalBytes: Int64(record.metadata?.fileSize ?? 0))
                        uploadGroup.enter()
                        Task {
                            await uploadSemaphore.wait()
                            defer {
                                self.monitor.finishUpload(id: fullPath)
                                uploadSemaphore.signal()
                                uploadGroup.leave()
                            }

                            do {
                                let remoteFileId = try await self.idPool.nextId()
                                let remoteParentId = await directoryTracker.awaitParentReady(parentRelPath: parentRel)

                                // 恒定内存流式计算 SHA-256 与文件大小
                                let fileURL = URL(fileURLWithPath: fullPath)
                                let (sha256Hex, fileSize) = try SyncEngine.computeFileSha256(at: fileURL)
                                self.monitor.startUpload(id: fullPath, name: name, totalBytes: fileSize)

                                let dev = Int64(record.metadata?.identity.device ?? 1)
                                let ino = Int64(record.metadata?.identity.inode ?? 0)
                                let mtime = (record.metadata?.modificationTime.seconds ?? 0) * 1_000_000_000 + Int64(record.metadata?.modificationTime.nanoseconds ?? 0)

                                // 意图持久化
                                let parentDirItemId = localDirMap.get(parentRel) ?? rootItemId
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

                                // 根据文件大小执行上传：≤ 8MB 走 Multipart，> 8MB 走 Resumable
                                let limit8MB: Int64 = 8 * 1024 * 1024
                                if fileSize <= limit8MB {
                                    let fileData = try Data(contentsOf: fileURL)
                                    _ = try await self.client.uploadMultipart(
                                        name: name,
                                        parentId: remoteParentId,
                                        remoteId: remoteFileId,
                                        content: fileData,
                                        expectedSha256: sha256Hex
                                    )
                                    self.monitor.reportUploadProgress(id: fullPath, additionalBytes: fileSize)
                                } else {
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
                                }

                                // 提交共同基线 B
                                try await self.store.batchWrite { conn in
                                    let updateStmt = try conn.cachedStatement("""
                                    UPDATE items SET
                                        base_sha256 = ?,
                                        base_size = ?,
                                        phase = 'committed',
                                        dirty_generation = 0,
                                        updated_at = ?
                                    WHERE root_id = ? AND remote_file_id = ?;
                                    """)
                                    updateStmt.bindText(sha256Hex, at: 1)
                                    updateStmt.bindInt64(fileSize, at: 2)
                                    updateStmt.bindDouble(Date().timeIntervalSince1970, at: 3)
                                    updateStmt.bindInt64(rootId, at: 4)
                                    updateStmt.bindText(remoteFileId, at: 5)
                                    _ = try updateStmt.step()
                                    updateStmt.reset()
                                }

                                progress.filesUploaded += 1
                                progress.bytesUploaded += fileSize
                            } catch {
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
        stats.bytesUploaded = progress.bytesUploaded
        stats.elapsedSeconds = elapsed

        return stats
    }

    // MARK: - 模式 2：远端目录 -> 本地空目录极速下载 (remoteToLocalEmpty)

    /// 将远端非空目录流式全量下载同步至本地空目录
    @discardableResult
    public func syncRemoteToLocalEmpty(
        localPath: String,
        remoteRootId: String,
        maxDownloadConcurrency: Int = 16
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()

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

        let downloadSemaphore = AsyncSemaphore(count: maxDownloadConcurrency)
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
                    self.monitor.enqueueDownload(id: item.id, name: item.name, totalBytes: item.sizeBytes ?? 0)
                    downloadGroup.enter()
                    Task {
                        await downloadSemaphore.wait()
                        defer {
                            self.monitor.finishDownload(id: item.id)
                            downloadSemaphore.signal()
                            downloadGroup.leave()
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
                                    local_generation, local_status, phase,
                                    created_at, updated_at
                                ) VALUES (
                                    ?, ?, ?, 'file', ?,
                                    ?, ?, ?,
                                    ?, ?,
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
                                let ts = Date().timeIntervalSince1970
                                stmt.bindDouble(ts, at: 10)
                                stmt.bindDouble(ts, at: 11)
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
        maxConcurrency: Int = 16
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
            maxConcurrency: maxConcurrency
        )
    }

    /// 执行双向增量同步
    @discardableResult
    public func syncIncremental(
        rootId: Int64,
        rootItemId: Int64,
        localPath: String,
        remoteRootId: String,
        maxConcurrency: Int = 16
    ) async throws -> SyncStats {
        let startTime = DispatchTime.now()
        var stats = SyncStats()
        let resolvedLocalPath = (localPath as NSString).expandingTildeInPath
        let rootURL = URL(fileURLWithPath: resolvedLocalPath)
        let now = Date().timeIntervalSince1970

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
                        if change.removed == true || change.file?.trashed == true {
                            // 远端删除 / 放入回收站
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
                                        remote_generation, phase, dirty_generation,
                                        created_at, updated_at
                                    ) VALUES (
                                        ?, ?, ?, 'file', ?,
                                        ?, ?, ?, 'present',
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
        let request = ScanRequest(root: resolvedLocalPath, filters: [], options: scanOptions)
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

                if relPath.isEmpty { continue }

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
                        let newDirRemoteId = (try? await self.idPool.nextId()) ?? UUID().uuidString

                        _ = try? await self.client.createDirectory(name: name, parentId: remoteParentId, remoteId: newDirRemoteId)
                        let dItemId: Int64 = try await self.store.write { conn in
                            let stmt = try conn.cachedStatement("""
                            INSERT INTO items (
                                root_id, parent_id, name, entry_kind, remote_file_id,
                                local_device, local_inode, local_status, phase, created_at, updated_at
                            ) VALUES (?, ?, ?, 'directory', ?, ?, ?, 'present', 'committed', ?, ?)
                            ON CONFLICT (root_id, parent_id, name) WHERE is_tombstone = 0 AND parent_id IS NOT NULL
                            DO UPDATE SET remote_file_id = excluded.remote_file_id, local_device = excluded.local_device, local_inode = excluded.local_inode, local_status = 'present', updated_at = excluded.updated_at;
                            """)
                            stmt.bindInt64(rootId, at: 1)
                            stmt.bindInt64(parentItemId, at: 2)
                            stmt.bindText(name, at: 3)
                            stmt.bindText(newDirRemoteId, at: 4)
                            stmt.bindInt64(dev, at: 5)
                            stmt.bindInt64(ino, at: 6)
                            stmt.bindDouble(now, at: 7)
                            stmt.bindDouble(now, at: 8)
                            _ = try stmt.step()
                            stmt.reset()

                            let q = try conn.cachedStatement("SELECT item_id FROM items WHERE root_id = ? AND parent_id = ? AND name = ? AND is_tombstone = 0;")
                            q.bindInt64(rootId, at: 1)
                            q.bindInt64(parentItemId, at: 2)
                            q.bindText(name, at: 3)
                            guard try q.step(), let id = q.columnInt64(at: 0) else {
                                throw NSError(domain: "SyncEngine", code: 22, userInfo: nil)
                            }
                            q.reset()
                            return id
                        }
                        dirContext.register(itemId: dItemId, parentItemId: parentItemId, name: name, remoteId: newDirRemoteId)
                        seenDirTracker.markSeen(parentId: parentItemId, name: name)
                        scanProgress.incDirs()
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
                            local_generation, local_status, phase, dirty_generation,
                            created_at, updated_at
                        ) VALUES (
                            ?, ?, ?, 'file',
                            ?, ?, ?, ?, ?,
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
        }

        let dirtyItems: [DirtyRecord] = try await store.read { conn in
            let stmt = try conn.cachedStatement("""
            SELECT item_id, parent_id, name, remote_file_id, entry_kind,
                   base_sha256, base_size,
                   local_sha256, local_size, local_status,
                   remote_sha256, remote_size, remote_status
            FROM items
            WHERE root_id = ? AND dirty_generation > 0 AND is_tombstone = 0;
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

                records.append(DirtyRecord(
                    itemId: iId, parentId: pId, name: name, remoteFileId: rFileId,
                    entryKind: kind, baseline: baseline, local: local, remote: remote
                ))
            }
            stmt.reset()
            return records
        }

        let syncSemaphore = AsyncSemaphore(count: maxConcurrency)
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

        for item in dirtyItems {
            let decision: ReconcileDecision
            if item.entryKind == "directory" {
                if item.local?.status == .absent && item.remote?.status != .absent && item.remote?.status != .trashed {
                    decision = .trashRemote
                } else if item.remote?.status == .trashed && item.local?.status == .present {
                    decision = .deleteLocal
                } else {
                    // 目录本身无需内容下载/上传，直接清除 dirty_generation
                    try await store.batchWrite { conn in
                        let stmt = try conn.cachedStatement("""
                        UPDATE items SET phase = 'committed', dirty_generation = 0, updated_at = ? WHERE item_id = ?;
                        """)
                        stmt.bindDouble(now, at: 1)
                        stmt.bindInt64(item.itemId, at: 2)
                        _ = try stmt.step()
                        stmt.reset()
                    }
                    continue
                }
            } else {
                decision = Reconciler.decide(baseline: item.baseline, local: item.local, remote: item.remote)
            }

            switch decision {
            case .upload, .keepModified(preferLocal: true):
                self.monitor.enqueueUpload(id: item.name, name: item.name, totalBytes: item.local?.size ?? 0)
                syncGroup.enter()
                Task {
                    await syncSemaphore.wait()
                    defer {
                        self.monitor.finishUpload(id: item.name)
                        syncSemaphore.signal()
                        syncGroup.leave()
                    }

                    do {
                        let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                        let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                        let localFileURL = rootURL.appendingPathComponent(relPath)
                        let remoteParentId = dirContext.getRemoteId(for: item.parentId) ?? remoteRootId

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

                        let finalRemoteId: String
                        if let existingRemoteId = item.remoteFileId {
                            // 远端文件已存在：使用 PATCH 更新文件正文
                            finalRemoteId = existingRemoteId
                            if fSize <= 8 * 1024 * 1024 {
                                let data = try Data(contentsOf: localFileURL)
                                _ = try await self.client.updateMultipart(
                                    remoteId: existingRemoteId,
                                    content: data,
                                    expectedSha256: sha256Hex
                                )
                                self.monitor.reportUploadProgress(id: item.name, additionalBytes: fSize)
                            } else {
                                _ = try await self.performResumableUpload(
                                    rootId: rootId,
                                    itemId: item.itemId,
                                    fileURL: localFileURL,
                                    fileSize: fSize,
                                    expectedSha256: sha256Hex,
                                    remoteId: existingRemoteId,
                                    parentId: remoteParentId,
                                    name: item.name,
                                    isUpdate: true
                                )
                            }
                        } else {
                            // 远端文件不存在：使用预分配 ID POST 新建
                            let newRemoteId = try await self.idPool.nextId()
                            finalRemoteId = newRemoteId
                            if fSize <= 8 * 1024 * 1024 {
                                let data = try Data(contentsOf: localFileURL)
                                _ = try await self.client.uploadMultipart(
                                    name: item.name,
                                    parentId: remoteParentId,
                                    remoteId: newRemoteId,
                                    content: data,
                                    expectedSha256: sha256Hex
                                )
                                self.monitor.reportUploadProgress(id: item.name, additionalBytes: fSize)
                            } else {
                                _ = try await self.performResumableUpload(
                                    rootId: rootId,
                                    itemId: item.itemId,
                                    fileURL: localFileURL,
                                    fileSize: fSize,
                                    expectedSha256: sha256Hex,
                                    remoteId: newRemoteId,
                                    parentId: remoteParentId,
                                    name: item.name,
                                    isUpdate: false
                                )
                            }
                        }

                        // 提交基线
                        try await self.store.batchWrite { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET
                                remote_file_id = ?,
                                local_sha256 = ?,
                                local_size = ?,
                                base_sha256 = ?,
                                base_size = ?,
                                phase = 'committed',
                                dirty_generation = 0,
                                updated_at = ?
                            WHERE item_id = ?;
                            """)
                            stmt.bindText(finalRemoteId, at: 1)
                            stmt.bindText(sha256Hex, at: 2)
                            stmt.bindInt64(fSize, at: 3)
                            stmt.bindText(sha256Hex, at: 4)
                            stmt.bindInt64(fSize, at: 5)
                            stmt.bindDouble(now, at: 6)
                            stmt.bindInt64(item.itemId, at: 7)
                            _ = try stmt.step()
                            stmt.reset()
                        }

                        actionTracker.uploaded += 1
                        actionTracker.bytesUp += fSize
                    } catch {
                        self.logger.error("增量上传失败 [\(item.name)]: \(error)")
                    }
                }

            case .download, .keepModified(preferLocal: false):
                let downId = item.remoteFileId ?? item.name
                let downSize = item.remote?.size ?? 0
                self.monitor.enqueueDownload(id: downId, name: item.name, totalBytes: downSize)
                syncGroup.enter()
                Task {
                    await syncSemaphore.wait()
                    defer {
                        self.monitor.finishDownload(id: downId)
                        syncSemaphore.signal()
                        syncGroup.leave()
                    }

                    do {
                        guard let remoteFileId = item.remoteFileId else { return }
                        let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                        let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                        let localFileURL = rootURL.appendingPathComponent(relPath)

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
                if FileManager.default.fileExists(atPath: localFileURL.path) {
                    var trashURL: NSURL?
                    do {
                        try FileManager.default.trashItem(at: localFileURL, resultingItemURL: &trashURL)
                    } catch {
                        try? FileManager.default.removeItem(at: localFileURL)
                    }
                }
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

            case .conflict(let winner, let conflictId):
                syncGroup.enter()
                Task {
                    await syncSemaphore.wait()
                    defer {
                        syncSemaphore.signal()
                        syncGroup.leave()
                    }
                    // 处理冲突版本保留 (§11.3)
                    let parentRel = dirContext.getRelPath(for: item.parentId) ?? ""
                    let relPath = parentRel.isEmpty ? item.name : "\(parentRel)/\(item.name)"
                    let originalURL = rootURL.appendingPathComponent(relPath)

                    let ext = originalURL.pathExtension
                    let base = (item.name as NSString).deletingPathExtension
                    let conflictName = ext.isEmpty ? "\(base) (Conflict \(conflictId))" : "\(base) (Conflict \(conflictId)).\(ext)"
                    let conflictURL = originalURL.deletingLastPathComponent().appendingPathComponent(conflictName)

                    do {
                        if winner == .remote, let rId = item.remoteFileId {
                            // 远端胜：将本地重命名为冲突副本，下载远端至原路径
                            try FileManager.default.moveItem(at: originalURL, to: conflictURL)
                            try await self.client.downloadFile(remoteId: rId, destinationURL: originalURL, expectedSha256: item.remote?.sha256)
                        } else if winner == .local, let rId = item.remoteFileId {
                            // 本地胜：下载远端至冲突副本，上传本地至原路径
                            try await self.client.downloadFile(remoteId: rId, destinationURL: conflictURL, expectedSha256: item.remote?.sha256)
                        }

                        try await self.store.write { conn in
                            let stmt = try conn.cachedStatement("""
                            UPDATE items SET
                                conflict_id = ?,
                                conflict_winner = ?,
                                dirty_generation = 0,
                                phase = 'committed',
                                updated_at = ?
                            WHERE item_id = ?;
                            """)
                            stmt.bindText(conflictId, at: 1)
                            stmt.bindText(winner.rawValue, at: 2)
                            stmt.bindDouble(Date().timeIntervalSince1970, at: 3)
                            stmt.bindInt64(item.itemId, at: 4)
                            _ = try stmt.step()
                            stmt.reset()
                        }
                        actionTracker.conflicts += 1
                    } catch {
                        self.logger.error("处理冲突失败 [\(item.name)]: \(error)")
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

        return stats
    }
}
