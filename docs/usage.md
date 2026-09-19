# GDrive 外部接口调用指南 (Usage Guide)

`GDrive` 是一个专为 macOS (14.0+) 设计的 Google Drive 双向流式同步库。核心架构采用 SQLite WAL 作为同步基线，结合 Darwin 原生 `DirectoryScanner` 进行极速文件树扫描与变更比对，依托预分配 ID 池和三方状态协调决策引擎（Reconciler）保证文件级最终一致性。

---

## 1. 架构总览与调用流程

外部调用方（如 UI 应用、常驻后台 Daemon、CLI 工具等）主要与以下四个核心组件交互：

```text
[调用方 / 宿主应用]
        │
        ├── 1. 凭据管理 ──────→ Auth (读取 ~/.gdrive/auth.json 或自定义 OAuth 刷新)
        ├── 2. 状态基线 ──────→ StateStore (SQLite WAL 数据库，单 Writer 多并发 Readers)
        ├── 3. 网络传输 ──────→ DriveClient (Google Drive REST / Multipart / Resumable)
        └── 4. 同步调度 ──────→ SyncEngine (驱动初始同步与增量双向同步)
```

---

## 2. 快速开始 (Quick Start)

### 2.1 依赖引入 (`Package.swift`)

在你的 `Package.swift` 中添加 `GDrive` 依赖：

```swift
// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "MyApp",
    platforms: [
        .macOS(.v14)
    ],
    dependencies: [
        .package(path: "/path/to/gdrive")
    ],
    targets: [
        .target(
            name: "MyApp",
            dependencies: [
                .product(name: "GDrive", package: "gdrive")
            ]
        )
    ]
)
```

### 2.2 初始化引擎组件

```swift
import Foundation
import GDrive

// 1. 初始化凭据（默认读取 ~/.gdrive/auth.json，支持自动 Refresh Token）
let auth = try Auth()

// 2. 初始化持久化 SQLite 状态存储（建议位于应用支持目录下）
let dbURL = FileManager.default.homeDirectoryForCurrentUser
    .appendingPathComponent(".gdrive/gdrive.sqlite")
let store = try await StateStore(path: dbURL.path)

// 3. 初始化 Google Drive 通信客户端
let client = DriveClient(auth: auth)

// 4. 创建同步引擎
let engine = try await SyncEngine(auth: auth, store: store, client: client)
```

### 2.3 设置下载临时目录

默认下载到 `~/.gdrive/<remoteRootId>/` 下的独立临时文件，SHA-256 校验完成后原子发布到同步目录。
这里的 `remoteRootId` 是 Google Drive 同步根文件夹 ID，不是 SQLite 的数字 `root_id`。
初始化下载、增量下载及冲突恢复都使用此设置；目录按需创建。

```swift
// 设置的是基目录；引擎自动追加远程根 ID，不需要调用方追加。
try engine.setDownloadTemporaryDirectory(
    URL(fileURLWithPath: "/Volumes/Data/gdrive-downloads", isDirectory: true)
)
print(engine.downloadTemporaryDirectory.path)

// 也可以在初始化时配置。
let configuredEngine = try await SyncEngine(
    auth: auth,
    store: store,
    client: client,
    downloadTemporaryDirectory: URL(fileURLWithPath: "/Volumes/Data/gdrive-downloads", isDirectory: true)
)

// 恢复默认设置。
try engine.setDownloadTemporaryDirectory(DriveClient.defaultDownloadTemporaryDirectory)
```

`setDownloadTemporaryDirectory(_ directory: URL) throws` 接受本地文件 URL，配置由引擎实例持有，
不写入数据库。同步轮次选定目录后，后续设置变更不会移动或重定向该轮在途下载。
目录不能位于当前同步根或同一 StateStore 中其他已激活的同步根内（包括符号链接指向这些目录的情况）。
临时目录与下载目标必须位于同一文件系统，以保持原子发布；跨卷会明确失败，
应将基目录设置到目标卷上、所有同步目录之外。配置多个目标卷时，分别使用对应配置的引擎实例。

普通失败会清理未发布的下载临时文件。替换本地文件时的恢复文件也保留在该临时目录：
成功后旧版本移至废纸篓，若无法移入废纸篓或检测到发布竞态，则保留恢复文件。
进程异常终止留下的临时文件不会在启动时自动清扫。

直接调用 `DriveClient.downloadFile` 时，可用 `temporaryDirectory:` 指定完整暂存目录；
该底层方法不知道同步根 ID，不会自动追加根 ID，省略时使用 `~/.gdrive`。

---

## 3. 同步接口调用方法

`SyncEngine` 提供了开箱即用的**统一同步入口**（内部自动探测状态并安全分流），同时也保留了底层明确意图的子接口：

### 3.1 统一智能同步入口 (`sync`) 【推荐】

调用方**无需关心**远端是否为空或本地是否已有同步基线，`engine.sync` 会自动向云端和本地发起状态探测：

```swift
let localDir = "/Users/username/Documents/MyProject"
let remoteFolderId = "1UCWm-xg7Ih8LL9C64pKdZJcIBhwx-z36" // Google Drive 目标文件夹 ID

// 单一接口直接调用，引擎全自动感知与决策
// 支持可选的 onProgress 回调（内置 500ms 防抖/节流合并，提供已完成与动态发现的总数）
let stats = try await engine.sync(
    localPath: localDir,
    remoteFolderId: remoteFolderId,
    concurrency: 16,
    onProgress: { progress in
        let pct = String(format: "%.1f", progress.percentage * 100)
        print("进度: [\(pct)%] 已完成 \(progress.completedFiles) / 当前已发现 \(progress.totalDiscoveredFiles) 项 (\(progress.completedBytes)/\(progress.totalDiscoveredBytes) 字节)")
    }
)

print("同步完成: 上传 \(stats.filesUploaded) 项, 下载 \(stats.filesDownloaded) 项, 失败 \(stats.filesFailed) 项, 耗时 \(String(format: "%.2f", stats.elapsedSeconds))s")
```

- **自动决策流程**：
  1. **已有基线**：自动执行增量双向同步 (`syncIncremental`)，毫秒级比对。
  2. **首次同步**：
     - 本地有文件且云端为空：自动路由至 `syncLocalToRemoteEmpty` 极速流式上传。
     - 云端有文件且本地为空：自动路由至 `syncRemoteToLocalEmpty` 极速下载。
     - 双端均为空：自动初始化空基线。
     - 双端皆非空且无基线：抛出清晰异常拦截，杜绝盲合并导致覆盖已有文件。

---

### 3.2 底层显式同步接口（进阶）

若调用方需要在特定业务流程中显式指定初始化方向，可调用专用子接口：

#### (1) 本地目录到远端空目录同步 (`syncLocalToRemoteEmpty`)

```swift
let localDir = "/Users/username/Documents/MyProject"
let remoteRootId = "1UCWm-xg7Ih8LL9C64pKdZJcIBhwx-z36" // Google Drive 目标文件夹 ID

let stats = try await engine.syncLocalToRemoteEmpty(
    localPath: localDir,
    remoteRootId: remoteRootId,
    maxConcurrency: 16 // 传输并发数，默认 16
)

print("初始上传完成:")
print("  - 上传文件: \(stats.filesUploaded)")
print("  - 上传字节: \(stats.bytesUploaded) bytes")
print("  - 创建目录: \(stats.directoriesCreated)")
print("  - 耗时: \(String(format: "%.2f", stats.durationSeconds))s")
```

- **特性**：采用“边扫描、边建目录、边上传”的流水线，首文件发现后立即开始网络请求，无需等待整树扫描完成。
- **保护**：若本地目录不存在或非目录，或远端目标不是有效目录，直接抛出异常保护。

---

### 3.2 远端目录到本地空目录同步 (`syncRemoteToLocalEmpty`)

适用于新设备首次将云端已有目录完整下载至本地空目录。

```swift
let emptyLocalDir = "/Users/username/Documents/RestoredProject"
let remoteRootId = "1UCWm-xg7Ih8LL9C64pKdZJcIBhwx-z36"

let stats = try await engine.syncRemoteToLocalEmpty(
    localPath: emptyLocalDir,
    remoteRootId: remoteRootId,
    maxDownloadConcurrency: 16 // 下载并发数，默认 16
)

print("初始下载完成:")
print("  - 下载文件: \(stats.filesDownloaded)")
print("  - 下载字节: \(stats.bytesDownloaded) bytes")
print("  - 本地建目录: \(stats.directoriesCreated)")
```

- **特性**：流式递归遍历，原子临时文件落地校验 SHA-256 后发布，建立完整的 SQLite 共同基线。
- **保护**：强制要求本地目标必须为空目录，防止误覆盖本地已有文件。

---

### 3.3 增量双向同步 (`syncIncremental`)

在完成上述任一初始同步后，日常的双向变更同步全部通过 `syncIncremental` 执行。

```swift
let stats = try await engine.syncIncremental(
    localPath: localDir,
    remoteRootId: remoteRootId,
    maxConcurrency: 16
)

print("增量同步完成:")
print("  - 上传文件: \(stats.filesUploaded)")
print("  - 下载文件: \(stats.filesDownloaded)")
print("  - 删除项数: \(stats.filesDeleted)")
print("  - 冲突保留: \(stats.conflicts)")
print("  - 跳过未变: \(stats.filesSkipped)")
```

- **三方仲裁（Reconciler）行为规范**：
  1. **文件修改**：本地修改上传至云端；云端修改原子下载至本地。
  2. **文件/目录重命名与移动**：
     - 本地重命名或移动：通过 Darwin `(device, inode)` 零传输秒级识别，仅向云端发送元数据 `PATCH` 更新名称或父级，**不重复上传文件正文**。
     - 云端重命名或移动：通过 Changes 事件感知，本地直接在磁盘执行原子 `moveItem`，保持两端拓扑对齐。
  3. **删除与废纸篓保护**：
     - 云端删除项：本地优先调用 `FileManager.default.trashItem` 移入 macOS 系统废纸篓，杜绝硬删除丢失数据。
     - 本地删除项：云端调用 `trash(remoteId:)` 移入 Google Drive 回收站（`trashed: true`），两端皆可安全找回。
  4. **冲突解决**：
     - 若双端同时修改且 SHA-256 摘要不同，双向保留版本（原路径保留胜者，另一方保存为 `... (Conflict <uuid>).ext` 副本），两端永久记录冲突标记，不丢任何数据。
  5. **秒级快速比对**：
     - 未修改的文件通过 `(dev, ino, mtime, size)` 缓存 100% 毫秒级跳过，不读盘、不算哈希。

---

## 4. 统计结果结构 (`SyncStats`)

每次同步调用均返回不可变的 `SyncStats` 结构体：

```swift
public struct SyncStats: Sendable {
    public var filesUploaded: Int       // 成功上传的文件数量
    public var bytesUploaded: Int64     // 成功上传的总字节数
    public var filesDownloaded: Int     // 成功下载的文件数量
    public var bytesDownloaded: Int64   // 成功下载的总字节数
    public var filesDeleted: Int        // 本地/远端删除或移入回收站的项数
    public var directoriesCreated: Int  // 创建的目录总数
    public var filesSkipped: Int        // 秒级跳过的无变更文件数量
    public var conflicts: Int           // 产生并妥善保留的冲突版本数量
    public var durationSeconds: Double  // 本次同步耗时（秒）
}
```

---

## 5. 外部调用方如何实现常驻监听与自动触发

`GDrive` 库遵循轻量化设计，将监听策略的控制权交由调用方（宿主应用）。调用方可以根据业务场景选择最适宜的触发模型：

### 示例 A：定时轮询 (Recurring Polling)

适合简单的后台同步守护任务：

```swift
Task {
    while !Task.isCancelled {
        do {
            try await engine.syncIncremental(
                localPath: localDir,
                remoteRootId: remoteRootId
            )
        } catch {
            print("增量同步出错: \(error)")
        }
        // 每 30 秒轮询一次云端与本地变化
        try await Task.sleep(nanoseconds: 30 * 1_000_000_000)
    }
}
```

### 示例 B：结合 macOS FSEvents 本地文件监听

适合需要本地变更即时同步（带有 200ms 防抖 Debounce）的高响应应用：

```swift
import CoreServices

final class DirectoryWatcher {
    private var stream: FSEventStreamRef?
    private let onChange: () -> Void

    init(path: String, onChange: @escaping () -> Void) {
        self.onChange = onChange
        var context = FSEventStreamContext(
            version: 0,
            info: Unmanaged.passRetained(self as AnyObject).toOpaque(),
            retain: nil,
            release: nil,
            copyDescription: nil
        )

        let callback: FSEventStreamCallback = { _, clientCallBackInfo, numEvents, eventPaths, eventFlags, eventIds in
            guard let info = clientCallBackInfo else { return }
            let watcher = Unmanaged<AnyObject>.fromOpaque(info).takeUnretainedValue() as! DirectoryWatcher
            watcher.onChange()
        }

        self.stream = FSEventStreamCreate(
            kCFAllocatorDefault,
            callback,
            &context,
            [path] as CFArray,
            FSEventStreamEventId(kFSEventStreamEventIdSinceNow),
            0.2, // 200ms 防抖窗口
            UInt32(kFSEventStreamCreateFlagFileEvents | kFSEventStreamCreateFlagNoDefer)
        )
        FSEventStreamSetDispatchQueue(stream!, DispatchQueue.global())
        FSEventStreamStart(stream!)
    }

    deinit {
        if let stream {
            FSEventStreamStop(stream)
            FSEventStreamInvalidate(stream)
            FSEventStreamRelease(stream)
        }
    }
}

// 宿主调用：
let watcher = DirectoryWatcher(path: localDir) {
    Task {
        try? await engine.syncIncremental(localPath: localDir, remoteRootId: remoteRootId)
    }
}
```

---

## 6. 异常与错误处理

所有公开方法均为标准 Swift `async throws`。可能抛出的主要异常类型包括：

- `DriveError.rateLimited(retryAfter:)`: Google Drive API 429 限流，包含建议退避等待时间。
- `DriveError.checksumMismatch(expected:actual:)`: 数据下载或上传的 SHA-256 校验不匹配。
- `DriveError.notFound(fileId:)`: 远端目录或文件不存在（404）。
- `CocoaError`: 本地文件读写、权限或磁盘已满错误。
- `NSError (domain: "SyncEngine")`: 核心状态机校验不通过（如目标路径非空、目录拓扑断链等）。

建议在外部使用 `do-catch` 捕获并记录日志，针对临时性网络或限流错误进行指数退避重试。

---

## 7. 大文件（> 8MB）分块断点续传与持久化机制

对于极端大文件（例如 500MB 或更大）：
1. **恒定低内存消耗**：
   - 使用 `FileHandle` 流式计算 SHA-256（1MB 缓冲切片），不将全文件载入内存。
   - 上传时按配置的块大小（默认 8MB，必须为 256KB 整数倍）单块切片读取并发送。
2. **块级 SQLite 原子落盘**：
   - 每完成一个分块并获得 Google Drive 服务端确认（HTTP 308）后，立即在 SQLite `operations` 表持久化更新 `confirmed_offset`、`session_uri` 与时间戳。
3. **断电/跨线程无缝续传**：
   - 同步引擎在发起上传前自动检索 `operations` 表中是否存在 `inFlight` 状态的会话。
   - 存在有效断点时，向 Google Drive 发送探测请求（`bytes */totalBytes`）核验云端真实确认的 offset。
   - 随后直接从该 offset 定位继续上传剩余分块，换线程、应用重启或网络中断后绝不重传已确认的数据块。

---

## 8. 实时传输与速率监控 (`TransferSnapshot`)

同步引擎内置了线程安全的 `TransferMonitor`，并在内存中每 500ms 自动采样并刷新一次最新状态。UI 或监控定时器可随时直接同步获取当前状态，耗时 0ms（无磁盘/无网络 I/O）：

```swift
// 直接从内存获取最新快照
let status = engine.transferStatus // 或 engine.getTransferStatus()

// 1. 正在活跃传输的文件列表
for item in status.activeUploads {
    print("正在上传: \(item.name), 进度: \(String(format: "%.1f", item.progress * 100))% (\(item.transferredBytes)/\(item.totalBytes) bytes)")
}
for item in status.activeDownloads {
    print("正在下载: \(item.name), 进度: \(String(format: "%.1f", item.progress * 100))% (\(item.transferredBytes)/\(item.totalBytes) bytes)")
}

// 2. 处于等待队列中的文件列表
for item in status.queuedUploads {
    print("排队上传: \(item.name), 大小: \(item.totalBytes) bytes")
}
for item in status.queuedDownloads {
    print("排队下载: \(item.name), 大小: \(item.totalBytes) bytes")
}

// 3. 当前合计网络实时传输速率（每 500ms 滑动窗口计算）
let uploadMBs = status.uploadSpeedBytesPerSecond / (1024 * 1024)
let downloadMBs = status.downloadSpeedBytesPerSecond / (1024 * 1024)
print("当前实时上传速度: \(String(format: "%.2f", uploadMBs)) MB/s")
print("当前实时下载速度: \(String(format: "%.2f", downloadMBs)) MB/s")
```
