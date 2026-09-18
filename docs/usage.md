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

---

## 3. 同步接口调用方法

`SyncEngine` 遵循 `v1.md` 规范，对外暴露三种明确的同步意图接口：

### 3.1 本地目录到远端空目录同步 (`syncLocalToRemoteEmpty`)

适用于首次将本地已有的大型项目或目录上传到 Google Drive 上一个全新的或空的远程目录。

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

