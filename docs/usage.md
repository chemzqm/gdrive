# GDrive 外部接口调用指南 (Usage Guide)

`GDrive` 是一个面向 macOS (14.0+) 的 Google Drive 双向流式同步库。核心架构采用 SQLite WAL 作为同步基线，结合 `DirectoryScanner` 扫描文件树，以 SHA-256 比较内容变化，由三方状态协调决策引擎（Reconciler）决定同步操作。

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

`SyncEngine`、`SyncStats` 和 `SyncEngineError` 的公共定义集中在
[`SyncEngineAPI.swift`](../Sources/GDrive/Engine/SyncEngineAPI.swift)，包括初始化、配置、
状态查询和同步入口。[`SyncEngine.swift`](../Sources/GDrive/Engine/SyncEngine.swift) 负责内部路由，
初始化与断点续传分别位于对应模式扩展，增量同步由
[`IncrementalSyncRun.swift`](../Sources/GDrive/Engine/IncrementalSyncRun.swift) 及其阶段扩展实现。
完整文件职责见 [架构设计](design.md)。
调用方仍通过 `import GDrive` 使用下述接口，文件拆分不改变调用方式。

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

初始化时仅 `auth` 必填。`store`、`client` 和 `idPool` 均可省略，由引擎创建；
需要共享已有组件时可显式传入。引擎通过同名只读属性公开这些组件，并通过 `monitor`
公开传输监控器（快照读取见第 8 节）。

### 2.3 设置下载临时目录

默认下载到 `~/.gdrive/remotes/<remoteRootId>/` 下的独立临时文件，SHA-256 校验完成后原子发布到同步目录。

如果同步目录中的同路径文件与远端 SHA-256 不同，远端版本保存在
`~/.gdrive/conflicts/<remoteRootId>/<relativePath>`。冲突会出现在 `SyncStats.conflicts`，进程重启后
也可查询并显式解决：

```swift
let conflicts = try await engine.listConflicts(localPath: localPath)
for conflict in conflicts {
    // 保留本地版本；后续增量同步会尝试把它同步到远端。
    try await engine.resolveConflict(id: conflict.id, resolution: .local)

    // 或者选择已下载并校验的远端版本：
    // try await engine.resolveConflict(id: conflict.id, resolution: .remote)
}
```

未解决期间，远端新版本会更新 conflicts 文件；远端删除只更新冲突状态。该路径不会传播上传或
删除操作，其他路径继续正常同步。

一侧删除而另一侧仍为已同步基线内容时会产生删除意图；本地删除使用原子隔离和版本复核后执行。
Google Drive 尚未提供本项目已验证的条件元数据 PATCH，因此本地到远端的自动删除会保持 blocked，
不会发送存在竞态窗口的无条件 Trash 请求。另一侧已修改时则进入冲突。
此时 resolution 表示调用方选择的最终状态：选择已删除的一侧会把删除扩散到另一侧，选择已修改的
一侧会恢复被删除的一侧。
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
同步结束（包括失败或取消）后删除空的远程根暂存目录；含有残留或恢复文件的目录保留。
进程异常终止留下的临时文件不会在启动时自动清扫。

直接调用 `DriveClient.downloadFile` 时，可用 `temporaryDirectory:` 指定完整暂存目录；
该底层方法不知道同步根 ID，不会自动追加根 ID，省略时使用 `~/.gdrive/remotes`。

---

## 3. 同步接口调用方法

`SyncEngine` 提供统一入口 `sync`，也提供显式初始化和增量同步入口。
这些执行整轮同步的入口均为 `async throws -> SyncStats`，支持可选的
`onProgress: (@Sendable (SyncProgress) -> Void)?` 回调，默认 `nil`。
回调不保证在主线程执行，更新 UI 时应切换到 `MainActor`。

| 同步入口 | 并发参数 | 默认值 |
| --- | --- | --- |
| `sync` | `concurrency` | `64` |
| `syncLocalToRemoteEmpty` | `maxUploadConcurrency` | `64` |
| `syncRemoteToLocalEmpty` | `maxDownloadConcurrency` | `64` |
| `syncIncremental` | `maxConcurrency` | `64` |

上述显式同步入口共用进程内根目录锁：同一本地根已有同步运行时，另一次显式调用立即抛出
`SyncEngineError.rootBusy(path:)`；不同引擎实例也共享这一限制。不同本地根可独立运行，
该锁不提供跨进程互斥。文件监听应使用 `notifyLocalChanges(_:)` 提交变化，
该接口接收并合并变化后返回，在当前整轮同步结束后自动执行。
pendingTask 执行期间同样持有根锁，显式同步调用仍返回 `rootBusy`；新的变化通知仍可接收。
显式同步自身成功后，会等待当时及排空期间接收的 pendingTask 逐批执行完毕再返回；返回的
`SyncStats` 只统计显式同步自身的轮次，不包含随后执行的 pendingTask。

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
  1. **已有基线**：已完成初始化时执行增量双向同步 (`syncIncremental`)；未完成时按记录的方向继续初始化。
  2. **首次同步**：
     - 本地有文件且云端为空：自动路由至 `syncLocalToRemoteEmpty` 流式上传。
     - 云端有文件且本地为空：自动路由至 `syncRemoteToLocalEmpty` 下载。
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
    maxUploadConcurrency: 16 // 上传并发数，默认 64
)

print("初始上传完成:")
print("  - 上传文件: \(stats.filesUploaded)")
print("  - 上传字节: \(stats.bytesUploaded) bytes")
print("  - 创建目录: \(stats.directoriesCreated)")
print("  - 耗时: \(String(format: "%.2f", stats.elapsedSeconds))s")
```

- **特性**：采用“边扫描、边建目录、边上传”的流水线，首文件发现后立即开始网络请求，无需等待整树扫描完成。
- **保护**：若本地目录不存在或非目录，或远端目标不是有效目录，直接抛出异常保护。

---

#### (2) 远端目录到本地空目录同步 (`syncRemoteToLocalEmpty`)

适用于新设备首次将云端已有目录完整下载至本地目录。本地独有文件会保留；同路径不同内容会进入冲突目录。

```swift
let emptyLocalDir = "/Users/username/Documents/RestoredProject"
let remoteRootId = "1UCWm-xg7Ih8LL9C64pKdZJcIBhwx-z36"

let stats = try await engine.syncRemoteToLocalEmpty(
    localPath: emptyLocalDir,
    remoteRootId: remoteRootId,
    maxDownloadConcurrency: 16 // 下载并发数，默认 64
)

print("初始下载完成:")
print("  - 下载文件: \(stats.filesDownloaded)")
print("  - 下载字节: \(stats.bytesDownloaded) bytes")
print("  - 本地建目录: \(stats.directoriesCreated)")
```

- **特性**：流式递归遍历，原子临时文件落地校验 SHA-256 后发布，建立完整的 SQLite 共同基线。
- **保护**：不覆盖本地已有的不同内容；远端版本写入 conflicts 目录并等待调用方显式选择。
- **失败恢复**：首次调用在下载前持久化远端观察。文件下载、校验或发布失败会计入
  `filesFailed`，保留待恢复观察且不把初始化标记为完成；再次调用 `sync()` 会继续已绑定的
  部分初始化目录，无需等待该文件产生新的 Drive Change。

---

### 3.3 增量双向同步 (`syncIncremental`)

完成初始化后，可通过 `syncIncremental` 主动执行整根双向同步，包括定时拉取远端变化。
本地文件监听回调使用 `notifyLocalChanges(_:)`，无需提供远端 ID，见第 5 节。

```swift
let stats = try await engine.syncIncremental(
    localPath: localDir,
    maxConcurrency: 16 // 并发数，默认 64
)

print("增量同步完成:")
print("  - 上传文件: \(stats.filesUploaded)")
print("  - 下载文件: \(stats.filesDownloaded)")
print("  - 删除项数: \(stats.filesDeleted)")
print("  - 已解决冲突: \(stats.conflictsResolved)")
print("  - 跳过未变: \(stats.filesSkipped)")
```

调用方只传本地根路径。GDrive 从 SQLite 中查询该路径对应的 active 根、根 item 和
Google Drive 文件夹 ID；找不到完整绑定时会在扫描和任何远端操作前抛出
`Directory has no available remote root ID: <localPath>`。

- **三方仲裁（Reconciler）行为规范**：
  1. **文件修改**：云端修改经校验后原子下载至本地。本地新文件可上传；已有远端文件的正文覆盖
     当前受 A11 保护限制，会计入 `filesFailed` 并保留待同步状态及旧基线，详见 [A11 验收记录](a11-validation.md)。
  2. **文件/目录重命名与移动**：
     - 本地重命名或移动：通过 Darwin `(device, inode)` 识别，仅向云端发送元数据 `PATCH` 更新名称或父级，**不重复上传文件正文**。
     - 云端重命名或移动：通过 Changes 事件感知，本地直接在磁盘执行原子 `moveItem`，保持两端拓扑对齐。
  3. **删除与废纸篓保护**：
     - 云端删除项：本地优先调用 `FileManager.default.trashItem` 移入 macOS 系统废纸篓，杜绝硬删除丢失数据。
     - 本地删除项：在 Google Drive 条件元数据 PATCH 契约得到真实验证前保留为 blocked，不发送无条件 Trash 请求。
  4. **冲突解决**：
     - 若双端同时修改且 SHA-256 摘要不同，先保留并上传本地冲突副本，再将远端内容发布到原路径；
       两项基线提交后才计入 `conflictsResolved`。失败时保留待恢复操作，见 [A12 验收记录](a12-validation.md)。
  5. **未变文件缓存**：
     - 文件身份、路径和元数据均命中缓存时，可跳过重复读取与哈希计算；内容变化仍以 SHA-256 判断。

---

## 4. 统计结果结构 (`SyncStats`)

显式同步调用返回自身轮次的 `SyncStats` 值，字段均为可读写的 `public var`，初始值为 `0`；
调用返回前排空的 pendingTask 不计入该值。
`notifyLocalChanges(_:)` 只确认接收，不返回同步统计；运行状态和待执行通知通过
`pendingLocalChangeStatus()` 查询，执行失败记录到日志。

```swift
public struct SyncStats: Sendable {
    public var filesScanned: Int        // 扫描的文件数量
    public var filesSkipped: Int        // 跳过的文件数量
    public var directoriesCreated: Int  // 创建的目录总数
    public var filesUploaded: Int       // 成功上传的文件数量
    public var bytesUploaded: Int64     // 成功上传的总字节数
    public var filesDownloaded: Int     // 成功下载的文件数量
    public var bytesDownloaded: Int64   // 成功下载的总字节数
    public var filesDeleted: Int        // 本地/远端删除或移入回收站的项数
    public var conflictsResolved: Int   // 完成恢复或解决的冲突数量
    public var remoteWorkPending: Int   // 留待后续同步轮次处理的远端工作数量
    public var remoteNameConflicts: Int // 因本地等价名称而受阻的远端对象数量
    public var filesFailed: Int         // 本轮处理失败的文件数量
    public var elapsedSeconds: Double  // 本次同步耗时（秒）
}
```

调用成功返回不代表所有文件均已收敛：应同时检查 `filesFailed` 和 `remoteWorkPending`。
`remoteWorkPending` 包含远端观察、目录分页等待处理工作，不是互不重复的文件数；非零时需在
后续同步轮次继续处理。`remoteNameConflicts` 表示需要在远端重命名以消除的名称冲突。
部分字段仅由相应同步模式填充，例如初始化模式不会填充 `filesScanned`。

---

## 5. 外部调用方如何实现常驻监听与自动触发

`GDrive` 库遵循轻量化设计，将监听策略的控制权交由调用方（宿主应用）。调用方可以根据业务场景选择最适宜的触发模型：

### 示例 A：定时轮询 (Recurring Polling)

适合简单的后台同步守护任务：

```swift
Task {
    while !Task.isCancelled {
        do {
            try await engine.syncIncremental(localPath: localDir)
        } catch {
            print("增量同步出错: \(error)")
        }
        // 每 30 秒轮询一次云端与本地变化
        try await Task.sleep(nanoseconds: 30 * 1_000_000_000)
    }
}
```

### 示例 B：提交本地文件和目录变化

宿主负责监听文件系统，将具体变化交给 gdrive。路径使用本地路径字符串，支持展开 `~`；
移动包含旧、新路径，删除事件也应保留原条目的 `isDirectory` 信息。

```swift
let changes: [LocalChange] = [
    .created(path: localDir + "/new.txt", isDirectory: false),
    .modified(path: localDir + "/notes.txt", isDirectory: false),
    .deleted(path: localDir + "/old-folder", isDirectory: true),
    .moved(from: localDir + "/draft.txt", to: localDir + "/final.txt", isDirectory: false)
]
try await engine.notifyLocalChanges(changes)
```

显式同步 API 在入口检查数据库连接，连接失败直接抛出；`notifyLocalChanges` 解析根绑定时的
数据库访问失败按其原有 `async throws` 契约直接返回。
接收后的后台同步中，可能影响后续传输或同步结果的数据库错误会终止本轮；能够在条目级
隔离的数据库错误只写入 error log。后台任务在边界记录本轮错误，不再提供额外错误事件。

调用方不需要 `remoteRootId`。库按路径查找所属的已配置同步根，或正在运行的父目录同步任务；
首次同步尚未写入根记录时也可接收变化。无法找到所属根时抛错，不会猜测远端目标。

每个根只有一个运行任务和一个合并中的 pendingTask：当前整轮同步结束后取出 pendingTask，
清空待执行槽位，再执行涉及文件和目录的增量同步。期间收到的新变化进入下一批，逐批执行
直到清空。根当前没有运行任务时，也会创建 pendingTask 并立即启动执行。
同一根的同步轮次串行，单轮内的传输仍可并发，不同根彼此独立。

执行前重新检查本地状态并与 SQLite 基线比较，淘汰过时操作：重复修改且 SHA-256 未变时
不传输；新增后已消失的文件不会按旧事件上传；删除后重新出现的路径不会按旧事件删除。
移动同时核实旧、新位置，目录按当前子树扫描。局部扫描只在覆盖范围内判断缺失，不会将
未扫描的兄弟目录视为删除。局部观察的文件会重新核实 SHA-256，避免移动或重建后的
路径误用旧的元数据缓存。

```swift
for status in await engine.pendingLocalChangeStatus() {
    print(status.localRootPath,
          "运行中:", status.isRunning,
          "待处理变化:", status.pendingChangeCount)
}
```
一次移动的两个路径必须属于同一个同步根；跨根移动应分别提交源路径删除和目标路径新增。
队列为进程内状态，不承诺通知的崩溃持久化；重启后应主动执行一次整根同步重新观察状态。

---

## 6. 异常与错误处理

初始化和同步入口使用 `async throws`；`setDownloadTemporaryDirectory(_:)` 为同步 `throws`，
读取配置和传输快照不抛出异常。可能抛出的主要异常类型包括：

- `SyncEngineError.rootBusy(path:)`：同一本地根在当前进程中已有同步运行；本地变化使用 `notifyLocalChanges(_:)` 合并提交。
- `SyncEngineError.localRootNotFound(path:)`：增量同步的本地根消失或不再是目录，停止同步以保护远端数据。
- `SyncEngineError.remoteRootLost(remoteId:reason:)`：远端同步根丢失、被移入回收站或不再是目录，停止同步以保护本地数据。
- `SyncEngineError.general(_:)`：配置或同步保护条件不满足，具体原因见错误描述。
- `DriveError.rateLimited429(retryAfter:)`: Google Drive API 429 请求过多（Too Many Requests），包含建议退避等待时间。
- `DriveError.rateLimited403(reason:retryAfter:)`: Google Drive API 403 用户配额或频次超限（如 `userRateLimitExceeded`、`rateLimitExceeded`），包含具体超限原因与建议退避等待时间。
- `DriveError.checksumMismatch(expected:actual:)`: 数据下载或上传的 SHA-256 校验不匹配。
- `DriveError.notFound(fileId:)`: 远端目录或文件不存在（404）。
- `DriveError.unsafeOverwrite(fileId:)`：已有远端文件的正文覆盖被保护机制阻止。
- `CocoaError`: 本地文件读写、权限或磁盘已满错误。
- `NSError (domain: "SyncEngine")`: 核心状态机校验不通过（如目标路径非空、目录拓扑断链等）。

同步可能将单项传输错误记录在日志及 `SyncStats.filesFailed` 中并继续处理其他文件；
调用方既要使用 `do-catch` 处理整轮失败，也要检查返回统计。临时性网络或限流错误可退避重试，
根丢失、名称冲突和覆盖保护等情况需要先解决相应原因。

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

同步引擎内置了线程安全的 `TransferMonitor`，并在内存中每 500ms 自动采样并刷新一次最新状态。
`transferStatus` 和 `getTransferStatus()` 均同步返回 `TransferSnapshot`，无需 `await` 或 `try`，
读取过程不涉及磁盘或网络 I/O：

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
