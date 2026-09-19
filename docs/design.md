# GDrive 同步核心库架构与模块设计规范

本文档详细说明 GDrive 多目录双向同步核心库的整体架构设计、各 Swift 模块的技术实
现方案以及高并发吞吐下的关键设计考量。

---

## 1. 架构总览

GDrive 采用**以 SQLite 数据库为三方同步基线 (Baseline)** 的架构，通过将本地文件
系统（使用纯 Swift `DirectoryScanner` 高并发扫描）与远端 Google Drive（基于
`Changes API` 增量通知与 `files.generateIds` 预分配）两端状态分别与本地 SQLite
基线进行比对，实现严谨可靠的双向同步与冲突决策。

```
┌────────────────────────────────────────────────────────────────────────┐
│                              SyncEngine                                │
│          (统一分流入口: localToRemoteEmpty / remoteToLocalEmpty / syncIncremental) │
└──────┬───────────────────────────┬───────────────────────────┬─────────┘
       │                           │                           │
       ▼                           ▼                           ▼
┌──────────────┐          ┌───────────────────┐       ┌─────────────────┐
│ Directory    │          │  Reconciler &     │       │ IDPool          │
│ Scanner      │          │  Baseline Cache   │       │ (预分配 ID 缓冲池)│
│ (纯 Swift     │          │  (三方决策 / 快检) │       └────────┬────────┘
│  并发扫描)   │          └─────────┬─────────┘                │
└──────┬───────┘                    │                          │
       │                            ▼                          ▼
       │                  ┌───────────────────┐       ┌─────────────────┐
       │                  │ StateStore        │       │ DriveClient     │
       │                  │ (SQLite WAL 连接池│       │ (URLSession 128 │
       │                  │  Group Commit)    │       │  Pipelining)    │
       └─────────────────►└───────────────────┘       └────────┬────────┘
                                                               │
                                                               ▼
                                                      ┌─────────────────┐
                                                      │ DriveRateLimiter│
                                                      │ (150QPS 平滑发包│
                                                      │  全局退避协同)  │
                                                      └─────────────────┘
```

---

## 2. 核心模块设计与职责划分

远端名称落盘前必须完成名称映射检查：同父目录的同名、大小写折叠及 Unicode 规范化等价名称采用明确阻塞策略，不覆盖现有 fileId。初始化按完整目录检查；Changes 通过持久 inbox 和名称表达式索引保留冲突，门控冲突路径，远端重命名后恢复。调用方通过 `remoteNameConflicts` 识别名称冲突；大小写敏感卷也采用此保守策略。

名称索引使用固定的大小写折叠和 NFC 规范化规则，规则改变时必须重建索引。冲突门控从持久 inbox 开始按索引查找，避免空队列扫描文件表。路径检查拒绝非法名称和祖先符号链接映射；父路径缺失时先解析最近的现存祖先，再验证根目录边界。

### 2.1 存储与基线层 (`Sources/GDrive/Storage/SQLite/`)

以 SQLite 作为同步状态的唯一真相来源（Ground Truth），确保进程崩溃或网络异常时数
据不损坏、不丢失。

* **`schema.sql`**：仅保留 6 张业务表。
  * `roots`：本地路径与远端目录的绑定、初始化方向和生命周期。
  * `items`：三方基线、文件身份、观察代次及墓碑；`remote_scope_excluded` 保存范围排除状态，保护对象及其后代免于反向写回。
  * `operations`：统一保存持久操作。分块上传保存 Session URI 和确认偏移；`resolveConflict` 的 `payload` 保存双方摘要、原文件和副本身份、路径及预期代次。
  * `cursors`：Changes 等事件流的持久游标。
  * `remote_change_inbox`：游标已确认但尚未成功应用的远端事件。
  * `remote_directory_scans`：目录补列任务、扫描身份和分页进度。
* **`SQLiteConnection.swift`**：
  * 封装底层的 SQLite3 C-API，全生命周期管理编译语句缓存（Prepared Statements），
    杜绝重复解析 SQL 语法开销。
* **`SQLitePool.swift`**：
  * 实现**单写多读（Single-Writer, Multiple-Readers）**连接池，基于 SQLite WAL
    (Write-Ahead Logging) 模式。
  * 读操作并发无锁执行；写操作由独立排他队列调度，配置 `busy_timeout = 5000ms`
    与 `synchronous = NORMAL`。
* **`StateStore.swift`**：
  * **Group Commit (组提交)**：将数十个高并发协程的写请求聚合为单次数据库事务，
    在达到批量上限或 5ms 超时时批量提交，消除了高并发下 SQLite 磁盘 fsync 争用瓶
    颈。

---

### 2.2 网络通信与防限流层 (`DriveClient.swift`, `DriveRateLimiter.swift`,
`Auth.swift`)

针对 Google Drive API 特性（200 QPS 上限、写操作命名空间锁、漏桶突发限流）实现的
弹性高效网络客户端。

* **`Auth.swift`**：
  * 实现 OAuth 2.0 PKCE 授权码流，本地启动 Loopback Server 接收回调；凭证持久化
    至 `~/.gdrive/auth.json` 并自动刷新。
* **`DriveClient.swift`**：
  * 底层定制 `URLSessionConfiguration`：开启 `httpShouldUsePipelining = true`，
    并将 `httpMaximumConnectionsPerHost` 提升至 **128**，支持海量请求的高速并发
    复用。
  * **内存原子 Token 缓存**：利用 macOS 14 原生 `OSAllocatedUnfairLock` 缓存有效
    Access Token，消除了每个并发任务访问 `Auth` Actor 带来的协程上下文切换。
  * **统一执行器 (`executeRequest`)**：
    * 接管所有 API 调用的网络发送。
    * **自动 401 修复**：遇到 Token 过期自动清空缓存并重新拉取有效 Token 重发。
    * **自愈重试**：遇到 429、503 或 403 限流响应，提取 `Retry-After` 并在本地执
      行带 Full Jitter（随机抖动）的指数退避重试（最多 5 次），对外透明自愈。
* **`DriveRateLimiter.swift`**：
  * **令牌桶平滑出包 (Pacing)**：目标速率设为 **150 QPS**（最高 180 QPS，突发容
    忍 24），防止 64 个并发工作任务在 1ms 内瞬时冲撞 Google 边缘网关。
  * **全局退避协同 (Coordinated Backoff)**：一旦任意一个请求捕获到 429 / 403 限
    流，立即触发全局冷却窗口。所有其他排队发包的协程在本地挂起等待，阻断重试风暴。
  * **自适应拥塞调整 (AIMD)**：遭遇限流自动乘法减速 25%，平稳运行阶段按微步长加
    法递增。

---

### 2.3 预分配 ID 管理池 (`IDPool.swift`)

*“google driver 支持本地 file id，先调用 Google Drive 的 files.generateIds 批量
拿到一批服务器认可的 ID，然后客户端本地缓存使用”*。

* **`IDPool.swift`**：
  * Actor 隔离的内存 ID 缓冲池，文件与目录在真正发起创建之前即可获取合法的
    `remoteFileId`。
  * **低水位静默预取**：当池内余量低于 200 时，后台静默拉取 1,000 个新 ID，将网
    络 ID 获取开销与实际传输完全解耦。
  * 支持 `ensureCapacity` 在大规模同步前并发批量拉取足够 ID。

---

### 2.4 同步引擎与调度层 (`Sources/GDrive/Engine/`)

* **`SyncEngine.swift`**：
  * **下载暂存目录**：通过初始化参数 `downloadTemporaryDirectory` 或
    `setDownloadTemporaryDirectory(_:)` 配置基目录，默认 `~/.gdrive`，按远程根 ID
    使用 `<基目录>/<remoteRootId>/`。初始化、增量和冲突恢复统一使用同步目录外的暂存路径，
    原子交换的恢复文件也留在该处，避免被增量扫描上传。暂存与目标需位于同一文件系统；
    跨卷明确失败，不回退到同步目录内暂存。使用方式见 [接口指南](usage.md#23-设置下载临时目录)。
  * **Changes 持久化（A13）**：每页观察、dirty 和游标原子落库，未知归属事件保留在
    `remote_change_inbox`；移出范围的对象保留本地内容并阻止反向写回。新/移入目录
    通过持久化分页任务补列，安排在已就绪传输之后。缺失游标先捕获 C0，再重建观察。
    `SyncStats.remoteWorkPending` 非零时由后续同步轮次继续处理，不能视为全部收敛。
    验证与调用约定见 [A13 验收记录](a13-validation.md)。
  * **冲突恢复（A12）**：文件变动前以 `operations` 中的 `resolveConflict` 操作持久化双方摘要、发布路径、
    副本 item/远端 ID 和观察代次。先保留并上传本地副本，再安全发布远端原文件，最后
    原子提交两个共同基线与完成回执。pending 在下轮扫描前恢复，失败不计已解决。
    采用并发执行、组提交与 clonefile 保留大文件；验收边界见 [A12 验收记录](a12-validation.md)。
  * **覆盖发布保护（A11）**：当前 Drive 正文条件写未通过实网契约验证，已有远端正文覆盖
    被阻断并保留 dirty/旧基线；新建上传继续运行。小文件使用不可变内存输入，大文件
    使用 clonefile 快照（不支持时明确失败，不回退到全量复制）。下载发布前校验
    identity/generation，使用排他 rename 或原子交换；完成回执按预期 generation 条件提交。
    具体限制与性能证据见 [A11 验收记录](a11-validation.md)。
  * **模式 1：`syncLocalToRemoteEmpty`**：
    * 调用纯 Swift `DirectoryScanner`，在 POSIX 系统调用底层遍历阶段即直接剪枝跳
      过 `.git` 目录。
    * 对 $\le 8\text{MB}$ 小文件执行单次读盘载入内存，并就地完成 SHA-256 计算，
      彻底消除 8,000+ 次重复磁盘读。
    * Multipart/related 单步直接上传，静态预计算 Boundary，不落任何磁盘临时文件。
  * **模式 2：`syncRemoteToLocalEmpty`**：
    * 流式递归列举远端目录树，边扫描边下载并流式计算 SHA-256，临时文件下载完成后
      通过原子操作重命名落盘。
  * **模式 3：`syncIncremental`**：
    * 本地变化观察按最多 64 条自然批次提交，首个变化文件立即提交；观察落库后即可
      调度上传/下载，无需等待全树扫描。哈希最多 6 个并行任务，传输先取得额度再创建
      Task；批次候选按主键读取，父目录创建未完成时延后子文件传输。
      缓存同时命中身份、路径和元数据的文件跳过逐项 SQLite 查询。缺失检测、冲突副本
      和删除保留完整扫描及后代屏障；扫描失败/取消会等待已启动任务收尾。
      证据与性能见 [A16 验收记录](a16-validation.md)。
    * 单个已枚举文件在哈希前消失或读取失败时，只将该项计入失败并保留既有数据库状态；
      同批及后续健康文件继续同步。该路径仍记为本轮已见，缺失检测不会把读取失败误判为删除。
      任务取消继续终止整轮，不降级为普通文件失败。
    * 双向增量同步：本地快速变更扫描 + Google Drive `Changes API` 增量变更拉取，
      由 Reconciler 驱动 3-way 差异决策。
  * **统一入口 `sync`**：
    * 空↔空首次绑定在一次事务内保存根身份、committed root item、existingKnown 状态及
      列举前捕获的 Changes 游标；本地顶层探测包含隐藏条目，仅排除 `.git` 目录，
      找到首个有效条目即返回，枚举错误直接传播。见 [A15 验收记录](a15-validation.md)。
    * 自动探测本地与远端目录是否包含文件，智能路由至初始化全量同步或已有基线增量
      同步。
* **`DirectoryTracker.swift`**：
  * **父目录就绪协调器**：文件或子目录在入队后先异步等待直接父目录在云端完成创建
    并获取其 Remote ID。
  * **防并发饥饿关键**：任务在父目录就绪前**不持有**上传信号量配额，确保并发上传
    槽位只被真正准备好发包的任务占用。
* **`AsyncSemaphore.swift`**：
  * 异步协程信号量，将全局并发严格硬性钳位在 **上限 64**。

---

### 2.5 快速变更与决策层 (`LocalBaselineCache.swift`, `Reconciler.swift`)

* **`LocalBaselineCache.swift`**：
  * 快速变更检测机制：利用文件系统元数据（`device` + `inode` + `mtime` +
    `fileSize`）比对已提交基线。
  * 四项元数据完全一致时，**不进行任何文件内容读取与 SHA-256 哈希计算**，在纳秒
    级秒级跳过数万个未修改文件。
* **`Reconciler.swift`**：
  * 三方决策器：基于共同基线 $B$、本地状态 $L$ 和远端状态 $R$，按照确定性规则产
    生动作（`upload` / `download` / `deleteRemote` / `deleteLocal` / `conflict`
    / `unchanged`）。
  * 严格遵循唯一校验和原则：无论元数据如何变化，最终以内容 `sha256` 是否一致判定
    是否需要传输。

---

### 2.6 进度看板与传输监控 (`ProgressNotifier.swift`, `TransferMonitor.swift`)

* **`ProgressNotifier.swift`**：
  * 拥有独立的后台定时轮询 Ticker（500ms），对外触发 `onProgress` 回调，避免将通
    知和计算开销反压给高并发的数据传输协程。
* **`TransferMonitor.swift`**：
  * 无锁环形采样窗口，实时追踪活跃传输项、排队项，计算当前准确的瞬时速度（MB/s
    与 项/s）。

---

## 3. 大文件 (> 8MB) 分块断点续传规范

严格遵循 AGENTS.md 规范：*“需要支持文件续传，但仅支持大于 8MB 的文件续传”*。

1. **分块策略**：
   * 仅当文件大小 $> 8\text{MB}$ 时，调用 `initiateResumableUpload` 开启
     Resumable 会话。
   * 每次上传固定为 **8MB**（$8 \times 1024 \times 1024$ 字节）的分块，满足
     Google Drive 对分块为 256KB 倍数的要求。
2. **状态机与断点持久化**：
   * 每完成一个 8MB 分块，立即向 SQLite `resumable_sessions` 记录确认的
     `uploaded_bytes`。
   * 若传输中途崩溃中断，重启后调用 `queryResumableOffset` 向 Google Drive 云端
     校验真实接收偏移量，云端确认无误后从断点无缝续传。
3. **脏会话防御**：
   * 在续传前比对本地文件当前 SHA-256 与会话记录的预期 SHA-256；若本地文件被修改，
     旧 Session 立即作废清理并启动全新全量上传。
