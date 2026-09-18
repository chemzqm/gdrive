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

### 2.1 存储与基线层 (`Sources/GDrive/Storage/SQLite/`)

以 SQLite 作为同步状态的唯一真相来源（Ground Truth），确保进程崩溃或网络异常时数
据不损坏、不丢失。

* **`schema.sql`**：
  * `roots`：管理本地路径与远端目录的映射、初始化同步方向与生命周期。
  * `items`：记录三方基线元数据，包含 `base_sha256`、`base_size`、`local_mtime`、
    `local_device`、`local_inode`、`remote_file_id`、`phase`（`inFlight` /
    `committed`）以及逻辑删除标记 `is_tombstone`。
  * `resumable_sessions`：持久化 $>8\text{MB}$ 大文件的分块上传 Session URI 与已
    确认断点偏移量，供跨进程恢复续传。
  * `sync_cursors`：持久化 Google Drive `Changes API` 的 `startPageToken`。
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
    * 双向增量同步：本地快速变更扫描 + Google Drive `Changes API` 增量变更拉取，
      由 Reconciler 驱动 3-way 差异决策。
  * **统一入口 `sync`**：
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
