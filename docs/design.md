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

* **`schema.sql`**：保存同步基线、持久操作和恢复状态。
  * `roots`：本地路径与远端目录的绑定、初始化方向和生命周期。
  * `items`：三方基线、文件身份、观察代次及墓碑；`remote_scope_excluded` 保存范围排除状态，保护对象及其后代免于反向写回。
  * `operations`：统一保存上传、下载、路径和删除操作；分块上传保存 Session URI 和确认偏移。
  * `cursors`：Changes 等事件流的持久游标。
  * `remote_change_inbox`：游标已确认但尚未成功应用的远端事件。
  * `remote_directory_scans`：目录补列任务、扫描身份和分页进度。
  * `sync_conflicts`：等待调用方明确选择版本的内容冲突。
  * `sync_issues`：条目级失败的结构化记录；同一根、条目和阶段重复失败时更新诊断并累计次数。
  * `trashed_local_changes`：远端目录删除时随目录移入废纸篓的本地修改记录。
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
* **请求速率与退避**：
  * `DriveClient` 的 `RequestRateGate` 是唯一的请求速率限制：默认按每秒 **65** 次平滑
    发放请求名额，并以 1.01 秒滚动窗口保守限制不超过配置值，覆盖普通请求、流式请求和重试。
    初始化参数 `requestsPerSecond` 可设置正整数上限，默认 65；传 `nil` 关闭速率限制，
    仍保留共享冷却。不支持运行中修改。
  * `DriveRateLimiter` 仅协调共享冷却：遇到限流或暂时性失败时延长冷却窗口，
    `acquire()` 等待冷却结束。不再维护目标速率、令牌桶或成功后的加速/失败后的降速。

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

* **`SyncEngineAPI.swift`**：
  * 定义公共 `SyncEngine`、`SyncStats` 和 `SyncEngineError`，集中初始化、配置、传输状态
    查询及所有同步入口。显式同步入口取得根目录锁后调用核心实现；整轮结束后自动接续
    期间收到的本地变化任务。类的存储属性与初始化保留在此文件，供核心扩展使用。
    公开增量入口只接收本地根路径，并从 SQLite 的 active 绑定查询 root、根 item 和远端根 ID；
    查不到完整绑定时在扫描、Changes 应用和远端请求前失败。
  * **运行状态隔离**：所有公开同步入口在远端请求和本地扫描前解析符号链接与缺失路径祖先，
    拒绝包含认证文件或 SQLite 数据库的同步根，也拒绝与下载暂存目录或冲突目录重叠的同步根，
    防止凭据、WAL、未完成下载和冲突副本被反向上传。
  * **根边界隔离**：新绑定会和同一账户的全部既有绑定比较规范化路径，祖先或后代关系均返回
    `rootBindingConflict`；进程内协调器用相同规则拒绝并发运行的嵌套根。路径比较会解析符号链接。
  * **下载暂存目录**：通过初始化参数 `downloadTemporaryDirectory` 或
    `setDownloadTemporaryDirectory(_:)` 配置基目录，默认 `~/.gdrive/remotes`，按远程根 ID
    使用 `<基目录>/<remoteRootId>/`。初始化、增量和冲突内容下载统一使用同步目录外的暂存路径，
    避免被增量扫描上传。同步退出时用 `rmdir` 删除空的根暂存目录，非空目录保留。
    暂存文件校验完成后，大于 8 MiB 且暂存与目标位于同一文件系统的文件通过原子
    `RENAME_SWAP`（新文件使用 `RENAME_EXCL`）发布，避免再次完整复制。删除换出的旧文件前，
    计算其 SHA-256 并与预期本地哈希比较：增量下载使用数据库中的本地哈希，其他覆盖调用
    在交换前采集稳定哈希。内容不同则交换回滚，交由现有碰撞流程重新裁决。
    回滚失败时再尽力恢复本地文件并报告同步失败，保留原有暂存清理逻辑，不另存恢复副本；
    接受恢复仍失败时暂存清理可能删除本地内容的风险。回滚后的再次编辑留给后续同步处理；
    小文件及跨文件系统的大文件以字节流写入目标原路径。
    使用方式见 [接口指南](usage.md#23-设置下载临时目录)。
* **`SyncEngine.swift`**：
  * 保留统一同步路由、空目录探测和增量同步根记录查询，将执行工作交给模式扩展或单轮增量运行对象。
    公共入口持有根目录锁，内部入口仅在模块内可见，不重复加锁。
  * **统一入口 `sync`**：
    * 空↔空首次绑定在一次事务内保存根身份、committed root item、existingKnown 状态及
      列举前捕获的 Changes 游标；本地顶层探测包含隐藏条目，仅排除 `.git` 目录，
      找到首个有效条目即返回，枚举错误直接传播。见 [A15 验收记录](a15-validation.md)。
    * 自动探测本地与远端目录是否包含文件，智能路由至初始化全量同步或已有基线增量
      同步。

同步执行实现按职责拆分：

| 文件 | 职责 |
| --- | --- |
| `SyncEngine+BootstrapUpload.swift` | 初始化上传：扫描、目录就绪、传输并发控制、统计与初始化收尾 |
| `SyncEngine+BootstrapDownload.swift` | 初始化下载：远端递归列举、暂存发布和基线建立 |
| `NewFileUpload.swift` | 初始化与增量共用的新文件上传：准备持久 intent、执行 multipart/resumable、按预期代次提交回执 |
| `FileDownload.swift` | 初始化与增量共用的文件下载：校验暂存、目标版本保护发布、按模式提交回执 |
| `PathOperation.swift` | 文件与目录共用的路径操作：远端重命名/移动及条件回执，本地安全移动/建目录及中断恢复识别 |
| `TaskLifecycle.swift` | 无结构任务的活跃句柄、取消与排空边界；完成后立即释放句柄，供初始化任务 registry 复用 |
| `SyncIssueStore.swift` | 条目级失败分类、SQLite 持久化、分页查询和增量轮次开始时的清理 |
| `SyncEngine+ResumableUpload.swift` | 分块上传、会话恢复、确认偏移和完成状态持久化 |
| `SyncEngine+Hashing.swift` | 内存和文件流式 SHA-256，保留现有辅助方法名称 |
| `IncrementalSyncRun.swift` | 单轮增量状态、根校验与恢复、阶段编排、统计和收尾 |
| `IncrementalSyncRun+Records.swift` | dirty 项查询、完整扫描后的缺失项登记 |
| `IncrementalSyncRun+Files.swift` | 扫描期间与最终收尾共用的文件决策、任务调度、回执与传输等待 |
| `IncrementalSyncRun+LocalScan.swift` | 本地扫描、路径与缓存识别、观察哈希/提交、已见集合和扫描统计 |
| `IncrementalSyncRun+Directories.swift` | 目录映射、创建恢复、后代屏障及自底向上处理 |

`IncrementalSyncRun` 每轮独立创建，持有不可变依赖和受现有锁保护的本轮状态；
引擎不保存运行对象。执行顺序为根校验与删除 intent 恢复、Changes 消费、冲突刷新、流式扫描与提前传输、
缺失登记、目录创建恢复、最终文件调度、目录处理、远端补列和持久化收尾。
扫描失败或取消时先等待已启动传输完成再抛错，不进入缺失检测；目录删除在文件任务及其回执完成后执行。

`RootSyncCoordinator` 仅提供进程内、按本地根路径的整轮同步互斥。同一本地根或其祖先、后代已有
同步运行时，新的显式同步调用返回 `rootBusy`；互不嵌套的根以及单轮内部传输仍可并行。库不接收文件系统事件，
本地变化由后续显式整根增量同步重新扫描并与 SQLite 基线比较。

本地扫描只接受已映射且已就绪的父目录，不把缺失父映射解释为同步根。既有
pending 目录在扫描前按父子顺序恢复，成功回执立即更新本轮目录 readiness；目录创建或
移动失败时后代停止调度并留待下次扫描。文件上传执行前再次验证父目录 readiness 和远端
ID，避免扫描后的父状态变化产生错误层级。

本地文件与目录发生同名类型替换时，扫描在当前条目内先将旧类型登记为本地缺失，按既有
Reconciler 和持久化 cleanup 语义清理旧远端对象；旧行删除后才继续创建扫描到的新类型，
不启动第二轮自动扫描。远端同时变化或清理失败时保留旧行，并写入建议重新同步的结构化问题。

显式同步入口先执行数据库连通性检查，失败直接抛给调用方。执行中可能影响后续传输或同步
结果的数据库错误会终止本轮；能够在条目级隔离的数据库错误只记录 error log。显式同步无法
生成可靠结果时仍按既有 `async throws` 契约返回错误。

可隔离的条目失败同时写入 `sync_issues`，公开为 `SyncIssue`。记录包含条目身份、失败阶段、
稳定原因分类、建议动作、可选重试时间和出现次数；调用方通过 `listSyncIssues` 分页查询，
无需解析日志或错误字符串。每轮增量同步在执行任何恢复、Changes 消费和本地扫描前，先清空
该同步根的旧问题；本轮随后只保留这次执行新产生的问题。初始化同步在成功完成时清空该根的
问题。问题分类和调用约定见 [同步问题指南](errors.md)。

本地重命名和移动沿用普通增量同步语义：远端元数据更新成功后才更新 items 中的路径；
失败只记录日志并保持原基线，后续增量同步重新扫描当前文件系统后再次识别差异。

各模式继续遵守以下同步约束：

* **Changes 持久化（A13）**：每页观察、dirty 和游标原子落库，未知归属事件保留在
  `remote_change_inbox`；移出范围的对象保留本地内容并阻止反向写回。新/移入目录
  通过持久化分页任务补列，安排在已就绪传输之后。缺失游标先捕获 C0，再重建观察。
  `SyncStats.remoteWorkPending` 非零时由后续同步轮次继续处理，不能视为全部收敛。
  验证与调用约定见 [A13 验收记录](a13-validation.md)。
* **内容冲突**：初始化和增量流程都把冲突写入 `sync_conflicts`，保留本地内容，并将已校验的
  远端内容放入 conflicts 目录。同步门禁阻止该条目的传输和删除，直到调用方通过
  `resolveConflict(id:resolution:)` 明确选择 `.local` 或 `.remote`。远端后续变化会刷新同一条
  冲突记录及其远端副本，不存在后台自动解决流程。
* **覆盖发布保护（A11）**：当前 Drive 正文条件写未通过实网契约验证，已有远端正文覆盖
  被阻断并保留 dirty/旧基线；新建上传继续运行。小文件使用不可变内存输入，大文件
  使用 clonefile 快照（不支持时明确失败，不回退到全量复制）。下载发布前校验
  identity/generation，通过目标原路径分块写入并复核 SHA-256；完成回执按预期 generation 条件提交。
  若写入开始前目标版本已变化，下载 Worker 将已校验的暂存文件暂存入下载缓存并向上汇报碰撞；
  传输排空后由调度层重新采集本地观察并走 Reconciler 重新裁决。发生冲突时直接复用缓存文件原子移入
  conflicts 路径，并在单次事务中写入 `sync_conflicts` 并阻断 item，无需重复下载或二次哈希。
  写入开始后的校验失败不再报告为本地修改。
  具体限制与性能证据见 [A11 验收记录](a11-validation.md)。
* **模式 1：`syncLocalToRemoteEmpty`**：
  * 调用纯 Swift `DirectoryScanner`，在 POSIX 系统调用底层遍历阶段即直接剪枝跳
    过 `.git` 目录。
  * 对 $\le 8\text{MB}$ 小文件执行单次读盘载入内存，并就地完成 SHA-256 计算，
    彻底消除 8,000+ 次重复磁盘读。
  * Multipart/related 单步直接上传，静态预计算 Boundary，不落任何磁盘临时文件。
  * 扫描发现的目录和文件直接创建任务，不限制等待任务数量。文件正文在父目录就绪后才取得最多 64 个传输槽并读取，目录创建使用独立的最多 8 个请求槽。任务完成后立即释放生命周期句柄。
* **模式 2：`syncRemoteToLocalEmpty`**：
  * 流式递归列举远端目录树，边扫描边下载并流式计算 SHA-256，临时文件下载完成后
    通过目标原路径分块写入落盘，使文件监听收到内容变化事件。
  * 枚举前持久化 Changes 边界 C0；每个文件在下载前写入远端观察，文件发布与基线回执
    成功后才清除观察。网络、校验或发布失败计入 `filesFailed`，保留 pending 观察并使根保持
    `freshCreated`。再次调用 `sync()` 时允许在已绑定的部分下载目录中继续初始化，重新枚举
    远端、跳过已确认文件并恢复未完成文件；全部确认后才进入 `existingKnown`。
  * 如果恢复期间本地同路径出现了不同 SHA-256 的文件，本地文件保持不动；远端文件下载并
    校验到 `~/.gdrive/conflicts/<remoteRootId>/<relativePath>`，同时写入 `sync_conflicts`。
    这种已持久化冲突不算下载失败，也不阻止根进入 `existingKnown`。
* **模式 3：`syncIncremental`**：
  * 本地变化观察按最多 64 条自然批次提交，首个变化文件立即提交；观察落库后即可
    调度上传/下载，无需等待全树扫描。哈希最多 6 个并行任务，传输先取得额度再创建
    Task；批次候选按主键读取，父目录创建未完成时延后子文件传输。
    缓存同时命中身份、路径和元数据的文件跳过逐项 SQLite 查询。缺失检测、冲突副本
    和删除保留完整扫描及后代屏障；扫描失败/取消会等待已启动任务收尾。
    证据与性能见 [A16 验收记录](a16-validation.md)。
  * 初始化和增量产生的内容冲突统一写入 `sync_conflicts`。未解决期间该相对路径被同步门禁
    屏蔽，不执行上传或两端删除。远端 Changes 出现新正文时，重新校验并原子更新 conflicts
    文件及数据库观察；远端删除只更新冲突状态，保留本地文件和最后下载的冲突副本。
    调用方使用 `listConflicts(localPath:)` 查询，并通过
    `resolveConflict(id:resolution:)` 明确选择 `.local` 或 `.remote`。
    本地文件已删除而远端正文已修改时，选择 `.remote` 会以独占创建方式重新生成本地文件；
    若创建前同路径重新出现文件，保留冲突等待再次处理。
    一侧删除而另一侧仍为基线内容时生成删除意图。本地文件响应远端删除前与 SQLite 基线比较一次，
    匹配后直接移入废纸篓；远端目录删除扩散到本地时，即使目录非空也直接将整个目录移入废纸篓。
    移入前按 SHA-256 找出相对 SQLite 基线已修改的文件，并把原相对路径、基线 SHA-256、删除前
    SHA-256 和删除时间写入独立的 `trashed_local_changes`。调用方通过
    `listTrashedLocalChanges(localPath:)` 查询这些记录；记录不依赖随后删除的 `items` 行。
    `FileManager.trashItem(at:resultingItemURL:)` 成功后，以返回的实际废纸篓目录路径和文件在原目录内的
    相对路径生成每个文件的 `trash_path` 并回填。记录先以 pending 状态写入，trash 成功并回填路径后
    改为 committed，避免文件已经移走却完全没有数据库记录。两种删除扩散都先在 `operations` 提交
    `trashRemote` 或 `deleteLocal` intent，再执行外部 trash，最后在一个 SQLite 事务中删除 item 子树
    和全部关联行；intent 随 item 级联删除。外部操作后数据库提交失败时，下轮同步在 Changes、扫描和
    冲突刷新之前重放 intent。远端 trash 可幂等重试；本地原路径不存在视为已经完成，若同路径出现不同
    inode、元数据或 SHA-256 的新文件则停止恢复。Trash 路径回填失败时，最终事务仍保留 committed、
    `trash_path` 为空的修改记录。本地删除扩散到远端时直接调用 Google Drive trash，不等待条件元数据
    PATCH 契约；若并发远端修改或新增内容随目录一起被移入垃圾桶，用户从 Google Drive 垃圾桶恢复。
  * 新增大文件的扫描哈希与稳定输入哈希均使用 1 MiB 固定缓冲，正文通过 8 MiB resumable 分块上传；不会构造与文件大小相等的 `Data`。初始化与增量的内存边界见 [A17 验收记录](a17-validation.md)。
  * 单个已枚举文件在哈希前消失或读取失败时，只将该项计入失败并保留既有数据库状态；
    同批及后续健康文件继续同步。该路径仍记为本轮已见，缺失检测不会把读取失败误判为删除。
    任务取消继续终止整轮，不降级为普通文件失败。
  * 双向增量同步：本地快速变更扫描 + Google Drive `Changes API` 增量变更拉取，
    由 Reconciler 驱动 3-way 差异决策。

* **`BootstrapTaskRegistry.swift`**：
  * 初始化上传依赖 scanner 父目录先于子内容、批次回调串行的契约。扫描回调同步登记
    目录创建任务，再处理后续条目；子项捕获直接父目录的依赖，等待同一任务返回远端 ID
    和数据库 item ID。缺少父目录登记时直接报错。
  * 目录任务在远端创建和 SQLite 提交均成功后返回；失败沿子树传播，健康兄弟继续处理。
    等待父目录时不占用文件传输或目录请求配额。
  * 恢复时仅将已提交且远端状态为 present、祖先链已解析的目录登记为就绪；未完成目录
    重新执行创建流程，复用持久化 intent 的身份。目录失败时不标记整轮初始化完成。
  * 扫描失败或取消会取消已登记任务，并等待全部任务收尾后抛错；取消开始后登记的任务
    也会被取消。生命周期管理只保留活跃任务句柄，完成后立即释放；父目录依赖由 registry
    自身维护，不使用额外等待表或本地目录 ID 表。
* **`AsyncSemaphore.swift`**：
  * 异步协程信号量，将全局并发严格硬性钳位在 **上限 64**。

---

### 2.5 快速变更与决策层 (`LocalBaselineCache.swift`, `Reconciler.swift`)

* **`LocalBaselineCache.swift`**：
  * 快速变更检测机制：利用文件系统元数据（`device` + `inode` + `mtime` + `ctime` +
    `fileSize`）比对已提交基线。缺少 ctime 的行不进入快缓存。
  * 五项元数据完全一致时，**不进行任何文件内容读取与 SHA-256 哈希计算**，在纳秒
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
   * 每完成一个 8MB 分块，立即向 SQLite `operations` 记录 Session URI 和
     `confirmed_offset`。
   * 若传输中途崩溃中断，重启后调用 `queryResumableOffset` 向 Google Drive 云端
     校验真实接收偏移量，云端确认无误后从断点无缝续传。
3. **脏会话防御**：
   * 在续传前比对本地文件当前 SHA-256 与会话记录的预期 SHA-256；若本地文件被修改，
     旧 Session 立即作废清理并启动全新全量上传。
