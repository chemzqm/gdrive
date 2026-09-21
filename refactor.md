最推荐的整改方向是：集中同步状态的变更规则，并让初始化、增量同步共用操作执行层。

SQLite 三方基线、SHA-256 判断、持久化 intent、纯函数 Reconciler 都值得保留。当前主要的架构负担是：状态虽然集中存储，维护这些状态的规则却分散在多条
流程里。

按整改收益排序，我建议处理以下五项。

1. 状态存储缺少明确的业务边界

StateStore (Sources/GDrive/Storage/SQLite/StateStore.swift:45) 主要提供执行 SQL 的能力。上传、下载、扫描、冲突处理各自决定如何更新基线、
generation、phase 和 operation。例如，增量上传回执 (Sources/GDrive/Engine/IncrementalSyncRun+Files.swift:440) 和初始化下载回执 (Sources/GDrive/
Engine/SyncEngine+BootstrapDownload.swift:146) 分别维护一整套状态提交逻辑。

这使每次修改同步规则，都需要检查多个入口是否一起遵守。

建议在 StateStore 上建立少量事务级业务接口，例如记录观察、准备操作、按预期代次提交回执、阻塞条目。由这一层统一维护状态转换条件，并返回明确的提
交结果。保留现有手写 SQL、批量提交和并发读能力，同时收窄底层连接的公开范围。现有 DurableCreateIntentStore 可以作为起点。

2. 观察、决策和执行之间耦合过深

本地扫描 (Sources/GDrive/Engine/IncrementalSyncRun+LocalScan.swift:529) 会直接调用远端重命名接口；RemoteChanges (Sources/GDrive/Engine/
RemoteChanges.swift:298) 在数据库写事务中调用应用逻辑，而该逻辑会移动本地文件 (Sources/GDrive/Engine/RemoteChanges.swift:422)。

因而修改路径处理规则时，必须同时理解扫描顺序、数据库事务、文件系统操作和恢复流程。文件系统操作的耗时与恢复责任也进入了存储层。

建议逐步形成“采集观察 → 生成操作计划 → 执行 → 提交回执”的边界。内容判断继续使用 Reconciler，目录和路径决策有自己的明确规则；初始化与增量复用上
传、下载和路径操作执行器。各阶段仍可按条目或批次流水执行，保留边扫描边传输。

3. 任务生命周期和资源预算分散在各模式中

初始化上传使用 BootstrapTaskRegistry (Sources/GDrive/Engine/BootstrapTaskRegistry.swift:34)，初始化下载使用 Task、Semaphore 和 DispatchGroup
(Sources/GDrive/Engine/SyncEngine+BootstrapDownload.swift:304)，增量又有另一套任务注册与收尾机制。

当前正文传输有并发限制，但初始化上传会保留全部任务句柄；不同 root 的额度也分别创建。维护者需要分别理解排队、取消、排空和父目录依赖。

建议统一任务生命周期管理，明确排队工作量及跨 root 的资源预算。哈希、目录请求、正文传输保留独立额度，QPS 继续由网络层控制。队列设计必须识别父目
录依赖，确保等待父目录的任务不会占满父目录推进所需的额度。

4. 历史实现与当前业务契约尚未收拢

当前冲突主流程写入 sync_conflicts，等待调用方选择；旧 ConflictOperation.prepare 在仓库中仅由测试调用，但增量入口仍保留旧操作的恢复流程
(Sources/GDrive/Engine/IncrementalSyncRun.swift:150)。DirectoryTracker 也已没有仓库内的生产调用。

设计文档 (docs/design.md:188) 同时保留自动生成冲突副本与人工选择两套描述，末尾还引用了已经不存在的 resumable_sessions 表。

按项目尚未发布、无需兼容旧数据的约束，建议明确当前唯一有效的冲突契约，清理退出主流程的实现、字段和公开类型，将有价值的恢复测试转到当前入口。文
档同步整理为现行职责、状态转换和恢复保证，降低后续重构的判断成本。

5. 公共接口提供统计较多，提供可行动的结果较少

SyncStats (Sources/GDrive/Engine/SyncEngineAPI.swift:7) 提供失败数、待处理数和冲突列表，但普通条目失败的原因主要进入日志。使用文档 (docs/
usage.md:319) 也明确说明，调用成功返回不代表全部收敛。

对使用这个基础库的应用而言，“继续同步”“稍后重试”“等待用户处理”需要稳定、可编程判断的依据。

建议提供结构化同步结果：保留统计，补充条目身份、失败阶段、原因分类和下一步处理方式；大量问题可分页查询。整轮致命错误继续抛出，条目级结果使用统
一类型贯穿执行器和公开接口。

第一批整改建议只选择“新文件上传”这一条完整链路：让初始化上传与增量上传共用准备 intent、执行上传、提交回执的实现。这个切口能同时验证前两项设计，并保留现有包结构与优化。随后再扩展到下载和路径操作，每批执行 make lint、make test，保留实网与中断恢复验证。
