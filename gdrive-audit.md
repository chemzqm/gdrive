# chemzqm/gdrive 对抗性代码审核

审核基准：`e038f665b768a56076289aa4795f0f45527731c8`（main）。提交时间：2026-09-20 17:54:20 UTC / 北京时间 2026-09-21 01:54:20。审核文档日期：2026-09-21（北京时间）。

## 1. 结论

**当前版本不宜用于无人值守的重要目录双向同步。** 本次确认 9 项源码层面的问题：5 项 P1、4 项 P2。最需要先处理的是初始化下载遗漏、重启恢复入口、根绑定校验以及删除前的证据复核。

这不是“代码完全不可用”的判断：持久化创建意图、预生成远端 ID、Changes inbox、下载原子发布、上传稳定输入和部分 generation 条件提交，已经提供了有价值的保护。但这些保护在不同执行路径上并不一致，局部安全不等于整条同步链能恢复并收敛。

重要能力边界：项目主动禁止已有远端文件正文覆盖。因此，本地新文件上传与远端修改下载可运行，但“修改一个已同步的本地文件并自动更新远端”目前被明确阻断。该行为已写入设计与使用文档，本报告不把它重复列为新缺陷，也不建议删除保护来恢复功能。

| 编号 | 优先级 | 问题 | 主要后果 |
|---|---|---|---|
| F7 | P2 | 过期冲突意图恢复失败阻断整个根 | 一个文件二次变化使健康文件也无法同步 |
| F8 | P2 | 把同 inode 的另一个路径直接当作重命名 | 硬链接路径丢失远端表示、反复重命名 |
| F9 | P2 | 初始化下载统计被并发无锁修改 | 下载数量及字节数不可靠，存在数据竞争 |

优先级含义：P1 为重要正确性或恢复阻塞问题，建议真实数据使用前修复；P2 为特定条件下的正确性、可用性或可观测性问题。没有将“移入可恢复回收站”夸大为“永久删除”，也没有给出未经验证的性能提升数字。

## 2. 审核范围与证据等级

以本次克隆的当前代码为准，重点阅读统一入口、初始化上传/下载、增量扫描/调度/目录处理、Reconciler、Changes inbox 与补列、冲突恢复、稳定上传输入、下载发布、分块上传、SQLite schema/读写池，以及相关测试和设计约束。不是历史报告的沿用，也不以测试文件数量推断可靠性。

- **已执行**：GitHub 元数据与 main 提交核对；克隆代码；源码交叉检查；在 SQLite 3.53.1 中执行从生产 Swift 文件直接提取的 SQL，验证绑定查询、类型替换、inode 查询和 pending 统计的数据库行为。附录保留完整命令、脚本及输出。
- **未执行**：Swift 编译、`swift test`、macOS/APFS 文件系统集成测试、Thread Sanitizer、真实 Google Drive 传输、kill-process 恢复以及性能基准。环境没有 `swift`，项目又依赖绝对路径 `/Users/chemzqm/lib/scanner`，且核心代码使用 Darwin/macOS API。
- 以下“复现场景”是根据完整调用链得出的确定性测试方案，不冒充已经运行过的 Swift 回归测试。SQL 验证仅证明相应 SQL 行为，不证明整个引擎已在本机复现。
- 没有修改仓库代码、提交 PR、运行用户 Drive 上的写入或删除操作。本次聚焦同步正确性、恢复和性能结构，不是网络安全审计。

## 3. 逐项发现

### F7 · P2 · 一个过期冲突意图可以阻断整个同步根

**位置**：[IncrementalSyncRun.swift L143–161](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Sources/GDrive/Engine/IncrementalSyncRun.swift#L143-L161)、[ConflictOperation.swift L181–185](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Sources/GDrive/Engine/ConflictOperation.swift#L181-L185)、[同文件 L212–237](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Sources/GDrive/Engine/ConflictOperation.swift#L212-L237)、[ConflictRecoveryTests.swift L269–302](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Tests/GDriveTests/ConflictRecoveryTests.swift#L269-L302)。

prepare 在消费 Changes、扫描本地之前，用 throwing task group 恢复全部 pending conflict；任意一个抛错就终止整个 prepare。恢复意图固定了原内容摘要和路径，而用户可以在中断期间继续编辑原件或冲突副本。摘要不再匹配时操作保留 pending，但没有进入新观察/重新规划的入口。

**触发步骤**：制造冲突并让回执丢失，产生 pending；修改原件或副本；再新建一个无关健康文件；反复 `sync()`。同一个旧冲突检查持续失败，健康文件不能进入扫描，远端新变化也不能消费。

保留冲突数据、不提交虚假成功是正确的；问题是把条目级、可能永久无法满足的旧计划变成根级前置屏障。现有 changedDuringRecovery 测试只证明不覆盖新数据且 pending 保留，没有验证健康项能继续或冲突最终可重新规划。

**修复建议**：将过期冲突转为可观察的条目级阻塞状态，门控原路径和副本后继续处理无关项；为新内容设计保留双方的重新规划或显式用户解决接口。不能直接删除意图和冲突副本。

**验收**：修改 pending 的原件/副本/远端后连续跑两轮，健康文件仍能同步；受影响内容完整保存；提供清晰可解除的阻塞结果。

### F8 · P2 · 同 inode 不等于重命名，硬链接会被错误合并

**位置**：[IncrementalSyncRun+LocalScan.swift L424–504](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Sources/GDrive/Engine/IncrementalSyncRun+LocalScan.swift#L424-L504)、[LocalBaselineCache.swift L44–99](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Sources/GDrive/Engine/LocalBaselineCache.swift#L44-L99)。

发现文件时用 device+inode 查询单个旧记录，只要名字或父项不同就更新远端元数据及本地 item 路径，没有验证旧路径已消失。cache 同样每个 inode 只保留一项。

**触发步骤**：同步 a.txt 后执行 `ln a.txt b.txt`，保留 a.txt，再整根同步。b 的 inode 查询会命中 a，触发 remote-a 重命名成 b，而不是为 b 建立独立远端表示。再次扫描可能反向重命名；具体最终名字取决于枚举顺序。

**证据**：原始身份查询在 observed_name=b、旧 a 仍存在的条件下返回 a 的 item 和 remote ID。SQL 没有路径存在性或 link count 条件。未运行 macOS 硬链接端到端测试。

**修复建议**：仅在旧路径确实消失且身份映射唯一时推断 rename。多链接文件按路径同步为独立远端内容，或明确标为不支持并隔离；不能对现有远端身份静默重命名。

**验收**：a/b 同 inode 且同时存在、跨目录硬链接、删除其中一个链接、相同 inode 的缓存路径选择，均不错误移动另一个路径的远端对象。

### F9 · P2 · 初始化下载计数有并发数据竞争

**位置**：[SyncEngine+BootstrapDownload.swift L120–126](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Sources/GDrive/Engine/SyncEngine+BootstrapDownload.swift#L120-L126)、[同文件 L181–187](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Sources/GDrive/Engine/SyncEngine+BootstrapDownload.swift#L181-L187)、[同文件 L265–266](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Sources/GDrive/Engine/SyncEngine+BootstrapDownload.swift#L265-L266)。

DownloadTracker 被声明为 `@unchecked Sendable`，但 filesDownloaded 与 bytesDownloaded 是普通可变属性。并发下载 Task 完成后直接 `+=`，没有锁或 actor 隔离。semaphore 上限 64 不等于互斥；等待 DispatchGroup 只能确保任务结束，不能修复已经发生的 lost update。

**影响**：成功下载数量及字节数可能少报；这是数据竞争，不只是 UI 显示延迟。没有据此推断文件正文也会损坏。

**修复建议**：采用与增量 ActionTracker 相同的加锁累计值，或让任务返回结果后统一聚合。将失败数一起纳入一致快照。

**验收**：macOS Thread Sanitizer 下并发下载多组文件，精确核对实际成功数、总字节数和统计值；保留 sanitizer 日志。该测试本次尚未执行。

## 4. 现有测试的可信度与需补充场景

当前测试覆盖了若干重要协议边界，如创建成功但回执丢失、目录后代屏障、发布时文件变更、Changes 归属恢复以及本地通知交接。不能因此推断所有错误路径均已覆盖。

[DeletionSafetyTests.swift](https://github.com/chemzqm/gdrive/blob/e038f665b768a56076289aa4795f0f45527731c8/Tests/GDriveTests/DeletionSafetyTests.swift) 中的测试存在特别需要修正的方式：测试自行写 SQL 模拟 removed 处理；另一个测试在局部函数 performLocalDelete 中模拟“trash 失败后 blocked”，并未调用生产删除方法。这些测试可以验证 schema 或期望示例，不能证明生产代码走到了同样的分支。应保留有价值的断言，但通过公开引擎入口加最小失败注入触发真实实现。

| 顺序 | 应增加的回归组 | 关键断言 |
|---|---|---|
| 1 | F1 根绑定错配 | 任何外部写入前拒绝，基线不变 |
| 2 | F2/F3 初始化下载故障及重启 | 不靠新 Changes 也能找回失败/未列举文件；只用公开 sync 恢复 |
| 3 | F4 删除竞态与隐藏目录内容 | 保留新修改、未知内容不当空目录、过期回执不提交 |
| 4 | F5/F6 父目录失败及类型替换 | 无错误根级条目，整根/局部语义一致 |
| 5 | F7/F8 冲突输入二次变化及硬链接 | 健康文件能继续，路径身份不误合并 |
| 6 | F9 并发计数 | sanitizer 无竞争，统计精确 |

每组除第一次调用外，至少应覆盖关闭并重开数据库后的后续收敛。只断言“抛了错”不足以证明恢复正确。

## 5. 已声明的限制与发布前条件

这些不是本次新增缺陷，不应与上面的 9 项混算：

1. 已有远端正文覆盖被 A11 主动关闭。恢复该能力前，需要真实服务条件写契约与冲突保留方案，不能把普通 GET+PATCH 当成原子条件写。
2. 大文件稳定输入和冲突副本依赖 clonefile；不支持的文件系统不做完整复制回退。下载暂存目录必须在同步根外且与目标同文件系统。
3. 根锁只在当前进程有效，本地变化通知队列不承诺崩溃持久化；这在文档中已经说明。宿主需要明确进程并发与周期性完整同步策略。
4. Changes 目录补列分多轮进行，remoteWorkPending 非零不等于同步完成。不能要求每轮都一次性列完整棵树。
5. Package.swift 的 scanner 是开发者绝对路径，仓库中未发现 .github 工作流目录。本次不能独立复现构建，不能据历史验收文档断言当前 SHA 的 CI 已通过。对于个人开发这可以是临时配置；要交付给其他 Agent/机器，应提供 scanner 的精确版本及可复现依赖入口。

建议发布门槛：先修复 5 项 P1；完成 P2 的回归或明确、可观察且不污染数据的隔离策略；在真实 macOS/APFS 和专用 Drive 测试根执行故障矩阵，并保存当前 SHA 的结果。未完成前仅用于可重建测试数据。

## 6. 性能结构与优化优先级

以下均为结构分析与待测建议，没有实测吞吐、没有宣称性能提升，也没有把 SQL 小实验当性能基准。

| 观察 | 对大量小文件的潜在影响 | 建议验证方式 |
|---|---|---|
| 初始化上传/下载先创建 Task，再在内部等待传输额度；上传 registry 保留任务句柄 | 并发请求有界，但排队 Task 和句柄数量随文件数增长；设计文档已承认上传这一点 | 真实 1万/10万文件记录 peak RSS、任务积压、首文件延迟，决定是否改有界生产队列 |
| 初始化下载使用 listChildren 合并一个目录的全部分页后才遍历子项 | 单个超大目录先积累全部 metadata，下载启动延后 | 真实高扇出目录测首文件落地时间及 RSS；评估持久分页处理，兼顾跨页名称冲突检查 |
| 增量新文件先哈希，StableUploadInput.capture 又读取/哈希 | 正确性保护导致额外读盘，不能随意删除第二次验证 | 对相同数据集测磁盘读取量与 CPU；研究有界复用不可变输入，不能复用可变用户路径 |
| 目录创建在增量 observeDirectory 中被逐个 await | 高延迟网络下新目录较多时，文件发现和目录请求串行影响吞吐 | 深树与宽树分别测时间线；复用明确父依赖并限制并发，先修 F5 |
| scoped watcher 仍加载整个根的 baselineCache，缺失检测查询也遍历根内候选 | 单文件事件在大根上仍可能产生 O(N) 数据库/内存工作 | 真实大基线下测每个单文件事件的延迟与扫描行数，评估按 scope 读取 |
| 每轮 consume 使用账户 Changes，并在多根分别保存/处理 | 多根有重复变化消费与归属判断成本 | 测单账户多根的请求数、inbox 数量及每轮耗时，再决定是否共享账户级消费 |

统一测量口径：固定机器、文件系统、scanner/GDrive SHA、文件树与内容、冷/热缓存、网络条件、并发与请求速率。保留原始日志，报告总请求数（含 generateIds、重试、token 与 Changes）、首文件延迟、完成时间、peak RSS、磁盘读取量、失败与 pending 数。不能只比较上传正文请求或挑选最优一轮。

## 7. 建议实施顺序

1. 先建立 F1–F5 的真实生产路径回归，修复根绑定和删除保护；这些路径可能影响用户现有数据。
2. 把初始化下载改成“先记录工作，再执行，再收据提交”，同时打通失败/kill 后公开 sync 恢复；F2/F3 共用设计，但保留两个独立测试。
3. 统一整根/局部的父链与类型验证，处理硬链接身份和过期冲突的条目级隔离。
4. 修复计数竞争，提供准确结果后再开展性能测量。优化不能删掉恢复记录、摘要或保护来换取好看的数字。

## 附录 A：实际执行的 SQL 证据

目的：验证生产查询及 UPSERT 的实际 SQLite 行为。不是 Swift 引擎测试、真实 Drive 测试或性能测试。gdrive_name_key 使用测试实现；本附录仅使用 ASCII 名字，因此不以它检验生产 Unicode 名称映射。

在指定提交的仓库目录准备好后，将附录 B 保存为 `sql_evidence.py`，执行：

```sh
git clone https://github.com/chemzqm/gdrive.git
cd gdrive
git checkout e038f665b768a56076289aa4795f0f45527731c8
python3 /path/to/sql_evidence.py "$PWD"
```

原始输出：

```json
{
  "sqlite_version": "3.53.1",
  "scope": "production SQL only; no Swift execution; no Drive requests",
  "results": [
    {
      "check": "F1_root_lookup",
      "query": "SELECT root_id FROM roots WHERE account_id = 'default' AND remote_root_id = ?;",
      "result": [
        [
          1
        ]
      ],
      "supplied_local_path": "/unbound/B",
      "local_path_is_query_parameter": false
    },
    {
      "check": "F6_directory_intent_on_file",
      "row": [
        "file",
        "old-file",
        88
      ]
    },
    {
      "check": "F6_file_observation_on_directory",
      "row": [
        "directory",
        "old-directory",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      ]
    },
    {
      "check": "F8_hardlink_lookup",
      "observed_name": "b.txt",
      "old_path_still_exists": true,
      "result": [
        6,
        1,
        "a.txt",
        "remote-a"
      ]
    },
    {
      "check": "F2_missing_download_not_pending",
      "unknown_failed_file_id": "never-persisted",
      "pending_count": 0
    }
  ]
}
```

解释：F2 输出 0 只证明数据库中从未记录的失败身份不会被 pendingCount 识别；“下载错误不记录身份”的依据是正文引用的生产 catch/INSERT 顺序。F1/F8 输出同样只证明查询行为，完整副作用结论由所述调用链支持。

## 附录 B：SQL 证据脚本

```python
"""Execute extracted production SQL, not Swift or live Drive tests; no benchmarks."""
import json, re, sqlite3, sys, unicodedata
from pathlib import Path
repo = Path(sys.argv[1])
def source(name): return (repo/'Sources/GDrive'/name).read_text()
def sql_blocks(name):
    return [s.strip() for s in re.findall(r'"""(.*?)"""', source(name), re.S)]
c=sqlite3.connect(':memory:')
c.create_function('gdrive_name_key', 1, lambda s: unicodedata.normalize('NFC', s).lower(), deterministic=True)
c.executescript(source('Storage/SQLite/schema.sql'))
c.execute("INSERT INTO roots(account_id,local_root_path,local_root_device,local_root_inode,remote_root_id,initial_sync_direction,bootstrap_state,created_at,updated_at) VALUES('default','/bound/A',1,1,'remote-root','remoteToLocalEmpty','existingKnown',0,0)")
c.execute("INSERT INTO items(root_id,name,entry_kind,remote_file_id,created_at,updated_at) VALUES(1,'root','directory','remote-root',0,0)")
results=[]
def record(name,**kw): results.append(dict(check=name,**kw))
query=re.search(r'cachedStatement\("(SELECT root_id FROM roots WHERE account_id[^"\n]+)"\)', source('Engine/SyncEngine.swift')).group(1)
record('F1_root_lookup', query=query, result=c.execute(query,('remote-root',)).fetchall(), supplied_local_path='/unbound/B', local_path_is_query_parameter=False)
# Exact directory intent UPSERT against an existing FILE.
c.execute("INSERT INTO items(root_id,parent_id,name,entry_kind,remote_file_id,local_status,remote_status,phase,created_at,updated_at) VALUES(1,1,'replaced','file','old-file','present','present','committed',0,0)")
query=next(q for q in sql_blocks('Engine/DurableCreateIntent.swift') if "VALUES (?, ?, ?, 'directory'" in q)
c.execute(query,(1,1,'replaced','new-directory',77,88,0,0))
record('F6_directory_intent_on_file', row=c.execute("SELECT entry_kind,remote_file_id,local_inode FROM items WHERE name='replaced'").fetchone())
# Exact file observation UPSERT against an existing DIRECTORY.
c.execute("INSERT INTO items(root_id,parent_id,name,entry_kind,remote_file_id,created_at,updated_at) VALUES(1,1,'opposite','directory','old-directory',0,0)")
query=next(q for q in sql_blocks('Engine/IncrementalSyncRun+LocalScan.swift') if 'RETURNING item_id' in q)
c.execute(query,(1,1,'opposite',77,99,123,4,'a'*64,0,0)).fetchall()
record('F6_file_observation_on_directory', row=c.execute("SELECT entry_kind,remote_file_id,local_sha256 FROM items WHERE name='opposite'").fetchone())
# Exact inode lookup does not include the newly observed name.
c.execute("INSERT INTO items(root_id,parent_id,name,entry_kind,remote_file_id,local_device,local_inode,created_at,updated_at) VALUES(1,1,'a.txt','file','remote-a',12,34,0,0)")
query=next(q for q in sql_blocks('Engine/IncrementalSyncRun+LocalScan.swift') if "entry_kind = 'file' AND local_device" in q)
record('F8_hardlink_lookup', observed_name='b.txt', old_path_still_exists=True, result=c.execute(query,(1,12,34)).fetchone())
# Exact production pendingCount: absent bootstrap failures have no representation.
query=next(q for q in sql_blocks('Engine/RemoteChanges.swift') if 'SELECT (SELECT count(*) FROM remote_change_inbox' in q)
record('F2_missing_download_not_pending', unknown_failed_file_id='never-persisted', pending_count=c.execute(query,(1,1,1,1)).fetchone()[0])
print(json.dumps({'sqlite_version':sqlite3.sqlite_version,'scope':'production SQL only; no Swift execution; no Drive requests','results':results},ensure_ascii=False,indent=2))
```
