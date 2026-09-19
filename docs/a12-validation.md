# A12 冲突恢复与双端收敛

## 修复契约

- Reconciler 根据规范化的 B/L/R SHA-256 生成确定性冲突标识；执行记录再按 item 和观察代次区分冲突。
- 冲突记录保存在 `operations` 的 `resolveConflict` 操作中，先在同一事务中预留副本 item、远端 ID、两个路径、两个摘要和预期代次，再触碰文件。
- 维持远端版本胜出的既有策略。先用排他 `clonefile` 保留本地版本，再上传副本，最后安全发布远端正文到原路径；原文件不会先被移走。
- 恢复查询固定副本 ID：404 才创建；已存在则核验名称、父目录、大小和 SHA-256。大文件复用既有分块续传和稳定输入实现。
- 本地发布后检查远端原文件仍符合预期；同一事务更新两项 B/L/R、文件身份缓存及操作完成状态，事务提交后才增加 `conflictsResolved`。错误计入 `filesFailed`，保留 pending 和旧基线。
- 下轮先恢复 pending，再运行 Changes 和扫描，避免把中间文件误判为新冲突。恢复有并发上限；不支持的文件系统、路径占用、输入再次变化或代次过期会明确失败，保留数据和 intent。

## 验证

`ConflictRecoveryTests` 使用隔离的 SQLite、本地临时目录与 URLProtocol Drive 模拟服务，覆盖：

1. 一次增量同步后，两端原文件和副本的摘要分别相同，下一轮没有冲突或正文传输。
2. 创建副本成功但响应丢失：失败轮不报 resolved，重试沿用同一远端 ID，无重复创建。
3. intent、clone、副本上传、原文件发布、完成提交前共五个阶段抛出中断，再重开数据库、创建新引擎恢复。
4. 本地原文件、副本、远端副本或数据库代次再次变化：不覆盖新内容，不推进旧基线。
5. 9 MiB 冲突副本走分块上传，随后两端收敛、下一轮零传输。

Reconciler 测试另覆盖有/无基线的重复决策及摘要大小写归一化。

这些阶段测试是故障注入和重新打开数据库，并非真实子进程 SIGKILL，也不证明掉电持久性。当前 SQLite `synchronous` 策略属于 A18。多对象远端观察并非服务端原子事务；本次没有启用 A11 所阻断的远端正文覆盖。实网冲突验收尚未执行。

完整 `swift test` 曾运行仓库默认启用的实网测试，85 个测试中有两项失败：`SyncEngineTests.testFastChangeSkippingOnUnchangedDirectory` 对应的快检测试要求修改后上传已有远端文件，以及增量测试要求上传两个文件；当前 A11 阻断覆盖，实际分别为 0 和 1。这两项失败的日志均为 `unsafeOverwrite`，不经过 A12 冲突分支。本次未修改这些测试或放松覆盖保护。

## 性能验证

普通文件的扫描、SHA-256 快检及上传调度不变。无冲突根每轮仅增加一次有部分索引的 pending 查询；没有逐文件新增 SQL 或哈希。冲突执行继续使用最多 64 个并发槽位，intent 和最终回执使用 `batchWrite`。大文件保留采用 copy-on-write clone，不增加一次全文件磁盘复制；不支持 clone 的文件系统明确失败。

性能对照使用 HEAD 的独立源码副本和当前源码，Swift 6.3.3 / macOS，release 构建，URLProtocol 服务、生产限流，每组五次 128 × 4 KiB 上传。`GDRIVE_PERF_SCANS` 可增加每组未变化扫描次数，用于区分毫秒级启动波动与持续吞吐。对照双方使用相同的扩展基准测试文件；库基线为修复前 HEAD，当前版本中最终追加的副本代次校验仅作用于冲突路径。

| 指标 | 修复前 | 修复后 |
|---|---:|---:|
| 128 个小文件上传，中位数（5 次） | 710.95 ms | 713.58 ms |
| 128 个未变化文件扫描，中位数（50 次） | 12.68 ms | 12.60 ms |

上传差异 +0.37%，未变化扫描 -0.66%；该有限、受生产限流约束的样本未显示明显吞吐倒退。最初每组只扫描一次时结果有启动波动（修复前中位数 6.80–11.28 ms，修复后 12.01–12.55 ms），因此补充了每组连续十次扫描的相同口径对照，未把单次结果当作结论。此基准不证明真实网络极限吞吐或所有负载下绝无回退。

复现命令：

```sh
GDRIVE_PERF=1 GDRIVE_PERF_SCANS=10 swift test -c release --filter TransferPerformanceTests
swift test --filter 'ConflictRecoveryTests|ReconcilerTests|PublicationSafetyTests|DurableIntentTests|BootstrapResumeSafetyTests|StateStoreTests'
```

最终相关回归为 6 个 suite、41 个测试通过（参数化测试另含多组 case）；`git diff --check` 通过。
