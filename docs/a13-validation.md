# A13 Changes 持久化与局部补列验收

## 修复结果与调用约定

A13 在修复前代码中成立，未按“不需要修复”关闭。

- `remote_change_inbox` 保存每个远端身份的待处理观察。每个 Changes 页的观察、已知项的失效/dirty 标记及下一页游标在同一事务中提交。下一页失败会抛出错误，不进入本地扫描和破坏性裁决；已提交页面可从 SQLite 恢复。
- 按远端 ID 关联原 item，再判断父目录归属。子项先到、祖先暂不可访问、路径被占用或无法发布的事件继续留在 inbox。补取祖先元数据有单轮预算，多个子项共享祖先查询结果。
- 已知对象移出同步根或失去访问权限时，保留本地内容，以 `items.remote_scope_excluded` 阻止反向写回；目录的后代也受保护。本地再次改名通过文件系统身份识别，不能绕过保护。再次观察到对象返回同步树时恢复处理。
- 新建、移入或重新进入范围的目录进入 `remote_directory_scans`。每次最多并发列举 `min(maxConcurrency, 64)` 个目录页；文件观察与目录分页位置原子提交。目录分页 token 被明确拒绝时，只重启该目录的列举，保留之前落库的子项。补列在本轮已就绪文件传输之后运行，后代通过下一轮消费，不引入等待整棵远端树的上传屏障。
- 新增 `SyncStats.remoteWorkPending`，表示待处理观察、目录页及尚未重新验证的观察数量（不是互不重复的文件数）。调用方应在后续同步轮次继续处理；非零值不能解释成所有文件已收敛。即使补列结果为空，也会报告一个后续发现轮次，让此前暂缓的本地扫描真正执行。权限恢复前的未知观察可能持续 pending；已确认移出范围的对象属于受保护的本地保留项。
- 缺失/显式无效游标，或服务端明确拒绝 pageToken 时：先获取 C0，再原子保存重建任务并使旧远端观察失效。随后按目录分页重新建立证据，继续消费 C0 之后的变化。旧 inbox 内容重新按身份取证，不能用旧 payload 跨越历史缺口。完整列举后只对尚未观察到的已知身份补查元数据。
- 初始化获取/保存游标不再使用 `try?`；重试不覆盖已有游标。显式初始化入口遇到“已有根但缺少游标”会要求走 `syncIncremental` 重建；统一 `sync` 入口对已有根本来就走增量路径。

暂缓处理的路径不会参与本地删除检测。分页或发布失败不会因推进游标而遗失事件；同名身份冲突继续 pending，不覆盖已有远端身份。本次没有实现 A14 的完整同名映射策略，也未改变 A11 对已有远端正文覆盖的阻断。

## API 契约

[changes.list 官方文档](https://developers.google.com/workspace/drive/api/reference/rest/v3/changes/list) 明确说明下一页及新起始 token 不会自然过期。因此代码不假定“运行一段时间就过期”，只对本地缺失/无效状态或服务端明确的 token 拒绝启动重建；权限、普通网络及其他错误不转换成新游标。

[files.list 官方文档](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/list) 的 `incompleteSearch` 表示结果可能不完整。分页接口请求并检查该字段，不以部分结果作为完整目录证据。

## 回归与边界

`ChangesRecoveryTests` 使用隔离 SQLite、临时本地目录和 URLProtocol 模拟服务，覆盖：

1. 子先父后跨页；第二页失败后检查已提交的 page2 游标和 inbox，重开数据库继续，不重放为重复文件。
2. 移入已有深层子树，递归补列并最终下载；同轮无关上传先于目录列举请求。
3. 缺失、`is_valid = 0`、服务端明确拒绝的游标，均能发现新 token 之前已经存在的文件。
4. 列举失败/不完整保留任务，尾页失败保留分页位置，先前子项不丢失。
5. 最后一个 Changes 页之后父目录仍未知，事件保持 pending；之后父目录可访问即可恢复。
6. 已知文件及目录移出范围，保留本地字节，不上传回去；目录在本地改名后新增子项同样受保护。
7. 初始化 token 请求失败、已有游标不被重置，以及已有根不能盲建新 token。
8. 注入 SQLite 游标写失败，验证观察与游标一起回滚。
9. 1000 条已知父目录的观察保持页级提交（不超过 3 次），而非 1000 次串行提交。
10. 暂缓目录不被误判为本地删除，重建不采用旧队列中的陈旧 payload。

相关旧测试的“已有基线”fixture 补上了真实协议要求的已保存游标；原有行为断言未放宽。缺失游标由新用例单独测试，不再沿用旧的“直接取当前 token”假设。

验证为离线模拟服务、事务故障注入及数据库重开；不作为实网 Drive 契约或真实进程 SIGKILL/掉电验收。SQLite 持久化级别仍属 A18，同根并发协调仍属 A19。

## 性能

按页处理移除了 Changes 路径的逐事件 `await batchWrite`，常规页面不补列整树，也不增加文件内容读取。重建、祖先取证和补列任务有边界，普通无待处理根不启动枚举任务组。计时包含本轮补列、待处理统计和最终 checkpoint。

对照基线为提交 `93ac669` 的独立源码副本；相同 macOS / Swift 6.3.3 release 构建，URLProtocol 服务及生产限流，5 次 128 × 4 KiB 上传和 50 次未变化扫描。

```sh
GDRIVE_PERF=1 GDRIVE_PERF_SCANS=10 swift test -c release --filter TransferPerformanceTests
```

| 指标 | 修复前 | 修复后 |
|---|---:|---:|
| 128 个小文件上传中位数（5 次） | 713.51 ms | 713.75 ms |
| 128 个未变化文件扫描中位数（50 次） | 12.39 ms | 11.60 ms |

上传差异约 +0.03%，未变化扫描约 -6.38%；有限、受生产限流约束的样本未显示明显性能倒退，不作为实网极限吞吐证明。最后追加的空目录后续轮次标记、失败祖先查询去重和目录 token 拒绝恢复只影响补列/异常路径，不进入上述常规无待处理基准。

最终离线回归 **86 个测试、13 个 suite 全部通过**，包含 14 个 A13 测试及其参数化 case。没有运行默认启用的实网测试。`git diff --check` 通过。

```sh
swift test --filter 'ChangesRecoveryTests|DirectoryBarrierTests|RootLossSafetyTests|ConflictRecoveryTests|PublicationSafetyTests|DurableIntentTests|BootstrapResumeSafetyTests|ReconcilerTests|StateStoreTests|IDPoolTests|DirectoryTrackerRecoveryTests|FailureStateSafetyTests|DeletionSafetyTests'
```
