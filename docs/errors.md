# 结构化同步问题

条目级失败会记录在 SQLite 的 `sync_issues` 表，并通过
`SyncEngine.listSyncIssues(localPath:limit:offset:)` 分页公开。调用方应根据枚举字段做判断，
`message` 只用于诊断。

每条 `SyncIssue` 包含：

- 条目身份：`itemId`、`remoteFileId` 和相对路径。
- `stage`：`localScan`、`createDirectory`、`upload`、`download`、`pathUpdate`、`delete`
  或 `conflictRefresh`。
- `category`：网络、限流、权限、远端缺失、远端冲突、内容校验、安全阻断、本地变化、
  本地 IO、不支持的文件系统、状态过期、无效响应或未知错误。
- `suggestedAction`：立即重试、稍后重试、检查权限、检查本地文件、远端重命名或人工解决。
- 可选 `retryAt`、诊断 `message`、首次/最近出现时间和累计次数。

同一同步根、条目和阶段再次失败时更新原记录并增加 `occurrenceCount`。任务取消不创建问题。
`DriveError`、`SyncEngineError`、`URLError` 及本地文件系统错误会映射到稳定分类；无法可靠分类
的错误使用 `unknown` 和 `retry`。

`SyncConflict` 继续由冲突 API 管理；远端名称冲突会额外投影为建议 `renameRemote` 的问题。
`remoteWorkPending` 只表示尚有工作，本身不会创建问题。

问题记录按同步根和同步轮次管理。每轮增量同步在执行恢复、Changes 消费和本地扫描前，先删除
该根的全部旧问题；本轮执行期间产生的问题保留到下一轮增量同步开始。同一轮内同一条目和阶段
重复失败仍会增加 `occurrenceCount`。初始化同步成功完成时也会清空该根的旧问题。

SQLite 读写失败、同步根丢失或类型错误、扫描无法完整枚举、Changes 状态违反一致性约束及
任务取消仍直接抛出，终止本轮同步。
