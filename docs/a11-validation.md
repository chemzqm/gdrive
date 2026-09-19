# A11 覆盖发布验收（2026-09-19）

## 远端真实契约

使用当前配置根目录中新建的隔离文件测试，没有修改已有用户文件。

| 请求 | 实测结果 |
| --- | --- |
| v3 files.get 元数据 | 未返回 ETag |
| uploadType=media PATCH，If-Match 为故意无效值 | 200，未拒绝覆盖 |
| 随后 files.get SHA-256 | 与覆盖正文一致，原正文确实被替换 |

测试对象在测试结束时移入 Drive 废纸篓，可恢复。没有将令牌、会话 URI 或凭据写入日志。
未证明 resumable 会话最终发布具备条件写能力；它保持阻断，不宣称该路径已实测失败。

实网资格测试（默认禁用）：

```sh
GDRIVE_CONDITIONAL_CONTRACT=1 swift test --filter DriveConditionalWriteContractTests
```

该测试当前失败是缺失能力的证据，不能将它改成 mock 成功来解除保护。
官方 [files.update 文档](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/update)
描述了 media/multipart/resumable 入口，但不足以替代上述服务端契约测试。

用户已确认：不安全覆盖阻断并保留待同步状态。因此现有文件的正文更新当前不会自动发送；
`phase = blocked`、dirty 保留、旧 base 保留，`filesFailed` 反映该结果。新建文件和新建文件的续传不受此门控影响。

## 本地与状态回归

- 同 inode 修改正文、替换 inode、下载期间新建目标：拒绝过期发布并保留新内容。
- 下载发布使用排他 rename 或原子交换，不先删除/移走目标制造空窗。
- 交换时出现的不同目标保留为可见冲突副本；无法保证外部写进程停止使用已打开的旧 inode，故旧版本保留在废纸篓或冲突副本，不直接删除。
- 小文件不可变 Data、大文件写时复制快照：后续修改原路径不改变已捕获正文。
- 上传初始/增量路径分别注入 local/remote/dirty generation 变化及源文件修改，旧回执不能提交基线或清除 dirty。
- 下载期间推进 generation，在发布前拒绝旧计划；提交时再次使用 generation 条件。
- 纯改名快跳、新建上传幂等恢复、组提交、父目录屏障和断点恢复仍有回归覆盖。

离线检查：

```sh
swift test --skip 'SyncEngineTests|DriveClientTests|AuthTests|IDPoolTests'
git diff --check
```

## 有界性能对比

同一机器、同一依赖版本、Debug 构建；基线为 `1e340ed`，比较修复工作树。
每轮 128 个 4 KiB 文件、64 并发、生产默认限速、模拟 HTTP，独立数据库，五轮。
随后执行全部未修改的增量扫描；每轮断言 128 次快跳、零正文上传。

| 指标（五轮中位数） | 修复前 | 修复后 |
| --- | ---: | ---: |
| 新建上传 | 714.206 ms | 713.397 ms |
| 未修改扫描 | 8.704 ms | 8.797 ms |

该范围内上传持平，快跳相差 0.093 ms，处于样本波动范围。不是所有设备、文件系统、文件规模或实网吞吐的零退化证明。

```sh
GDRIVE_PERF=1 swift test --filter TransferPerformanceTests
```

性能设计约束：不增加远端元数据请求，不降低并发，不引入逐文件事务；初始化小文件仍一次读盘。
大文件使用 clonefile，不增加整文件复制 I/O；文件系统不支持 clone 时明确失败，不作慢速复制回退。
稳定性检查增加常数次 stat，增量新文件增加对已有内存正文的摘要核验，下载发布增加一次 SQLite generation 查询。
大文件快照正确性已测，尚未做跨文件系统或真实 Drive 大文件吞吐对比。
