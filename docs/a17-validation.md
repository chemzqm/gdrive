# A17 初始化背压与增量大文件流式上传

2026-09-20，macOS x86_64，Swift 6 Package debug 构建。

## 修复范围

- `syncLocalToRemoteEmpty` 在 scanner 之外最多保留 512 个目录/文件任务；窗口满时扫描消费端等待，不丢条目。文件仍先等待父目录就绪，再取得最多 64 个正文传输槽。
- 目录创建使用独立的最多 8 个请求槽，不占文件正文传输槽，也不再随目录数量无限并发。
- 排队任务只持有路径和 scanner 元数据。小文件正文只在取得传输槽后读取；大文件保持 clonefile 稳定输入、1 MiB 流式 SHA-256 和 8 MiB resumable 分块。
- A16 已将增量扫描窗口限制为最多 64 条观察、最多 6 个哈希任务，并对大文件使用 1 MiB 流式 SHA-256。本次补齐其后续上传：大于 8 MiB 的新增文件不再通过 `Data(contentsOf:)` 整块读取并走 multipart，而是复用 resumable 分块路径。
- 小文件 multipart 协议没有改变，正文传输并发上限没有降低。

## 验证

```sh
swift build
swift test --filter incrementalLargeFileUsesResumableChunks
swift test --filter 'SyncEngineTests|DirectoryTrackerRecoveryTests|BootstrapResumeSafetyTests|DurableIntentTests|PublicationSafetyTests|ChangesRecoveryTests'
```

新增回归用例创建 9 MiB 增量文件，断言只发起 resumable 会话和两个分块 PUT，不出现 multipart，并核验最终共同基线。初始化、父目录失败/取消、持久创建意图、稳定输入、增量扫描与 Changes 回归也一并执行。

## 性能边界

这次修改保留既有 scanner worker 数、文件传输并发上限、小文件读取方式和 8 MiB 分块大小。512 条任务窗口是 64 个正文传输槽的 8 倍，使扫描仍可在网络前有限预取。

当前没有同机、同网络、同 Drive 数据集的前后实测日志，因此真实吞吐与 RSS 仍为**尚未验证**；不能据单元测试声称“更快”或“无性能回退”。
