# A15 空根绑定与首次探测验收

2026-09-19，macOS x86_64，本机 Swift Package 测试；网络使用 URLProtocol 模拟，没有访问真实 Drive。

## 修复

- 空绑定一次 SQLite 事务写入 roots、root item、Changes 游标；保存实际 device/inode，状态为 existingKnown/committed。事务失败不会留下部分绑定。
- 本地目录不存在时先创建；无法打开或枚举时抛错，不提交空基线。
- 探测与 scanner 的 includeHidden=true / excludeDirectory(".git") 语义一致：隐藏文件与隐藏目录计入，`.git` 普通文件和符号链接也计入，仅排除 `.git` 目录。
- 使用 opendir/readdir 顶层短路，无递归、无文件内容读取、常量内存；只有 `.git` 类型未知时才补 fstatat。
- 本地为空时在远端列举之前取得 Changes 游标，覆盖绑定期间的远端新增；下载初始化复用此游标。

历史审计中“第二次直接增量并失败”的描述已被 bootstrap 恢复分流部分消解，但原空绑定仍不完整，隐藏文件与错误吞没问题仍需修复。

## 验证

命令：`swift test --filter 'ChangesRecoveryTests|RootLossSafetyTests|BootstrapResumeSafetyTests'`

38 项测试全部通过。A15 新增覆盖：空绑定后的本地/远端新增、完整根状态、单次事务与三次 API 请求、游标先于列举、`.env` 首次上传、`.git` 类型过滤、不可读目录不建基线、游标插入失败整体回滚、本地目录缺失、下载初始化只取一次游标。

## 性能范围

增量扫描与上传/下载并发路径保持不变；上传首次路由不增加网络请求，下载路由复用游标，没有额外取游标请求。空绑定仍为一次事务，但相对原来不完整的绑定增加一次必要的 startPageToken 请求及两个 INSERT，不能声称首次空绑定耗时完全不增加。它避免第二轮恢复完整初始化，并保证后续 Changes 边界完整。

本机 `swiftc -O` 微基准直接使用生产探测函数，与旧 contentsOfDirectory + filter 比较；每组 101 次取中位数，创建空文件在计时之外：

| 顶层文件数 | 旧探测 | 新探测 |
| --- | ---: | ---: |
| 1 | 0.148616 ms | 0.041882 ms |
| 10,000 | 40.323274 ms | 0.241042 ms |

这是本地目录探测成绩，不代表真实 Drive 总吞吐或所有文件系统表现。原全量数组与过滤改为常量内存短路；测试断言单次事务和游标请求复用，避免用时间阈值制造不稳定测试。
