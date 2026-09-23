# Swift 非测试代码对抗性审计（2026-09-23）

审计基于 `797ba46`。范围为 `Sources/` 与 `Vendor/scanner/Sources/` 下的 55 个 Swift 非测试源文件，包括库、认证 CLI、两个基准可执行程序和目录扫描器；测试文件只用于核对已有覆盖。按公开入口追踪 SQLite 基线、远端 Changes、本地扫描、文件传输、删除收尾与恢复、并发控制和认证回调，并与 [设计规范](design.md) 比对。工作树起始时干净；本次仅新增本文档，未修改程序行为。

以下问题是源码中可到达的路径或明确的校验缺口，不等同于已在真实 Google Drive 上完成故障注入。等级：P1 为可能造成数据意外移走/删除的高优先级问题；P2 为可用性或安全边界问题。

## 发现

### F2 · P1 · 远端目录删除时，未验证的本地内容随目录入废纸篓却没有恢复索引

**触发：** 已同步目录在远端被移入废纸篓，同时其本地子文件已修改但在删除前无法读取或计算 SHA-256（例如权限或 I/O 错误）。本地扫描把读取失败项记为 `unknown` 并保留既有状态（[源码](Sources/GDrive/Engine/IncrementalSyncRun+LocalScan.swift#L165-L188)、[源码](Sources/GDrive/Engine/IncrementalSyncRun+LocalScan.swift#L254-L294)）；目录删除候选只看目录自身的本地/远端状态（[源码](Sources/GDrive/Engine/IncrementalSyncRun+Directories.swift#L232-L269)），不以子文件 `unknown` 为阻断条件。删除前逐文件计算 SHA-256 失败会记录一条日志并 `continue`（[源码](Sources/GDrive/Engine/ItemCleanup.swift#L538-L570)），随后整个目录被移入废纸篓并清除数据库子树（[源码](Sources/GDrive/Engine/ItemCleanup.swift#L231-L248)、[源码](Sources/GDrive/Engine/ItemCleanup.swift#L656-L684)）。

如果目录最终被清理，同一索引缺口也覆盖没有 SQLite `base_sha256` 的新本地文件（`guard let baseline ... else { continue }`），包括扫描结束后、目录清理前才出现的文件：它们不会生成 `trashed_local_changes` 记录；该表也要求 `baseline_sha256` 非空（[schema](Sources/GDrive/Storage/SQLite/schema.sql#L260-L272)）。现有测试只验证了能成功哈希的、已有基线的修改文件（[测试](Tests/GDriveTests/ItemCleanupTests.swift#L237-L309)）。

**影响：** 文件仍在系统废纸篓，不能称为不可恢复；但 `listTrashedLocalChanges(localPath:)` 无法指出这些文件或其废纸篓路径，数据库子树也已删除。特别是已知哈希失败仍继续清理，与设计文档所述“读取失败保留既有数据库状态”的保护目标存在冲突（[设计](design.md#L269-L285)）。

**建议：** 对无法验证的后代停止目录删除，或在征得产品语义确认后为“未验证/仅本地”内容建立独立的可查询恢复记录。增加“子文件哈希失败”和“扫描后新增文件”两条删除回归测试。

## 验证与边界

- `make lint`：通过，86 个文件中 0 个违规。
- `make test`：通过，35 个套件共 240 个测试通过，3 个按既有条件跳过；运行了仓库现有的真实 Drive 测试，但未为上述发现执行破坏性、并发竞争或 OAuth 慢连接故障注入。
- `swift build`：通过，包含正常构建的两个基准可执行程序。
- `ocr delegate preview` 对干净工作树返回 0 个差异文件；本次改用完整 Swift 源文件清单做全仓筛查。55 个目标文件均纳入静态筛查，0 个按文件跳过；上述关键路径做了调用链深查。静态筛查不保证不存在其他缺陷。

本文档不包含修复。涉及删除策略、公开 API 参数行为或其他程序行为变化的处理，应先由用户确认。
