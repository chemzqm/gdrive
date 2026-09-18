import Foundation
import GDrive
import Logging

// 关闭 verbose 详细日志，仅保留 warn/error，避免刷屏干扰进度看板
LoggingSystem.bootstrap { label in
    var handler = StreamLogHandler.standardError(label: label)
    handler.logLevel = .warning
    return handler
}

struct Config {
    var localPath: String = "/Users/chemzqm/lib/vim"
    var remoteRootId: String? = nil
    var concurrency: Int = 64
    var cleanAfter: Bool = false
    var dbPath: String = "/tmp/gdrive_vim_bench.sqlite"
}

func parseArguments() -> Config {
    var config = Config()
    let args = CommandLine.arguments

    var i = 1
    var positionals = [String]()

    while i < args.count {
        let arg = args[i]
        switch arg {
        case "-h", "--help":
            print("""
            使用方法: gdrive-upload [local_path] [remote_folder_id] [options]

            参数:
              local_path          要上传的本地目录路径 (默认: /Users/chemzqm/lib/vim)
              remote_folder_id    Google Drive 远端目标目录 ID (默认: 自动在根目录创建测试文件夹)

            选项:
              -c, --concurrency   并发网络上传线程数 (默认: 64, 上限: 64)
              --db <path>         测试用 SQLite 数据库路径 (默认: /tmp/gdrive_vim_bench.sqlite)
              --clean             测试完成后自动删除云端测试目录与本地数据库
              -h, --help          显示帮助信息
            """)
            exit(0)
        case "-c", "--concurrency":
            if i + 1 < args.count, let val = Int(args[i + 1]) {
                config.concurrency = max(1, min(64, val))
                i += 1
            }
        case "--db":
            if i + 1 < args.count {
                config.dbPath = args[i + 1]
                i += 1
            }
        case "--clean":
            config.cleanAfter = true
        default:
            if !arg.hasPrefix("-") {
                positionals.append(arg)
            }
        }
        i += 1
    }

    if positionals.count >= 1 {
        config.localPath = positionals[0]
    }
    if positionals.count >= 2 {
        config.remoteRootId = positionals[1]
    }

    return config
}

func formatBytes(_ bytes: Int64) -> String {
    let mb = Double(bytes) / (1024.0 * 1024.0)
    if mb >= 1024 {
        return String(format: "%.2f GB", mb / 1024.0)
    }
    return String(format: "%.2f MB", mb)
}

func main() async {
    let config = parseArguments()
    let resolvedLocalPath = (config.localPath as NSString).expandingTildeInPath

    print("================================================================================")
    print("🚀 Google Drive 大规模文件上传速度基准测试")
    print("================================================================================")
    print("📁 本地目标目录: \(resolvedLocalPath)")
    print("🔍 过滤规则:     自动忽略 .git 目录")
    print("⚡ 并发上传线程: \(config.concurrency)")
    print("🗄️  测试数据库:   \(config.dbPath)")

    // 检查本地目录
    var isDir: ObjCBool = false
    guard FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir), isDir.boolValue else {
        print("❌ 错误: 本地目录不存在: \(resolvedLocalPath)")
        exit(1)
    }

    // 1. 初始化 Google Drive 凭据
    let auth: Auth
    do {
        auth = try Auth()
        _ = try await auth.token()
        print("🔑 Google Drive 认证成功")
    } catch {
        print("❌ 认证失败: \(error)")
        print("👉 请先运行 swift run gdrive-auth 完成授权")
        exit(1)
    }

    // 2. 初始化 DriveClient
    let client = DriveClient(auth: auth)

    // 3. 确定或创建远端目标空目录
    let remoteRootId: String
    var createdRemoteFolder = false

    if let id = config.remoteRootId {
        remoteRootId = id
        print("☁️ 使用指定远端文件夹 ID: \(remoteRootId)")
    } else {
        do {
            print("🌐 正在 Google Drive 根目录创建测试文件夹...")
            let idBatch = try await client.generateIds(count: 1)
            let folderName = "bench_vim_\(Int(Date().timeIntervalSince1970))"
            let folder = try await client.createDirectory(name: folderName, parentId: "root", remoteId: idBatch[0])
            remoteRootId = folder.id
            createdRemoteFolder = true
            print("✅ 成功创建远端测试目录: \(folderName) (ID: \(remoteRootId))")
        } catch {
            print("❌ 创建远端目录失败: \(error)")
            exit(1)
        }
    }

    // 清理旧的测试数据库文件保证测试基线干净
    try? FileManager.default.removeItem(atPath: config.dbPath)
    try? FileManager.default.removeItem(atPath: config.dbPath + "-wal")
    try? FileManager.default.removeItem(atPath: config.dbPath + "-shm")

    // 4. 初始化 StateStore 与 SyncEngine
    let store: StateStore
    let engine: SyncEngine
    do {
        store = try await StateStore(path: config.dbPath)
        engine = try await SyncEngine(auth: auth, store: store, client: client)
    } catch {
        print("❌ 初始化存储引擎失败: \(error)")
        exit(1)
    }

    print("--------------------------------------------------------------------------------")
    print("⏳ 开始并发流式扫描与上传，实时统计中...")
    print("--------------------------------------------------------------------------------")

    let startTime = DispatchTime.now()

    // 实时仪表盘刷新
    let onProgress: @Sendable (SyncProgress) -> Void = { progress in
        let now = DispatchTime.now()
        let elapsed = Double(now.uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000.0

        let snapshot = engine.transferStatus
        let speedMB = snapshot.uploadSpeedBytesPerSecond / (1024.0 * 1024.0)
        let filesPerSec = elapsed > 0 ? Double(progress.completedFiles) / elapsed : 0

        let pct = String(format: "%5.1f%%", progress.percentage * 100.0)
        let compMB = Double(progress.completedBytes) / (1024.0 * 1024.0)
        let totalMB = Double(progress.totalDiscoveredBytes) / (1024.0 * 1024.0)
        let activeCount = snapshot.activeUploads.count
        let queueCount = snapshot.queuedUploads.count

        let line = String(
            format: "\r\u{1B}[K⏳ [%6.1fs] 进度: %5d / %5d (%@) | %6.1f/%6.1f MB | 速度: %5.2f MB/s (%4.0f 项/s) | 活跃: %2d | 排队: %5d",
            elapsed,
            progress.completedFiles,
            progress.totalDiscoveredFiles,
            pct,
            compMB,
            totalMB,
            speedMB,
            filesPerSec,
            activeCount,
            queueCount
        )
        FileHandle.standardOutput.write(line.data(using: .utf8)!)
    }

    // 5. 执行同步
    do {
        let stats = try await engine.syncLocalToRemoteEmpty(
            localPath: resolvedLocalPath,
            remoteRootId: remoteRootId,
            maxUploadConcurrency: config.concurrency,
            onProgress: onProgress
        )

        let totalElapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000.0

        print("\n\n================================================================================")
        print("🎉 上传测试圆满完成！")
        print("================================================================================")
        print(String(format: "⏱️  总耗时:          %.2f 秒", totalElapsed))
        print("📁 创建远端子目录数: \(stats.directoriesCreated)")
        print("📄 成功上传文件数:   \(stats.filesUploaded)")
        print("❌ 上传失败文件数:   \(stats.filesFailed)")
        print("⏭️  跳过文件数:       \(stats.filesSkipped)")
        print("💾 总上传数据量:     \(formatBytes(stats.bytesUploaded)) (\(stats.bytesUploaded) 字节)")

        let avgMBPerSec = totalElapsed > 0 ? (Double(stats.bytesUploaded) / (1024.0 * 1024.0)) / totalElapsed : 0.0
        let avgFilesPerSec = totalElapsed > 0 ? Double(stats.filesUploaded) / totalElapsed : 0.0
        print(String(format: "🚀 平均上传速率:     %.2f MB/s", avgMBPerSec))
        print(String(format: "⚡ 平均文件吞吐:     %.1f 文件/秒", avgFilesPerSec))

        let writerStats = await store.getWriterStats()
        print("🗄️  SQLite 写入统计: 共 \(writerStats.totalCommits) 次事务提交 (平均每批合并 \(String(format: "%.1f", writerStats.averageBatchSize)) 个操作)")
        print("🌐 远端目录链接:     https://drive.google.com/drive/folders/\(remoteRootId)")
        print("================================================================================")

        if config.cleanAfter {
            if createdRemoteFolder {
                print("🧹 正在清理远端测试目录...")
                try? await client.trash(remoteId: remoteRootId)
                print("✅ 远端目录已移至回收站")
            }
            try? FileManager.default.removeItem(atPath: config.dbPath)
            try? FileManager.default.removeItem(atPath: config.dbPath + "-wal")
            try? FileManager.default.removeItem(atPath: config.dbPath + "-shm")
            print("✅ 本地测试数据库已清除")
        } else {
            print("💡 提示: 远端目录与本地数据库均已保留。若需自动清理，请添加 --clean 参数。")
        }

    } catch {
        print("\n\n❌ 上传过程异常中断: \(error)")
        exit(1)
    }
}

await main()
