import Foundation
import GDrive
import Logging

// Keep only warning and error logs so verbose output does not disrupt the progress display.
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
            Usage: gdrive-upload [local_path] [remote_folder_id] [options]

            Arguments:
              local_path          Local directory to upload (default: /Users/chemzqm/lib/vim)
              remote_folder_id    Google Drive destination folder ID (default: create a test folder under My Drive)

            Options:
              -c, --concurrency   Concurrent uploads (default: 64, maximum: 64)
              --db <path>         SQLite test database path (default: /tmp/gdrive_vim_bench.sqlite)
              --clean             Delete the remote test directory and local database after the run
              -h, --help          Show this help
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
    print("🚀 Google Drive large-file upload benchmark")
    print("================================================================================")
    print("📁 Local source directory: \(resolvedLocalPath)")
    print("🔍 Filter:                 ignore .git directories")
    print("⚡ Concurrent uploads:     \(config.concurrency)")
    print("🗄️  Test database:         \(config.dbPath)")

    // Check local directory
    var isDir: ObjCBool = false
    guard FileManager.default.fileExists(atPath: resolvedLocalPath, isDirectory: &isDir), isDir.boolValue else {
        print("❌ Error: The local directory does not exist: \(resolvedLocalPath)")
        exit(1)
    }

    // 1. Initialize Google Drive credentials.
    let auth: Auth
    do {
        auth = try Auth()
        _ = try await auth.token()
        print("🔑 Google Drive authentication succeeded")
    } catch {
        print("❌ Authentication failed: \(error)")
        print("👉 Run `swift run gdrive-auth` to complete authorization first")
        exit(1)
    }

    // 2. Initialize DriveClient.
    let client = DriveClient(auth: auth)

    // 3. Determine or create an empty directory on the remote target
    let remoteRootId: String
    var createdRemoteFolder = false

    if let id = config.remoteRootId {
        remoteRootId = id
        print("☁️ Using remote folder ID: \(remoteRootId)")
    } else {
        do {
            print("🌐 Creating a test folder under My Drive...")
            let idBatch = try await client.generateIds(count: 1)
            let folderName = "bench_vim_\(Int(Date().timeIntervalSince1970))"
            let folder = try await client.createDirectory(name: folderName, parentId: "root", remoteId: idBatch[0])
            remoteRootId = folder.id
            createdRemoteFolder = true
            print("✅ Successfully created remote test directory: \(folderName) (ID: \(remoteRootId))")
        } catch {
            print("❌ Failed to create remote directory: \(error)")
            exit(1)
        }
    }

    // Clean up old test database files to ensure a clean test baseline
    try? FileManager.default.removeItem(atPath: config.dbPath)
    try? FileManager.default.removeItem(atPath: config.dbPath + "-wal")
    try? FileManager.default.removeItem(atPath: config.dbPath + "-shm")

    // 4. Initialize StateStore and SyncEngine.
    let store: StateStore
    let engine: SyncEngine
    do {
        store = try await StateStore(path: config.dbPath)
        engine = try await SyncEngine(auth: auth, store: store, client: client)
    } catch {
        print("❌ Failed to initialize storage engine: \(error)")
        exit(1)
    }

    print("--------------------------------------------------------------------------------")
    print("⏳ Starting concurrent streaming scan and upload...")
    print("--------------------------------------------------------------------------------")

    let startTime = DispatchTime.now()

    // Real-time dashboard refresh
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
            format: "\r\u{1B}[K⏳ [%6.1fs] Progress: %5d / %5d (%@) | %6.1f/%6.1f MB | speed: %5.2f MB/s (%4.0f items/s) | active: %2d | queued: %5d",
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

    // 5. Perform synchronization
    do {
        let stats = try await engine.syncLocalToRemoteEmpty(
            localPath: resolvedLocalPath,
            remoteRootId: remoteRootId,
            maxUploadConcurrency: config.concurrency,
            onProgress: onProgress
        )

        let totalElapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000.0

        print("\n\n================================================================================")
        print("🎉 Upload benchmark completed")
        print("================================================================================")
        print(String(format: "⏱️  Elapsed:                %.2f seconds", totalElapsed))
        print("📁 Remote directories created: \(stats.directoriesCreated)")
        print("📄 Files uploaded:             \(stats.filesUploaded)")
        print("❌ Upload failures:            \(stats.filesFailed)")
        print("⏭️  Files skipped:              \(stats.filesSkipped)")
        print("💾 Data uploaded:              \(formatBytes(stats.bytesUploaded)) (\(stats.bytesUploaded) bytes)")

        let avgMBPerSec = totalElapsed > 0 ? (Double(stats.bytesUploaded) / (1024.0 * 1024.0)) / totalElapsed : 0.0
        let avgFilesPerSec = totalElapsed > 0 ? Double(stats.filesUploaded) / totalElapsed : 0.0
        print(String(format: "🚀 Average upload rate:     %.2f MB/s", avgMBPerSec))
        print(String(format: "⚡ Average file throughput: %.1f files/s", avgFilesPerSec))

        let writerStats = await store.getWriterStats()
        print("🗄️  SQLite writes: \(writerStats.totalCommits) commits (average batch: \(String(format: "%.1f", writerStats.averageBatchSize)) operations)")
        print("🌐 Remote folder: https://drive.google.com/drive/folders/\(remoteRootId)")
        print("================================================================================")

        if config.cleanAfter {
            if createdRemoteFolder {
                print("🧹 Cleaning up the remote test directory...")
                try? await client.trash(remoteId: remoteRootId)
                print("✅ Remote test directory moved to Trash")
            }
            try? FileManager.default.removeItem(atPath: config.dbPath)
            try? FileManager.default.removeItem(atPath: config.dbPath + "-wal")
            try? FileManager.default.removeItem(atPath: config.dbPath + "-shm")
            print("✅ Local test database cleared")
        } else {
            print("💡 The remote directory and local database were preserved. Pass --clean to remove them automatically.")
        }

    } catch {
        print("\n\n❌ Upload failed: \(error)")
        exit(1)
    }
}

await main()
