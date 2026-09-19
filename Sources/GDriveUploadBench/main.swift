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
    var remoteRootId: String?
    var concurrency: Int = 64
    var cleanAfter: Bool = false
    var dbPath: String = "/tmp/gdrive_vim_bench.sqlite"
}

/// Records the first request that carries file content. URLSession reports
/// `requestStartDate` after a task completes, so this is the start of the HTTP
/// request as observed by URLSession, not an exact first-byte-on-the-wire time.
final class UploadRequestMetrics: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    private let lock = NSLock()
    private var firstRequestStart: Date?
    let delegateQueue: OperationQueue

    override init() {
        let queue = OperationQueue()
        queue.name = "gdrive.upload-benchmark.metrics"
        queue.maxConcurrentOperationCount = 1
        delegateQueue = queue
        super.init()
    }

    static func isContentUpload(_ request: URLRequest) -> Bool {
        let method = request.httpMethod?.uppercased()

        if method == "POST",
           let url = request.url,
           let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
           components.queryItems?.contains(where: { $0.name == "uploadType" && $0.value == "multipart" }) == true {
            return true
        }

        guard method == "PUT",
              let contentRange = request.value(forHTTPHeaderField: "Content-Range"),
              contentRange.hasPrefix("bytes "),
              !contentRange.hasPrefix("bytes */") else {
            return false
        }
        return true
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didFinishCollecting metrics: URLSessionTaskMetrics
    ) {
        let starts = metrics.transactionMetrics.compactMap { transaction -> Date? in
            guard Self.isContentUpload(transaction.request) else { return nil }
            return transaction.requestStartDate
        }
        guard let earliest = starts.min() else { return }
        lock.lock()
        if firstRequestStart == nil || earliest < firstRequestStart! {
            firstRequestStart = earliest
        }
        lock.unlock()
    }

    func firstRequestElapsed(since applicationStart: Date) -> TimeInterval? {
        lock.lock()
        let start = firstRequestStart
        lock.unlock()
        guard let start else { return nil }
        return max(0, start.timeIntervalSince(applicationStart))
    }

    /// Wait for delegate callbacks already submitted by completed data tasks.
    func drain() async {
        await withCheckedContinuation { continuation in
            delegateQueue.addOperation {
                continuation.resume()
            }
        }
    }
}

func makeBenchmarkSession(delegate: UploadRequestMetrics) -> URLSession {
    let configuration = URLSessionConfiguration.default
    configuration.httpMaximumConnectionsPerHost = 128
    configuration.httpShouldUsePipelining = true
    configuration.timeoutIntervalForRequest = 60
    configuration.timeoutIntervalForResource = 300
    configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
    return URLSession(configuration: configuration, delegate: delegate, delegateQueue: delegate.delegateQueue)
}

func elapsedSeconds(since start: DispatchTime) -> Double {
    Double(DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000_000.0
}

func elapsedSeconds(from start: DispatchTime, to end: DispatchTime) -> Double {
    Double(end.uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000_000.0
}

private func parseArgument(_ arg: String, args: [String], index argumentIndex: inout Int,
                           config: inout Config, positionals: inout [String]) {
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
        if argumentIndex + 1 < args.count, let val = Int(args[argumentIndex + 1]) {
            config.concurrency = max(1, min(64, val))
            argumentIndex += 1
        }
    case "--db":
        if argumentIndex + 1 < args.count {
            config.dbPath = args[argumentIndex + 1]
            argumentIndex += 1
        }
    case "--clean":
        config.cleanAfter = true
    default:
        if !arg.hasPrefix("-") {
            positionals.append(arg)
        }
    }
}

func parseArguments() -> Config {
    var config = Config()
    let args = CommandLine.arguments

    var argumentIndex = 1
    var positionals = [String]()

    while argumentIndex < args.count {
        let arg = args[argumentIndex]
        parseArgument(arg, args: args, index: &argumentIndex, config: &config, positionals: &positionals)
        argumentIndex += 1
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
    let megabytes = Double(bytes) / (1024.0 * 1024.0)
    if megabytes >= 1024 {
        return String(format: "%.2f GB", megabytes / 1024.0)
    }
    return String(format: "%.2f MB", megabytes)
}

func main() async -> Int32 {
    let applicationStart = DispatchTime.now()
    let applicationWallStart = Date()
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
        return 1
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
        return 1
    }

    // 2. Initialize DriveClient.
    let uploadMetrics = UploadRequestMetrics()
    let session = makeBenchmarkSession(delegate: uploadMetrics)
    let client = DriveClient(auth: auth, session: session)

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
            return 1
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
        return 1
    }

    print("--------------------------------------------------------------------------------")
    print("⏳ Starting concurrent streaming scan and upload...")
    print("--------------------------------------------------------------------------------")

    let engineStart = DispatchTime.now()
    let setupElapsed = elapsedSeconds(from: applicationStart, to: engineStart)

    // Real-time dashboard refresh
    let onProgress: @Sendable (SyncProgress) -> Void = { progress in
        let elapsed = elapsedSeconds(since: engineStart)

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

        let syncCompleted = DispatchTime.now()
        let engineElapsed = elapsedSeconds(from: engineStart, to: syncCompleted)
        let applicationElapsed = elapsedSeconds(from: applicationStart, to: syncCompleted)
        await uploadMetrics.drain()
        let firstUploadRequestElapsed = uploadMetrics.firstRequestElapsed(since: applicationWallStart)

        print("\n\n================================================================================")
        if stats.filesFailed == 0 {
            print("🎉 Upload benchmark completed successfully")
        } else {
            print("❌ Upload benchmark failed: \(stats.filesFailed) upload(s) failed")
        }
        print("================================================================================")
        print(String(format: "⏱️  Start through sync completion: %.2f seconds", applicationElapsed))
        print(String(format: "⏱️  Setup before SyncEngine:       %.2f seconds", setupElapsed))
        print(String(format: "⏱️  SyncEngine sync window:    %.2f seconds", engineElapsed))
        if let firstUploadRequestElapsed {
            print(String(format: "🌐 First upload request start: %.2f seconds after application start", firstUploadRequestElapsed))
        } else {
            print("🌐 First upload request start: unavailable (no upload content request observed)")
        }
        print("ℹ️  Request start is URLSession's requestStartDate, not an exact first wire-byte timestamp.")
        print("📁 Remote directories created: \(stats.directoriesCreated)")
        print("📄 Files uploaded:             \(stats.filesUploaded)")
        print("❌ Upload failures:            \(stats.filesFailed)")
        print("⏭️  Files skipped:              \(stats.filesSkipped)")
        print("💾 Data uploaded:              \(formatBytes(stats.bytesUploaded)) (\(stats.bytesUploaded) bytes)")

        let avgMBPerSec = engineElapsed > 0 ? (Double(stats.bytesUploaded) / (1024.0 * 1024.0)) / engineElapsed : 0.0
        let avgFilesPerSec = engineElapsed > 0 ? Double(stats.filesUploaded) / engineElapsed : 0.0
        print(String(format: "📊 Sync-window aggregate:   %.2f MB/s", avgMBPerSec))
        print(String(format: "📊 Sync-window aggregate:   %.1f uploaded files/s", avgFilesPerSec))

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

        return stats.filesFailed == 0 ? 0 : 1

    } catch {
        print("\n\n❌ Upload failed: \(error)")
        return 1
    }
}

exit(await main())
