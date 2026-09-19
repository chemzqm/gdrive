import Darwin
import Foundation
import CommonCrypto
import GDrive
import DirectoryScanner

/// thread-safe memory ID Distributor (guarantees that each round of benchmarking uses exactly the same batch of real Google Drive ID)
final class IDDispenser: @unchecked Sendable {
    private var ids: [String]
    private var index: Int = 0
    private var lock = os_unfair_lock()

    init(ids: [String]) {
        self.ids = ids
    }

    func nextId() -> String {
        os_unfair_lock_lock(&lock)
        defer { os_unfair_lock_unlock(&lock) }
        if index < ids.count {
            let id = ids[index]
            index += 1
            return id
        }
        return "mock_id_\(UUID().uuidString)"
    }

    func reset() {
        os_unfair_lock_lock(&lock)
        index = 0
        os_unfair_lock_unlock(&lock)
    }
}

/// Dedicated to SHA256 Task queue for batch calculation
final class ComputeQueue: @unchecked Sendable {
    private var mutex = pthread_mutex_t()
    private var notEmpty = pthread_cond_t()
    private var notFull = pthread_cond_t()

    private let capacity: Int
    private var queue: [ScanBatch] = []
    private var isFinished = false

    init(capacity: Int) {
        self.capacity = max(4, capacity)
        pthread_mutex_init(&mutex, nil)
        pthread_cond_init(&notEmpty, nil)
        pthread_cond_init(&notFull, nil)
    }

    deinit {
        pthread_mutex_destroy(&mutex)
        pthread_cond_destroy(&notEmpty)
        pthread_cond_destroy(&notFull)
    }

    func push(_ batch: ScanBatch) {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }

        while queue.count >= capacity && !isFinished {
            pthread_cond_wait(&notFull, &mutex)
        }

        if isFinished { return }
        queue.append(batch)
        pthread_cond_signal(&notEmpty)
    }

    func finish() {
        pthread_mutex_lock(&mutex)
        isFinished = true
        pthread_cond_broadcast(&notEmpty)
        pthread_cond_broadcast(&notFull)
        pthread_mutex_unlock(&mutex)
    }

    func pop() -> ScanBatch? {
        pthread_mutex_lock(&mutex)
        defer { pthread_mutex_unlock(&mutex) }

        while queue.isEmpty && !isFinished {
            pthread_cond_wait(&notEmpty, &mutex)
        }

        if !queue.isEmpty {
            let item = queue.removeFirst()
            pthread_cond_signal(&notFull)
            return item
        }

        return nil
    }
}

/// Write pipeline gating (limit the maximum number of tasks in transit, provide backpressure and graceful ending)
final class WritePipeline: @unchecked Sendable {
    private let store: StateStore
    private let sema: DispatchSemaphore
    private let group = DispatchGroup()

    init(store: StateStore, maxInFlight: Int = 256) {
        self.store = store
        self.sema = DispatchSemaphore(value: maxInFlight)
    }

    func submit(
        rootId: Int64,
        parentId: Int64,
        name: String,
        remoteFileId: String,
        device: Int64,
        inode: Int64,
        mtime: Int64,
        size: Int64,
        sha256: String,
        now: Double
    ) {
        sema.wait()
        group.enter()
        Task {
            defer {
                group.leave()
                sema.signal()
            }
            try await store.batchWrite { conn in
                let stmt = try conn.cachedStatement("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind,
                    remote_file_id, local_device, local_inode,
                    local_mtime, local_size, local_sha256,
                    local_generation, local_status, phase,
                    dirty_generation, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, 'file',
                    ?, ?, ?,
                    ?, ?, ?,
                    1, 'present', 'discovered',
                    1, ?, ?
                );
                """)
                stmt.bindInt64(rootId, at: 1)
                stmt.bindInt64(parentId, at: 2)
                stmt.bindText(name, at: 3)
                stmt.bindText(remoteFileId, at: 4)
                stmt.bindInt64(device, at: 5)
                stmt.bindInt64(inode, at: 6)
                stmt.bindInt64(mtime, at: 7)
                stmt.bindInt64(size, at: 8)
                stmt.bindText(sha256, at: 9)
                stmt.bindDouble(now, at: 10)
                stmt.bindDouble(now, at: 11)
                _ = try stmt.step()
                stmt.reset()
            }
        }
    }

    func waitUntilDrained() async {
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            group.notify(queue: .global(qos: .userInitiated)) {
                cont.resume()
            }
        }
    }
}

/// Benchmark test results record
struct BenchResult: Sendable {
    let name: String
    let batchCapacity: Int
    let batchTimeoutMs: Int
    let totalFiles: Int
    let elapsedSeconds: Double
    let throughputFilesPerSec: Double
    let throughputMBPerSec: Double
    let totalCommits: Int
    let capacityCommits: Int
    let timeoutCommits: Int
    let averageBatchSize: Double
    let dbSizeBytes: Int64
    let walSizeBytes: Int64
}

/// Pre-collect and build directory topology trees
func buildDirectoryTopology(rootPath: String) -> [String] {
    let url = URL(fileURLWithPath: rootPath)
    guard let enumerator = FileManager.default.enumerator(
        at: url,
        includingPropertiesForKeys: [.isDirectoryKey],
        options: []
    ) else {
        return []
    }

    var dirs: [String] = []
    let rootPrefix = rootPath.hasSuffix("/") ? rootPath : rootPath + "/"
    for case let fileURL as URL in enumerator {
        var isDir: AnyObject?
        try? (fileURL as NSURL).getResourceValue(&isDir, forKey: .isDirectoryKey)
        if (isDir as? Bool) == true {
            let path = fileURL.path
            if path.hasPrefix(rootPrefix) {
                let rel = String(path.dropFirst(rootPrefix.count))
                if !rel.isEmpty {
                    dirs.append(rel)
                }
            }
        }
    }

    // Arrange in ascending order by path slash level, ensuring that parent directories are always created before subdirectories.
    return dirs.sorted {
        let c1 = $0.filter { $0 == "/" }.count
        let c2 = $1.filter { $0 == "/" }.count
        return c1 == c2 ? $0 < $1 : c1 < c2
    }
}

@main
struct GDriveBenchMain {
    static func main() async {
        signal(SIGPIPE, SIG_IGN)

        let targetPath = "/Users/chemzqm/lib/vim"
        print("==================================================================")
        print("🚀 GDrive sync-baseline SQLite group-commit benchmark")
        print("Target directory: \(targetPath)")
        print("==================================================================")

        // 1. Populate the Google Drive ID cache.
        let idDispenser: IDDispenser
        do {
            let auth = try Auth()
            let api = DriveAPI(auth: auth)
            let idPool = IDPool(api: api)

            let existingCount = await idPool.count
            let requiredCount = 16000
            if existingCount < requiredCount {
                print("⚡ Concurrently prefetching \(requiredCount - existingCount) server-issued IDs from Google Drive...")
                let fetchStart = DispatchTime.now()
                try await idPool.ensureCapacity(requiredCount)
                let elapsed = Double(DispatchTime.now().uptimeNanoseconds - fetchStart.uptimeNanoseconds) / 1_000_000_000
                print("✅ Prefetching completed, time taken: \(String(format: "%.2f", elapsed)) seconds")
            }
            let ids = await idPool.takeIds(count: requiredCount)
            print("✅ Prepared \(ids.count) Google Drive IDs in memory\n")
            idDispenser = IDDispenser(ids: ids)
        } catch {
            print("❌ Failed to initialize Google Drive IDs: \(error)")
            exit(1)
        }

        // 2. Pre-built directory tree topology
        print("📂 Pre-parsing directory tree structure...")
        let relativeDirs = buildDirectoryTopology(rootPath: targetPath)
        print("✅ Found \(relativeDirs.count) subdirectories\n")

        setbuf(stdout, nil)

        // 3. Test configuration matrix
        let configs: [(name: String, capacity: Int, timeoutMs: Int)] = [
            ("Mode 1: 32 operations (5 ms timeout)", 32, 5),
            ("Mode 2: 64 operations (5 ms timeout)", 64, 5),
            ("Mode 3: 128 operations (5 ms timeout)", 128, 5),
            ("Mode 4: 256 operations (5 ms timeout)", 256, 5),
            ("Mode 5: 5 ms timeout only (capacity 10000)", 10000, 5),
            ("Mode 6: 10 ms timeout only (capacity 10000)", 10000, 10),
            ("Mode 7: 64 operations only (no practical timeout)", 64, 10000),
        ]

        var results: [BenchResult] = []

        for config in configs {
            print("------------------------------------------------------------------")
            print("▶️ Start running: [\(config.name)]")
            print("   batchCapacity = \(config.capacity), batchTimeoutMs = \(config.timeoutMs)ms")
            idDispenser.reset()

            let dbPath = "/tmp/gdrive_bench_\(config.capacity)_\(config.timeoutMs).sqlite"
            // Clean up old library files
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: dbPath + ext)
            }

            let result = await runBenchmark(
                targetPath: targetPath,
                relativeDirs: relativeDirs,
                dbPath: dbPath,
                batchCapacity: config.capacity,
                batchTimeoutMs: config.timeoutMs,
                idDispenser: idDispenser,
                configName: config.name
            )
            results.append(result)

            print(String(
                format: "⏱️  Time: %.3f s | Throughput: %.1f files/s (%.2f MB/s) | commits: %d | capacity flushes: %d | timeout flushes: %d | average batch: %.1f",
                result.elapsedSeconds,
                result.throughputFilesPerSec,
                result.throughputMBPerSec,
                result.totalCommits,
                result.capacityCommits,
                result.timeoutCommits,
                result.averageBatchSize
            ))
            print("   Main library size: \(result.dbSizeBytes / 1024) KB | WAL size: \(result.walSizeBytes / 1024) KB\n")

            // Clean test library
            for ext in ["", "-wal", "-shm"] {
                try? FileManager.default.removeItem(atPath: dbPath + ext)
            }
        }

        // 4. Output final comparison table
        print("==========================================================================================================")
        print("📊 Final comparison summary table (target: \(targetPath))")
        print("==========================================================================================================")
        print("| Configuration plan | Total time spent (s) | Throughput (File/s) | Total number of transaction commits | Full batch trigger (times) | Timeout trigger (times) | Average number of items per batch | WAL size |")
        print("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")
        for r in results {
            let numPart = String(
                format: "%8.3f | %13.1f | %12d | %13d | %13d | %12.1f | %7lld KB |",
                r.elapsedSeconds,
                r.throughputFilesPerSec,
                r.totalCommits,
                r.capacityCommits,
                r.timeoutCommits,
                r.averageBatchSize,
                r.walSizeBytes / 1024
            )
            print("| \(r.name) | \(numPart)")
        }
        print("==========================================================================================================")
    }

    static func runBenchmark(
        targetPath: String,
        relativeDirs: [String],
        dbPath: String,
        batchCapacity: Int,
        batchTimeoutMs: Int,
        idDispenser: IDDispenser,
        configName: String
    ) async -> BenchResult {
        let store: StateStore
        do {
            store = try await StateStore(
                path: dbPath,
                batchCapacity: batchCapacity,
                batchTimeoutMs: batchTimeoutMs
            )
        } catch {
            fatalError("Failed to create StateStore: \(error)")
        }

        let now = Date().timeIntervalSince1970

        // 1. Create Root with underlying directory tree topology
        let (staticRootId, staticRootItemId, staticDirIdMap): (Int64, Int64, [String: Int64])
        do {
            (staticRootId, staticRootItemId, staticDirIdMap) = try await store.write { conn in
                var localDirIdMap: [String: Int64] = [:]
                let rootStmt = try conn.cachedStatement("""
                INSERT INTO roots (
                    account_id, local_root_path, local_root_device, local_root_inode,
                    remote_root_id, initial_sync_direction, bootstrap_state, created_at, updated_at
                ) VALUES ('bench_account', ?, 1, 1, 'bench_remote_root', 'localToRemoteEmpty', 'freshCreated', ?, ?);
                """)
                rootStmt.bindText(targetPath, at: 1)
                rootStmt.bindDouble(now, at: 2)
                rootStmt.bindDouble(now, at: 3)
                _ = try rootStmt.step()
                rootStmt.reset()
                let localRootId = conn.lastInsertRowId

                let rootItemStmt = try conn.cachedStatement("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
                ) VALUES (?, NULL, 'vim', 'directory', 'bench_remote_root', ?, ?);
                """)
                rootItemStmt.bindInt64(localRootId, at: 1)
                rootItemStmt.bindDouble(now, at: 2)
                rootItemStmt.bindDouble(now, at: 3)
                _ = try rootItemStmt.step()
                rootItemStmt.reset()
                let localRootItemId = conn.lastInsertRowId
                localDirIdMap[""] = localRootItemId

                let dirInsertStmt = try conn.cachedStatement("""
                INSERT INTO items (
                    root_id, parent_id, name, entry_kind, remote_file_id, created_at, updated_at
                ) VALUES (?, ?, ?, 'directory', ?, ?, ?);
                """)

                for dirRel in relativeDirs {
                    let parentRel = (dirRel as NSString).deletingLastPathComponent
                    let name = (dirRel as NSString).lastPathComponent
                    let parentId = localDirIdMap[parentRel] ?? localRootItemId
                    let dirRemoteId = idDispenser.nextId()

                    dirInsertStmt.bindInt64(localRootId, at: 1)
                    dirInsertStmt.bindInt64(parentId, at: 2)
                    dirInsertStmt.bindText(name, at: 3)
                    dirInsertStmt.bindText(dirRemoteId, at: 4)
                    dirInsertStmt.bindDouble(now, at: 5)
                    dirInsertStmt.bindDouble(now, at: 6)
                    _ = try dirInsertStmt.step()
                    localDirIdMap[dirRel] = conn.lastInsertRowId
                    dirInsertStmt.reset()
                }

                return (localRootId, localRootItemId, localDirIdMap)
            }
        } catch {
            fatalError("Failed to initialize directory topology: \(error)")
        }

        // 2. Preparing parallel hashing and writing pipelines
        let cpuCount = ProcessInfo.processInfo.activeProcessorCount
        let scanWorkers = min(6, max(1, cpuCount / 2))
        let hashWorkers = max(1, cpuCount)

        let computeQueue = ComputeQueue(capacity: hashWorkers * 4)
        let writePipeline = WritePipeline(store: store, maxInFlight: 256)
        let computeGroup = DispatchGroup()

        final class HashCounter: @unchecked Sendable {
            var fileCount = 0
            var byteCount: Int64 = 0
        }
        let counters = (0..<hashWorkers).map { _ in HashCounter() }

        for idx in 0..<hashWorkers {
            let counter = counters[idx]
            computeGroup.enter()
            let thread = Thread {
                var readBuf = [UInt8](repeating: 0, count: 64 * 1024)
                var ctx = CC_SHA256_CTX()
                var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))

                while let batch = computeQueue.pop() {
                    batch.withRawData { rawBuf in
                        guard let basePtr = rawBuf.baseAddress else { return }
                        for i in 0..<batch.count {
                            let record = batch.records[i]
                            let rawPtr = UnsafeRawPointer(basePtr + Int(record.offset))
                            let cPath = rawPtr.assumingMemoryBound(to: CChar.self)

                            let fd = open(cPath, O_RDONLY)
                            if fd < 0 { continue }

                            CC_SHA256_Init(&ctx)
                            var fileSize: Int64 = 0
                            while true {
                                let n = read(fd, &readBuf, readBuf.count)
                                if n <= 0 { break }
                                CC_SHA256_Update(&ctx, &readBuf, CC_LONG(n))
                                fileSize += Int64(n)
                            }
                            close(fd)
                            CC_SHA256_Final(&digest, &ctx)

                            let hexDigits = digest.map { String(format: "%02x", $0) }.joined()
                            counter.fileCount += 1
                            counter.byteCount += fileSize

                            // Resolve relative paths and parent directories
                            let fullPath = String(cString: cPath)
                            let prefix = targetPath.hasSuffix("/") ? targetPath : targetPath + "/"
                            let relPath: String
                            if fullPath.hasPrefix(prefix) {
                                relPath = String(fullPath.dropFirst(prefix.count))
                            } else {
                                relPath = (fullPath as NSString).lastPathComponent
                            }
                            let parentRel = (relPath as NSString).deletingLastPathComponent
                            let fileName = (relPath as NSString).lastPathComponent
                            let parentId = staticDirIdMap[parentRel] ?? staticRootItemId
                            let remoteFileId = idDispenser.nextId()

                            let dev = Int64(record.metadata?.identity.device ?? 1)
                            let ino = Int64(record.metadata?.identity.inode ?? 0)
                            let mtimeSec = record.metadata?.modificationTime.seconds ?? 0
                            let mtimeNanos = record.metadata?.modificationTime.nanoseconds ?? 0
                            let mtime = mtimeSec * 1_000_000_000 + Int64(mtimeNanos)

                            writePipeline.submit(
                                rootId: staticRootId,
                                parentId: parentId,
                                name: fileName,
                                remoteFileId: remoteFileId,
                                device: dev,
                                inode: ino,
                                mtime: mtime,
                                size: fileSize,
                                sha256: hexDigits,
                                now: now
                            )
                        }
                    }
                }
                computeGroup.leave()
            }
            thread.qualityOfService = .userInitiated
            thread.start()
        }

        // 3. Start high concurrency DirectoryScanner scan
        var prefix = targetPath
        if !prefix.hasSuffix("/") { prefix += "/" }
        let prefixBytes = Array(prefix.utf8)

        let options = ScanOptions(
            mode: .basic,
            emission: .regularFiles,
            includeHidden: true,
            workers: scanWorkers,
            batchCapacity: 1024,
            batchBytesLimit: 256 * 1024,
            delimiter: 0x00,
            pathPrefix: prefixBytes
        )
        let request = ScanRequest(root: targetPath, filters: [], options: options)
        let scanner = DirectoryScanner()

        let startNanos = DispatchTime.now().uptimeNanoseconds

        do {
            _ = try await scanner.scan(request) { batch in
                computeQueue.push(batch)
            }
        } catch {
            print("Scan error occurred: \(error)")
        }

        computeQueue.finish()

        // 4. Wait for all hash calculation threads to end
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            computeGroup.notify(queue: .global(qos: .userInitiated)) {
                cont.resume()
            }
        }

        // 5. Wait for the database write pipeline to drain and force the last batch to be committed
        await writePipeline.waitUntilDrained()
        try? await store.flush()

        let endNanos = DispatchTime.now().uptimeNanoseconds
        let elapsed = Double(endNanos - startNanos) / 1_000_000_000.0

        let stats = await store.getWriterStats()
        let totalFiles = counters.reduce(0) { $0 + $1.fileCount }
        let totalBytes = counters.reduce(0) { $0 + $1.byteCount }

        let throughputFiles = Double(totalFiles) / elapsed
        let throughputMB = (Double(totalBytes) / (1024.0 * 1024.0)) / elapsed

        // Read library file size
        let dbSize = (try? FileManager.default.attributesOfItem(atPath: dbPath)[.size] as? Int64) ?? 0
        let walSize = (try? FileManager.default.attributesOfItem(atPath: dbPath + "-wal")[.size] as? Int64) ?? 0

        // Verify the number of written records
        let verifyCount: Int64 = (try? await store.read { conn -> Int64 in
            let stmt = try conn.cachedStatement("SELECT count(*) FROM items WHERE entry_kind = 'file';")
            if try stmt.step() {
                return stmt.columnInt64(at: 0) ?? 0
            }
            return 0
        }) ?? 0
        if verifyCount != Int64(totalFiles) {
            print("⚠️ Warning: file record count mismatch; expected \(totalFiles), found \(verifyCount)")
        }

        return BenchResult(
            name: configName,
            batchCapacity: batchCapacity,
            batchTimeoutMs: batchTimeoutMs,
            totalFiles: totalFiles,
            elapsedSeconds: elapsed,
            throughputFilesPerSec: throughputFiles,
            throughputMBPerSec: throughputMB,
            totalCommits: stats.totalCommits,
            capacityCommits: stats.capacityTriggeredCommits,
            timeoutCommits: stats.timeoutTriggeredCommits,
            averageBatchSize: stats.averageBatchSize,
            dbSizeBytes: dbSize,
            walSizeBytes: walSize
        )
    }
}
