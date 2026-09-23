import DirectoryScanner
import Foundation
import os

/// A fresh view of regular files in a directory about to be removed locally.
/// Paths are relative to that directory, matching ItemCleanupPlan.Node paths.
struct DirectoryDeletionInventory {
    struct Candidate {
        let relativePath: String
        let url: URL
    }

    static func inspect(
        directory: URL,
        baselines: [String: String],
        hash: (URL) throws -> StableLocalFileDigest = {
            try StableLocalFileDigest.capture(at: $0)
        }
    ) async throws -> [Candidate] {
        let paths = OSAllocatedUnfairLock(initialState: [String]())
        let request = ScanRequest(root: directory.path, options: ScanOptions(
            mode: .paths,
            emission: .regularFiles,
            includeHidden: true,
            workers: min(6, ProcessInfo.processInfo.activeProcessorCount),
            batchCapacity: 512
        ))
        _ = try await DirectoryScanner().scan(request) { batch in
            let discovered = (0..<batch.count).map { batch.relativePath(at: $0) }
            paths.withLock { $0.append(contentsOf: discovered) }
        }

        var candidates: [Candidate] = []
        for relativePath in paths.withLock({ $0 }) {
            let url = directory.appendingPathComponent(relativePath)
            do {
                let observed = try hash(url)
                let baseline = baselines[relativePath]
                if let baseline,
                   observed.sha256Hex.caseInsensitiveCompare(baseline) == .orderedSame {
                    continue
                }
                candidates.append(.init(
                    relativePath: relativePath, url: url))
            } catch {
                continue
            }
        }
        return candidates
    }
}
