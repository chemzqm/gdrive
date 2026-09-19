import Foundation

/// Metadata-only scan window. Hash workers are bounded; no file data survives preparation.
struct IncrementalLocalObservation: Sendable {
    let parentID: Int64
    let name: String
    let url: URL
    let device: Int64
    let inode: Int64
    let mtime: Int64
    let size: Int64
    struct Hashed: Sendable {
        let observation: IncrementalLocalObservation
        let sha256: String
    }

    static func hash(_ pending: [Self], concurrency: Int) async throws -> [Hashed] {
        try await withThrowingTaskGroup(of: Hashed.self) { group in
            var observations: [Hashed] = []
            observations.reserveCapacity(pending.count)
            for (index, observation) in pending.enumerated() {
                if index >= concurrency, let completed = try await group.next() { observations.append(completed) }
                group.addTask {
                    try Task.checkCancellation()
                    let sha256: String
                    if observation.size <= 8 * 1024 * 1024 {
                        sha256 = SyncEngine.computeSha256(of: try Data(contentsOf: observation.url))
                    } else {
                        sha256 = try SyncEngine.computeFileSha256(at: observation.url).sha256Hex
                    }
                    return Hashed(observation: observation, sha256: sha256)
                }
            }
            for try await observation in group { observations.append(observation) }
            return observations
        }
    }
}
