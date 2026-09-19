import Foundation

/// Has Batch Build Google Drive File ID Capability Generator Protocol
public protocol DriveIDGenerator: Sendable {
    func generateIds(count: Int, space: String) async throws -> [String]
}

extension DriveIDGenerator {
    public func generateIds(count: Int) async throws -> [String] {
        try await generateIds(count: count, space: "drive")
    }
}

extension DriveClient: DriveIDGenerator {}

/// In-memory buffer of Google Drive IDs.
/// Follow the AGENTS.md Specification:
/// "google driver Support local file id,Call first Google Drive of files.generateIds Batch approved by a batch of servers ID,Then the client local cache uses"
public actor IDPool {
    private let api: (any DriveIDGenerator)?
    private var availableIds: [String] = []

    /// Currently in progress Single Fly Pool Missions and Generations
    private var currentFetchTask: Task<Void, any Error>?
    private var currentGeneration: UInt64 = 0

    public init(api: (any DriveIDGenerator)? = nil, initialIds: [String] = []) {
        self.api = api
        self.availableIds = initialIds
    }

    /// Currently Available ID Quantity
    public var count: Int {
        availableIds.count
    }

    /// Get a batch of available ID
    public func takeIds(count: Int) -> [String] {
        let availableCount = min(count, availableIds.count)
        guard availableCount > 0 else { return [] }
        let sub = Array(availableIds.suffix(availableCount))
        availableIds.removeLast(availableCount)
        triggerPrefetchIfNeeded()
        return sub
    }

    /// Get Individual Available ID(Automatically when memory buffer is exhausted Google Drive Bulk Replenishment 1000 , automatic background prefetching when there is insufficient margin)
    public func nextId() async throws -> String {
        while true {
            // If available ID,Go straight back and silently prefetch in the background when the water level is low
            if let id = availableIds.popLast() {
                triggerPrefetchIfNeeded()
                return id
            }

            // Memory depletion: fetch or initiate single-flight Pool task and wait for it to complete
            let task = try getOrStartFetchTask()
            try await task.value
            // Automatic recirculation after completion of replenishment pool from availableIds Take sign
        }
    }

    /// Preallocate specified number of servers ID(concurrent batch pull into memory)
    public func ensureCapacity(_ targetCount: Int) async throws {
        let needed = targetCount - availableIds.count
        guard needed > 0, let api = self.api else { return }

        let batchSize = 1000
        let batches = (needed + batchSize - 1) / batchSize

        let fetched = try await withThrowingTaskGroup(of: [String].self) { group -> [String] in
            for _ in 0..<batches {
                group.addTask {
                    try await api.generateIds(count: batchSize)
                }
            }
            var accumulated: [String] = []
            for try await ids in group {
                accumulated.append(contentsOf: ids)
            }
            return accumulated
        }
        availableIds.append(contentsOf: fetched)
    }

    // MARK: - Internal Single-flight Pool replenishment implementation

    /// Check and trigger background prefetch when water level is low
    private func triggerPrefetchIfNeeded() {
        guard availableIds.count < 200, currentFetchTask == nil, self.api != nil else { return }
        _ = try? getOrStartFetchTask()
    }

    /// Get existing in-transit refill missions or launch new solo missions
    private func getOrStartFetchTask() throws -> Task<Void, any Error> {
        if let existing = currentFetchTask {
            return existing
        }
        guard let api = self.api else {
            throw NSError(domain: "IDPool", code: 1, userInfo: [NSLocalizedDescriptionKey: "ID buffer is exhausted and no API client is configured"])
        }

        currentGeneration &+= 1
        let gen = currentGeneration

        let task = Task { [weak self, api] in
            do {
                let ids = try await api.generateIds(count: 1000, space: "drive")
                guard let self else { return }
                try await self.didFetchBatch(ids, generation: gen)
            } catch {
                if let self {
                    await self.didFailFetch(error, generation: gen)
                }
                throw error
            }
        }
        currentFetchTask = task
        return task
    }

    /// Single-writer merge logic after successful replenishment of the pool (only the current generation can be merged, enter the pool and enter the pool only once)
    private func didFetchBatch(_ ids: [String], generation: UInt64) throws {
        defer {
            if currentGeneration == generation {
                currentFetchTask = nil
            }
        }
        guard currentGeneration == generation else { return }
        guard !ids.isEmpty else {
            throw NSError(domain: "IDPool", code: 2, userInfo: [NSLocalizedDescriptionKey: "Failed to obtain a valid ID from Google Drive"])
        }
        availableIds.append(contentsOf: ids)
    }

    /// Clean up the current generation when the replenishment pool fails to avoid permanent failure of subsequent calls due to residual poison pills
    private func didFailFetch(_ error: any Error, generation: UInt64) {
        if currentGeneration == generation {
            currentFetchTask = nil
        }
    }
}
