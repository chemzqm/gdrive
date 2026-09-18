import Foundation

/// 具有批量生成 Google Drive 文件 ID 能力的生成器协议
public protocol DriveIDGenerator: Sendable {
    func generateIds(count: Int, space: String) async throws -> [String]
}

extension DriveIDGenerator {
    public func generateIds(count: Int) async throws -> [String] {
        try await generateIds(count: count, space: "drive")
    }
}

extension DriveClient: DriveIDGenerator {}

/// Google Drive 内存 ID 缓冲池
/// 遵循 AGENTS.md 规范：
/// "google driver 支持本地 file id，先调用 Google Drive 的 files.generateIds 批量拿到一批服务器认可的 ID，然后客户端本地缓存使用"
public actor IDPool {
    private let api: (any DriveIDGenerator)?
    private var availableIds: [String] = []

    /// 当前进行中的单飞补池任务与代次
    private var currentFetchTask: Task<Void, any Error>?
    private var currentGeneration: UInt64 = 0

    public init(api: (any DriveIDGenerator)? = nil, initialIds: [String] = []) {
        self.api = api
        self.availableIds = initialIds
    }

    /// 当前可用 ID 数量
    public var count: Int {
        availableIds.count
    }

    /// 获取一批可用 ID
    public func takeIds(count: Int) -> [String] {
        let n = min(count, availableIds.count)
        guard n > 0 else { return [] }
        let sub = Array(availableIds.suffix(n))
        availableIds.removeLast(n)
        triggerPrefetchIfNeeded()
        return sub
    }

    /// 获取单个可用 ID（内存缓冲耗尽时自动向 Google Drive 批量补充 1000 个，余量不足时后台自动预取）
    public func nextId() async throws -> String {
        while true {
            // 如果有现成 ID，直接返回，并在水位低时后台静默预取
            if let id = availableIds.popLast() {
                triggerPrefetchIfNeeded()
                return id
            }

            // 内存耗尽：获取或发起 single-flight 补池任务并等待其完成
            let task = try getOrStartFetchTask()
            try await task.value
            // 补池完成后自动循环重新从 availableIds 取号
        }
    }

    /// 预分配指定数量的服务器 ID（并发批量拉取至内存中）
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

    // MARK: - 内部 Single-flight 补池实现

    /// 检查并在水位低时触发后台预取
    private func triggerPrefetchIfNeeded() {
        guard availableIds.count < 200, currentFetchTask == nil, self.api != nil else { return }
        _ = try? getOrStartFetchTask()
    }

    /// 获取现有在途补池任务，或发起新的单飞任务
    private func getOrStartFetchTask() throws -> Task<Void, any Error> {
        if let existing = currentFetchTask {
            return existing
        }
        guard let api = self.api else {
            throw NSError(domain: "IDPool", code: 1, userInfo: [NSLocalizedDescriptionKey: "ID 缓冲池已耗尽且未配置 API 客户端"])
        }

        currentGeneration &+= 1
        let gen = currentGeneration

        let task = Task { [weak self, api] () -> Void in
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

    /// 补池成功后的单写者合并逻辑（仅当前代次可合并，入池且仅入池一次）
    private func didFetchBatch(_ ids: [String], generation: UInt64) throws {
        defer {
            if currentGeneration == generation {
                currentFetchTask = nil
            }
        }
        guard currentGeneration == generation else { return }
        guard !ids.isEmpty else {
            throw NSError(domain: "IDPool", code: 2, userInfo: [NSLocalizedDescriptionKey: "未能从 Google Drive 获取有效 ID"])
        }
        availableIds.append(contentsOf: ids)
    }

    /// 补池失败时清理当前代次，避免毒丸残留导致后续调用永久失败
    private func didFailFetch(_ error: any Error, generation: UInt64) {
        if currentGeneration == generation {
            currentFetchTask = nil
        }
    }
}
