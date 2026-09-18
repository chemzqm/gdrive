import Foundation

/// Google Drive 内存 ID 缓冲池
/// 遵循 AGENTS.md 规范：
/// "google driver 支持本地 file id，先调用 Google Drive 的 files.generateIds 批量拿到一批服务器认可的 ID，然后客户端本地缓存使用"
public actor IDPool {
    private let api: DriveAPI?
    private var availableIds: [String] = []

    private var inFlightFetch: Task<[String], Error>?

    public init(api: DriveAPI? = nil, initialIds: [String] = []) {
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
        return sub
    }

    /// 获取单个可用 ID（内存缓冲耗尽时自动向 Google Drive 批量补充 1000 个，余量不足时后台自动预取）
    public func nextId() async throws -> String {
        // 如果有现成 ID，直接返回，并在水位低时后台静默预取
        if let id = availableIds.popLast() {
            if availableIds.count < 200 && inFlightFetch == nil, let api = self.api {
                inFlightFetch = Task {
                    try await api.generateIds(count: 1000)
                }
            }
            return id
        }

        // 内存耗尽：等待在途预取或发起新请求
        if let inFlight = inFlightFetch {
            let batch = try await inFlight.value
            inFlightFetch = nil
            availableIds.append(contentsOf: batch)
        } else if let api = self.api {
            let batch = try await api.generateIds(count: 1000)
            availableIds.append(contentsOf: batch)
        } else {
            throw NSError(domain: "IDPool", code: 1, userInfo: [NSLocalizedDescriptionKey: "ID 缓冲池已耗尽且未配置 API 客户端"])
        }

        guard let id = availableIds.popLast() else {
            throw NSError(domain: "IDPool", code: 2, userInfo: [NSLocalizedDescriptionKey: "未能从 Google Drive 获取有效 ID"])
        }
        return id
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
}
