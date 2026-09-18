import Foundation

/// 共同基线 (B: Baseline)
public struct ItemBaseline: Sendable, Equatable {
    public let sha256: String?
    public let size: Int64?
    public let version: Int64?

    public init(sha256: String?, size: Int64?, version: Int64? = nil) {
        self.sha256 = sha256
        self.size = size
        self.version = version
    }
}

/// 本地观察 (L: Local Observation)
public struct LocalObservation: Sendable, Equatable {
    public enum Status: String, Sendable {
        case present
        case absent
        case unstable
        case unknown
    }

    public let status: Status
    public let sha256: String?
    public let size: Int64?
    public let mtime: Int64?

    public init(status: Status, sha256: String? = nil, size: Int64? = nil, mtime: Int64? = nil) {
        self.status = status
        self.sha256 = sha256
        self.size = size
        self.mtime = mtime
    }
}

/// 远端观察 (R: Remote Observation)
public struct RemoteObservation: Sendable, Equatable {
    public enum Status: String, Sendable {
        case present
        case trashed
        case absent
        case unknown
    }

    public let status: Status
    public let sha256: String?
    public let size: Int64?
    public let version: Int64?

    public init(status: Status, sha256: String? = nil, size: Int64? = nil, version: Int64? = nil) {
        self.status = status
        self.sha256 = sha256
        self.size = size
        self.version = version
    }
}

/// 冲突保留胜者
public enum ConflictWinner: String, Sendable {
    case local
    case remote
}

/// Reconciler 协调决策动作
public enum ReconcileDecision: Sendable, Equatable {
    /// 仅本地修改或新增 -> 上传至远端
    case upload(reason: String)
    /// 仅远端修改或新增 -> 下载至本地
    case download(reason: String)
    /// 双方皆有修改但 SHA-256 摘要相同 -> 零传输推进共同基线
    case matchUpdateBaseline(sha256: String, size: Int64)
    /// 双方皆有修改且 SHA-256 不一致 -> 保留双方版本产生冲突副本
    case conflict(winner: ConflictWinner, conflictId: String)
    /// 本地已删除且远端未修改 -> 将远端移入回收站
    case trashRemote
    /// 远端已删除且本地未修改 -> 安全可恢复删除本地文件
    case deleteLocal
    /// 一侧删除另一侧修改 -> 保留修改版本，杜绝误删数据
    case keepModified(preferLocal: Bool)
    /// 两端完全一致 -> 无需操作
    case unchanged
    /// 证据不足或等待输入稳定
    case waitingEvidence(reason: String)
}

/// 三方状态协调决策引擎（Reconciler）
/// 遵循 v1.md §9.1 核心状态决策规范：
/// 以共同基线 B、本地观察 L、远端观察 R 进行无偏逐项比对，mtime 不决定谁覆盖谁
public struct Reconciler: Sendable {
    public static func decide(
        baseline: ItemBaseline?,
        local: LocalObservation?,
        remote: RemoteObservation?
    ) -> ReconcileDecision {
        let localStatus = local?.status ?? .unknown
        let remoteStatus = remote?.status ?? .unknown

        // 1. 任一侧状态不明或不稳定
        if localStatus == .unstable {
            return .waitingEvidence(reason: "本地文件正文不稳定，正在写入")
        }
        if localStatus == .unknown && remoteStatus == .unknown {
            return .waitingEvidence(reason: "两端观察状态未知")
        }

        let baseSha = baseline?.sha256?.lowercased()
        let localSha = local?.sha256?.lowercased()
        let remoteSha = remote?.sha256?.lowercased()

        // 2. 无共同基线 B（初次同步或新创文件）
        guard let baseSha else {
            if localStatus == .present && (remoteStatus == .absent || remoteStatus == .unknown) {
                return .upload(reason: "本地新文件")
            }
            if remoteStatus == .present && (localStatus == .absent || localStatus == .unknown) {
                return .download(reason: "远端新文件")
            }
            if localStatus == .present && remoteStatus == .present {
                if let localSha, let remoteSha, localSha == remoteSha {
                    return .matchUpdateBaseline(sha256: localSha, size: local?.size ?? 0)
                } else {
                    let conflictId = UUID().uuidString.prefix(8)
                    return .conflict(winner: .remote, conflictId: String(conflictId))
                }
            }
            return .unchanged
        }

        // 3. 有共同基线 B：计算两端相对基线的变更状态
        let localChanged = (localStatus == .present && localSha != baseSha)
        let remoteChanged = (remoteStatus == .present && remoteSha != baseSha)
        let localDeleted = (localStatus == .absent)
        let remoteDeleted = (remoteStatus == .absent || remoteStatus == .trashed)

        // 3.1 删除场景
        if localDeleted && !remoteDeleted {
            if remoteChanged {
                // 本地删除了，但远端又有了新的修改 -> 保留修改版本
                return .keepModified(preferLocal: false)
            } else {
                // 本地删除，远端未变 -> 传播至远端回收站
                return .trashRemote
            }
        }

        if remoteDeleted && !localDeleted {
            if localChanged {
                // 远端删除了，但本地有了新的修改 -> 保留修改版本
                return .keepModified(preferLocal: true)
            } else {
                // 远端删除，本地未变 -> 本地可恢复移除
                return .deleteLocal
            }
        }

        if localDeleted && remoteDeleted {
            return .unchanged
        }

        // 3.2 内容变更场景
        if localChanged && !remoteChanged {
            return .upload(reason: "仅本地内容更新")
        }

        if remoteChanged && !localChanged {
            return .download(reason: "仅远端内容更新")
        }

        if localChanged && remoteChanged {
            if let localSha, let remoteSha, localSha == remoteSha {
                return .matchUpdateBaseline(sha256: localSha, size: local?.size ?? 0)
            } else {
                let conflictId = UUID().uuidString.prefix(8)
                return .conflict(winner: .remote, conflictId: String(conflictId))
            }
        }

        return .unchanged
    }
}
