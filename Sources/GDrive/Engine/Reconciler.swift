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
/// 遵循 v1.md §9.1 核心状态决策规范及 A02 审计规范：
/// 以共同基线 B、本地观察 L、远端观察 R 进行无偏逐项比对，mtime 不决定谁覆盖谁。
/// 严格保证“存在性证据”与“内容证据”完备性：拒绝任何单侧 unknown 或缺少 SHA-256 摘要的无证据删除、覆盖决策。
public struct Reconciler: Sendable {
    public static func decide(
        baseline: ItemBaseline?,
        local: LocalObservation?,
        remote: RemoteObservation?
    ) -> ReconcileDecision {
        let localStatus = local?.status ?? .unknown
        let remoteStatus = remote?.status ?? .unknown

        // 1. 本地状态处于写入不稳定中，等待写入稳定
        if localStatus == .unstable {
            return .waitingEvidence(reason: "本地文件正文不稳定，正在写入")
        }

        // 2. 任一侧状态未知 (unknown)：绝不把 unknown 当作未变化
        if localStatus == .unknown && remoteStatus == .unknown {
            return .waitingEvidence(reason: "两端观察状态未知")
        }
        if localStatus == .unknown {
            return .waitingEvidence(reason: "本地观察状态未知，拒绝无证据决策")
        }
        if remoteStatus == .unknown {
            return .waitingEvidence(reason: "远端观察状态未知，拒绝无证据决策")
        }

        // 3. 内容证据完备性检查：只要一侧状态为 present，必须具备有效 SHA-256 摘要证据
        if localStatus == .present && (local?.sha256 == nil || local?.sha256?.isEmpty == true) {
            return .waitingEvidence(reason: "本地文件正文 SHA-256 待获取")
        }
        if remoteStatus == .present && (remote?.sha256 == nil || remote?.sha256?.isEmpty == true) {
            return .waitingEvidence(reason: "远端文件正文 SHA-256 待获取")
        }

        let baseSha = baseline?.sha256?.lowercased()
        let localSha = local?.sha256?.lowercased()
        let remoteSha = remote?.sha256?.lowercased()

        // 4. 无共同基线 B（初次同步或新创文件）
        guard let baseSha else {
            // 本地存在，且远端已明确确认不存在 (absent)
            if localStatus == .present && remoteStatus == .absent {
                return .upload(reason: "本地新文件")
            }
            // 远端存在，且本地已明确确认不存在 (absent)
            if remoteStatus == .present && localStatus == .absent {
                return .download(reason: "远端新文件")
            }
            // 双方皆存在但未有基线
            if localStatus == .present && remoteStatus == .present {
                if let localSha, let remoteSha, localSha == remoteSha {
                    return .matchUpdateBaseline(sha256: localSha, size: local?.size ?? 0)
                } else {
                    let conflictId = UUID().uuidString.prefix(8)
                    return .conflict(winner: .remote, conflictId: String(conflictId))
                }
            }
            // 本地与远端皆不存在
            if localStatus == .absent && remoteStatus == .absent {
                return .unchanged
            }
            // 远端在回收站，本地不存在
            if localStatus == .absent && remoteStatus == .trashed {
                return .unchanged
            }
            // 远端在回收站，本地有新文件
            if localStatus == .present && remoteStatus == .trashed {
                return .keepModified(preferLocal: true)
            }
            return .waitingEvidence(reason: "无基线且状态未满足明确创建条件")
        }

        // 5. 有共同基线 B：计算两端相对基线的变更状态
        // 此时由于前置完备性检查，present 状态下的 localSha 和 remoteSha 必然非空
        let localChanged = (localStatus == .present && localSha != baseSha)
        let remoteChanged = (remoteStatus == .present && remoteSha != baseSha)
        let localUnchanged = (localStatus == .present && localSha == baseSha)
        let remoteUnchanged = (remoteStatus == .present && remoteSha == baseSha)

        let localDeleted = (localStatus == .absent)
        let remoteDeleted = (remoteStatus == .absent || remoteStatus == .trashed)

        // 5.1 双端皆删除
        if localDeleted && remoteDeleted {
            return .unchanged
        }

        // 5.2 本地已删除，远端未删除
        if localDeleted && !remoteDeleted {
            if remoteChanged {
                // 本地删除了，但远端又有新修改 -> 保留修改版本
                return .keepModified(preferLocal: false)
            } else if remoteUnchanged {
                // 本地删除，远端确认未变 -> 安全移入远端回收站
                return .trashRemote
            }
        }

        // 5.3 远端已删除，本地未删除
        if remoteDeleted && !localDeleted {
            if localChanged {
                // 远端删除了，但本地又有新修改 -> 保留修改版本
                return .keepModified(preferLocal: true)
            } else if localUnchanged {
                // 远端删除，本地确认未变 -> 安全删除本地文件
                return .deleteLocal
            }
        }

        // 5.4 内容变更场景
        if localChanged && remoteUnchanged {
            return .upload(reason: "仅本地内容更新")
        }

        if remoteChanged && localUnchanged {
            return .download(reason: "仅远端内容更新")
        }

        if localUnchanged && remoteUnchanged {
            return .unchanged
        }

        if localChanged && remoteChanged {
            if let localSha, let remoteSha, localSha == remoteSha {
                return .matchUpdateBaseline(sha256: localSha, size: local?.size ?? 0)
            } else {
                let conflictId = UUID().uuidString.prefix(8)
                return .conflict(winner: .remote, conflictId: String(conflictId))
            }
        }

        return .waitingEvidence(reason: "状态证据不满足任何明确决策路径")
    }
}
