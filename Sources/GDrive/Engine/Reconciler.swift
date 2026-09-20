import Foundation
import CryptoKit

/// Common baseline (B: Baseline)
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

/// Local Observation (L: Local Observation)
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

/// Remote Observation (R: Remote Observation)
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

/// Conflict Reservation Winner
public enum ConflictWinner: String, Sendable {
    case local
    case remote
}

/// Reconciler Coordinate Decision Actions
public enum ReconcileDecision: Sendable, Equatable {
    /// Modify or add locally only -> Upload to remote
    case upload(reason: String)
    /// Modify or add remotely only -> Download locally
    case download(reason: String)
    /// Modified by both parties but SHA-256 Same digest -> Zero Transmission Propulsion Common Baseline
    case matchUpdateBaseline(sha256: String, size: Int64)
    /// Modified by both parties and SHA-256 Inconsistent -> Keep conflicting copies of both versions
    case conflict(winner: ConflictWinner, conflictId: String)
    /// Local deleted and not modified remotely -> Move Remote to Trash
    case trashRemote
    /// Remote deleted and not modified locally -> Secure Recoverable Delete Local Files
    case deleteLocal
    /// Delete on one side Modify on the other side -> Keep the modified version to prevent erroneous deletion of data
    case keepModified(preferLocal: Bool)
    /// Exactly the same on both ends -> No action required
    case unchanged
    /// Insufficient evidence or waiting for stable input
    case waitingEvidence(reason: String)
}

/// Tripartite state coordination decision engine (Reconciler)
/// Follow the v1.md §9.1 Core State Decision Specification and A02 Audit Specification:
/// with a common baseline B,Local Observation L,Remote Observation R unbiased item-by-item comparison,mtime Doesn't decide who covers who.
/// Strictly guaranteed“Existence Evidence”And“Content Evidence”Completeness: Reject any one-sided unknown or missing SHA-256 No evidence of summary deletion, override decision.
public struct Reconciler: Sendable {
    // Content identity is deterministic; execution scopes it to the persisted item ID.
    private static func conflictIdentity(base: String?, local: String?, remote: String?) -> String {
        let input = [base ?? "", local ?? "", remote ?? ""].joined(separator: ":")
        return SHA256.hash(data: Data(input.utf8)).map { String(format: "%02x", $0) }.joined()
    }

    public static func decide(
        baseline: ItemBaseline?,
        local: LocalObservation?,
        remote: RemoteObservation?
    ) -> ReconcileDecision {
        let localStatus = local?.status ?? .unknown
        let remoteStatus = remote?.status ?? .unknown

        if let decision = waitingForEvidence(localStatus: localStatus, remoteStatus: remoteStatus,
                                             local: local, remote: remote) {
            return decision
        }

        let baseSha = baseline?.sha256?.lowercased()
        let localSha = local?.sha256?.lowercased()
        let remoteSha = remote?.sha256?.lowercased()

        // 4. No common baseline B(Initial sync or startup file)
        guard let baseSha else {
            return decideWithoutBaseline(baseSha: baseSha, localSha: localSha, remoteSha: remoteSha,
                                         localStatus: localStatus, remoteStatus: remoteStatus, local: local)
        }

        // 5. Has common baseline B:Calculate the change status of both ends from baseline
        // At this time, due to the pre-completeness check,present in the state of localSha and remoteSha Definitely not empty
        let localChanged = (localStatus == .present && localSha != baseSha)
        let remoteChanged = (remoteStatus == .present && remoteSha != baseSha)
        let localUnchanged = (localStatus == .present && localSha == baseSha)
        let remoteUnchanged = (remoteStatus == .present && remoteSha == baseSha)

        let localDeleted = (localStatus == .absent)
        let remoteDeleted = (remoteStatus == .absent || remoteStatus == .trashed)

        if let decision = deletionDecision(localDeleted: localDeleted, remoteDeleted: remoteDeleted,
                                           localChanged: localChanged, remoteChanged: remoteChanged,
                                           localUnchanged: localUnchanged, remoteUnchanged: remoteUnchanged,
                                           baseSha: baseSha, localSha: localSha, remoteSha: remoteSha) {
            return decision
        }

        // 5.4 Content Change Scenario
        if localChanged && remoteUnchanged {
            return .upload(reason: "Only local content changed")
        }

        if remoteChanged && localUnchanged {
            return .download(reason: "Only remote content changed")
        }

        if localUnchanged && remoteUnchanged {
            return .unchanged
        }

        if localChanged && remoteChanged {
            if let localSha, let remoteSha, localSha == remoteSha {
                return .matchUpdateBaseline(sha256: localSha, size: local?.size ?? 0)
            } else {
                let conflictId = Self.conflictIdentity(base: baseSha, local: localSha, remote: remoteSha)
                return .conflict(winner: .remote, conflictId: String(conflictId))
            }
        }

        return .waitingEvidence(reason: "Status evidence does not satisfy any explicit decision path")
    }

    private static func waitingForEvidence(
        localStatus: LocalObservation.Status, remoteStatus: RemoteObservation.Status,
        local: LocalObservation?, remote: RemoteObservation?
    ) -> ReconcileDecision? {
        // 1. Local state is in write instability, waiting for write stability
        if localStatus == .unstable {
            return .waitingEvidence(reason: "Local file body is unstable, writing")
        }

        // 2. Unknown status on either side (unknown):Never put unknown as unchanged
        if localStatus == .unknown && remoteStatus == .unknown {
            return .waitingEvidence(reason: "Observation status unknown at both ends")
        }
        if localStatus == .unknown {
            return .waitingEvidence(reason: "Local observation status is unknown, no evidence decision is rejected")
        }
        if remoteStatus == .unknown {
            return .waitingEvidence(reason: "The remote observation status is unknown, refuse to make an unsubstantiated decision")
        }

        // 3. Content Evidence Completeness Check: As long as one side status is present,Must have valid SHA-256 Summary Evidence
        if localStatus == .present && (local?.sha256 == nil || local?.sha256?.isEmpty == true) {
            return .waitingEvidence(reason: "Local file body SHA-256 To be acquired")
        }
        if remoteStatus == .present && (remote?.sha256 == nil || remote?.sha256?.isEmpty == true) {
            return .waitingEvidence(reason: "Remote file body SHA-256 To be acquired")
        }
        return nil
    }

    private static func decideWithoutBaseline(
        baseSha: String?, localSha: String?, remoteSha: String?,
        localStatus: LocalObservation.Status, remoteStatus: RemoteObservation.Status,
        local: LocalObservation?
    ) -> ReconcileDecision {
        // Locally present and remotely explicitly confirmed not to exist (absent)
        if localStatus == .present && remoteStatus == .absent {
            return .upload(reason: "New local file")
        }
        // Remotely present and locally explicitly confirmed not to exist (absent)
        if remoteStatus == .present && localStatus == .absent {
            return .download(reason: "New remote file")
        }
        // Both present but no baseline
        if localStatus == .present && remoteStatus == .present {
            if let localSha, let remoteSha, localSha == remoteSha {
                return .matchUpdateBaseline(sha256: localSha, size: local?.size ?? 0)
            } else {
                let conflictId = Self.conflictIdentity(base: baseSha, local: localSha, remote: remoteSha)
                return .conflict(winner: .remote, conflictId: String(conflictId))
            }
        }
        // Neither local nor remote
        if localStatus == .absent && remoteStatus == .absent {
            return .unchanged
        }
        // Remote end in trash, local does not exist
        if localStatus == .absent && remoteStatus == .trashed {
            return .unchanged
        }
        // Remote end in Trash, new file locally
        if localStatus == .present && remoteStatus == .trashed {
            return .keepModified(preferLocal: true)
        }
        return .waitingEvidence(reason: "No baseline and status does not meet explicit creation criteria")
    }

    private static func deletionDecision(
        localDeleted: Bool, remoteDeleted: Bool,
        localChanged: Bool, remoteChanged: Bool,
        localUnchanged: Bool, remoteUnchanged: Bool,
        baseSha: String?, localSha: String?, remoteSha: String?
    ) -> ReconcileDecision? {
        // 5.1 Delete on both ends
        if localDeleted && remoteDeleted {
            return .unchanged
        }

        // 5.2 Local deleted, remote not deleted
        if localDeleted && !remoteDeleted {
            if remoteChanged {
                let id = conflictIdentity(base: baseSha, local: localSha, remote: remoteSha)
                return .conflict(winner: .remote, conflictId: id)
            } else if remoteUnchanged {
                // Local deletion, remote confirmation unchanged -> Safely move to Remote Recycle Bin
                return .trashRemote
            }
        }

        // 5.3 Remote deleted, local not deleted
        if remoteDeleted && !localDeleted {
            if localChanged {
                let id = conflictIdentity(base: baseSha, local: localSha, remote: remoteSha)
                return .conflict(winner: .remote, conflictId: id)
            } else if localUnchanged {
                // Remotely deleted, local confirmation unchanged -> Safely delete local files
                return .deleteLocal
            }
        }
        return nil
    }
}
