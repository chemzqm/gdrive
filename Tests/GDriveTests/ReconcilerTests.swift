import Foundation
import Testing
@testable import GDrive

@Suite("Reconciler Decision Tests")
struct ReconcilerTests {
    let baseSha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    let modSha1 = "a1b2c3d4e5f678901234567890abcdef1234567890abcdef1234567890abcdef"
    let modSha2 = "f0e1d2c3b4a59876543210fedcba0987654321fedcba0987654321fedcba0987"

    @Test("Only local changed triggers upload")
    func testOnlyLocalChanged() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: modSha1, size: 120)
        let remote = RemoteObservation(status: .present, sha256: baseSha, size: 100)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision == .upload(reason: "Only local content changed"))
    }

    @Test("Only remote changed triggers download")
    func testOnlyRemoteChanged() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: baseSha, size: 100)
        let remote = RemoteObservation(status: .present, sha256: modSha1, size: 150)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision == .download(reason: "Only remote content changed"))
    }

    @Test("Both changed with identical SHA-256 advances baseline without transfer")
    func testBothChangedIdenticalSha() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: modSha1, size: 120)
        let remote = RemoteObservation(status: .present, sha256: modSha1, size: 120)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision == .matchUpdateBaseline(sha256: modSha1, size: 120))
    }

    @Test("Both changed with divergent SHA-256 creates conflict")
    func testBothChangedDivergentSha() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: modSha1, size: 120)
        let remote = RemoteObservation(status: .present, sha256: modSha2, size: 130)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        if case .conflict(let winner, _) = decision {
            #expect(winner == .remote)
        } else {
            Issue.record("Expected conflict decision")
        }
    }

    @Test("Conflict identity is stable with and without a baseline", arguments: [true, false])
    func stableConflictIdentity(hasBaseline: Bool) {
        let baseline = hasBaseline ? ItemBaseline(sha256: baseSha, size: 100) : nil
        let local = LocalObservation(status: .present, sha256: modSha1, size: 120)
        let remote = RemoteObservation(status: .present, sha256: modSha2, size: 130)
        let first = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(first == Reconciler.decide(baseline: baseline, local: local, remote: remote))
        #expect(first == Reconciler.decide(baseline: baseline,
            local: LocalObservation(status: .present, sha256: modSha1.uppercased(), size: 120), remote: remote))
    }

    @Test("Local deleted while remote unchanged trashes remote")
    func testLocalDeletedRemoteUnchanged() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .absent)
        let remote = RemoteObservation(status: .present, sha256: baseSha, size: 100)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision == .trashRemote)
    }

    @Test("Remote deleted while local unchanged deletes local")
    func testRemoteDeletedLocalUnchanged() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: baseSha, size: 100)
        let remote = RemoteObservation(status: .trashed, sha256: baseSha, size: 100)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision == .deleteLocal)
    }

    @Test("One side deleted and other side modified creates a conflict")
    func testDeleteAndModifyConflict() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .absent)
        let remote = RemoteObservation(status: .present, sha256: modSha1, size: 200)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        guard case .conflict = decision else {
            Issue.record("Expected deletion versus modification conflict")
            return
        }
    }

    @Test("Completely unchanged file produces unchanged")
    func testCompletelyUnchanged() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: baseSha, size: 100)
        let remote = RemoteObservation(status: .present, sha256: baseSha, size: 100)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision == .unchanged)
    }

    @Test("P05 Regression: Baseline exists, L absent, R unknown -> waitingEvidence (refuses trashRemote without remote evidence)")
    func testP05AbsentUnknown() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .absent)
        let remote = RemoteObservation(status: .unknown)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        if case .waitingEvidence = decision {
            // Success: refuses to trash remote without evidence
        } else {
            Issue.record("Expected waitingEvidence, got: \(decision)")
        }
    }

    @Test("P06 Regression: Baseline exists, L unknown, R trashed -> waitingEvidence (refuses deleteLocal without local evidence)")
    func testP06UnknownTrashed() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .unknown)
        let remote = RemoteObservation(status: .trashed, sha256: baseSha, size: 100)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        if case .waitingEvidence = decision {
            // Success: refuses to delete local without evidence
        } else {
            Issue.record("Expected waitingEvidence, got: \(decision)")
        }
    }

    @Test("P07 Regression: Baseline exists, L changed, R unknown -> waitingEvidence (refuses upload without remote evidence)")
    func testP07ChangedUnknown() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: modSha1, size: 120)
        let remote = RemoteObservation(status: .unknown)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        if case .waitingEvidence = decision {
            // Success: refuses to overwrite remote without evidence
        } else {
            Issue.record("Expected waitingEvidence, got: \(decision)")
        }
    }

    @Test("P08 Regression: Baseline exists, L unchanged, R present without hash -> waitingEvidence (refuses false download)")
    func testP08PresentWithoutHash() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: baseSha, size: 100)
        let remote = RemoteObservation(status: .present, sha256: nil, size: 100)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        if case .waitingEvidence = decision {
            // Success: refuses to trigger download when hash is missing
        } else {
            Issue.record("Expected waitingEvidence, got: \(decision)")
        }
    }

    @Test("Local unstable returns waitingEvidence")
    func testLocalUnstable() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .unstable, sha256: modSha1, size: 120)
        let remote = RemoteObservation(status: .present, sha256: baseSha, size: 100)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        if case .waitingEvidence = decision {
            // Success
        } else {
            Issue.record("Expected waitingEvidence, got: \(decision)")
        }
    }

    @Test("No baseline: new local file proven absent on remote uploads")
    func testNoBaselineNewLocalFile() {
        let local = LocalObservation(status: .present, sha256: modSha1, size: 120)
        let remote = RemoteObservation(status: .absent)

        let decision = Reconciler.decide(baseline: nil, local: local, remote: remote)
        #expect(decision == .upload(reason: "New local file"))
    }

    @Test("No baseline: new remote file proven absent on local downloads")
    func testNoBaselineNewRemoteFile() {
        let local = LocalObservation(status: .absent)
        let remote = RemoteObservation(status: .present, sha256: modSha1, size: 120)

        let decision = Reconciler.decide(baseline: nil, local: local, remote: remote)
        #expect(decision == .download(reason: "New remote file"))
    }

    @Test("No baseline: new local file with unknown remote returns waitingEvidence")
    func testNoBaselineLocalPresentRemoteUnknown() {
        let local = LocalObservation(status: .present, sha256: modSha1, size: 120)
        let remote = RemoteObservation(status: .unknown)

        let decision = Reconciler.decide(baseline: nil, local: local, remote: remote)
        if case .waitingEvidence = decision {
            // Success
        } else {
            Issue.record("Expected waitingEvidence, got: \(decision)")
        }
    }

    @Test("No baseline: new file missing hash returns waitingEvidence")
    func testNoBaselineMissingHash() {
        let local = LocalObservation(status: .present, sha256: nil, size: 120)
        let remote = RemoteObservation(status: .absent)

        let decision = Reconciler.decide(baseline: nil, local: local, remote: remote)
        if case .waitingEvidence = decision {
            // Success
        } else {
            Issue.record("Expected waitingEvidence, got: \(decision)")
        }
    }
}
