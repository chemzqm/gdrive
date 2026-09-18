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
        #expect(decision == .upload(reason: "仅本地内容更新"))
    }

    @Test("Only remote changed triggers download")
    func testOnlyRemoteChanged() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: baseSha, size: 100)
        let remote = RemoteObservation(status: .present, sha256: modSha1, size: 150)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision == .download(reason: "仅远端内容更新"))
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

    @Test("One side deleted and other side modified preserves modified content")
    func testDeleteAndModifyConflict() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .absent)
        let remote = RemoteObservation(status: .present, sha256: modSha1, size: 200)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision == .keepModified(preferLocal: false))
    }

    @Test("Completely unchanged file produces unchanged")
    func testCompletelyUnchanged() {
        let baseline = ItemBaseline(sha256: baseSha, size: 100)
        let local = LocalObservation(status: .present, sha256: baseSha, size: 100)
        let remote = RemoteObservation(status: .present, sha256: baseSha, size: 100)

        let decision = Reconciler.decide(baseline: baseline, local: local, remote: remote)
        #expect(decision == .unchanged)
    }
}
