import Foundation
import Testing
@testable import GDrive

@Suite("ProgressNotifier Tests")
struct ProgressNotifierTests {
    @Test("ProgressNotifier aggregates discovery and completion before final progress")
    func testProgressNotifierAggregation() {
        final class CallbackRecord: @unchecked Sendable {
            var calls = [SyncProgress]()
            private var lock = os_unfair_lock()

            func record(_ progress: SyncProgress) {
                os_unfair_lock_lock(&lock)
                calls.append(progress)
                os_unfair_lock_unlock(&lock)
            }

            var count: Int {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return calls.count
            }

            var last: SyncProgress? {
                os_unfair_lock_lock(&lock)
                defer { os_unfair_lock_unlock(&lock) }
                return calls.last
            }
        }

        let tracker = CallbackRecord()
        let notifier = ProgressNotifier(
            interval: 0.05,
            startsTicker: false,
            onProgress: { progress in tracker.record(progress) })

        // 1. Intensive and rapid triggering 100 Updates (simulating uploading while scanning)
        for index in 1...100 {
            notifier.addDiscovered(files: 1, bytes: 1024)
            if index % 2 == 0 {
                notifier.addCompleted(files: 1, bytes: 1024)
            }
        }

        #expect(tracker.count == 0)
        notifier.notifyIfChanged()
        #expect(tracker.count == 1)
        if let latest = tracker.last {
            #expect(latest.totalDiscoveredFiles == 100)
            #expect(latest.completedFiles == 50)
            #expect(latest.totalDiscoveredBytes == 100 * 1024)
            #expect(latest.completedBytes == 50 * 1024)
            #expect(latest.percentage == 0.5)
        }

        // 3. Complete the rest 50 file and call finish()
        notifier.addCompleted(files: 50, bytes: 50 * 1024)
        notifier.finish()

        // Final state after strong brushing 100%
        let finalSnapshot = tracker.last!
        #expect(finalSnapshot.completedFiles == 100)
        #expect(finalSnapshot.totalDiscoveredFiles == 100)
        #expect(finalSnapshot.completedBytes == 100 * 1024)
        #expect(finalSnapshot.totalDiscoveredBytes == 100 * 1024)
        #expect(finalSnapshot.percentage == 1.0)
    }
}
