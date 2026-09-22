import Foundation
import os
import Testing
@testable import GDrive

@Suite("ProgressNotifier Tests")
struct ProgressNotifierTests {
    private final class LifetimeSignal: Sendable {
        let released: AsyncSemaphore
        init(_ released: AsyncSemaphore) { self.released = released }
        deinit { released.signal() }
    }

    @Test("Ticker releases its owner without finish", .timeLimit(.minutes(1)))
    func tickerDoesNotRetainNotifier() async throws {
        let tick = AsyncSemaphore(count: 0)
        let released = AsyncSemaphore(count: 0)
        var notifier: ProgressNotifier? = ProgressNotifier(interval: 0.001) { [lifetime = LifetimeSignal(released)] _ in
            _ = lifetime
            tick.signal()
        }
        notifier?.addDiscovered()
        try await tick.wait()
        notifier = nil
        try await released.wait()
    }

    @Test("Finish serializes with an in-flight callback and prevents later delivery", .timeLimit(.minutes(1)))
    func finishSerializesCallbacks() async throws {
        let entered = AsyncSemaphore(count: 0)
        let release = DispatchSemaphore(value: 0)
        let finishing = AsyncSemaphore(count: 0)
        let done = AsyncSemaphore(count: 0)
        let deliveries = OSAllocatedUnfairLock(initialState: [Int]())
        let notifier = ProgressNotifier(interval: 1, startsTicker: false) { progress in
            if progress.completedFiles == 0 {
                entered.signal()
                release.wait()
            }
            deliveries.withLock { $0.append(progress.completedFiles) }
        }
        notifier.addDiscovered()
        DispatchQueue.global().async {
            notifier.notifyIfChanged()
            done.signal()
        }
        try await entered.wait()
        notifier.addCompleted()
        DispatchQueue.global().async {
            finishing.signal()
            notifier.finish()
            done.signal()
        }
        try await finishing.wait()
        release.signal()
        try await done.wait()
        try await done.wait()
        notifier.addCompleted()
        notifier.notifyIfChanged()
        notifier.finish()
        #expect(deliveries.withLock { $0 } == [0, 1])
    }

    @Test("Stop suppresses delivery and callbacks can reenter stop")
    func stopIsReentrant() {
        let holder = OSAllocatedUnfairLock<ProgressNotifier?>(initialState: nil)
        let calls = OSAllocatedUnfairLock(initialState: 0)
        let notifier = ProgressNotifier(interval: 1, startsTicker: false) { _ in
            calls.withLock { $0 += 1 }
            holder.withLock { $0 }?.stop()
        }
        holder.withLock { $0 = notifier }
        notifier.notifyIfChanged()
        notifier.addCompleted()
        notifier.notifyIfChanged()
        notifier.finish()
        #expect(calls.withLock { $0 } == 1)
        holder.withLock { $0 = nil }
    }

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

    @Test("Byte-only completion changes publish progress")
    func byteOnlyProgressIsPublished() {
        let deliveries = OSAllocatedUnfairLock(initialState: [SyncProgress]())
        let notifier = ProgressNotifier(interval: 1, startsTicker: false) { progress in
            deliveries.withLock { $0.append(progress) }
        }
        notifier.addDiscovered(files: 1, bytes: 100)
        notifier.notifyIfChanged()
        notifier.addCompleted(files: 0, bytes: 25)
        notifier.notifyIfChanged()

        let snapshots = deliveries.withLock { $0 }
        #expect(snapshots.count == 2)
        #expect(snapshots.last?.completedFiles == 0)
        #expect(snapshots.last?.completedBytes == 25)
        notifier.stop()
    }
}
