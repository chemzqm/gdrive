import Foundation
import os
import Testing
@testable import GDrive

@Suite("Item task cancellation", .timeLimit(.minutes(1)))
struct ItemTaskRegistryTests {
    @Test("Cancellation closes admission while draining existing work")
    func cancellationRejectsLateRegistration() async throws {
        let registry = ItemTaskRegistry()
        let started = AsyncSemaphore(count: 0)
        let cancelled = AsyncSemaphore(count: 0)
        let release = AsyncSemaphore(count: 0)
        let finished = OSAllocatedUnfairLock(initialState: false)
        _ = try await registry.start(itemIDs: [1]) {
            await withTaskCancellationHandler {
                started.signal()
                await release.wait()
                finished.withLock { $0 = true }
            } onCancel: { cancelled.signal() }
        }
        await started.wait()
        let cancellation = Task { await registry.cancelAll() }
        await cancelled.wait()
        await #expect(throws: CancellationError.self) {
            _ = try await registry.start(itemIDs: [2]) {}
        }
        #expect(!finished.withLock { $0 })
        release.signal()
        await cancellation.value
        await registry.drainAll()
        #expect(finished.withLock { $0 })
        await #expect(throws: CancellationError.self) {
            _ = try await registry.start(itemIDs: [3]) {}
        }
    }

    @Test("Every admitted operation runs its cleanup when registration races cancellation")
    func admittedOperationsReleaseResources() async throws {
        for _ in 0..<200 {
            let registry = ItemTaskRegistry()
            let cleaned = OSAllocatedUnfairLock(initialState: false)
            async let registration: UUID = registry.start(itemIDs: [1]) {
                defer { cleaned.withLock { $0 = true } }
                guard !Task.isCancelled else { return }
            }
            async let cancellation: Void = registry.cancelAll()
            do {
                _ = try await registration
            } catch is CancellationError {
                // Rejected work remains owned by the caller, as in scheduleUpload.
                cleaned.withLock { $0 = true }
            }
            await cancellation
            await registry.drainAll()
            #expect(cleaned.withLock { $0 })
        }
    }
}
