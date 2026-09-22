import Testing
@testable import GDrive

@Suite("AsyncSemaphore cancellation")
struct AsyncSemaphoreTests {
    @Test("A cancelled waiter does not consume the next signal", .timeLimit(.minutes(1)))
    func cancelledWaiterDoesNotConsumeSignal() async throws {
        let semaphore = AsyncSemaphore(count: 0)
        let cancelledStarted = AsyncSemaphore(count: 0)
        let liveStarted = AsyncSemaphore(count: 0)

        let cancelledWaiter = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            cancelledStarted.signal()
            try await semaphore.wait()
        }
        try await cancelledStarted.wait()
        try await Task.sleep(for: .milliseconds(10))

        let liveWaiter = Task {
            liveStarted.signal()
            try await semaphore.wait()
        }
        try await liveStarted.wait()
        try await Task.sleep(for: .milliseconds(10))

        let fallback = Task {
            do {
                try await Task.sleep(for: .milliseconds(100))
            } catch {
                return false
            }
            semaphore.signal()
            return true
        }
        semaphore.signal()
        do {
            try await cancelledWaiter.value
            Issue.record("The cancelled waiter acquired a semaphore permit")
        } catch is CancellationError {
            // Expected: cancellation removes and resumes the queued waiter.
        }
        try await liveWaiter.value
        fallback.cancel()
        #expect(await fallback.value == false)
    }
}
