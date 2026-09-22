import Foundation
import Testing
@testable import GDrive

struct DriveRateLimiterTests {
    @Test func cooldownCannotBeShortenedAndExpires() async throws {
        let limiter = DriveRateLimiter()
        let clock = ContinuousClock()
        let start = clock.now
        await limiter.reportRateLimit(retryAfter: 1.2)
        await limiter.reportRateLimit(retryAfter: 0) // Clamped to 1s, must not shorten 1.2s.
        #expect(await limiter.isCoolingDown)
        try await limiter.acquire()
        #expect(start.duration(to: clock.now) >= .milliseconds(1200))
        #expect(await !limiter.isCoolingDown)
    }

    @Test func waitingAcquireObservesCooldownExtension() async throws {
        let limiter = DriveRateLimiter()
        await limiter.reportRateLimit(retryAfter: 1)
        let waiter = Task { try await limiter.acquire() }
        try await Task.sleep(for: .milliseconds(100))
        let clock = ContinuousClock()
        let extendedAt = clock.now
        await limiter.reportRateLimit(retryAfter: 1.2)
        try await waiter.value
        #expect(extendedAt.duration(to: clock.now) >= .milliseconds(1200))
        #expect(await !limiter.isCoolingDown)
    }

    @Test func cancelledAcquireThrowsEvenWithoutCooldown() async {
        let limiter = DriveRateLimiter()
        let waiter = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            try await limiter.acquire()
        }
        do {
            try await waiter.value
            Issue.record("Cancelled acquire must throw")
        } catch {
            #expect(error is CancellationError)
        }
    }

    @Test func extremeRetryAfterDoesNotOverflowSleepConversion() async {
        let limiter = DriveRateLimiter()
        await limiter.reportRateLimit(retryAfter: .greatestFiniteMagnitude)
        let waiter = Task { try await limiter.acquire() }
        await Task.yield()
        waiter.cancel()
        await #expect(throws: CancellationError.self) {
            try await waiter.value
        }
    }
}
