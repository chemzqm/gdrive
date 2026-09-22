import Foundation
import Testing
@testable import GDrive

struct RequestRateGateTests {
    @Test func configuredCapSpacesRequests() async throws {
        let gate = RequestRateGate(requestsPerSecond: 10)
        let clock = ContinuousClock()
        try await gate.wait()
        let start = clock.now
        try await gate.wait()
        #expect(start.duration(to: clock.now) >= .milliseconds(100))
    }

    @Test func rollingWindowLimitsRequests() async throws {
        let gate = RequestRateGate(requestsPerSecond: 1)
        let clock = ContinuousClock()
        try await gate.wait()
        let start = clock.now
        try await gate.wait()
        #expect(start.duration(to: clock.now) >= .milliseconds(1010))
    }

    @Test func concurrentBurstReceivesSpacedStarts() async throws {
        let gate = RequestRateGate(requestsPerSecond: 20)
        let clock = ContinuousClock()
        let start = clock.now
        let starts = try await withThrowingTaskGroup(of: ContinuousClock.Instant.self) { group in
            for _ in 0..<4 {
                group.addTask {
                    try await gate.wait()
                    return clock.now
                }
            }
            return try await group.reduce(into: []) { $0.append($1) }.sorted()
        }

        #expect(starts.count == 4)
        #expect(start.duration(to: starts[3]) >= .milliseconds(145))
    }

    @Test func disabledCapDoesNotPaceRequests() async throws {
        let gate = RequestRateGate(requestsPerSecond: nil)
        let clock = ContinuousClock()
        let start = clock.now
        for _ in 0..<100 { try await gate.wait() }
        // A 65 requests/s cap would require over 1.5s for these permits.
        #expect(start.duration(to: clock.now) < .seconds(1))
    }

    @Test func disabledCapStillChecksCancellation() async {
        let gate = RequestRateGate(requestsPerSecond: nil)
        let waiter = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            try await gate.wait()
        }
        do {
            try await waiter.value
            Issue.record("Cancelled wait must throw")
        } catch {
            #expect(error is CancellationError)
        }
    }
}
