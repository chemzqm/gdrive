import Foundation
import Testing
@testable import GDrive

actor MockIDGenerator: DriveIDGenerator {
    private var requestCount: Int = 0
    private var generatedCount: Int = 0
    private var shouldFailNext: Int = 0
    private var emptyResponse: Bool = false
    private let delayMs: UInt64

    init(delayMs: UInt64 = 0) {
        self.delayMs = delayMs
    }

    var totalRequests: Int {
        requestCount
    }

    func setFailNext(_ count: Int) {
        shouldFailNext = count
    }

    func setEmptyResponse(_ empty: Bool) {
        emptyResponse = empty
    }

    func generateIds(count: Int, space: String) async throws -> [String] {
        if delayMs > 0 {
            try await Task.sleep(nanoseconds: delayMs * 1_000_000)
        }

        requestCount += 1

        if shouldFailNext > 0 {
            shouldFailNext -= 1
            throw NSError(domain: "MockIDGenerator", code: 500, userInfo: [NSLocalizedDescriptionKey: "Simulated network failure"])
        }

        if emptyResponse {
            return []
        }

        var batch: [String] = []
        batch.reserveCapacity(count)
        for _ in 0..<count {
            generatedCount += 1
            batch.append("mock_id_\(generatedCount)")
        }
        return batch
    }
}

@Suite("IDPool Tests")
struct IDPoolTests {

    @Test("P01 Regression: Concurrent callers waiting on in-flight fetch receive unique IDs and consume batch once")
    func testConcurrentWaitersOnInFlightFetch() async throws {
        let mock = MockIDGenerator(delayMs: 30)
        let pool = IDPool(api: mock, initialIds: ["initial_id"])

        // Pop the only initial ID; this triggers background prefetch
        let firstId = try await pool.nextId()
        #expect(firstId == "initial_id")

        // Concurrently request 64 IDs while the prefetch is in flight
        let concurrency = 64
        let ids = try await withThrowingTaskGroup(of: String.self) { group -> [String] in
            for _ in 0..<concurrency {
                group.addTask {
                    try await pool.nextId()
                }
            }
            var collected: [String] = []
            for try await id in group {
                collected.append(id)
            }
            return collected
        }

        #expect(ids.count == 64)
        #expect(Set(ids).count == 64, "All 64 returned IDs must be completely unique")
        let totalReqs = await mock.totalRequests
        #expect(totalReqs == 1, "Only 1 prefetch API request should have been dispatched")

        let remaining = await pool.count
        #expect(remaining == 1000 - 64, "Pool remaining should be exactly 936")
    }

    @Test("P02 Regression: Cold start with 64 concurrent callers dispatches only 1 API request")
    func testColdStartSingleFlight() async throws {
        let mock = MockIDGenerator(delayMs: 30)
        let pool = IDPool(api: mock, initialIds: [])

        let concurrency = 64
        let ids = try await withThrowingTaskGroup(of: String.self) { group -> [String] in
            for _ in 0..<concurrency {
                group.addTask {
                    try await pool.nextId()
                }
            }
            var collected: [String] = []
            for try await id in group {
                collected.append(id)
            }
            return collected
        }

        #expect(ids.count == 64)
        #expect(Set(ids).count == 64, "All 64 returned IDs must be completely unique")
        let totalReqs = await mock.totalRequests
        #expect(totalReqs == 1, "Cold start should coalesce into 1 single-flight API request")

        let remaining = await pool.count
        #expect(remaining == 1000 - 64, "Pool remaining should be exactly 936")
    }

    @Test("P03 Regression: Failed fetch is cleaned up and subsequent requests can recover")
    func testFailureCleanupAndRecovery() async throws {
        let mock = MockIDGenerator(delayMs: 10)
        await mock.setFailNext(1)
        let pool = IDPool(api: mock, initialIds: [])

        // First call fails
        do {
            _ = try await pool.nextId()
            Issue.record("Expected first call to fail due to simulated network error")
        } catch {
            // Expected
        }
        let totalReqs1 = await mock.totalRequests
        #expect(totalReqs1 == 1)

        // Second call should NOT be stuck with the old failed task; it must trigger a new fetch and succeed
        let recoveredId = try await pool.nextId()
        #expect(recoveredId == "mock_id_1000")
        let totalReqs2 = await mock.totalRequests
        #expect(totalReqs2 == 2, "Second attempt should issue a new API request")

        let remaining = await pool.count
        #expect(remaining == 999)
    }

    @Test("Demand exceeding batch size automatically triggers subsequent batch without deadlock")
    func testDemandExceedingBatchSize() async throws {
        let mock = MockIDGenerator(delayMs: 10)
        let pool = IDPool(api: mock, initialIds: [])

        let concurrency = 1500
        let ids = try await withThrowingTaskGroup(of: String.self) { group -> [String] in
            for _ in 0..<concurrency {
                group.addTask {
                    try await pool.nextId()
                }
            }
            var collected: [String] = []
            for try await id in group {
                collected.append(id)
            }
            return collected
        }

        #expect(ids.count == 1500)
        #expect(Set(ids).count == 1500, "All 1500 IDs must be distinct")
        let totalReqs = await mock.totalRequests
        #expect(totalReqs == 2, "1500 requests require 2 batches of 1000")

        let remaining = await pool.count
        #expect(remaining == 2000 - 1500)
    }

    @Test("Empty pool without API throws error code 1")
    func testEmptyPoolNoAPI() async throws {
        let pool = IDPool(api: nil, initialIds: [])
        do {
            _ = try await pool.nextId()
            Issue.record("Expected error when no API is configured")
        } catch let err as NSError {
            #expect(err.domain == "IDPool")
            #expect(err.code == 1)
        }
    }

    @Test("API returning empty list throws error code 2")
    func testEmptyResponseError() async throws {
        let mock = MockIDGenerator()
        await mock.setEmptyResponse(true)
        let pool = IDPool(api: mock, initialIds: [])
        do {
            _ = try await pool.nextId()
            Issue.record("Expected error when API returns empty IDs")
        } catch let err as NSError {
            #expect(err.domain == "IDPool")
            #expect(err.code == 2)
        }
    }

    @Test("takeIds drains available IDs and triggers background prefetch if low")
    func testTakeIds() async throws {
        let mock = MockIDGenerator(delayMs: 10)
        let pool = IDPool(api: mock, initialIds: ["id1", "id2", "id3"])

        let taken = await pool.takeIds(count: 2)
        #expect(taken == ["id2", "id3"])
        let countAfterTake = await pool.count
        #expect(countAfterTake == 1)

        // Remaining 1 id is below threshold of 200, so prefetch is triggered
        // Wait a short moment for prefetch to finish
        try await Task.sleep(nanoseconds: 50_000_000)
        let countAfterPrefetch = await pool.count
        #expect(countAfterPrefetch == 1001)
    }

    @Test("ensureCapacity pulls multiple batches up to target capacity")
    func testEnsureCapacity() async throws {
        let mock = MockIDGenerator(delayMs: 10)
        let pool = IDPool(api: mock, initialIds: [])

        try await pool.ensureCapacity(2500)
        let count = await pool.count
        #expect(count == 3000, "3 batches of 1000 should be fetched")
        let totalReqs = await mock.totalRequests
        #expect(totalReqs == 3)
    }
}
