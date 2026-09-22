import Foundation

/// Coordinates a shared cooldown after rate-limit or transient failures.
/// RequestRateGate in DriveClient independently enforces the configurable requests/second cap.
public actor DriveRateLimiter {
    private let clock = ContinuousClock()
    private var cooldownUntil: ContinuousClock.Instant?

    public init() {}

    /// Wait until the shared cooldown expires, including extensions by other requests.
    public func acquire() async throws {
        while true {
            try Task.checkCancellation()
            guard let cooldown = cooldownUntil else { return }
            if clock.now >= cooldown {
                cooldownUntil = nil
                return
            }
            try await clock.sleep(until: cooldown)
        }
    }

    /// Extend the shared cooldown without changing the request rate.
    public func reportRateLimit(retryAfter: Double? = nil) {
        let requestedDelay = retryAfter ?? 1.5
        let delay = requestedDelay.isNaN ? 1.5 : min(max(requestedDelay, 1.0), 3_600.0)
        let newCooldown = clock.now.advanced(by: .seconds(delay))
        if let existing = cooldownUntil {
            cooldownUntil = max(existing, newCooldown)
        } else {
            cooldownUntil = newCooldown
        }
    }

    public var isCoolingDown: Bool {
        guard let cooldown = cooldownUntil else { return false }
        return cooldown > clock.now
    }
}
