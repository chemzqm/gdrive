import Foundation

/// Coordinates a shared cooldown after rate-limit or transient failures.
/// RequestRateGate in DriveClient independently enforces the configurable requests/second cap.
public actor DriveRateLimiter {
    private var cooldownUntil: Date?

    public init() {}

    /// Wait until the shared cooldown expires, including extensions by other requests.
    public func acquire() async throws {
        while true {
            try Task.checkCancellation()
            guard let cooldown = cooldownUntil else { return }
            let remaining = cooldown.timeIntervalSinceNow
            if remaining <= 0 {
                cooldownUntil = nil
                return
            }
            try await Task.sleep(nanoseconds: UInt64(remaining * 1_000_000_000))
        }
    }

    /// Extend the shared cooldown without changing the request rate.
    public func reportRateLimit(retryAfter: Double? = nil) {
        let delay = max(retryAfter ?? 1.5, 1.0)
        let newCooldown = Date().addingTimeInterval(delay)
        if let existing = cooldownUntil {
            cooldownUntil = max(existing, newCooldown)
        } else {
            cooldownUntil = newCooldown
        }
    }

    public var isCoolingDown: Bool {
        guard let cooldown = cooldownUntil else { return false }
        return cooldown > Date()
    }
}
