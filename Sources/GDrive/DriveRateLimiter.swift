import Foundation

/// Google Drive API Adaptive Smooth Current Limiting and Congestion Controller
///
/// Core Competencies:
/// 1. Smooth Packet Dispatch (Token Bucket / Pacing):
///    Prevent dozens of concurrent worker threads from colliding instantaneously on the millisecond scale, smoothing the request evenly within the safe time window (default target 150 QPS).
/// 2. Global Exit Synergy (Coordinated Backoff):
///    When any request captures 429 or 403 (rateLimitExceeded / userRateLimitExceeded) When, immediately set the global cooling window.
///    All other concurrent worker threads automatically hang and wait for cooling before initiating a request, completely eliminating the cluster retry storm.
/// 3. Adaptive Congestion Adjustment (AIMD):
///    Multiply decrement when encountering current throttling (Multiplicative Decrease),Smooth addition increment by step size after successive stabilizations (Additive Increase).
public actor DriveRateLimiter {
    private var tokens: Double
    private let burstCapacity: Double
    private var currentRate: Double // Objectives QPS
    private let minRate: Double
    private let maxRate: Double

    private var lastRefill: Date
    private var cooldownUntil: Date?

    public init(
        targetRate: Double = 150.0,
        burstCapacity: Double = 24.0,
        minRate: Double = 50.0,
        maxRate: Double = 180.0
    ) {
        self.currentRate = targetRate
        self.burstCapacity = burstCapacity
        self.minRate = minRate
        self.maxRate = maxRate
        self.tokens = burstCapacity
        self.lastRefill = Date()
    }

    /// Initiate any HTTP Get token before request (non-blocking pending if in cooldown or insufficient token)
    public func acquire() async {
        while true {
            let now = Date()

            // 1. Check Global Cooling Period
            if let cooldown = cooldownUntil {
                let remaining = cooldown.timeIntervalSince(now)
                if remaining > 0 {
                    try? await Task.sleep(nanoseconds: UInt64(remaining * 1_000_000_000))
                    continue
                } else {
                    cooldownUntil = nil
                }
            }

            // 2. Supplemental token
            let elapsed = max(0, now.timeIntervalSince(lastRefill))
            lastRefill = now
            tokens = min(burstCapacity, tokens + elapsed * currentRate)

            // 3. If there are tokens, consume one directly and return
            if tokens >= 1.0 {
                tokens -= 1.0
                return
            }

            // 4. Insufficient tokens, calculate the micro-delay to the next token and hang
            let deficit = 1.0 - tokens
            let waitSeconds = min(0.1, max(0.004, deficit / currentRate))
            try? await Task.sleep(nanoseconds: UInt64(waitSeconds * 1_000_000_000))
        }
    }

    /// Reporting current limitation encountered (429 / 403 userRateLimitExceeded / 503)
    /// Trigger global cooldown and multiply decrement
    public func reportRateLimit(retryAfter: Double? = nil) {
        let now = Date()
        let delay = max(retryAfter ?? 1.5, 1.0)
        let newCooldown = now.addingTimeInterval(delay)

        if let existing = cooldownUntil {
            if newCooldown > existing {
                cooldownUntil = newCooldown
            }
        } else {
            cooldownUntil = newCooldown
        }

        // Multiply Descending (Deceleration 25%)
        currentRate = max(minRate, currentRate * 0.75)
        tokens = 0
    }

    /// Report request succeeded, smooth addition increment in steady state (Additive Increase)
    public func reportSuccess() {
        if cooldownUntil == nil && currentRate < maxRate {
            currentRate = min(maxRate, currentRate + 0.05)
        }
    }

    /// Current Actual QPS Rate
    public var effectiveRate: Double {
        currentRate
    }

    /// Is currently in global cooldown
    public var isCoolingDown: Bool {
        if let cd = cooldownUntil {
            return cd > Date()
        }
        return false
    }
}
