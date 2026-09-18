import Foundation

/// Google Drive API 自适应平滑限流与拥塞控制器
///
/// 核心能力：
/// 1. 平滑发包调度 (Token Bucket / Pacing)：
///    防止几十个并发工作线程在毫秒级瞬间爆发冲撞，将请求均匀平滑在安全时间窗内（默认目标 150 QPS）。
/// 2. 全局退避协同 (Coordinated Backoff)：
///    当任意请求捕获 429 或 403 (rateLimitExceeded / userRateLimitExceeded) 时，立即设置全局冷却窗口。
///    所有其他并发工作线程在发起请求前会自动挂起等待冷却，彻底杜绝群集重试风暴。
/// 3. 自适应拥塞调整 (AIMD)：
///    遭遇限流时进行乘法递减 (Multiplicative Decrease)，连续稳定成功后按步长平滑加法递增 (Additive Increase)。
public actor DriveRateLimiter {
    private var tokens: Double
    private let burstCapacity: Double
    private var currentRate: Double // 目标 QPS
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

    /// 在发起任何 HTTP 请求前获取令牌（若处于冷却期或令牌不足则非阻塞挂起）
    public func acquire() async {
        while true {
            let now = Date()

            // 1. 检查全局冷却期
            if let cooldown = cooldownUntil {
                let remaining = cooldown.timeIntervalSince(now)
                if remaining > 0 {
                    try? await Task.sleep(nanoseconds: UInt64(remaining * 1_000_000_000))
                    continue
                } else {
                    cooldownUntil = nil
                }
            }

            // 2. 补充令牌
            let elapsed = max(0, now.timeIntervalSince(lastRefill))
            lastRefill = now
            tokens = min(burstCapacity, tokens + elapsed * currentRate)

            // 3. 若有令牌则直接消耗一个并返回
            if tokens >= 1.0 {
                tokens -= 1.0
                return
            }

            // 4. 令牌不足，计算到下一个令牌产生的微延迟并挂起
            let deficit = 1.0 - tokens
            let waitSeconds = min(0.1, max(0.004, deficit / currentRate))
            try? await Task.sleep(nanoseconds: UInt64(waitSeconds * 1_000_000_000))
        }
    }

    /// 报告遭遇限流（429 / 403 userRateLimitExceeded / 503）
    /// 触发全局冷却并进行乘法递减
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

        // 乘法递减（降速 25%）
        currentRate = max(minRate, currentRate * 0.75)
        tokens = 0
    }

    /// 报告请求成功，平稳状态下执行平滑加法递增 (Additive Increase)
    public func reportSuccess() {
        if cooldownUntil == nil && currentRate < maxRate {
            currentRate = min(maxRate, currentRate + 0.05)
        }
    }

    /// 当前实际 QPS 速率
    public var effectiveRate: Double {
        currentRate
    }

    /// 当前是否处于全局冷却中
    public var isCoolingDown: Bool {
        if let cd = cooldownUntil {
            return cd > Date()
        }
        return false
    }
}
