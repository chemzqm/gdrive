import Foundation
import CommonCrypto
import Logging
import os
import Darwin

/// Google Drive File and Catalog Metadata Resource Model
public struct DriveFile: Codable, Sendable {
    public let id: String
    public let name: String
    public let mimeType: String?
    public let parents: [String]?
    public let size: String?
    public let sha256Checksum: String?
    public let version: String?
    public let trashed: Bool?

    public init(
        id: String,
        name: String,
        mimeType: String? = nil,
        parents: [String]? = nil,
        size: String? = nil,
        sha256Checksum: String? = nil,
        version: String? = nil,
        trashed: Bool? = nil
    ) {
        self.id = id
        self.name = name
        self.mimeType = mimeType
        self.parents = parents
        self.size = size
        self.sha256Checksum = sha256Checksum
        self.version = version
        self.trashed = trashed
    }

    public var isDirectory: Bool {
        mimeType == "application/vnd.google-apps.folder"
    }

    public var sizeBytes: Int64? {
        size.flatMap(Int64.init)
    }

    public var versionNumber: Int64? {
        version.flatMap(Int64.init)
    }
}

/// Resumable upload session state reported by Google Drive.
public enum ResumableUploadResult: Sendable {
    /// The session is active. The associated value is the first byte not yet
    /// confirmed by the server.
    case incomplete(confirmedOffset: Int64)
    /// The upload has completed and Drive returned the resulting file.
    case complete(DriveFile)
    /// The session no longer exists and must be recreated.
    case expired
}

/// Google Drive API Error type
public enum DriveError: Error, Sendable, CustomStringConvertible, Equatable {
    case rateLimited429(retryAfter: TimeInterval?)
    case rateLimited403(reason: String, retryAfter: TimeInterval?)
    case notFound(fileId: String)
    case conflict(fileId: String, message: String)
    case checksumMismatch(expected: String, actual: String?)
    case sizeMismatch(expected: Int64, actual: Int64?)
    case serverError(statusCode: Int, message: String)
    case invalidResponse(message: String)
    case unsafeOverwrite(fileId: String)
    case stableInputUnavailable(path: String)

    public var description: String {
        switch self {
        case .rateLimited429(let delay):
            return "Google Drive API 429 请求过多 (Too Many Requests)，建议等待: \(delay ?? 1.0)s"
        case .rateLimited403(let reason, let delay):
            return "Google Drive API 403 配额/频次超限 (\(reason))，建议等待: \(delay ?? 2.0)s"
        case .notFound(let id):
            return "File or directory not found (404): ID \(id)"
        case .conflict(let id, let msg):
            return "ID Conflict (409): ID \(id), \(msg)"
        case .checksumMismatch(let exp, let act):
            return "SHA-256 Validation mismatch: Expected \(exp), Actual \(act ?? "nil")"
        case .sizeMismatch(let exp, let act):
            return "File size mismatch: Expected \(exp) Bytes, Actual \(act ?? -1) Bytes"
        case .serverError(let code, let msg):
            return "Server-side error (\(code)): \(msg)"
        case .invalidResponse(let msg):
            return "Invalid API Response: \(msg)"
        case .unsafeOverwrite(let id):
            return "Remote body overlay blocked,Drive Conditional write has not been verified, keep pending sync: \(id)"
        case .stableInputUnavailable(let path):
            return "File system does not support copy-on-write snapshots, failing to capture large file upload inputs safely and efficiently: \(path)"
        }
    }
}

/// Changes Change Entry
public struct DriveChange: Codable, Sendable {
    public let fileId: String
    public let removed: Bool?
    public let file: DriveFile?
}

/// Changes Response Model
public struct DriveChangesPage: Codable, Sendable {
    public let nextPageToken: String?
    public let newStartPageToken: String?
    public let changes: [DriveChange]
}

private enum RateLimitEncounter: Sendable {
    case rateLimited429(retryAfter: Double?)
    case rateLimited403(reason: String, retryAfter: Double?)
    case transientServer503(retryAfter: Double?)

    var retryDelay: Double? {
        switch self {
        case .rateLimited429(let delay),
             .rateLimited403(_, let delay),
             .transientServer503(let delay):
            return delay
        }
    }
}

private struct GoogleAPIErrorDetail: Decodable, Sendable {
    let domain: String?
    let reason: String?
    let message: String?
}

private struct GoogleAPIErrorBody: Decodable, Sendable {
    let code: Int?
    let message: String?
    let errors: [GoogleAPIErrorDetail]?
}

private struct GoogleAPIErrorEnvelope: Decodable, Sendable {
    let error: GoogleAPIErrorBody?
}

private final class DownloadTaskBox: @unchecked Sendable {
    private struct State {
        var task: URLSessionDataTask?
        var cancelled = false
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    func store(_ task: URLSessionDataTask) {
        let shouldCancel = state.withLock { state in
            state.task = task
            return state.cancelled
        }
        if shouldCancel { task.cancel() }
    }

    func cancel() {
        let task = state.withLock { state in
            state.cancelled = true
            return state.task
        }
        task?.cancel()
    }
}

private struct DownloadContextState {
    var response: HTTPURLResponse?
    var writeError: Error?
    var errorBodyBytesWritten = 0
}

private final class DownloadProgressHighWater: Sendable {
    private struct State {
        var attemptBytes: Int64 = 0
        var reportedBytes: Int64 = 0
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    func beginAttempt() {
        state.withLock { $0.attemptBytes = 0 }
    }

    func record(_ delta: Int64) -> Int64 {
        guard delta > 0 else { return 0 }
        return state.withLock { state in
            state.attemptBytes += delta
            guard state.attemptBytes > state.reportedBytes else { return 0 }
            let newlyReported = state.attemptBytes - state.reportedBytes
            state.reportedBytes = state.attemptBytes
            return newlyReported
        }
    }
}

private final class DownloadSessionDelegate: NSObject, URLSessionDataDelegate, @unchecked Sendable {
    private final class Context: @unchecked Sendable {
        let handle: FileHandle
        let onProgress: (@Sendable (Int64) -> Void)?
        let continuation: CheckedContinuation<HTTPURLResponse, Error>
        let state = OSAllocatedUnfairLock(initialState: DownloadContextState())

        init(
            handle: FileHandle,
            onProgress: (@Sendable (Int64) -> Void)?,
            continuation: CheckedContinuation<HTTPURLResponse, Error>
        ) {
            self.handle = handle
            self.onProgress = onProgress
            self.continuation = continuation
        }
    }

    private let contexts = OSAllocatedUnfairLock<[Int: Context]>(initialState: [:])

    func download(
        using session: URLSession,
        request: URLRequest,
        destinationURL: URL,
        onProgress: (@Sendable (Int64) -> Void)?
    ) async throws -> HTTPURLResponse {
        _ = FileManager.default.createFile(atPath: destinationURL.path, contents: nil)
        let handle = try FileHandle(forWritingTo: destinationURL)
        let taskBox = DownloadTaskBox()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let task = session.dataTask(with: request)
                let context = Context(
                    handle: handle, onProgress: onProgress, continuation: continuation)
                contexts.withLock { $0[task.taskIdentifier] = context }
                taskBox.store(task)
                task.resume()
            }
        } onCancel: {
            taskBox.cancel()
        }
    }

    func urlSession(
        _ session: URLSession,
        dataTask: URLSessionDataTask,
        didReceive response: URLResponse,
        completionHandler: @escaping @Sendable (URLSession.ResponseDisposition) -> Void
    ) {
        guard let context = contexts.withLock({ $0[dataTask.taskIdentifier] }) else {
            completionHandler(.cancel)
            return
        }
        context.state.withLock { $0.response = response as? HTTPURLResponse }
        completionHandler(.allow)
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        guard let context = contexts.withLock({ $0[dataTask.taskIdentifier] }) else { return }
        let shouldReport = context.state.withLock { state in
            guard state.writeError == nil else { return false }
            let isSuccessfulResponse = state.response.map {
                (200..<300).contains($0.statusCode)
            } ?? false
            let dataToWrite: Data
            if isSuccessfulResponse {
                dataToWrite = data
            } else {
                let remaining = max(0, 4096 - state.errorBodyBytesWritten)
                guard remaining > 0 else { return false }
                dataToWrite = Data(data.prefix(remaining))
            }
            do {
                try context.handle.write(contentsOf: dataToWrite)
                if !isSuccessfulResponse {
                    state.errorBodyBytesWritten += dataToWrite.count
                }
                return isSuccessfulResponse
            } catch {
                state.writeError = error
                return false
            }
        }
        if shouldReport { context.onProgress?(Int64(data.count)) }
        if context.state.withLock({ $0.writeError != nil }) { dataTask.cancel() }
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: Error?
    ) {
        guard let context = contexts.withLock({ $0.removeValue(forKey: task.taskIdentifier) }) else {
            return
        }
        try? context.handle.close()
        let result = context.state.withLock { state -> Result<HTTPURLResponse, Error> in
            if let writeError = state.writeError { return .failure(writeError) }
            if let error { return .failure(error) }
            guard let response = state.response else {
                return .failure(DriveError.invalidResponse(message: "Response is not HTTP"))
            }
            return .success(response)
        }
        context.continuation.resume(with: result)
    }
}

/// Google Drive Core communication and transport client
/// Follow the v1.md Transmission specification:
/// - Single-session multiplexing URLSession
/// - Pregeneration ID Idempotent Creation and Upload
/// - Small Files (≤ 8MB) Zero Disk Staging Multipart One-step upload
/// - Large Files (> 8MB) Continuing in chunks Resumable Upload
/// - Response Field One-Time Validation (id,name,mimeType,parents,size,sha256Checksum,version)
public final class DriveClient: Sendable {
    typealias RetrySleep = @Sendable (TimeInterval) async throws -> Void
    typealias DownloadAttempt = @Sendable (
        URLRequest, URL, (@Sendable (Int64) -> Void)?
    ) async throws -> HTTPURLResponse

    public let auth: Auth
    public let rateLimiter: DriveRateLimiter
    private let session: URLSession
    private let downloadAttempt: DownloadAttempt
    private let retrySleep: RetrySleep
    private let retryLimitOverride: Int?
    private let logger = Logger(label: "gdrive.client")
    private let requestGate: RequestRateGate

    public static let fields = "id,name,mimeType,parents,size,sha256Checksum,version,trashed"

    private struct CachedToken: Sendable {
        let token: String
        let expiresAt: Date
    }

    private let tokenState = OSAllocatedUnfairLock<CachedToken?>(initialState: nil)

    /// Create exclusive optimizations for high concurrency transmissions URLSession
    public static func makeDefaultSession() -> URLSession {
        let config = URLSessionConfiguration.default
        config.httpMaximumConnectionsPerHost = 128
        config.httpShouldUsePipelining = true
        config.timeoutIntervalForRequest = 60
        config.timeoutIntervalForResource = 300
        config.requestCachePolicy = .reloadIgnoringLocalCacheData
        return URLSession(configuration: config)
    }

    /// requestsPerSecond defaults to 65; pass a positive cap or nil to disable pacing.
    public init(
        auth: Auth,
        session: URLSession = DriveClient.makeDefaultSession(),
        rateLimiter: DriveRateLimiter = DriveRateLimiter(),
        requestsPerSecond: Int? = 65
    ) {
        self.auth = auth
        self.session = session
        let downloadDelegate = DownloadSessionDelegate()
        let downloadSession = URLSession(
            configuration: session.configuration, delegate: downloadDelegate, delegateQueue: nil)
        self.downloadAttempt = { request, destinationURL, onProgress in
            try await downloadDelegate.download(
                using: downloadSession, request: request,
                destinationURL: destinationURL, onProgress: onProgress)
        }
        self.rateLimiter = rateLimiter
        self.requestGate = RequestRateGate(requestsPerSecond: requestsPerSecond)
        self.retryLimitOverride = nil
        self.retrySleep = { delay in
            try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        }
    }

    init(
        auth: Auth,
        session: URLSession,
        rateLimiter: DriveRateLimiter = DriveRateLimiter(),
        requestsPerSecond: Int? = 65,
        maxRetries: Int,
        retrySleep: @escaping RetrySleep,
        downloadAttempt: DownloadAttempt? = nil
    ) {
        self.auth = auth
        self.session = session
        let downloadDelegate = DownloadSessionDelegate()
        let downloadSession = URLSession(
            configuration: session.configuration, delegate: downloadDelegate, delegateQueue: nil)
        self.downloadAttempt = downloadAttempt ?? { request, destinationURL, onProgress in
            try await downloadDelegate.download(
                using: downloadSession, request: request,
                destinationURL: destinationURL, onProgress: onProgress)
        }
        self.rateLimiter = rateLimiter
        self.requestGate = RequestRateGate(requestsPerSecond: requestsPerSecond)
        self.retryLimitOverride = max(0, maxRetries)
        self.retrySleep = retrySleep
    }

    /// High concurrency fast get effective Access Token(Memory atomic level cache, avoiding Actor contention)
    public func getValidToken() async throws -> String {
        let cached = tokenState.withLock { $0 }
        if let cached, cached.expiresAt.timeIntervalSinceNow > 60 {
            return cached.token
        }

        let freshToken = try await auth.token()
        let authData = await auth.authData()
        let exp = authData.expiresAt ?? Date(timeIntervalSinceNow: 3500)

        tokenState.withLock { $0 = CachedToken(token: freshToken, expiresAt: exp) }
        return freshToken
    }

    // MARK: - Core Actuators (Adaptive current limiting and resilient retries)

    private enum RetryDecision {
        case shouldRetry
        case notHandled
    }

    private func handleRateLimitOrTransientError(
        data: Data,
        http: HTTPURLResponse,
        attempt: Int,
        retryLimit: Int
    ) async throws -> RetryDecision {
        if let encounter = parseRateLimit(data: data, response: http) {
            guard attempt <= retryLimit else {
                switch encounter {
                case .rateLimited429(let delay):
                    throw DriveError.rateLimited429(retryAfter: delay)
                case .rateLimited403(let reason, let delay):
                    throw DriveError.rateLimited403(reason: reason, retryAfter: delay)
                case .transientServer503:
                    let detail = String(bytes: data, encoding: .utf8) ?? "Service Unavailable"
                    throw DriveError.serverError(statusCode: 503, message: detail)
                }
            }

            let jitter = Double.random(in: 0.2...0.8)
            let exponential = min(16.0, pow(2.0, Double(attempt)) + jitter)
            let backoff = max(encounter.retryDelay ?? 0, exponential)
            await rateLimiter.reportRateLimit(retryAfter: backoff)
            try await retrySleep(backoff)
            return .shouldRetry
        }

        if [500, 502, 504].contains(http.statusCode), attempt <= retryLimit {
            let retryDelay = http.value(forHTTPHeaderField: "Retry-After").flatMap(Double.init)
            let jitter = Double.random(in: 0.2...0.8)
            let exponential = min(16.0, pow(2.0, Double(attempt)) + jitter)
            let backoff = max(retryDelay ?? 0, exponential)
            try await retrySleep(backoff)
            return .shouldRetry
        }

        return .notHandled
    }

    /// Unified Execution HTTP Request, with smooth current limiting scheduling, global withdrawal coordination,401 Auto Refresh vs. 429/503/403 Exponential back-off retry
    public func executeRequest(
        _ request: URLRequest,
        maxRetries: Int = 5,
        acceptableStatusCodes: Set<Int> = Set(200..<300)
    ) async throws -> (Data, HTTPURLResponse) {
        let retryLimit = retryLimitOverride ?? maxRetries
        var attempt = 0
        var currentReq = request
        var didRefreshAfterUnauthorized = false

        while true {
            attempt += 1
            try await rateLimiter.acquire()

            try await requestGate.wait()
            let data: Data
            let response: URLResponse
            do {
                (data, response) = try await session.data(for: currentReq)
            } catch {
                try Task.checkCancellation()
                guard attempt <= retryLimit else { throw error }
                let jitter = Double.random(in: 0.1...0.5)
                let delay = min(16.0, pow(2.0, Double(attempt - 1)) + jitter)
                await rateLimiter.reportRateLimit(retryAfter: delay)
                try await retrySleep(delay)
                continue
            }

            guard let http = response as? HTTPURLResponse else {
                throw DriveError.invalidResponse(message: "Response is not HTTP")
            }

            if http.statusCode == 401 && !didRefreshAfterUnauthorized {
                didRefreshAfterUnauthorized = true
                try await refreshAuthorization(in: &currentReq)
                continue
            }

            let decision = try await handleRateLimitOrTransientError(
                data: data, http: http, attempt: attempt, retryLimit: retryLimit
            )
            if decision == .shouldRetry {
                continue
            }

            if http.statusCode == 409 || http.statusCode == 308 || acceptableStatusCodes.contains(http.statusCode) {
                return (data, http)
            }

            let detail = (String(bytes: data, encoding: .utf8) ?? "Invalid UTF-8 data")
            throw DriveError.serverError(statusCode: http.statusCode, message: detail)
        }
    }

    private func parseRateLimit(data: Data, response http: HTTPURLResponse) -> RateLimitEncounter? {
        let retryDelay = http.value(forHTTPHeaderField: "Retry-After").flatMap(Double.init)

        if http.statusCode == 429 {
            return .rateLimited429(retryAfter: retryDelay)
        }

        if http.statusCode == 503 {
            return .transientServer503(retryAfter: retryDelay)
        }

        if http.statusCode == 403 {
            let envelope = try? JSONDecoder().decode(GoogleAPIErrorEnvelope.self, from: data)
            let parsedReason = envelope?.error?.errors?.first?.reason
            let detail = (String(bytes: data, encoding: .utf8) ?? "")

            let rateLimitReasons = [
                "userRateLimitExceeded",
                "rateLimitExceeded",
                "quotaExceeded",
                "sharingRateLimitExceeded",
                "dailyLimitExceeded"
            ]

            let reason: String?
            if let parsedReason, rateLimitReasons.contains(parsedReason) {
                reason = parsedReason
            } else if detail.contains("userRateLimitExceeded") {
                reason = "userRateLimitExceeded"
            } else if detail.contains("rateLimitExceeded") {
                reason = "rateLimitExceeded"
            } else if detail.contains("quotaExceeded") {
                reason = "quotaExceeded"
            } else if detail.contains("sharingRateLimitExceeded") {
                reason = "sharingRateLimitExceeded"
            } else if detail.contains("dailyLimitExceeded") {
                reason = "dailyLimitExceeded"
            } else {
                reason = nil
            }

            if let reason {
                return .rateLimited403(reason: reason, retryAfter: retryDelay ?? 2.0)
            }
        }

        return nil
    }

    private func refreshAuthorization(in request: inout URLRequest) async throws {
        let rejectedToken = request.value(forHTTPHeaderField: "Authorization")?
            .replacingOccurrences(of: "Bearer ", with: "")
        tokenState.withLock { $0 = nil }
        let freshToken = try await auth.forceRefresh(rejecting: rejectedToken)
        let authData = await auth.authData()
        let expiry = authData.expiresAt ?? Date(timeIntervalSinceNow: 3500)
        tokenState.withLock { $0 = CachedToken(token: freshToken, expiresAt: expiry) }
        request.setValue("Bearer \(freshToken)", forHTTPHeaderField: "Authorization")
    }

    private func executeDownloadRequest(
        _ request: URLRequest,
        remoteId: String,
        destinationURL: URL,
        onProgress: (@Sendable (Int64) -> Void)?,
        maxRetries: Int = 5
    ) async throws -> HTTPURLResponse {
        let retryLimit = retryLimitOverride ?? maxRetries
        var attempt = 0
        var currentRequest = request
        var didRefreshAfterUnauthorized = false
        let progressHighWater = DownloadProgressHighWater()

        while true {
            attempt += 1
            progressHighWater.beginAttempt()
            try await rateLimiter.acquire()

            try await requestGate.wait()
            let http: HTTPURLResponse
            do {
                http = try await downloadAttempt(
                    currentRequest, destinationURL, { delta in
                        let newlyReported = progressHighWater.record(delta)
                        if newlyReported > 0 { onProgress?(newlyReported) }
                    })
            } catch {
                try? FileManager.default.removeItem(at: destinationURL)
                try Task.checkCancellation()
                guard attempt <= retryLimit else { throw error }
                let jitter = Double.random(in: 0.1...0.5)
                let delay = min(16.0, pow(2.0, Double(attempt - 1)) + jitter)
                await rateLimiter.reportRateLimit(retryAfter: delay)
                try await retrySleep(delay)
                continue
            }

            if http.statusCode == 401 && !didRefreshAfterUnauthorized {
                try? FileManager.default.removeItem(at: destinationURL)
                didRefreshAfterUnauthorized = true
                try await refreshAuthorization(in: &currentRequest)
                continue
            }

            guard (200..<300).contains(http.statusCode) else {
                let handle = try? FileHandle(forReadingFrom: destinationURL)
                let errorData = (try? handle?.read(upToCount: 4096)) ?? Data()
                try? handle?.close()
                try? FileManager.default.removeItem(at: destinationURL)
                let decision = try await handleRateLimitOrTransientError(
                    data: errorData, http: http, attempt: attempt, retryLimit: retryLimit
                )
                if decision == .shouldRetry { continue }
                let detail = String(bytes: errorData, encoding: .utf8) ?? "Invalid UTF-8 data"
                if http.statusCode == 404 {
                    throw DriveError.notFound(fileId: remoteId)
                }
                if http.statusCode == 403 {
                    throw DriveError.serverError(statusCode: 403, message: detail)
                }
                throw DriveError.serverError(statusCode: http.statusCode, message: detail)
            }

            return http
        }
    }

    // MARK: - Pre-allocation ID

    /// Batch Prefetch Server Approved ID
    public func generateIds(count: Int = 100, space: String = "drive") async throws -> [String] {
        var remaining = count
        var result: [String] = []

        while remaining > 0 {
            let batch = min(remaining, 1000)
            let token = try await getValidToken()
            var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files/generateIds")!
            components.queryItems = [
                URLQueryItem(name: "count", value: String(batch)),
                URLQueryItem(name: "space", value: space)
            ]
            guard let url = components.url else {
                throw DriveError.invalidResponse(message: "Unable to build the generateIds URL")
            }

            var req = URLRequest(url: url)
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

            let (data, _) = try await executeRequest(req, acceptableStatusCodes: [200])

            struct GenerateIdsResponse: Decodable {
                let ids: [String]
            }
            let decoded = try JSONDecoder().decode(GenerateIdsResponse.self, from: data)
            result.append(contentsOf: decoded.ids)
            remaining -= decoded.ids.count
            if decoded.ids.isEmpty { break }
        }

        return result
    }

    // MARK: - Directory Creation (createDirectory)

    /// Create a remote directory (using pre-allocation ID)
    /// If you encounter 409,Automatic remote verification ID Has it been created correctly?
    public func createDirectory(
        name: String,
        parentId: String,
        remoteId: String
    ) async throws -> DriveFile {
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files")!
        components.queryItems = [
            URLQueryItem(name: "fields", value: Self.fields)
        ]
        var req = URLRequest(url: components.url!)
        req.httpMethod = "POST"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue("application/json; charset=UTF-8", forHTTPHeaderField: "Content-Type")

        let body: [String: Any] = [
            "id": remoteId,
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parentId]
        ]
        req.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, http) = try await executeRequest(req, acceptableStatusCodes: [200, 201])

        // 409 Conflict: Pre-Built ID Retry or concurrency has been created, verify validation object
        if http.statusCode == 409 {
            return try await verifyExistingFolder(remoteId: remoteId, expectedName: name, expectedParentId: parentId)
        }

        return try JSONDecoder().decode(DriveFile.self, from: data)
    }

    private func verifyExistingFolder(remoteId: String, expectedName: String, expectedParentId: String) async throws -> DriveFile {
        let existing = try await getFile(remoteId: remoteId)
        guard existing.isDirectory else {
            throw DriveError.conflict(fileId: remoteId, message: "Existing object is not a directory")
        }
        guard existing.name == expectedName else {
            throw DriveError.conflict(fileId: remoteId, message: "Existing directory names are inconsistent: \(existing.name) != \(expectedName)")
        }
        if let parents = existing.parents, !parents.contains(expectedParentId) {
            throw DriveError.conflict(fileId: remoteId, message: "Existing directory parent mismatch: \(parents)")
        }
        return existing
    }

    public static let multipartBoundary = "-------GDriveMultipartBoundary7MA4YWxkTrZu0gW"

    /// Use Multipart/related Upload the file in one step (do not lose the temporary disk, the body is sent directly and the checksum is verified)
    public func uploadMultipart(
        name: String,
        parentId: String,
        remoteId: String,
        mimeType: String = "application/octet-stream",
        content: Data,
        expectedSha256: String
    ) async throws -> DriveFile {
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/upload/drive/v3/files")!
        components.queryItems = [
            URLQueryItem(name: "uploadType", value: "multipart"),
            URLQueryItem(name: "fields", value: Self.fields)
        ]

        let boundary = Self.multipartBoundary
        var req = URLRequest(url: components.url!)
        req.httpMethod = "POST"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue("multipart/related; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")

        let metadataObj: [String: Any] = [
            "id": remoteId,
            "name": name,
            "parents": [parentId]
        ]
        let metadataData = try JSONSerialization.data(withJSONObject: metadataObj)

        // Construction multipart Request body
        var body = Data()
        body.reserveCapacity(content.count + metadataData.count + 256)
        body.append(Data("--\(boundary)\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".utf8))
        body.append(metadataData)
        body.append(Data("\r\n--\(boundary)\r\nContent-Type: \(mimeType)\r\n\r\n".utf8))
        body.append(content)
        body.append(Data("\r\n--\(boundary)--\r\n".utf8))

        req.httpBody = body

        let (data, http) = try await executeRequest(req, acceptableStatusCodes: [200, 201])

        // 409 Conflict check
        if http.statusCode == 409 {
            return try await verifyExistingFile(
                remoteId: remoteId,
                expectedName: name,
                expectedParentId: parentId,
                expectedSize: Int64(content.count),
                expectedSha256: expectedSha256
            )
        }

        let driveFile = try JSONDecoder().decode(DriveFile.self, from: data)

        // Validate size in response vs. SHA-256
        if let actualSize = driveFile.sizeBytes, actualSize != Int64(content.count) {
            throw DriveError.sizeMismatch(expected: Int64(content.count), actual: actualSize)
        }
        if let actualChecksum = driveFile.sha256Checksum,
           actualChecksum.caseInsensitiveCompare(expectedSha256) != .orderedSame {
            throw DriveError.checksumMismatch(expected: expectedSha256, actual: actualChecksum)
        }

        return driveFile
    }

    private func verifyExistingFile(
        remoteId: String,
        expectedName: String,
        expectedParentId: String,
        expectedSize: Int64,
        expectedSha256: String
    ) async throws -> DriveFile {
        let existing = try await getFile(remoteId: remoteId)
        guard existing.name == expectedName else {
            throw DriveError.conflict(fileId: remoteId, message: "File name mismatch")
        }
        if let parents = existing.parents, !parents.contains(expectedParentId) {
            throw DriveError.conflict(fileId: remoteId, message: "Parent directory mismatch")
        }
        if let size = existing.sizeBytes, size != expectedSize {
            throw DriveError.sizeMismatch(expected: expectedSize, actual: size)
        }
        if let checksum = existing.sha256Checksum, checksum.caseInsensitiveCompare(expectedSha256) != .orderedSame {
            throw DriveError.checksumMismatch(expected: expectedSha256, actual: checksum)
        }
        return existing
    }

    // MARK: - Document content update (Update)

    /// Existing file body overwrite is currently blocked safely: thrown unsafeOverwrite,Do not issue HTTP Request.
    /// It can only be re-enabled after the real server-side atomic conditional write contract validation is passed.
    public func updateMultipart(
        remoteId: String,
        mimeType: String = "application/octet-stream",
        content: Data,
        expectedSha256: String
    ) async throws -> DriveFile {
        // Real Drive contract probe: uploadType=media accepts an invalid
        // If-Match and overwrites the object. A preceding GET cannot close that race.
        throw DriveError.unsafeOverwrite(fileId: remoteId)
    }

    // MARK: - Large Files Resumable Upload (> 8MB)

    /// Initiate a create-only resumable upload session for a preallocated remote ID.
    /// Existing remote body overwrites remain prohibited because Drive has no validated
    /// atomic conditional-write contract for the final resumable chunk.
    public func initiateResumableUpload(
        name: String,
        parentId: String,
        remoteId: String,
        totalBytes: Int64,
        mimeType: String = "application/octet-stream"
    ) async throws -> URL {
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/upload/drive/v3/files")!
        components.queryItems = [
            URLQueryItem(name: "uploadType", value: "resumable"),
            URLQueryItem(name: "fields", value: Self.fields)
        ]

        var req = URLRequest(url: components.url!)
        req.httpMethod = "POST"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue("application/json; charset=UTF-8", forHTTPHeaderField: "Content-Type")
        req.setValue(mimeType, forHTTPHeaderField: "X-Upload-Content-Type")
        req.setValue(String(totalBytes), forHTTPHeaderField: "X-Upload-Content-Length")

        let metadata: [String: Any] = [
            "id": remoteId,
            "name": name,
            "parents": [parentId]
        ]
        req.httpBody = try JSONSerialization.data(withJSONObject: metadata)

        let (data, http) = try await executeRequest(req, acceptableStatusCodes: [200])
        guard let location = http.value(forHTTPHeaderField: "Location"), let sessionURL = URL(string: location) else {
            let detail = (String(bytes: data, encoding: .utf8) ?? "Invalid UTF-8 data")
            throw DriveError.serverError(statusCode: http.statusCode, message: "Failed to create resumable upload session: \(detail)")
        }

        return sessionURL
    }

    /// to the existing Resumable Session sends chunked data
    /// - Returns: The session state confirmed by the server.The caller must press confirmedOffset Propulsion, cannot be inferred by the length of the transmission.
    public func uploadResumableChunk(
        sessionURL: URL,
        chunkData: Data,
        offset: Int64,
        totalBytes: Int64
    ) async throws -> ResumableUploadResult {
        let chunkEnd = offset + Int64(chunkData.count) - 1
        var req = URLRequest(url: sessionURL)
        req.httpMethod = "PUT"
        req.setValue("bytes \(offset)-\(chunkEnd)/\(totalBytes)", forHTTPHeaderField: "Content-Range")
        req.setValue(String(chunkData.count), forHTTPHeaderField: "Content-Length")
        req.httpBody = chunkData

        let (data, http) = try await executeRequest(req, acceptableStatusCodes: [200, 201, 308, 404])

        if http.statusCode == 308 {
            return .incomplete(confirmedOffset: try parseResumableConfirmedOffset(http, totalBytes: totalBytes))
        } else if http.statusCode == 200 || http.statusCode == 201 {
            return .complete(try JSONDecoder().decode(DriveFile.self, from: data))
        } else if http.statusCode == 404 {
            return .expired
        } else {
            let detail = (String(bytes: data, encoding: .utf8) ?? "Invalid UTF-8 data")
            throw DriveError.serverError(statusCode: http.statusCode, message: "Chunked upload failed: \(detail)")
        }
    }

    /// Query Resumable Acknowledged breakpoint offset for the session
    public func queryResumableOffset(sessionURL: URL, totalBytes: Int64) async throws -> ResumableUploadResult {
        var req = URLRequest(url: sessionURL)
        req.httpMethod = "PUT"
        req.setValue("bytes */\(totalBytes)", forHTTPHeaderField: "Content-Range")
        req.setValue("0", forHTTPHeaderField: "Content-Length")

        let (data, http) = try await executeRequest(req, acceptableStatusCodes: [200, 201, 308, 404])

        switch http.statusCode {
        case 200, 201:
            return .complete(try JSONDecoder().decode(DriveFile.self, from: data))
        case 308:
            return .incomplete(confirmedOffset: try parseResumableConfirmedOffset(http, totalBytes: totalBytes))
        case 404:
            return .expired
        default:
            let detail = (String(bytes: data, encoding: .utf8) ?? "Invalid UTF-8 data")
            throw DriveError.serverError(statusCode: http.statusCode, message: detail)
        }
    }

    private func parseResumableConfirmedOffset(_ response: HTTPURLResponse, totalBytes: Int64) throws -> Int64 {
        // Google omits Range when it has not persisted any bytes yet.
        guard let range = response.value(forHTTPHeaderField: "Range") else { return 0 }
        let prefix = "bytes=0-"
        guard range.hasPrefix(prefix),
              let last = Int64(range.dropFirst(prefix.count)),
              last >= 0,
              last < totalBytes else {
            throw DriveError.invalidResponse(message: "Invalid resumable upload range: \(range)")
        }
        return last + 1
    }

    // MARK: - Metadata Capture and Verification

    /// Get metadata for a single file or directory
    public func getFile(remoteId: String) async throws -> DriveFile {
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files/\(remoteId)")!
        components.queryItems = [
            URLQueryItem(name: "fields", value: Self.fields),
            URLQueryItem(name: "supportsAllDrives", value: "true")
        ]

        var req = URLRequest(url: components.url!)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let (data, http) = try await executeRequest(req, acceptableStatusCodes: [200, 404])
        if http.statusCode == 404 {
            throw DriveError.notFound(fileId: remoteId)
        }
        return try JSONDecoder().decode(DriveFile.self, from: data)
    }

    // MARK: - List table of contents sub-items (List Files)

    /// List direct children of the specified parent directory (automatic paging)
    public func listChildren(parentId: String) async throws -> [DriveFile] {
        var items: [DriveFile] = []
        var token: String?
        repeat {
            let page = try await listChildrenPage(parentId: parentId, pageToken: token)
            items.append(contentsOf: page.files)
            token = page.nextPageToken
        } while token != nil
        return items
    }

    struct ChildrenPage: Decodable, Sendable {
        let nextPageToken: String?
        let files: [DriveFile]
        let incompleteSearch: Bool?
    }

    /// One durable enumeration unit. Partial search results are never evidence of absence.
    func listChildrenPage(parentId: String, pageToken: String? = nil) async throws -> ChildrenPage {
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files")!
        var query = [
            URLQueryItem(name: "q", value: "'\(parentId)' in parents and trashed = false"),
            URLQueryItem(name: "fields", value: "nextPageToken,incompleteSearch,files(\(Self.fields))"),
            URLQueryItem(name: "pageSize", value: "1000"),
            URLQueryItem(name: "supportsAllDrives", value: "true"),
            URLQueryItem(name: "includeItemsFromAllDrives", value: "true")
        ]
        if let pageToken { query.append(URLQueryItem(name: "pageToken", value: pageToken)) }
        components.queryItems = query
        var req = URLRequest(url: components.url!)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        let (data, _) = try await executeRequest(req, acceptableStatusCodes: [200])
        let page = try JSONDecoder().decode(ChildrenPage.self, from: data)
        guard page.incompleteSearch != true, page.nextPageToken != "" else {
            throw DriveError.invalidResponse(message: "Incomplete enumeration of remote directories: \(parentId)")
        }
        return page
    }

    // MARK: - File Download (Download)

    public static var defaultDownloadTemporaryDirectory: URL {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".gdrive/remotes", isDirectory: true)
    }

    public static var defaultConflictDirectory: URL {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".gdrive/conflicts", isDirectory: true)
    }

    /// Download file body and stream checksum SHA-256,Atomic Fall to Target Path
    public func downloadFile(
        remoteId: String,
        destinationURL: URL,
        expectedSha256: String? = nil,
        temporaryDirectory: URL = DriveClient.defaultDownloadTemporaryDirectory,
        onProgress: (@Sendable (Int64) -> Void)? = nil
    ) async throws {
        _ = try await downloadFileSafely(
            remoteId: remoteId, destinationURL: destinationURL, expectedSha256: expectedSha256,
            expectedDestination: LocalFileVersion.read(at: destinationURL),
            temporaryDirectory: temporaryDirectory, onProgress: onProgress
        )
    }

    @discardableResult
    func downloadFileSafely(
        remoteId: String,
        destinationURL: URL,
        expectedSha256: String?,
        expectedDestination: LocalFileVersion?,
        temporaryDirectory: URL = DriveClient.defaultDownloadTemporaryDirectory,
        beforePublish: (@Sendable () async throws -> Void)? = nil,
        onProgress: (@Sendable (Int64) -> Void)? = nil
    ) async throws -> LocalFileVersion {
        guard try LocalFileVersion.read(at: destinationURL) == expectedDestination else {
            throw SyncEngineError.localFileModified(path: destinationURL.path)
        }
        let download = try await downloadVerifiedFile(
            remoteId: remoteId, expectedSha256: expectedSha256,
            temporaryDirectory: temporaryDirectory, onProgress: onProgress)
        var shouldRemoveDownload = true
        defer {
            if shouldRemoveDownload { try? FileManager.default.removeItem(at: download.url) }
        }
        try await beforePublish?()
        switch try LocalFilePublication.publish(
            download.url, to: destinationURL, expected: expectedDestination,
            expectedSHA256: download.sha256) {
        case .published(let version): return version
        case .destinationChanged:
            throw SyncEngineError.localFileModified(path: destinationURL.path)
        case .failedAfterWriteStarted(let failure):
            shouldRemoveDownload = false
            throw LocalFilePublicationRecoveryError(
                destinationPath: failure.destinationPath,
                stagingURL: download.url,
                sha256: download.sha256,
                size: download.size,
                reason: failure.reason)
        }
    }

    struct VerifiedDownload: Sendable {
        let url: URL
        let sha256: String
        let size: Int64
    }

    func downloadVerifiedFile(
        remoteId: String,
        expectedSha256: String?,
        temporaryDirectory: URL = DriveClient.defaultDownloadTemporaryDirectory,
        onProgress: (@Sendable (Int64) -> Void)? = nil
    ) async throws -> VerifiedDownload {
        try DownloadStaging.prepare(temporaryDirectory)
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files/\(remoteId)")!
        components.queryItems = [
            URLQueryItem(name: "alt", value: "media"),
            URLQueryItem(name: "supportsAllDrives", value: "true")
        ]

        var req = URLRequest(url: components.url!)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let tempURL = temporaryDirectory.appendingPathComponent(".tmp_\(UUID().uuidString)")
        do {
            _ = try await executeDownloadRequest(
                req, remoteId: remoteId, destinationURL: tempURL, onProgress: onProgress)

            let fileHandle = try FileHandle(forReadingFrom: tempURL)
            defer { try? fileHandle.close() }

            var ctx = CC_SHA256_CTX()
            CC_SHA256_Init(&ctx)
            var byteCount: Int64 = 0
            while let data = try fileHandle.read(upToCount: 8 * 1024 * 1024), !data.isEmpty {
                _ = data.withUnsafeBytes { bytes in
                    CC_SHA256_Update(&ctx, bytes.baseAddress, CC_LONG(data.count))
                }
                byteCount += Int64(data.count)
            }

            var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
            CC_SHA256_Final(&digest, &ctx)
            let actualSha256 = digest.map { String(format: "%02x", $0) }.joined()

            if let expected = expectedSha256,
               actualSha256.caseInsensitiveCompare(expected) != .orderedSame {
                throw DriveError.checksumMismatch(expected: expected, actual: actualSha256)
            }
            return VerifiedDownload(url: tempURL, sha256: actualSha256, size: byteCount)
        } catch {
            try? FileManager.default.removeItem(at: tempURL)
            throw error
        }
    }

    // MARK: - Changes Incremental Changes

    /// Get current latest start Changes Token
    public func getStartPageToken() async throws -> String {
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/changes/startPageToken")!
        components.queryItems = [
            URLQueryItem(name: "supportsAllDrives", value: "true")
        ]

        var req = URLRequest(url: components.url!)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let (data, _) = try await executeRequest(req, acceptableStatusCodes: [200])

        struct StartTokenResponse: Decodable {
            let startPageToken: String
        }
        let decoded = try JSONDecoder().decode(StartTokenResponse.self, from: data)
        return decoded.startPageToken
    }

    /// Listed by pageToken All remote changes that occur after
    public func listChanges(pageToken: String, pageSize: Int = 1000) async throws -> DriveChangesPage {
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/changes")!
        components.queryItems = [
            URLQueryItem(name: "pageToken", value: pageToken),
            URLQueryItem(name: "pageSize", value: String(pageSize)),
            URLQueryItem(name: "fields", value: "nextPageToken,newStartPageToken,changes(fileId,removed,file(\(Self.fields)))"),
            URLQueryItem(name: "includeItemsFromAllDrives", value: "true"),
            URLQueryItem(name: "supportsAllDrives", value: "true")
        ]

        var req = URLRequest(url: components.url!)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let (data, _) = try await executeRequest(req, acceptableStatusCodes: [200])
        return try JSONDecoder().decode(DriveChangesPage.self, from: data)
    }

    // MARK: - Metadata updates and renaming (Rename / Move)

    /// Rename or move remote objects (files or directories)
    @discardableResult
    public func updateMetadata(
        remoteId: String,
        newName: String? = nil,
        addParentId: String? = nil,
        removeParentId: String? = nil
    ) async throws -> DriveFile {
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files/\(remoteId)")!
        var queryItems = [
            URLQueryItem(name: "fields", value: Self.fields),
            URLQueryItem(name: "supportsAllDrives", value: "true")
        ]
        if let addParentId {
            queryItems.append(URLQueryItem(name: "addParents", value: addParentId))
        }
        if let removeParentId {
            queryItems.append(URLQueryItem(name: "removeParents", value: removeParentId))
        }
        components.queryItems = queryItems

        var req = URLRequest(url: components.url!)
        req.httpMethod = "PATCH"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue("application/json; charset=UTF-8", forHTTPHeaderField: "Content-Type")

        var body: [String: Any] = [:]
        if let newName {
            body["name"] = newName
        }
        req.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, _) = try await executeRequest(req, acceptableStatusCodes: [200])
        return try JSONDecoder().decode(DriveFile.self, from: data)
    }

    // MARK: - Recycle Bin Actions (Trash)

    public func trash(remoteId: String) async throws {
        let token = try await getValidToken()
        let url = URL(string: "https://www.googleapis.com/drive/v3/files/\(remoteId)")!

        var req = URLRequest(url: url)
        req.httpMethod = "PATCH"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")

        let body = ["trashed": true]
        req.httpBody = try JSONSerialization.data(withJSONObject: body)

        _ = try await executeRequest(req, acceptableStatusCodes: [200, 204])
    }

    /// Move remote objects out of the Trash (Restore)
    public func untrash(remoteId: String) async throws {
        let token = try await getValidToken()
        let url = URL(string: "https://www.googleapis.com/drive/v3/files/\(remoteId)")!

        var req = URLRequest(url: url)
        req.httpMethod = "PATCH"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")

        let body = ["trashed": false]
        req.httpBody = try JSONSerialization.data(withJSONObject: body)

        _ = try await executeRequest(req, acceptableStatusCodes: [200, 204])
    }

}

/// Optional smooth pacing with a conservative 1.01s rolling window.
/// Shared by ordinary requests, streaming requests, and retries for one client.
actor RequestRateGate {
    private struct Reservation {
        let id: UUID
        let start: TimeInterval
    }

    private let requestsPerSecond: Int?
    private var reservations: [Reservation] = []

    init(requestsPerSecond: Int? = 65) {
        precondition(requestsPerSecond == nil || requestsPerSecond! > 0,
                     "requestsPerSecond must be positive or nil")
        self.requestsPerSecond = requestsPerSecond
    }

    func wait() async throws {
        try Task.checkCancellation()
        guard let limit = requestsPerSecond else { return }
        let now = ProcessInfo.processInfo.systemUptime
        reservations.removeAll { $0.start <= now - 1.01 }
        let windowReady = reservations.count < limit
            ? now : reservations[reservations.count - limit].start + 1.01
        let pacedReady = reservations.last.map { $0.start + 1.0 / Double(limit) } ?? now
        let ready = max(pacedReady, windowReady)
        let reservation = Reservation(id: UUID(), start: ready)
        reservations.append(reservation)
        do {
            let delay = ready - now
            if delay > 0 {
                try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
            }
            try Task.checkCancellation()
        } catch {
            reservations.removeAll { $0.id == reservation.id }
            throw error
        }
    }
}
