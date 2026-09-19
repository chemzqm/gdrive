import Foundation
import CommonCrypto
import Logging
import os
import Darwin

/// Google Drive 文件及目录元数据资源模型
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

/// Google Drive API 错误类型
public enum DriveError: Error, Sendable, CustomStringConvertible {
    case rateLimited(retryAfter: TimeInterval?)
    case notFound(fileId: String)
    case conflict(fileId: String, message: String)
    case checksumMismatch(expected: String, actual: String?)
    case sizeMismatch(expected: Int64, actual: Int64?)
    case serverError(statusCode: Int, message: String)
    case invalidResponse(message: String)
    case fileModifiedDuringUpload(path: String)
    case unsafeOverwrite(fileId: String)
    case stableInputUnavailable(path: String)

    public var description: String {
        switch self {
        case .rateLimited(let delay):
            return "Google Drive API 限流 (429/403)，建议等待: \(delay ?? 1.0)s"
        case .notFound(let id):
            return "文件或目录未找到 (404): ID \(id)"
        case .conflict(let id, let msg):
            return "ID 冲突 (409): ID \(id), \(msg)"
        case .checksumMismatch(let exp, let act):
            return "SHA-256 校验不匹配: 预期 \(exp), 实际 \(act ?? "nil")"
        case .sizeMismatch(let exp, let act):
            return "文件大小不匹配: 预期 \(exp) 字节, 实际 \(act ?? -1) 字节"
        case .serverError(let code, let msg):
            return "服务端错误 (\(code)): \(msg)"
        case .invalidResponse(let msg):
            return "无效的 API 响应: \(msg)"
        case .fileModifiedDuringUpload(let path):
            return "本地文件在传输或发布期间发生变化: \(path)"
        case .unsafeOverwrite(let id):
            return "已阻断远端正文覆盖，Drive 条件写尚未验证，保留待同步状态: \(id)"
        case .stableInputUnavailable(let path):
            return "文件系统不支持写时复制快照，无法安全且高效地捕获大文件上传输入: \(path)"
        }
    }
}

/// Changes 变更条目
public struct DriveChange: Codable, Sendable {
    public let fileId: String
    public let removed: Bool?
    public let file: DriveFile?
}

/// Changes 响应模型
public struct DriveChangesPage: Codable, Sendable {
    public let nextPageToken: String?
    public let newStartPageToken: String?
    public let changes: [DriveChange]
}

/// Google Drive 核心通信与传输客户端
/// 遵循 v1.md 传输规范：
/// - 单会话复用 URLSession
/// - 预生成 ID 幂等创建与上传
/// - 小文件 (≤ 8MB) 零磁盘暂存 Multipart 一步上传
/// - 大文件 (> 8MB) 分块续传 Resumable Upload
/// - 响应字段一次性校验 (id,name,mimeType,parents,size,sha256Checksum,version)
public final class DriveClient: Sendable {
    public let auth: Auth
    public let rateLimiter: DriveRateLimiter
    private let session: URLSession
    private let logger = Logger(label: "gdrive.client")

    public static let fields = "id,name,mimeType,parents,size,sha256Checksum,version,trashed"

    private struct CachedToken: Sendable {
        let token: String
        let expiresAt: Date
    }

    private let tokenState = OSAllocatedUnfairLock<CachedToken?>(initialState: nil)

    /// 创建针对高并发传输优化的专属 URLSession
    public static func makeDefaultSession() -> URLSession {
        let config = URLSessionConfiguration.default
        config.httpMaximumConnectionsPerHost = 128
        config.httpShouldUsePipelining = true
        config.timeoutIntervalForRequest = 60
        config.timeoutIntervalForResource = 300
        config.requestCachePolicy = .reloadIgnoringLocalCacheData
        return URLSession(configuration: config)
    }

    public init(
        auth: Auth,
        session: URLSession = DriveClient.makeDefaultSession(),
        rateLimiter: DriveRateLimiter = DriveRateLimiter()
    ) {
        self.auth = auth
        self.session = session
        self.rateLimiter = rateLimiter
    }

    /// 高并发快速获取有效 Access Token（内存原子级缓存，避免 Actor 争用）
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

    // MARK: - 核心执行器 (自适应限流与弹性重试)

    /// 统一执行 HTTP 请求，具备平滑限流调度、全局退避协同、401 自动刷新与 429/503/403 指数退避重试
    public func executeRequest(
        _ request: URLRequest,
        maxRetries: Int = 5,
        acceptableStatusCodes: Set<Int> = Set(200..<300)
    ) async throws -> (Data, HTTPURLResponse) {
        var attempt = 0
        var currentReq = request

        while true {
            attempt += 1
            await rateLimiter.acquire()

            let data: Data
            let response: URLResponse
            do {
                (data, response) = try await session.data(for: currentReq)
            } catch {
                if attempt <= maxRetries {
                    let jitter = Double.random(in: 0.1...0.5)
                    let delay = min(16.0, pow(2.0, Double(attempt - 1)) + jitter)
                    await rateLimiter.reportRateLimit(retryAfter: delay)
                    try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                    continue
                }
                throw error
            }

            guard let http = response as? HTTPURLResponse else {
                throw DriveError.invalidResponse(message: "非 HTTP 响应")
            }

            // 401 凭证过期：清空内存 Token 缓存，重新拉取有效 Token 自动重试
            if http.statusCode == 401 && attempt <= 2 {
                tokenState.withLock { $0 = nil }
                let freshToken = try await getValidToken()
                currentReq.setValue("Bearer \(freshToken)", forHTTPHeaderField: "Authorization")
                continue
            }

            // 检查限流 (429, 503, 或 403 包含 rateLimitExceeded / userRateLimitExceeded / quotaExceeded)
            let isRateLimit: Bool
            var retryDelay: Double? = http.value(forHTTPHeaderField: "Retry-After").flatMap(Double.init)

            if http.statusCode == 429 || http.statusCode == 503 {
                isRateLimit = true
            } else if http.statusCode == 403 {
                let detail = String(decoding: data, as: UTF8.self)
                if detail.contains("rateLimitExceeded") || detail.contains("userRateLimitExceeded") || detail.contains("quotaExceeded") {
                    isRateLimit = true
                    if retryDelay == nil { retryDelay = 2.0 }
                } else {
                    isRateLimit = false
                }
            } else {
                isRateLimit = false
            }

            if isRateLimit {
                if attempt <= maxRetries {
                    let jitter = Double.random(in: 0.2...0.8)
                    let backoff = min(16.0, (retryDelay ?? pow(2.0, Double(attempt))) + jitter)
                    await rateLimiter.reportRateLimit(retryAfter: backoff)
                    try? await Task.sleep(nanoseconds: UInt64(backoff * 1_000_000_000))
                    continue
                }
                throw DriveError.rateLimited(retryAfter: retryDelay)
            }

            // 遇到 409（通常作为业务已存在核验）或 308（Resumable 分块未完成），直接返回供上层处理
            if http.statusCode == 409 || http.statusCode == 308 || acceptableStatusCodes.contains(http.statusCode) {
                await rateLimiter.reportSuccess()
                return (data, http)
            }

            // 其他 HTTP 错误状态
            let detail = String(decoding: data, as: UTF8.self)
            throw DriveError.serverError(statusCode: http.statusCode, message: detail)
        }
    }

    // MARK: - 预分配 ID

    /// 批量预取服务器认可的 ID
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
                throw DriveError.invalidResponse(message: "无法构建 generateIds URL")
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

    // MARK: - 目录创建 (createDirectory)

    /// 创建远端目录（使用预分配 ID）
    /// 若遇到 409，自动向远端核验同 ID 是否已正确创建
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

        // 409 冲突：预生成 ID 重试或并发已被创建，核验证实对象
        if http.statusCode == 409 {
            return try await verifyExistingFolder(remoteId: remoteId, expectedName: name, expectedParentId: parentId)
        }

        return try JSONDecoder().decode(DriveFile.self, from: data)
    }

    private func verifyExistingFolder(remoteId: String, expectedName: String, expectedParentId: String) async throws -> DriveFile {
        let existing = try await getFile(remoteId: remoteId)
        guard existing.isDirectory else {
            throw DriveError.conflict(fileId: remoteId, message: "已有对象不是目录")
        }
        guard existing.name == expectedName else {
            throw DriveError.conflict(fileId: remoteId, message: "已有目录名称不一致: \(existing.name) != \(expectedName)")
        }
        if let parents = existing.parents, !parents.contains(expectedParentId) {
            throw DriveError.conflict(fileId: remoteId, message: "已有目录父级不匹配: \(parents)")
        }
        return existing
    }

    public static let multipartBoundary = "-------GDriveMultipartBoundary7MA4YWxkTrZu0gW"

    /// 使用 Multipart/related 一步上传文件（不落临时磁盘，正文直接发送并校验校验和）
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

        // 构造 multipart 请求体
        var body = Data()
        body.reserveCapacity(content.count + metadataData.count + 256)
        body.append("--\(boundary)\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".data(using: .utf8)!)
        body.append(metadataData)
        body.append("\r\n--\(boundary)\r\nContent-Type: \(mimeType)\r\n\r\n".data(using: .utf8)!)
        body.append(content)
        body.append("\r\n--\(boundary)--\r\n".data(using: .utf8)!)

        req.httpBody = body

        let (data, http) = try await executeRequest(req, acceptableStatusCodes: [200, 201])

        // 409 冲突核验
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

        // 校验响应中的大小与 SHA-256
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
            throw DriveError.conflict(fileId: remoteId, message: "文件名不匹配")
        }
        if let parents = existing.parents, !parents.contains(expectedParentId) {
            throw DriveError.conflict(fileId: remoteId, message: "父目录不匹配")
        }
        if let size = existing.sizeBytes, size != expectedSize {
            throw DriveError.sizeMismatch(expected: expectedSize, actual: size)
        }
        if let checksum = existing.sha256Checksum, checksum.caseInsensitiveCompare(expectedSha256) != .orderedSame {
            throw DriveError.checksumMismatch(expected: expectedSha256, actual: checksum)
        }
        return existing
    }

    // MARK: - 文件内容更新 (Update)

    /// 已有文件正文覆盖目前被安全阻断：抛出 unsafeOverwrite，不发出 HTTP 请求。
    /// 仅在真实服务端原子条件写契约验证通过后才能重新启用。
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

    /// 已有文件 Resumable 覆盖目前被安全阻断，不创建上传会话。
    public func initiateResumableUpdate(
        remoteId: String,
        totalBytes: Int64,
        mimeType: String = "application/octet-stream"
    ) async throws -> URL {
        // Session initiation alone would not prove an atomic condition on the
        // final chunk. Keep updates blocked until that separate contract is proven.
        throw DriveError.unsafeOverwrite(fileId: remoteId)
    }

    // MARK: - 大文件 Resumable Upload (> 8MB)

    /// 发起大文件 Resumable 上传会话，返回用于分块续传的 sessionURI
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
            let detail = String(decoding: data, as: UTF8.self)
            throw DriveError.serverError(statusCode: http.statusCode, message: "创建 Resumable 会话失败: \(detail)")
        }

        return sessionURL
    }

    /// 向已有的 Resumable 会话发送分块数据
    /// - Returns: 服务端确认的会话状态。调用方必须按 confirmedOffset 推进，不能按发送长度推断。
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
            let detail = String(decoding: data, as: UTF8.self)
            throw DriveError.serverError(statusCode: http.statusCode, message: "分块上传失败: \(detail)")
        }
    }

    /// 查询 Resumable 会话的已确认断点偏移量
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
            let detail = String(decoding: data, as: UTF8.self)
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
            throw DriveError.invalidResponse(message: "无效的 Resumable Range: \(range)")
        }
        return last + 1
    }

    // MARK: - 元数据获取与核验

    /// 获取单个文件或目录的元数据
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

    // MARK: - 列举目录子项 (List Files)

    /// 列举指定父目录下的直接子项（自动处理分页）
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
            throw DriveError.invalidResponse(message: "远端目录列举不完整: \(parentId)")
        }
        return page
    }

    // MARK: - 文件下载 (Download)

    /// 下载文件正文并流式校验 SHA-256，原子落盘到目标路径
    public func downloadFile(
        remoteId: String,
        destinationURL: URL,
        expectedSha256: String? = nil,
        onProgress: (@Sendable (Int64) -> Void)? = nil
    ) async throws {
        _ = try await downloadFileSafely(
            remoteId: remoteId, destinationURL: destinationURL, expectedSha256: expectedSha256,
            expectedDestination: LocalFileVersion.read(at: destinationURL), onProgress: onProgress
        )
    }

    @discardableResult
    func downloadFileSafely(
        remoteId: String,
        destinationURL: URL,
        expectedSha256: String?,
        expectedDestination: LocalFileVersion?,
        beforePublish: (@Sendable () async throws -> Void)? = nil,
        onProgress: (@Sendable (Int64) -> Void)? = nil
    ) async throws -> LocalFileVersion {
        guard try LocalFileVersion.read(at: destinationURL) == expectedDestination else {
            throw DriveError.fileModifiedDuringUpload(path: destinationURL.path)
        }
        let token = try await getValidToken()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files/\(remoteId)")!
        components.queryItems = [
            URLQueryItem(name: "alt", value: "media"),
            URLQueryItem(name: "supportsAllDrives", value: "true")
        ]

        var req = URLRequest(url: components.url!)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        await rateLimiter.acquire()
        let tempURL = destinationURL.deletingLastPathComponent()
            .appendingPathComponent(".tmp_\(UUID().uuidString)")
        var publishedSuccessfully = false
        defer {
            if !publishedSuccessfully {
                try? FileManager.default.removeItem(at: tempURL)
            }
        }

        let (asyncBytes, response) = try await session.bytes(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw DriveError.invalidResponse(message: "非 HTTP 响应")
        }
        if http.statusCode == 404 {
            throw DriveError.notFound(fileId: remoteId)
        }
        if !(200..<300).contains(http.statusCode) {
            throw DriveError.serverError(statusCode: http.statusCode, message: "下载失败")
        }

        FileManager.default.createFile(atPath: tempURL.path, contents: nil)
        let fileHandle = try FileHandle(forWritingTo: tempURL)
        defer { try? fileHandle.close() }

        var ctx = CC_SHA256_CTX()
        CC_SHA256_Init(&ctx)

        var buffer = [UInt8]()
        buffer.reserveCapacity(64 * 1024)

        for try await byte in asyncBytes {
            buffer.append(byte)
            if buffer.count >= 64 * 1024 {
                CC_SHA256_Update(&ctx, buffer, CC_LONG(buffer.count))
                try fileHandle.write(contentsOf: buffer)
                onProgress?(Int64(buffer.count))
                buffer.removeAll(keepingCapacity: true)
            }
        }
        if !buffer.isEmpty {
            CC_SHA256_Update(&ctx, buffer, CC_LONG(buffer.count))
            try fileHandle.write(contentsOf: buffer)
            onProgress?(Int64(buffer.count))
        }
        try fileHandle.close()

        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        CC_SHA256_Final(&digest, &ctx)
        let actualSha256 = digest.map { String(format: "%02x", $0) }.joined()

        if let expected = expectedSha256, actualSha256.caseInsensitiveCompare(expected) != .orderedSame {
            try? FileManager.default.removeItem(at: tempURL)
            throw DriveError.checksumMismatch(expected: expected, actual: actualSha256)
        }

        try await beforePublish?()
        let published = try LocalFilePublication.publish(tempURL, to: destinationURL, expected: expectedDestination)
        publishedSuccessfully = true
        return published
    }

    // MARK: - Changes 增量变更

    /// 获取当前最新起始 Changes Token
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

    /// 列举自 pageToken 之后发生的所有远端变更
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

    // MARK: - 元数据更新与重命名 (Rename / Move)

    /// 重命名或移动远端对象（文件或目录）
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

    // MARK: - 回收站操作 (Trash)

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

    /// 将远端对象移出回收站 (恢复)
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

    // MARK: - 私有辅助

    private func checkHTTPStatus(response: URLResponse, data: Data) throws {
        guard let http = response as? HTTPURLResponse else {
            throw DriveError.invalidResponse(message: "非 HTTP 响应")
        }
        if http.statusCode == 429 {
            let retryAfter = http.value(forHTTPHeaderField: "Retry-After").flatMap(Double.init)
            throw DriveError.rateLimited(retryAfter: retryAfter)
        }
        if http.statusCode == 403 {
            let detail = String(decoding: data, as: UTF8.self)
            if detail.contains("rateLimitExceeded") || detail.contains("userRateLimitExceeded") {
                throw DriveError.rateLimited(retryAfter: 1.0)
            }
            throw DriveError.serverError(statusCode: 403, message: detail)
        }
        if !(200..<300).contains(http.statusCode) {
            let detail = String(decoding: data, as: UTF8.self)
            throw DriveError.serverError(statusCode: http.statusCode, message: detail)
        }
    }
}

/// 兼容别名
public typealias DriveAPI = DriveClient
