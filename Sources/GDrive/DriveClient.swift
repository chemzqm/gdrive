import Foundation
import CommonCrypto
import Logging

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
            return "本地文件在上传中途被修改: \(path)"
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
    private let session: URLSession
    private let logger = Logger(label: "gdrive.client")

    public static let fields = "id,name,mimeType,parents,size,sha256Checksum,version,trashed"

    public init(auth: Auth, session: URLSession = .shared) {
        self.auth = auth
        self.session = session
    }

    // MARK: - 预分配 ID

    /// 批量预取服务器认可的 ID
    public func generateIds(count: Int = 100, space: String = "drive") async throws -> [String] {
        var remaining = count
        var result: [String] = []

        while remaining > 0 {
            let batch = min(remaining, 1000)
            let token = try await auth.token()
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

            let (data, response) = try await session.data(for: req)
            try checkHTTPStatus(response: response, data: data)

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
        let token = try await auth.token()
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

        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw DriveError.invalidResponse(message: "非 HTTP 响应")
        }

        // 409 冲突：预生成 ID 重试或并发已被创建，核验证实对象
        if http.statusCode == 409 {
            return try await verifyExistingFolder(remoteId: remoteId, expectedName: name, expectedParentId: parentId)
        }

        try checkHTTPStatus(response: response, data: data)
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

    // MARK: - 小文件 Multipart 上传 (≤ 8MB)

    /// 使用 Multipart/related 一步上传文件（不落临时磁盘，正文直接发送并校验校验和）
    public func uploadMultipart(
        name: String,
        parentId: String,
        remoteId: String,
        mimeType: String = "application/octet-stream",
        content: Data,
        expectedSha256: String
    ) async throws -> DriveFile {
        let token = try await auth.token()
        var components = URLComponents(string: "https://www.googleapis.com/upload/drive/v3/files")!
        components.queryItems = [
            URLQueryItem(name: "uploadType", value: "multipart"),
            URLQueryItem(name: "fields", value: Self.fields)
        ]

        let boundary = "-------GDriveBoundary\(UUID().uuidString.replacingOccurrences(of: "-", with: ""))"
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
        body.append("--\(boundary)\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".data(using: .utf8)!)
        body.append(metadataData)
        body.append("\r\n--\(boundary)\r\nContent-Type: \(mimeType)\r\n\r\n".data(using: .utf8)!)
        body.append(content)
        body.append("\r\n--\(boundary)--\r\n".data(using: .utf8)!)

        req.httpBody = body

        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw DriveError.invalidResponse(message: "非 HTTP 响应")
        }

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

        try checkHTTPStatus(response: response, data: data)
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

    /// 更新已存在文件的正文内容（≤ 8MB）
    public func updateMultipart(
        remoteId: String,
        mimeType: String = "application/octet-stream",
        content: Data,
        expectedSha256: String
    ) async throws -> DriveFile {
        let token = try await auth.token()
        var components = URLComponents(string: "https://www.googleapis.com/upload/drive/v3/files/\(remoteId)")!
        components.queryItems = [
            URLQueryItem(name: "uploadType", value: "media"),
            URLQueryItem(name: "fields", value: Self.fields)
        ]

        var req = URLRequest(url: components.url!)
        req.httpMethod = "PATCH"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue(mimeType, forHTTPHeaderField: "Content-Type")
        req.httpBody = content

        let (data, response) = try await session.data(for: req)
        try checkHTTPStatus(response: response, data: data)
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

    /// 发起大文件已存在文件的 Resumable 内容更新会话 (> 8MB)
    public func initiateResumableUpdate(
        remoteId: String,
        totalBytes: Int64,
        mimeType: String = "application/octet-stream"
    ) async throws -> URL {
        let token = try await auth.token()
        var components = URLComponents(string: "https://www.googleapis.com/upload/drive/v3/files/\(remoteId)")!
        components.queryItems = [
            URLQueryItem(name: "uploadType", value: "resumable"),
            URLQueryItem(name: "fields", value: Self.fields)
        ]

        var req = URLRequest(url: components.url!)
        req.httpMethod = "PATCH"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue(mimeType, forHTTPHeaderField: "X-Upload-Content-Type")
        req.setValue(String(totalBytes), forHTTPHeaderField: "X-Upload-Content-Length")

        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw DriveError.invalidResponse(message: "非 HTTP 响应")
        }
        guard http.statusCode == 200, let location = http.value(forHTTPHeaderField: "Location"), let sessionURL = URL(string: location) else {
            let detail = String(decoding: data, as: UTF8.self)
            throw DriveError.serverError(statusCode: http.statusCode, message: "创建 Resumable 更新会话失败: \(detail)")
        }

        return sessionURL
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
        let token = try await auth.token()
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

        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw DriveError.invalidResponse(message: "非 HTTP 响应")
        }
        guard http.statusCode == 200, let location = http.value(forHTTPHeaderField: "Location"), let sessionURL = URL(string: location) else {
            let detail = String(decoding: data, as: UTF8.self)
            throw DriveError.serverError(statusCode: http.statusCode, message: "创建 Resumable 会话失败: \(detail)")
        }

        return sessionURL
    }

    /// 向已有的 Resumable 会话发送分块数据
    /// - Returns: 若上传完成则返回 DriveFile，若尚有剩余未完成分块则返回 nil
    public func uploadResumableChunk(
        sessionURL: URL,
        chunkData: Data,
        offset: Int64,
        totalBytes: Int64
    ) async throws -> DriveFile? {
        let chunkEnd = offset + Int64(chunkData.count) - 1
        var req = URLRequest(url: sessionURL)
        req.httpMethod = "PUT"
        req.setValue("bytes \(offset)-\(chunkEnd)/\(totalBytes)", forHTTPHeaderField: "Content-Range")
        req.setValue(String(chunkData.count), forHTTPHeaderField: "Content-Length")
        req.httpBody = chunkData

        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw DriveError.invalidResponse(message: "非 HTTP 响应")
        }

        if http.statusCode == 308 {
            // 分块已成功接收，后续仍需继续上传
            return nil
        } else if http.statusCode == 200 || http.statusCode == 201 {
            // 全部分块完成，返回最终对象
            return try JSONDecoder().decode(DriveFile.self, from: data)
        } else {
            let detail = String(decoding: data, as: UTF8.self)
            throw DriveError.serverError(statusCode: http.statusCode, message: "分块上传失败: \(detail)")
        }
    }

    /// 查询 Resumable 会话的已确认断点偏移量
    public func queryResumableOffset(sessionURL: URL, totalBytes: Int64) async throws -> Int64 {
        var req = URLRequest(url: sessionURL)
        req.httpMethod = "PUT"
        req.setValue("bytes */\(totalBytes)", forHTTPHeaderField: "Content-Range")
        req.setValue("0", forHTTPHeaderField: "Content-Length")

        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw DriveError.invalidResponse(message: "非 HTTP 响应")
        }

        if http.statusCode == 308 {
            guard let range = http.value(forHTTPHeaderField: "Range") else {
                return 0
            }
            // 格式: bytes=0-4194303
            if let lastStr = range.split(separator: "-").last, let last = Int64(lastStr) {
                return last + 1
            }
            return 0
        } else if http.statusCode == 200 || http.statusCode == 201 {
            return totalBytes
        } else {
            let detail = String(decoding: data, as: UTF8.self)
            throw DriveError.serverError(statusCode: http.statusCode, message: "查询会话状态失败: \(detail)")
        }
    }

    // MARK: - 元数据获取与核验

    /// 获取单个文件或目录的元数据
    public func getFile(remoteId: String) async throws -> DriveFile {
        let token = try await auth.token()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files/\(remoteId)")!
        components.queryItems = [
            URLQueryItem(name: "fields", value: Self.fields),
            URLQueryItem(name: "supportsAllDrives", value: "true")
        ]

        var req = URLRequest(url: components.url!)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw DriveError.invalidResponse(message: "非 HTTP 响应")
        }
        if http.statusCode == 404 {
            throw DriveError.notFound(fileId: remoteId)
        }
        try checkHTTPStatus(response: response, data: data)
        return try JSONDecoder().decode(DriveFile.self, from: data)
    }

    // MARK: - 列举目录子项 (List Files)

    /// 列举指定父目录下的直接子项（自动处理分页）
    public func listChildren(parentId: String) async throws -> [DriveFile] {
        var items: [DriveFile] = []
        var pageToken: String?

        while true {
            let token = try await auth.token()
            var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files")!
            var queryItems = [
                URLQueryItem(name: "q", value: "'\(parentId)' in parents and trashed = false"),
                URLQueryItem(name: "fields", value: "nextPageToken,files(\(Self.fields))"),
                URLQueryItem(name: "pageSize", value: "1000"),
                URLQueryItem(name: "supportsAllDrives", value: "true"),
                URLQueryItem(name: "includeItemsFromAllDrives", value: "true")
            ]
            if let pageToken {
                queryItems.append(URLQueryItem(name: "pageToken", value: pageToken))
            }
            components.queryItems = queryItems

            var req = URLRequest(url: components.url!)
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

            let (data, response) = try await session.data(for: req)
            try checkHTTPStatus(response: response, data: data)

            struct ListFilesResponse: Decodable {
                let nextPageToken: String?
                let files: [DriveFile]
            }

            let decoded = try JSONDecoder().decode(ListFilesResponse.self, from: data)
            items.append(contentsOf: decoded.files)

            if let next = decoded.nextPageToken, !next.isEmpty {
                pageToken = next
            } else {
                break
            }
        }

        return items
    }

    // MARK: - 文件下载 (Download)

    /// 下载文件正文并流式校验 SHA-256，原子落盘到目标路径
    public func downloadFile(
        remoteId: String,
        destinationURL: URL,
        expectedSha256: String? = nil
    ) async throws {
        let token = try await auth.token()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/files/\(remoteId)")!
        components.queryItems = [
            URLQueryItem(name: "alt", value: "media"),
            URLQueryItem(name: "supportsAllDrives", value: "true")
        ]

        var req = URLRequest(url: components.url!)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let tempURL = destinationURL.deletingLastPathComponent()
            .appendingPathComponent(".tmp_\(UUID().uuidString)")

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

        var ctx = CC_SHA256_CTX()
        CC_SHA256_Init(&ctx)

        var buffer = [UInt8]()
        buffer.reserveCapacity(64 * 1024)

        for try await byte in asyncBytes {
            buffer.append(byte)
            if buffer.count >= 64 * 1024 {
                CC_SHA256_Update(&ctx, buffer, CC_LONG(buffer.count))
                try fileHandle.write(contentsOf: buffer)
                buffer.removeAll(keepingCapacity: true)
            }
        }
        if !buffer.isEmpty {
            CC_SHA256_Update(&ctx, buffer, CC_LONG(buffer.count))
            try fileHandle.write(contentsOf: buffer)
        }
        try fileHandle.close()

        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        CC_SHA256_Final(&digest, &ctx)
        let actualSha256 = digest.map { String(format: "%02x", $0) }.joined()

        if let expected = expectedSha256, actualSha256.caseInsensitiveCompare(expected) != .orderedSame {
            try? FileManager.default.removeItem(at: tempURL)
            throw DriveError.checksumMismatch(expected: expected, actual: actualSha256)
        }

        // 原子替换目标路径（优先移动旧文件至废纸篓）
        if FileManager.default.fileExists(atPath: destinationURL.path) {
            var trashURL: NSURL?
            if (try? FileManager.default.trashItem(at: destinationURL, resultingItemURL: &trashURL)) != nil {
                try FileManager.default.moveItem(at: tempURL, to: destinationURL)
            } else {
                _ = try FileManager.default.replaceItemAt(destinationURL, withItemAt: tempURL)
            }
        } else {
            try FileManager.default.moveItem(at: tempURL, to: destinationURL)
        }
    }

    // MARK: - Changes 增量变更

    /// 获取当前最新起始 Changes Token
    public func getStartPageToken() async throws -> String {
        let token = try await auth.token()
        var components = URLComponents(string: "https://www.googleapis.com/drive/v3/changes/startPageToken")!
        components.queryItems = [
            URLQueryItem(name: "supportsAllDrives", value: "true")
        ]

        var req = URLRequest(url: components.url!)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await session.data(for: req)
        try checkHTTPStatus(response: response, data: data)

        struct StartTokenResponse: Decodable {
            let startPageToken: String
        }
        let decoded = try JSONDecoder().decode(StartTokenResponse.self, from: data)
        return decoded.startPageToken
    }

    /// 列举自 pageToken 之后发生的所有远端变更
    public func listChanges(pageToken: String, pageSize: Int = 1000) async throws -> DriveChangesPage {
        let token = try await auth.token()
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

        let (data, response) = try await session.data(for: req)
        try checkHTTPStatus(response: response, data: data)

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
        let token = try await auth.token()
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

        let (data, response) = try await session.data(for: req)
        try checkHTTPStatus(response: response, data: data)
        return try JSONDecoder().decode(DriveFile.self, from: data)
    }

    // MARK: - 回收站操作 (Trash)

    /// 将远端对象移至回收站
    public func trash(remoteId: String) async throws {
        let token = try await auth.token()
        let url = URL(string: "https://www.googleapis.com/drive/v3/files/\(remoteId)")!

        var req = URLRequest(url: url)
        req.httpMethod = "PATCH"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")

        let body = ["trashed": true]
        req.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await session.data(for: req)
        try checkHTTPStatus(response: response, data: data)
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
