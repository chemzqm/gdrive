import Foundation
import Testing
import CommonCrypto
@testable import GDrive

@Suite("DriveClient Live Tests")
struct DriveClientTests {
    @Test("Folder creation and multipart upload with verification")
    func testFolderAndMultipartUpload() async throws {
        let auth = try Auth()
        let authData = await auth.authData()
        guard let rootID = authData.rootID else {
            print("未配置 rootID，跳过云端测试")
            return
        }

        let client = DriveClient(auth: auth)

        // 1. 预取 2 个 ID：1 个用于测试目录，1 个用于测试文件
        let ids = try await client.generateIds(count: 2)
        #expect(ids.count == 2)
        let folderId = ids[0]
        let fileId = ids[1]

        let folderName = "test_dir_\(UUID().uuidString.prefix(8))"
        print("创建测试目录: \(folderName) (ID: \(folderId))")
        let folder = try await client.createDirectory(name: folderName, parentId: rootID, remoteId: folderId)
        #expect(folder.id == folderId)
        #expect(folder.name == folderName)
        #expect(folder.isDirectory == true)

        // 2. 上传一个小文件并校验 SHA-256
        let fileName = "hello.txt"
        let fileContent = "Hello Google Drive from GDrive client at \(Date())!\n".data(using: .utf8)!

        var ctx = CC_SHA256_CTX()
        CC_SHA256_Init(&ctx)
        _ = fileContent.withUnsafeBytes { ptr in
            CC_SHA256_Update(&ctx, ptr.baseAddress, CC_LONG(fileContent.count))
        }
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        CC_SHA256_Final(&digest, &ctx)
        let expectedSha256 = digest.map { String(format: "%02x", $0) }.joined()

        print("上传测试文件: \(fileName) (ID: \(fileId), SHA: \(expectedSha256))")
        let uploadedFile = try await client.uploadMultipart(
            name: fileName,
            parentId: folderId,
            remoteId: fileId,
            mimeType: "text/plain",
            content: fileContent,
            expectedSha256: expectedSha256
        )

        #expect(uploadedFile.id == fileId)
        #expect(uploadedFile.name == fileName)
        #expect(uploadedFile.sizeBytes == Int64(fileContent.count))
        #expect(uploadedFile.sha256Checksum?.lowercased() == expectedSha256.lowercased())

        // 3. 清理：将测试目录放入回收站
        print("清理测试目录: \(folderId)")
        try await client.trash(remoteId: folderId)
        print("✅ DriveClient 目录创建与 Multipart 上传端到端校验成功")
    }
}
