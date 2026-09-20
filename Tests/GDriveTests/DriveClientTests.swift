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
        let rootID = try #require(
            authData.rootID,
            "Live Drive tests require rootID in ~/.gdrive/auth.json; configure a disposable test root before running."
        )

        let client = DriveClient(auth: auth, requestsPerSecond: nil)

        // 1. prefetch 2 a ID:1 directory for testing,1 files for testing
        let ids = try await client.generateIds(count: 2)
        #expect(ids.count == 2)
        let folderId = ids[0]
        let fileId = ids[1]

        let folderName = "test_dir_\(UUID().uuidString.prefix(8))"
        print("Create test directory: \(folderName) (ID: \(folderId))")
        let folder = try await client.createDirectory(name: folderName, parentId: rootID, remoteId: folderId)
        #expect(folder.id == folderId)
        #expect(folder.name == folderName)
        #expect(folder.isDirectory == true)

        // 2. Upload a small file and verify it SHA-256
        let fileName = "hello.txt"
        let fileContent = Data("Hello Google Drive from GDrive client at \(Date())!\n".utf8)

        var ctx = CC_SHA256_CTX()
        CC_SHA256_Init(&ctx)
        _ = fileContent.withUnsafeBytes { ptr in
            CC_SHA256_Update(&ctx, ptr.baseAddress, CC_LONG(fileContent.count))
        }
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        CC_SHA256_Final(&digest, &ctx)
        let expectedSha256 = digest.map { String(format: "%02x", $0) }.joined()

        print("Upload test files: \(fileName) (ID: \(fileId), SHA: \(expectedSha256))")
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

        // 3. Cleanup: Put the test directory into the recycle bin
        print("Clean test directory: \(folderId)")
        try await client.trash(remoteId: folderId)
        print("✅ DriveClient Directory creation and Multipart Upload end-to-end verification successful")
    }
}
