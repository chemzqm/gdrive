import Foundation
import Testing
@testable import GDrive

/// Explicit opt-in: operates only on a newly allocated file in the configured root.
@Suite("Drive conditional write contract", .enabled(if: ProcessInfo.processInfo.environment["GDRIVE_CONDITIONAL_CONTRACT"] == "1"))
struct DriveConditionalWriteContractTests {
    @Test("Real Drive rejects stale media writes and accepts a matching ETag")
    func mediaPreconditions() async throws {
        let auth = try Auth()
        let config = await auth.authData()
        let root = try #require(config.rootID)
        let client = DriveClient(auth: auth)
        let id = try #require(try await client.generateIds(count: 1).first)
        let initial = Data("A11 original".utf8)
        _ = try await client.uploadMultipart(
            name: "a11-contract-\(UUID().uuidString).txt", parentId: root, remoteId: id,
            content: initial, expectedSha256: SyncEngine.computeSha256(of: initial)
        )
        do {
            let token = try await client.getValidToken()
            var read = URLRequest(url: URL(string: "https://www.googleapis.com/drive/v3/files/\(id)?fields=id,name,version,sha256Checksum")!)
            read.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
            let (_, response) = try await client.executeRequest(read)
            let etag = response.value(forHTTPHeaderField: "ETag")
            print("A11 contract: metadata GET ETag present = \(etag != nil)")

            var update = URLRequest(url: URL(string: "https://www.googleapis.com/upload/drive/v3/files/\(id)?uploadType=media&fields=id,name,sha256Checksum")!)
            update.httpMethod = "PATCH"
            update.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
            update.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
            update.setValue("\"a11-never-valid\"", forHTTPHeaderField: "If-Match")
            update.httpBody = Data("A11 concurrent overwrite".utf8)
            let (_, rejected) = try await client.executeRequest(update, maxRetries: 0, acceptableStatusCodes: [200, 412])
            print("A11 contract: invalid If-Match media PATCH status = \(rejected.statusCode)")
            #expect(rejected.statusCode == 412)
            let after = try await client.getFile(remoteId: id)
            #expect(after.sha256Checksum == SyncEngine.computeSha256(of: initial))

            if let etag, rejected.statusCode == 412 {
                update.setValue(etag, forHTTPHeaderField: "If-Match")
                let (_, accepted) = try await client.executeRequest(update, maxRetries: 0, acceptableStatusCodes: [200, 412])
                print("A11 contract: matching If-Match media PATCH status = \(accepted.statusCode)")
                #expect(accepted.statusCode == 200)
                let (_, stale) = try await client.executeRequest(update, maxRetries: 0, acceptableStatusCodes: [200, 412])
                print("A11 contract: stale If-Match media PATCH status = \(stale.statusCode)")
                #expect(stale.statusCode == 412)
            } else {
                Issue.record("Drive has not demonstrated usable conditional media writes")
            }
        } catch {
            try await client.trash(remoteId: id)
            throw error
        }
        try await client.trash(remoteId: id)
        print("A11 contract: isolated test file moved to Drive trash")
    }
}
