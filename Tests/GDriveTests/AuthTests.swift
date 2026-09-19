import Foundation
import Testing
@testable import GDrive

@Suite("Auth Tests")
struct AuthTests {
    @Test("AuthData round trip")
    func testAuthData() throws {
        let auth = AuthData(
            clientId: "test_id",
            clientSecret: "test_secret",
            rootID: "test_root",
            scopes: ["openid"],
            accessToken: "access",
            refreshToken: "refresh",
            expiresAt: Date()
        )
        let data = try JSONEncoder().encode(auth)
        let decoded = try JSONDecoder().decode(AuthData.self, from: data)
        #expect(decoded.clientId == "test_id")
        #expect(decoded.rootID == "test_root")
        #expect(decoded.accessToken == "access")
    }

    @Test("Fetch 1000 IDs from Google Drive files.generateIds")
    func testGenerate1000Ids() async throws {
        let auth = try Auth()
        let api = DriveAPI(auth: auth)
        let ids = try await api.generateIds(count: 1000)
        #expect(ids.count == 1000)
        #expect(Set(ids).count == 1000)
        print("Retrieved 1000 Google Drive IDs: \(ids.prefix(3).joined(separator: ", ")) ... \(ids.suffix(3).joined(separator: ", "))")
    }
}
