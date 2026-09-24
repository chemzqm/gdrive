import CryptoKit
import Foundation
import Security

/// Only these fields are read from the production configuration file.
struct AuthConfiguration: Decodable {
    let clientId: String
    let rootID: String?
    let scopes: [String]?
}

struct AuthSecrets: Codable {
    let clientSecret: String?
    let accessToken: String?
    let refreshToken: String?
    let expiresAt: Date?

    init(_ data: AuthData) {
        clientSecret = data.clientSecret
        accessToken = data.accessToken
        refreshToken = data.refreshToken
        expiresAt = data.expiresAt
    }

    func apply(to data: inout AuthData) {
        data.clientSecret = clientSecret
        data.accessToken = accessToken
        data.refreshToken = refreshToken
        data.expiresAt = expiresAt
    }
}

/// Stores one complete OAuth secret record for each configuration path and client ID.
struct AuthKeychain {
    private static let service = "com.chemzqm.gdrive.oauth"
    private let account: String

    init(path: String, clientID: String) {
        let resolvedPath = URL(fileURLWithPath: path).standardizedFileURL.resolvingSymlinksInPath().path
        let identity = Data("\(resolvedPath)\u{0}\(clientID)".utf8)
        account = SHA256.hash(data: identity).map { String(format: "%02x", $0) }.joined()
    }

    func load() throws -> AuthSecrets? {
        var query = itemQuery
        query[kSecReturnData as String] = true
        query[kSecMatchLimit as String] = kSecMatchLimitOne
        var result: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        if status == errSecItemNotFound { return nil }
        try Self.check(status, operation: "read")
        guard let raw = result as? Data else {
            throw NSError(domain: "GDriveAuthKeychain", code: -1, userInfo: [
                NSLocalizedDescriptionKey: "Keychain returned invalid credential data"])
        }
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(AuthSecrets.self, from: raw)
    }

    func save(_ data: AuthData) throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let raw = try encoder.encode(AuthSecrets(data))
        let update: [String: Any] = [kSecValueData as String: raw]
        let status = SecItemUpdate(itemQuery as CFDictionary, update as CFDictionary)
        if status == errSecSuccess { return }
        if status != errSecItemNotFound {
            try Self.check(status, operation: "update")
        }

        var add = itemQuery
        add[kSecValueData as String] = raw
        add[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        let addStatus = SecItemAdd(add as CFDictionary, nil)
        if addStatus == errSecDuplicateItem {
            try Self.check(SecItemUpdate(itemQuery as CFDictionary, update as CFDictionary),
                           operation: "update")
        } else {
            try Self.check(addStatus, operation: "save")
        }
    }

    private var itemQuery: [String: Any] {
        [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: Self.service,
            kSecAttrAccount as String: account,
            kSecAttrSynchronizable as String: false,
            kSecUseDataProtectionKeychain as String: true
        ]
    }

    private static func check(_ status: OSStatus, operation: String) throws {
        guard status == errSecSuccess else {
            throw NSError(domain: "GDriveAuthKeychain", code: Int(status), userInfo: [
                NSLocalizedDescriptionKey: "Could not \(operation) Google Drive credentials in Keychain (\(status))"])
        }
    }
}
