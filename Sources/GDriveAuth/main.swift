import Foundation
import GDrive

@main
struct GDriveAuthCLI {
    static func main() async {
        do {
            let auth = try Auth()
            if CommandLine.arguments.contains("status") {
                let token = try await auth.token()
                let data = await auth.authData()
                print("✅ Credentials are valid")
                print("Client ID: \(data.clientId)")
                if let rootID = data.rootID { print("Root ID: \(rootID)") }
                print("Access Token: \(token.prefix(12))...")
                if let exp = data.expiresAt { print("Expiration time: \(exp)") }
            } else if CommandLine.arguments.contains("ids") || CommandLine.arguments.contains("generate-ids") {
                let count = CommandLine.arguments.dropFirst(2).first.flatMap(Int.init) ?? 10
                let api = DriveClient(auth: auth)
                let ids = try await api.generateIds(count: count)
                print("✅ Retrieved \(ids.count) IDs:")
                for id in ids.prefix(10) { print("  \(id)") }
                if ids.count > 10 { print("  ... (\(ids.count) total)") }
            } else {
                try await auth.login()
                print("✅ Authorization complete. Token saved to ~/.gdrive/auth.json")
            }
        } catch {
            print("❌ Failed: \(error.localizedDescription)")
            exit(1)
        }
    }
}
