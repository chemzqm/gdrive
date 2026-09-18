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
                print("✅ 凭据有效")
                print("Client ID: \(data.clientId)")
                if let rootID = data.rootID { print("Root ID: \(rootID)") }
                print("Access Token: \(token.prefix(12))...")
                if let exp = data.expiresAt { print("过期时间: \(exp)") }
            } else if CommandLine.arguments.contains("ids") || CommandLine.arguments.contains("generate-ids") {
                let count = CommandLine.arguments.dropFirst(2).first.flatMap(Int.init) ?? 10
                let api = DriveAPI(auth: auth)
                let ids = try await api.generateIds(count: count)
                print("✅ 成功获取 \(ids.count) 个 ID:")
                for id in ids.prefix(10) { print("  \(id)") }
                if ids.count > 10 { print("  ... (共 \(ids.count) 个)") }
            } else {
                try await auth.login()
                print("✅ 授权完成，Token 已保存至 ~/.gdrive/auth.json")
            }
        } catch {
            print("❌ 失败: \(error.localizedDescription)")
            exit(1)
        }
    }
}
