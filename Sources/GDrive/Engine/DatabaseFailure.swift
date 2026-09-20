import Foundation

enum DatabaseFailure {
    static func isSQLite(_ error: Error) -> Bool {
        let domain = (error as NSError).domain
        return domain == "SQLiteConnection" || domain == "SQLiteStatement"
    }
}
