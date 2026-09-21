import Foundation

enum DatabaseFailure {
    static func isSQLite(_ error: Error) -> Bool {
        if error is SQLiteError.Connection || error is SQLiteError.Statement { return true }
        let domain = (error as NSError).domain
        return domain == "SQLiteConnection" || domain == "SQLiteStatement"
    }
}
