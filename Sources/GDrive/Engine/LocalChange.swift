import Foundation

/// A concrete local filesystem observation supplied by a file watcher.
public enum LocalChange: Sendable, Hashable {
    case created(path: String, isDirectory: Bool)
    case modified(path: String, isDirectory: Bool)
    case deleted(path: String, isDirectory: Bool)
    // Keep the conventional public from/to spelling for watcher clients.
    // swiftlint:disable:next identifier_name
    case moved(from: String, to: String, isDirectory: Bool)

    var paths: [String] {
        switch self {
        case .created(let path, _), .modified(let path, _), .deleted(let path, _): [path]
        case .moved(let from, let destination, _): [from, destination]
        }
    }

    static func coalescing(_ changes: [LocalChange]) -> [LocalChange] {
        var result: [LocalChange] = []
        // Remove exact duplicate reports but preserve different concrete hints
        // for one path. An explicit modified event can require a SHA check
        // despite matching metadata, and every move link retains its old path.
        var seen = Set<LocalChange>()
        for change in changes.reversed() where seen.insert(change).inserted {
            result.append(change)
        }
        return result.reversed()
    }
}

/// Process-local state for watcher-triggered local work.
public struct PendingLocalChangeStatus: Sendable, Equatable {
    public let localRootPath: String
    public let isRunning: Bool
    public let pendingChangeCount: Int

    public init(localRootPath: String, isRunning: Bool, pendingChangeCount: Int) {
        self.localRootPath = localRootPath
        self.isRunning = isRunning
        self.pendingChangeCount = pendingChangeCount
    }
}
