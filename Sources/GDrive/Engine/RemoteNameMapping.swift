import Foundation

/// Conservatively reserves case/normalization-equivalent names even on case-sensitive volumes.
/// Keep this function stable: SQLite persists its output in an expression index.
enum RemoteNameMapping {
    static func key(_ name: String) -> String {
        name.folding(options: [.caseInsensitive], locale: Locale(identifier: "en_US_POSIX"))
            .precomposedStringWithCanonicalMapping
    }

    static func validate(_ name: String) throws {
        guard !name.isEmpty, name != ".", name != "..", !name.contains("/"),
              !name.contains("\0"), name.utf8.count <= 255 else {
            throw SyncEngineError.general("Remote name cannot be mapped safely to a local path: \(name.debugDescription)")
        }
    }

    static func validateSiblings(_ children: [DriveFile]) throws {
        var owners: [String: String] = [:]
        for file in children {
            try validate(file.name)
            let key = key(file.name)
            if let owner = owners[key], owner != file.id {
                throw SyncEngineError.general("Remote name conflict: \(file.name.debugDescription), fileIds=\(owner),\(file.id). Rename the remote item and try again.")
            }
            owners[key] = file.id
        }
    }

    static func validateDestination(_ destination: URL, root: URL) throws {
        let resolvedRoot = root.resolvingSymlinksInPath().standardizedFileURL.path
        let lexicalRoot = root.standardizedFileURL.path + "/"
        let lexical = destination.standardizedFileURL.path
        // Foundation leaves a nonexistent path unresolved, including symlinks in its
        // ancestors. Resolve the nearest existing ancestor before appending missing names.
        var ancestor = destination.deletingLastPathComponent().standardizedFileURL
        var missing = [destination.lastPathComponent]
        while ancestor.path.hasPrefix(lexicalRoot),
              (try? FileManager.default.attributesOfItem(atPath: ancestor.path)) == nil {
            missing.append(ancestor.lastPathComponent)
            ancestor.deleteLastPathComponent()
        }
        if let attrs = try? FileManager.default.attributesOfItem(atPath: ancestor.path),
           attrs[.type] as? FileAttributeType == .typeSymbolicLink, ancestor.path != root.standardizedFileURL.path {
            throw SyncEngineError.general("Remote parent directory maps to a symbolic link: \(ancestor.path)")
        }
        var resolvedURL = ancestor.resolvingSymlinksInPath()
        for name in missing.reversed() { resolvedURL.appendPathComponent(name) }
        let resolved = resolvedURL.standardizedFileURL.path
        guard lexical.hasPrefix(lexicalRoot), resolved.hasPrefix(resolvedRoot + "/"),
              resolved == resolvedRoot + "/" + String(lexical.dropFirst(lexicalRoot.count)) else {
            throw SyncEngineError.general("Remote path resolves outside the sync root: \(destination.path)")
        }
        if let attrs = try? FileManager.default.attributesOfItem(atPath: destination.path),
           attrs[.type] as? FileAttributeType == .typeSymbolicLink {
            throw SyncEngineError.general("Remote path maps to a symbolic link: \(destination.path)")
        }
    }
}
