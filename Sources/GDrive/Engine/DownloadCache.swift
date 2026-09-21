import Darwin
import Foundation
import os

final class DownloadCache: @unchecked Sendable {
    struct CachedDownload: Sendable {
        let url: URL
        let sha256: String
        let size: Int64
    }

    private let lock = OSAllocatedUnfairLock(initialState: [String: CachedDownload]())

    func store(
        _ download: DriveClient.VerifiedDownload,
        remoteID: String,
        temporaryDirectory: URL
    ) -> URL? {
        let cacheURL = temporaryDirectory.appendingPathComponent(".cache_\(UUID().uuidString)")
        guard rename(download.url.path, cacheURL.path) == 0 else {
            return nil
        }
        let cached = CachedDownload(url: cacheURL, sha256: download.sha256, size: download.size)
        let key = cacheKey(remoteID: remoteID, sha256: download.sha256)
        let old = lock.withLock { state -> CachedDownload? in
            let existing = state.removeValue(forKey: key)
            state[key] = cached
            return existing
        }
        if let old {
            try? FileManager.default.removeItem(at: old.url)
        }
        return cacheURL
    }

    func take(remoteID: String, sha256: String) -> CachedDownload? {
        let key = cacheKey(remoteID: remoteID, sha256: sha256)
        return lock.withLock { state in
            state.removeValue(forKey: key)
        }
    }

    func clear() {
        let items = lock.withLock { state -> [CachedDownload] in
            let all = Array(state.values)
            state.removeAll()
            return all
        }
        for item in items {
            try? FileManager.default.removeItem(at: item.url)
        }
    }

    private func cacheKey(remoteID: String, sha256: String) -> String {
        "\(remoteID):\(sha256.lowercased())"
    }
}
