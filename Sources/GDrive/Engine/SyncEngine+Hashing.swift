import Darwin
import Foundation
import CommonCrypto
import Logging
import DirectoryScanner
import os

// Internal SyncEngine implementation split by synchronization phase.
extension SyncEngine {
    /// adopt 1MB Constant memory streaming file SHA-256 and byte length
    static func computeFileSha256(at url: URL) throws -> (sha256Hex: String, fileSize: Int64) {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var ctx = CC_SHA256_CTX()
        CC_SHA256_Init(&ctx)
        var totalSize: Int64 = 0
        let bufferSize = 1024 * 1024 // 1MB Streaming sharding to avoid large files occupying memory
        while let chunk = try handle.read(upToCount: bufferSize), !chunk.isEmpty {
            totalSize += Int64(chunk.count)
            _ = chunk.withUnsafeBytes { ptr in
                CC_SHA256_Update(&ctx, ptr.baseAddress, CC_LONG(chunk.count))
            }
        }
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        CC_SHA256_Final(&digest, &ctx)
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return (hex, totalSize)
    }

    /// Extremely fast calculations in memory Data of SHA-256 String (only for small files, time-consuming for a single time) ~10us)
    static func computeSha256(of data: Data) -> String {
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        data.withUnsafeBytes { ptr in
            _ = CC_SHA256(ptr.baseAddress, CC_LONG(data.count), &digest)
        }
        return digest.map { String(format: "%02x", $0) }.joined()
    }

}


