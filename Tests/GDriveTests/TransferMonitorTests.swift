import Foundation
import Testing
@testable import GDrive

@Suite("TransferMonitor Tests")
struct TransferMonitorTests {
    @Test("TransferMonitor tracks queue, active items and calculates transfer speeds")
    func testTransferMonitorQueueAndSpeed() async throws {
        let monitor = TransferMonitor()

        // 1. 初始状态快照为空
        let initialSnapshot = monitor.getSnapshot()
        #expect(initialSnapshot.activeUploads.isEmpty)
        #expect(initialSnapshot.activeDownloads.isEmpty)
        #expect(initialSnapshot.queuedUploads.isEmpty)
        #expect(initialSnapshot.queuedDownloads.isEmpty)
        #expect(initialSnapshot.uploadSpeedBytesPerSecond == 0.0)
        #expect(initialSnapshot.downloadSpeedBytesPerSecond == 0.0)

        // 2. 将文件加入上传与下载等待队列
        monitor.enqueueUpload(id: "up_1", name: "upload1.bin", totalBytes: 10 * 1024 * 1024)
        monitor.enqueueDownload(id: "down_1", name: "download1.bin", totalBytes: 5 * 1024 * 1024)

        monitor.refreshSnapshot()
        let queuedSnapshot = monitor.getSnapshot()
        #expect(queuedSnapshot.queuedUploads.count == 1)
        #expect(queuedSnapshot.queuedUploads[0].fileId == "up_1")
        #expect(queuedSnapshot.queuedUploads[0].name == "upload1.bin")
        #expect(queuedSnapshot.queuedDownloads.count == 1)
        #expect(queuedSnapshot.queuedDownloads[0].fileId == "down_1")
        #expect(queuedSnapshot.queuedDownloads[0].name == "download1.bin")

        // 3. 开始上传与下载任务（从队列移入 active）
        monitor.startUpload(id: "up_1", name: "upload1.bin", totalBytes: 10 * 1024 * 1024)
        monitor.startDownload(id: "down_1", name: "download1.bin", totalBytes: 5 * 1024 * 1024)

        monitor.refreshSnapshot()
        let activeSnapshot = monitor.getSnapshot()
        #expect(activeSnapshot.queuedUploads.isEmpty)
        #expect(activeSnapshot.queuedDownloads.isEmpty)
        #expect(activeSnapshot.activeUploads.count == 1)
        #expect(activeSnapshot.activeUploads[0].fileId == "up_1")
        #expect(activeSnapshot.activeDownloads.count == 1)
        #expect(activeSnapshot.activeDownloads[0].fileId == "down_1")

        // 4. 模拟传输数据与滑动速率计算 (等待 > 100ms 模拟字节流)
        try await Task.sleep(nanoseconds: 150_000_000) // 150ms
        monitor.reportUploadProgress(id: "up_1", additionalBytes: 2 * 1024 * 1024) // 2MB
        monitor.reportDownloadProgress(id: "down_1", additionalBytes: 1 * 1024 * 1024) // 1MB

        monitor.refreshSnapshot()
        let progressSnapshot = monitor.getSnapshot()
        #expect(progressSnapshot.activeUploads[0].transferredBytes == 2 * 1024 * 1024)
        #expect(progressSnapshot.activeDownloads[0].transferredBytes == 1 * 1024 * 1024)
        #expect(progressSnapshot.uploadSpeedBytesPerSecond > 0)
        #expect(progressSnapshot.downloadSpeedBytesPerSecond > 0)

        // 5. 完成任务并清空
        monitor.finishUpload(id: "up_1")
        monitor.finishDownload(id: "down_1")

        monitor.refreshSnapshot()
        let finishedSnapshot = monitor.getSnapshot()
        #expect(finishedSnapshot.activeUploads.isEmpty)
        #expect(finishedSnapshot.activeDownloads.isEmpty)
    }

    @Test("TransferMonitor automatic 500ms periodic background refresh")
    func testAutomaticPeriodicRefresh() async throws {
        let monitor = TransferMonitor()
        monitor.enqueueUpload(id: "auto_1", name: "auto.bin", totalBytes: 1000)

        let initialTime = monitor.getSnapshot().refreshedAt

        // 等待后台 timer 周期自动刷新 (> 600ms)
        try await Task.sleep(nanoseconds: 650_000_000)

        let nextSnapshot = monitor.getSnapshot()
        #expect(nextSnapshot.queuedUploads.count == 1)
        #expect(nextSnapshot.refreshedAt > initialTime)
    }
}
