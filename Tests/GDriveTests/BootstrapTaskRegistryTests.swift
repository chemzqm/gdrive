import Foundation
import Testing
@testable import GDrive

@Suite("Bootstrap directory dependencies")
struct BootstrapTaskRegistryTests {
    @Test("A registered parent can be awaited before its creation finishes")
    func pendingParentSharesBothIDs() async throws {
        let registry = BootstrapTaskRegistry(root: .init(remoteID: "root", itemID: 1))
        let gate = AsyncSemaphore(count: 0)
        let task = Task<BootstrapDirectoryTarget, Error> {
            await gate.wait()
            return .init(remoteID: "parent", itemID: 42)
        }
        registry.registerDirectoryTask(task, for: "parent")
        let dependency = try registry.dependency(for: "parent")
        let children = (0..<8).map { _ in Task { try await dependency.value() } }
        gate.signal()
        for child in children {
            let target = try await child.value
            #expect(target.remoteID == "parent")
            #expect(target.itemID == 42)
        }
        await registry.waitForAll()
    }

    @Test("Missing parents fail and parent failures propagate through descendants")
    func failuresPropagate() async throws {
        enum Failure: Error { case parent }
        let registry = BootstrapTaskRegistry(root: .init(remoteID: "root", itemID: 1))
        #expect(throws: BootstrapDirectoryDependencyError.self) {
            _ = try registry.dependency(for: "missing")
        }
        let task = Task<BootstrapDirectoryTarget, Error> { throw Failure.parent }
        registry.registerDirectoryTask(task, for: "parent")
        let parent = try registry.dependency(for: "parent")
        let child = Task<BootstrapDirectoryTarget, Error> { try await parent.value() }
        registry.registerDirectoryTask(child, for: "parent/child")
        let descendant = try registry.dependency(for: "parent/child")
        await #expect(throws: Failure.self) { _ = try await descendant.value() }
        let root = try await registry.dependency(for: "").value()
        #expect(root.itemID == 1)
        await registry.waitForAll()
    }

    @Test("Cancellation also reaches tasks registered after cancellation begins")
    func lateRegistrationIsCancelled() async throws {
        let registry = BootstrapTaskRegistry(root: .init(remoteID: "root", itemID: 1))
        registry.cancelAll()
        let task = Task<BootstrapDirectoryTarget, Error> {
            try await Task.sleep(for: .seconds(60))
            return .init(remoteID: "unexpected", itemID: 2)
        }
        registry.registerDirectoryTask(task, for: "parent")
        await #expect(throws: CancellationError.self) { _ = try await task.value }
        await registry.waitForAll()
    }
}
