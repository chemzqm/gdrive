import Foundation
import os

/// Routes URLProtocol callbacks to one test case's state, including parameterized cases.
/// URLSession callbacks do not inherit the test task's task-local values.
private enum TestHTTPRegistry {
    static let states = OSAllocatedUnfairLock(initialState: [String: any Sendable]())
    static let header = "X-GDrive-Test-Context"
}

final class TestHTTPContext<State: Sendable>: Sendable {
    let value: State
    private let id = UUID().uuidString

    init(_ value: State) {
        self.value = value
        TestHTTPRegistry.states.withLock { $0[id] = value }
    }

    deinit {
        TestHTTPRegistry.states.withLock { _ = $0.removeValue(forKey: id) }
    }

    func configure(_ configuration: URLSessionConfiguration) {
        var headers = configuration.httpAdditionalHeaders ?? [:]
        headers[TestHTTPRegistry.header] = id
        configuration.httpAdditionalHeaders = headers
    }

    static func value(for request: URLRequest) -> State? {
        guard let id = request.value(forHTTPHeaderField: TestHTTPRegistry.header) else { return nil }
        return TestHTTPRegistry.states.withLock { $0[id] as? State }
    }
}

final class TestRequestHandler: Sendable {
    typealias Handler = @Sendable (URLRequest) throws -> (HTTPURLResponse, Data)
    let handler = OSAllocatedUnfairLock<Handler?>(initialState: nil)

    var requestHandler: Handler? {
        get { handler.withLock { $0 } }
        set { handler.withLock { $0 = newValue } }
    }

    func setHandler(_ value: Handler?) {
        requestHandler = value
    }
}
