import Foundation
import Logging

public enum GDriveLogComponent: String, Sendable, CaseIterable {
    case auth = "auth"
    case drive = "drive"
    case transfer = "transfer"
    case runtime = "runtime"
    case storage = "storage"
    case workspace = "workspace"
}

public struct GDriveLogger: Sendable {
    private let logger: Logger
    public let component: GDriveLogComponent

    public init(label: String = "gdrive", component: GDriveLogComponent = .workspace) {
        var base = Logger(label: label)
        base[metadataKey: "component"] = "\(component.rawValue)"
        self.logger = base
        self.component = component
    }

    private init(logger: Logger, component: GDriveLogComponent) {
        var scoped = logger
        scoped[metadataKey: "component"] = "\(component.rawValue)"
        self.logger = scoped
        self.component = component
    }

    public func scoped(component: GDriveLogComponent) -> GDriveLogger {
        GDriveLogger(logger: self.logger, component: component)
    }

    public static func noOp() -> GDriveLogger {
        var logger = Logger(label: "gdrive.noop")
        logger.logLevel = .critical
        return GDriveLogger(logger: logger, component: .workspace)
    }

    public func trace(_ message: @autoclosure () -> String) {
        logger.trace("\(message())")
    }

    public func debug(_ message: @autoclosure () -> String) {
        logger.debug("\(message())")
    }

    public func info(_ message: @autoclosure () -> String) {
        logger.info("\(message())")
    }

    public func notice(_ message: @autoclosure () -> String) {
        logger.notice("\(message())")
    }

    public func warning(_ message: @autoclosure () -> String) {
        logger.warning("\(message())")
    }

    public func error(_ message: @autoclosure () -> String) {
        logger.error("\(message())")
    }

    public func critical(_ message: @autoclosure () -> String) {
        logger.critical("\(message())")
    }
}
