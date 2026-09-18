// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "GDrive",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .library(name: "GDrive", targets: ["GDrive"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0")
    ],
    targets: [
        .target(
            name: "GDrive",
            dependencies: [
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/GDrive",
            resources: [
                .copy("Storage/SQLite/schema.sql")
            ],
            swiftSettings: [
                .swiftLanguageMode(.v6)
            ]
        ),
        .testTarget(
            name: "GDriveTests",
            dependencies: [
                "GDrive",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Tests/GDriveTests",
            swiftSettings: [
                .swiftLanguageMode(.v6)
            ]
        )
    ]
)
