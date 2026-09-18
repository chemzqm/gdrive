// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "GDrive",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .library(name: "GDrive", targets: ["GDrive"]),
        .executable(name: "gdrive-auth", targets: ["GDriveAuth"]),
        .executable(name: "gdrive-bench", targets: ["GDriveBench"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
        .package(path: "/Users/chemzqm/lib/scanner")
    ],
    targets: [
        .target(
            name: "GDrive",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
                .product(name: "DirectoryScanner", package: "scanner")
            ],
            path: "Sources/GDrive",
            resources: [
                .copy("Storage/SQLite/schema.sql")
            ],
            swiftSettings: [
                .swiftLanguageMode(.v6)
            ]
        ),
        .executableTarget(
            name: "GDriveAuth",
            dependencies: [
                "GDrive",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/GDriveAuth",
            swiftSettings: [
                .swiftLanguageMode(.v6)
            ]
        ),
        .executableTarget(
            name: "GDriveBench",
            dependencies: [
                "GDrive",
                .product(name: "DirectoryScanner", package: "scanner"),
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/GDriveBench",
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
