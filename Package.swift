// swift-tools-version: 6.0
import PackageDescription

let isTesting = Context.environment["GDRIVE_TESTING"] == "1"

var products: [Product] = [
    .library(name: "GDrive", targets: ["GDrive"]),
    .executable(name: "gdrive-auth", targets: ["GDriveAuth"])
]

var targets: [Target] = [
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
        swiftSettings: isTesting
            ? [.swiftLanguageMode(.v6), .define("GDRIVE_TESTING")]
            : [.swiftLanguageMode(.v6)]
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
    )
]

if !isTesting {
    products += [
        .executable(name: "gdrive-bench", targets: ["GDriveBench"]),
        .executable(name: "gdrive-upload", targets: ["GDriveUploadBench"])
    ]
    targets += [
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
        .executableTarget(
            name: "GDriveUploadBench",
            dependencies: [
                "GDrive",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/GDriveUploadBench",
            swiftSettings: [
                .swiftLanguageMode(.v6)
            ]
        )
    ]
}

// `make test` enables this helper; regular builds do not need it.
if isTesting {
    targets.append(.executableTarget(
        name: "GDriveKillProcessTestHelper",
        dependencies: ["GDrive"],
        path: "Tests/GDriveKillProcessTestHelper",
        swiftSettings: [
            .swiftLanguageMode(.v6)
        ]
    ))
}

targets += [
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

let package = Package(
    name: "GDrive",
    platforms: [
        .macOS(.v14)
    ],
    products: products,
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
        .package(path: "Vendor/scanner")
    ],
    targets: targets
)
