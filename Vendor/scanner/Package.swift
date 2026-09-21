// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "scanner",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .library(
            name: "DirectoryScanner",
            targets: ["DirectoryScanner"]
        )
    ],
    targets: [
        .target(name: "CNameMatcher"),
        .target(
            name: "DirectoryScanner",
            dependencies: ["CNameMatcher"],
            path: "Sources/DirectoryScanner",
            swiftSettings: [
                .swiftLanguageMode(.v6)
            ]
        )
    ]
)
