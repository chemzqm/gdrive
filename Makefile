.PHONY: all build debug clean test lint bench upload

all: build

build:
	swift build -c release

debug:
	swift build

clean:
	swift package clean
	rm -rf .build

test:
	GDRIVE_TESTING=1 swift test

lint:
	swiftlint lint --strict Sources Tests

bench:
	swift run -c release gdrive-bench

upload:
	swift run -c release gdrive-upload
