.PHONY: all build debug clean test bench upload

all: build

build:
	swift build -c release

debug:
	swift build

clean:
	swift package clean
	rm -rf .build

test:
	swift test

bench:
	swift run -c release gdrive-bench

upload:
	swift run -c release gdrive-upload

