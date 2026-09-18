.PHONY: all build debug clean test bench

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
	swift run gdrive-bench
