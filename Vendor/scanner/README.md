# DirectoryScanner source snapshot

This directory contains the `DirectoryScanner` library used by GDrive. It is
kept as a nested Swift package so a clean GDrive checkout can resolve and build
without an author-specific filesystem layout.

Only the library targets are included. Scanner command-line tools, benchmarks,
and its standalone test suite are maintained outside this repository.
