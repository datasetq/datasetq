# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- SECURITY.md file with security reporting process and guidelines
- CHANGELOG.md for tracking project changes
- Comprehensive security policy documentation

### Changed
- Refactored `unnest` function from array flattening to object flattening
- Standardized code formatting across crates
- Enhanced crate-level documentation

### Fixed
- Various code quality improvements and formatting fixes

## [0.1.0] - Initial Development

### Added
- Core DSQ functionality with jq-like syntax for structured data
- Support for multiple data formats: CSV, JSON, Parquet, Avro, Arrow
- DataFrame operations using Polars backend
- Filter and transformation capabilities
- Function library for data manipulation
- CLI interface for command-line usage
- WASM support for browser and WASI environments
- Buffer functions for data processing
- Comprehensive test suite
- Benchmarking infrastructure

### Architecture
- Workspace organization with multiple crates:
  - `dsq-core`: Core data processing engine
  - `dsq-shared`: Shared types and utilities
  - `dsq-parser`: Query language parser
  - `dsq-filter`: Filter operations and transformations
  - `dsq-functions`: Function library
  - `dsq-formats`: Format readers and writers
  - `dsq-cli`: Command-line interface
  - `dsq-io`: I/O operations

### Documentation
- README.md with usage examples and feature overview
- ARCHITECTURE.md describing system design
- API.md for API documentation
- CONFIGURATION.md for configuration options
- FORMATS.md detailing supported formats
- FUNCTIONS.md listing available functions
- WASM.md for WebAssembly usage
- CONTRIBUTING.md with contribution guidelines
- Individual crate documentation

[Unreleased]: https://github.com/durableprogramming/dsq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/durableprogramming/dsq/releases/tag/v0.1.0
