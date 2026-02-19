#!/bin/bash

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CACHE_DIR="$PROJECT_ROOT/../.local/zig-cache"

mkdir -p "$CACHE_DIR"
export ZIG_GLOBAL_CACHE_DIR="$CACHE_DIR"

# Build the CLI binary
cd "$PROJECT_ROOT"
cargo build --release --package dsq-cli --bin dsq

