#!/bin/bash

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

CACHE_DIR="$PROJECT_ROOT/../.local/zig-cache"

mkdir -p "$CACHE_DIR"

export ZIG_GLOBAL_CACHE_DIR="$CACHE_DIR"
# Build the main package with bin feature (produces libdsq.so and dsq binary)
cargo build --release --features bin

