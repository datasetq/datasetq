#!/bin/bash

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Build the main package with bin feature (produces libdsq.so and dsq binary)
cd "$PROJECT_ROOT"
cargo build --release --features bin

