#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run release scripts in order
"$SCRIPT_DIR/release/build_cli.sh"
"$SCRIPT_DIR/release/build_libdsq.sh"
"$SCRIPT_DIR/release/build_wasm.sh"
"$SCRIPT_DIR/release/build_debian.sh"
"$SCRIPT_DIR/release/build_rpm.sh"
"$SCRIPT_DIR/release/create_tarball.sh"
"$SCRIPT_DIR/release/create_github_release.sh"
