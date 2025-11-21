#!/usr/bin/env bash
# Code coverage script using cargo-llvm-cov
# Install: cargo install cargo-llvm-cov

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Default output directory
COVERAGE_DIR="${COVERAGE_DIR:-target/coverage}"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
    --html          Generate HTML report (default)
    --lcov          Generate LCOV report
    --json          Generate JSON report
    --text          Print text summary to stdout
    --open          Open HTML report in browser
    --clean         Clean coverage data before running
    -h, --help      Show this help message

Environment variables:
    COVERAGE_DIR    Output directory (default: target/coverage)

Examples:
    $0 --html --open    # Generate HTML report and open in browser
    $0 --lcov           # Generate LCOV for CI integration
    $0 --text           # Quick text summary
EOF
}

FORMAT="html"
OPEN=false
CLEAN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --html)  FORMAT="html"; shift ;;
        --lcov)  FORMAT="lcov"; shift ;;
        --json)  FORMAT="json"; shift ;;
        --text)  FORMAT="text"; shift ;;
        --open)  OPEN=true; shift ;;
        --clean) CLEAN=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

# Check if cargo-llvm-cov is installed
if ! command -v cargo-llvm-cov &> /dev/null; then
    echo "cargo-llvm-cov not found. Install with: cargo install cargo-llvm-cov"
    exit 1
fi

# Check if llvm-tools-preview is installed
if ! rustup component list --installed | grep -q llvm-tools; then
    echo "llvm-tools-preview not found. Install with: rustup component add llvm-tools-preview"
    exit 1
fi

mkdir -p "$COVERAGE_DIR"

if [[ "$CLEAN" == true ]]; then
    echo "Cleaning coverage data..."
    cargo llvm-cov clean --workspace
fi

echo "Running tests with coverage..."

case $FORMAT in
    html)
        cargo llvm-cov --workspace --html --output-dir "$COVERAGE_DIR"
        echo "HTML report generated at: $COVERAGE_DIR/html/index.html"
        if [[ "$OPEN" == true ]]; then
            xdg-open "$COVERAGE_DIR/html/index.html" 2>/dev/null || open "$COVERAGE_DIR/html/index.html" 2>/dev/null || echo "Could not open browser"
        fi
        ;;
    lcov)
        cargo llvm-cov --workspace --lcov --output-path "$COVERAGE_DIR/lcov.info"
        echo "LCOV report generated at: $COVERAGE_DIR/lcov.info"
        ;;
    json)
        cargo llvm-cov --workspace --json --output-path "$COVERAGE_DIR/coverage.json"
        echo "JSON report generated at: $COVERAGE_DIR/coverage.json"
        ;;
    text)
        cargo llvm-cov --workspace
        ;;
esac
