#!/bin/bash

# Comprehensive test script that runs unit tests and example tests

set -e  # Exit on any error

echo "Running all tests..."

echo "Running cargo test..."
cargo test --workspace

