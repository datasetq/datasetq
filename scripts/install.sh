#!/usr/bin/env bash

set -e

echo "Building dsq..."
cargo build --release -p dsq-cli --bin dsq

echo "Creating ~/.local/bin directory if it doesn't exist..."
mkdir -p ~/.local/bin

echo "Copying dsq binary to ~/.local/bin..."
cp target/release/dsq ~/.local/bin/

echo "Installation complete!"
echo "Make sure ~/.local/bin is in your PATH"

