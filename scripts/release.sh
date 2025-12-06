#!/bin/bash

set -e


VERSION=$(grep '^version' Cargo.toml | sed 's/version = "\(.*\)"/\1/')

# Try to build static binaries with musl, fallback to regular build if it fails
echo "Building static binaries with musl..."
cargo install --locked cargo-zigbuild
cargo zigbuild --release --target x86_64-unknown-linux-musl
TARGET_DIR="target/x86_64-unknown-linux-musl/release"
DEB_TARGET="--target x86_64-unknown-linux-musl"
RPM_TARGET="--target x86_64-unknown-linux-musl"

# Build Debian package
echo "Building Debian package..."
cargo install cargo-deb 2>/dev/null || true
if [ -n "$DEB_TARGET" ]; then
    cargo deb $DEB_TARGET --no-build
else
    cargo deb --no-build
fi

# Build RPM package
echo "Building RPM package..."
cargo install cargo-generate-rpm 2>/dev/null || true
if [ -n "$RPM_TARGET" ]; then
    cargo generate-rpm $RPM_TARGET
else
    cargo generate-rpm
fi

# Create tar.gz
echo "Creating tar.gz package..."
mkdir -p release
cp $TARGET_DIR/dsq release/
tar -czf dsq-$VERSION.tar.gz -C release .

echo "Release packages created:"
echo "- dsq-$VERSION.tar.gz"

# Collect all release artifacts
RELEASE_FILES=("dsq-$VERSION.tar.gz")

# Check for Debian packages in appropriate directory
if [ -n "$DEB_TARGET" ]; then
    DEB_DIR="target/x86_64-unknown-linux-musl/debian"
else
    DEB_DIR="target/debian"
fi

if ls $DEB_DIR/*.deb 1> /dev/null 2>&1; then
  RELEASE_FILES+=($DEB_DIR/*.deb)
  echo "- $DEB_DIR/*.deb"
fi

# Check for RPM packages in appropriate directory
if [ -n "$RPM_TARGET" ]; then
    RPM_DIR="target/x86_64-unknown-linux-musl/generate-rpm"
else
    RPM_DIR="target/generate-rpm"
fi

if ls $RPM_DIR/*.rpm 1> /dev/null 2>&1; then
  RELEASE_FILES+=($RPM_DIR/*.rpm)
  echo "- $RPM_DIR/*.rpm"
fi

echo "- Published to crates.io"

# Create GitHub release
echo ""
echo "Creating GitHub release v$VERSION..."
gh release create "v$VERSION" \
  --title "v$VERSION" \
  --generate-notes \
  "${RELEASE_FILES[@]}"

echo ""
echo "GitHub release v$VERSION created successfully!"
