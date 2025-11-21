#!/bin/bash

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Get version from Cargo.toml
VERSION=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')

if [ -z "$VERSION" ]; then
    echo "Error: Could not extract version from Cargo.toml"
    exit 1
fi

TAG="v$VERSION"

echo "Releasing version $VERSION (tag: $TAG)"

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Error: You have uncommitted changes. Please commit or stash them first."
    exit 1
fi

# Check if tag already exists
if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "Error: Tag $TAG already exists"
    exit 1
fi

# Build release binary
echo "Building release binary..."
"$SCRIPT_DIR/build.sh"

# Run tests
echo "Running tests..."
cargo test --release

# Create and push tag
echo "Creating tag $TAG..."
git tag -a "$TAG" -m "Release $VERSION"

echo "Pushing tag to origin..."
git push origin "$TAG"

echo ""
echo "Release $VERSION complete!"
echo "GitHub Actions should now create the release at:"
echo "  https://github.com/durableprogramming/dsq/releases/tag/$TAG"
