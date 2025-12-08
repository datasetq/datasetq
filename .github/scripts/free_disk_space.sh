#!/bin/bash

# Free up disk space on GitHub Actions runners
# Based on Apache Flink's approach

set -e

echo "============================================"
echo "Disk space before cleanup:"
df -h
echo "============================================"

# Remove large packages that are not needed for Rust builds
echo "Removing unnecessary packages..."

sudo apt-get remove -y \
    '^aspnetcore-.*' \
    '^dotnet-.*' \
    '^llvm-.*' \
    'php.*' \
    '^mongodb-.*' \
    '^mysql-.*' \
    azure-cli \
    google-chrome-stable \
    firefox \
    powershell \
    mono-devel \
    libgl1-mesa-dri \
    google-cloud-sdk \
    || true

sudo apt-get autoremove -y
sudo apt-get clean

# Remove large directories
echo "Removing large directories..."

sudo rm -rf /usr/share/dotnet
sudo rm -rf /usr/local/lib/android
sudo rm -rf /opt/ghc
sudo rm -rf /opt/hostedtoolcache/CodeQL
sudo rm -rf /usr/local/share/boost
sudo rm -rf "$AGENT_TOOLSDIRECTORY"

echo "============================================"
echo "Disk space after cleanup:"
df -h
echo "============================================"
