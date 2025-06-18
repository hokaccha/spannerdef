#!/bin/bash

set -e

VERSION=${1:-"dev"}
BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "Building spannerdef version: $VERSION (built: $BUILD_DATE)"

mkdir -p dist

# macOS ARM64
echo "Building macOS ARM64..."
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build \
  -ldflags="-s -w -X main.version=$VERSION -X main.buildDate=$BUILD_DATE" \
  -o dist/spannerdef-$VERSION-darwin-arm64 \
  ./cmd/spannerdef

# macOS AMD64
echo "Building macOS AMD64..."
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build \
  -ldflags="-s -w -X main.version=$VERSION -X main.buildDate=$BUILD_DATE" \
  -o dist/spannerdef-$VERSION-darwin-amd64 \
  ./cmd/spannerdef

# Linux ARM64
echo "Building Linux ARM64..."
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -ldflags="-s -w -X main.version=$VERSION -X main.buildDate=$BUILD_DATE" \
  -o dist/spannerdef-$VERSION-linux-arm64 \
  ./cmd/spannerdef

# Linux AMD64
echo "Building Linux AMD64..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -ldflags="-s -w -X main.version=$VERSION -X main.buildDate=$BUILD_DATE" \
  -o dist/spannerdef-$VERSION-linux-amd64 \
  ./cmd/spannerdef

echo "Generating checksums..."
cd dist
sha256sum * > checksums.txt
cd ..

echo "Build complete! Binaries are in ./dist/"
ls -la dist/