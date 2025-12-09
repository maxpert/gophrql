#!/usr/bin/env bash
set -euo pipefail

# clones or updates `tmp/prql` from upstream and runs the Go integration tests that compare
# against the upstream SQL snapshots.

REPO_URL="https://github.com/PRQL/prql"
TARGET_DIR="tmp/prql"

mkdir -p "$(dirname "$TARGET_DIR")"

if [ -d "$TARGET_DIR/.git" ]; then
  echo "Updating existing upstream checkout at $TARGET_DIR"
  git -C "$TARGET_DIR" fetch --all --prune
  git -C "$TARGET_DIR" reset --hard origin/main
else
  echo "Cloning upstream repository into $TARGET_DIR"
  git clone "$REPO_URL" "$TARGET_DIR"
fi

echo "Running Go integration suite"
env GOCACHE=/tmp/go-build go test ./...
