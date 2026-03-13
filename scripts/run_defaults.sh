#!/usr/bin/env bash

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

SOURCE_DIRS=(
  "$HOME/Pictures/Import-Temp-Folder"
)

DEST_DIR="$HOME/Pictures/darktable-archive"

if [[ -x "/Applications/darktable.app/Contents/MacOS/darktable-cli" ]]; then
  DARKTABLE_CLI="/Applications/darktable.app/Contents/MacOS/darktable-cli"
elif command -v darktable-cli >/dev/null 2>&1; then
  DARKTABLE_CLI="$(command -v darktable-cli)"
else
  DARKTABLE_CLI=""
fi

DARKTABLE_CLI_ARGS=(
  # "--style" "my_style"
  # "--out-ext" "tif"
)

source "$PROJECT_ROOT/scripts/_run_ingest.sh"
