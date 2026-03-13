#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="${0:A:h}"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd -- "$SCRIPT_DIR/.." && pwd)}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/logs}"
MODE="${MODE:-cli}"

if [[ -z "${SOURCE_DIRS+x}" ]]; then
  typeset -a SOURCE_DIRS=()
fi

if [[ -z "${DARKTABLE_CLI_ARGS+x}" ]]; then
  typeset -a DARKTABLE_CLI_ARGS=()
fi

if [[ $# -gt 0 ]]; then
  case "$1" in
    --guard)
      MODE="guard"
      shift
      ;;
    --cli)
      MODE="cli"
      shift
      ;;
  esac
fi

if [[ ${#SOURCE_DIRS[@]} -eq 0 ]]; then
  printf 'No SOURCE_DIRS configured in %s.\n' "$0" >&2
  exit 1
fi

if [[ -z "${DEST_DIR:-}" ]]; then
  printf 'DEST_DIR is not configured in %s.\n' "$0" >&2
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  printf 'Python executable not found: %s\n' "$PYTHON_BIN" >&2
  exit 1
fi

common_args=(
  "$PYTHON_BIN"
  "$PROJECT_ROOT/darktable_ingest_guard.py"
  "--dest" "$DEST_DIR"
  "--log-dir" "$LOG_DIR"
)

if [[ "$MODE" == "cli" ]]; then
  if [[ -z "${DARKTABLE_CLI:-}" ]]; then
    printf 'DARKTABLE_CLI is not configured. Use --guard or set DARKTABLE_CLI.\n' >&2
    exit 1
  fi
  common_args+=( "--darktable-cli" "$DARKTABLE_CLI" )
fi

processed=0
for source_dir in "${SOURCE_DIRS[@]}"; do
  if [[ ! -d "$source_dir" ]]; then
    printf 'Skipping missing source directory: %s\n' "$source_dir" >&2
    continue
  fi

  if [[ $processed -gt 0 ]]; then
    printf '\n'
  fi

  printf 'Running %s mode for source: %s\n' "$MODE" "$source_dir"

  cmd=( "${common_args[@]}" "--source" "$source_dir" "$@" )
  if [[ "$MODE" == "cli" && ${#DARKTABLE_CLI_ARGS[@]} -gt 0 ]]; then
    cmd+=( "--darktable-cli-args" "${DARKTABLE_CLI_ARGS[@]}" )
  fi

  "${cmd[@]}"
  processed=$((processed + 1))
done

if [[ $processed -eq 0 ]]; then
  printf 'No configured source directories were found.\n' >&2
  exit 1
fi
