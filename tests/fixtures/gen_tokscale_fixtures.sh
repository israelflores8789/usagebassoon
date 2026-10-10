#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

# Usage from the repository root: edit VERSION, DAYS, and MODELS below, then run
#   bash tests/fixtures/gen_tokscale_fixtures.sh
# Raw captures go into ./tmp/ relative to the current working directory. Sanitize with
#   python3 tests/fixtures/sanitize_fixtures.py tmp/ --output-dir sanitized/
# Move previous captures out of tmp/ before rerunning; existing directories are not overwritten.
# Each run copies native settings and caches into a disposable profile with a fresh wiki.
set -euo pipefail
umask 077

# Edit these settings and arrays before running; their git history records the capture.
# Requires bunx and jq. Outputs are raw; sanitize the entire directory separately.
VERSION="4.18.0"
OUTPUT_DIR="./tmp"

# Dates are inclusive YYYY-MM-DD daily targets; array order does not matter.
DAYS=(
  "2026-08-22"
  "2026-08-23"
  "2026-08-24"
  "2026-08-25"
  "2026-08-26"
  "2026-08-27"
  "2026-08-28"
  "2026-08-29"
  "2026-08-30"
  "2026-08-31"
  "2026-09-01"
  "2026-09-02"
  "2026-09-03"
  "2026-09-04"
  "2026-09-05"
  "2026-09-06"
  "2026-09-07"
  "2026-09-08"
  "2026-09-09"
  "2026-09-10"
  "2026-09-14"
  "2026-09-15"
  "2026-09-16"
  "2026-09-17"
  "2026-09-18"
  "2026-09-19"
  "2026-09-20"
  "2026-09-21"
  "2026-09-22"
  "2026-09-23"
  "2026-09-24"
  "2026-09-25"
  "2026-09-26"
  "2026-09-27"
  "2026-09-28"
  "2026-09-29"
  "2026-09-30"
)

# Graph and full-history reports cover the earliest through latest configured day.
if [[ ${#DAYS[@]} -eq 0 ]]; then
  printf 'DAYS must contain at least one date.\n' >&2
  exit 2
fi
FIRST_DAY="${DAYS[0]}"
FINAL_DAY="$FIRST_DAY"
for day in "${DAYS[@]}"; do
  if [[ "$day" < "$FIRST_DAY" ]]; then
    FIRST_DAY="$day"
  fi
  if [[ "$day" > "$FINAL_DAY" ]]; then
    FINAL_DAY="$day"
  fi
done

# Pricing is observed now, once per listed model, without forcing a provider.
MODELS=(
  "gemini-3.7-flash"
  "gemini-3.8-flash"
  "gpt-5.6-luna"
  "gpt-5.6-terra"
  "gpt-6-luna"
  "gpt-6-sol"
  "gpt-6.1-sol"
)

if [[ $# -ne 0 ]]; then
  printf 'Configure VERSION, OUTPUT_DIR, DAYS, and MODELS in the script; no arguments are accepted.\n' >&2
  exit 2
fi

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
  printf 'VERSION must be an exact tokscale version, including any prerelease suffix.\n' >&2
  exit 2
fi

for dependency in bunx jq; do
  if ! command -v "$dependency" > /dev/null; then
    printf 'Required command is unavailable: %s\n' "$dependency" >&2
    exit 1
  fi
done

# UTC keeps date-filtered commands and capture filenames on the same day boundary.
export TZ=UTC

# A new destination avoids overwriting previously captured or committed fixtures.
if [[ -e "$OUTPUT_DIR" || -L "$OUTPUT_DIR" ]]; then
  printf 'Output already exists; choose a new OUTPUT_DIR: %s\n' "$OUTPUT_DIR" >&2
  exit 1
fi

# Resolve the native profile before overriding it for every capture subprocess.
if [[ -n "${TOKSCALE_CONFIG_DIR:-}" ]]; then
  native_profile="$TOKSCALE_CONFIG_DIR"
else
  case "$(uname -s)" in
    Darwin) native_profile="${HOME:?}/.config/tokscale" ;;
    MINGW*|MSYS*|CYGWIN*)
      native_profile="${APPDATA:?APPDATA is required to resolve the native Tokscale profile}/tokscale"
      if command -v cygpath > /dev/null; then
        native_profile="$(cygpath -u "$native_profile")"
      fi
      ;;
    *) native_profile="${XDG_CONFIG_HOME:-${HOME:?}/.config}/tokscale" ;;
  esac
fi

profile="$(mktemp -d "${TMPDIR:-/tmp}/usagebassoon-tokscale-fixtures.XXXXXXXX")"
cleanup_profile() {
  local status=$?
  if ! rm -rf -- "$profile"; then
    printf 'Could not clean up the temporary Tokscale profile.\n' >&2
    if [[ "$status" -eq 0 ]]; then
      status=1
    fi
  fi
  exit "$status"
}
trap cleanup_profile EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for filename in settings.json custom-pricing.json; do
  if [[ -f "$native_profile/$filename" ]]; then
    cp "$native_profile/$filename" "$profile/$filename"
  fi
done
if [[ -d "$native_profile/cache" ]]; then
  # Dereference cache links so subprocesses cannot write through to native state.
  cp -RL "$native_profile/cache" "$profile/cache"
fi
export TOKSCALE_CONFIG_DIR="$profile"

mkdir -p "$(dirname "$OUTPUT_DIR")"
mkdir "$OUTPUT_DIR"

TOKSCALE=(bunx "tokscale@${VERSION}" --no-spinner)

capture() {
  local destination="$1"
  shift
  if [[ -e "$destination" || -L "$destination" ]]; then
    printf 'Duplicate fixture destination: %s\n' "$destination" >&2
    return 1
  fi
  printf '[+] Capturing %s\n' "$destination"
  # Failed commands retain .partial evidence rather than a completed JSON fixture.
  "${TOKSCALE[@]}" "$@" > "${destination}.partial"
  jq -e -s 'length == 1 and (.[0] | type == "object" or type == "array")' \
    "${destination}.partial" > /dev/null
  mv "${destination}.partial" "$destination"
}

capture "${OUTPUT_DIR}/golden-${FINAL_DAY}-tokscale-${VERSION}.graph.json" \
  graph --since "$FIRST_DAY" --until "$FINAL_DAY"
capture "${OUTPUT_DIR}/golden-${FINAL_DAY}-tokscale-${VERSION}.report-full-history.json" \
  report --json --no-summarize --since "$FIRST_DAY" --until "$FINAL_DAY"

for day in "${DAYS[@]}"; do
  prefix="${OUTPUT_DIR}/golden-${day}-tokscale-${VERSION}"
  capture "${prefix}.daily.json" \
    models --json --group-by client,session,model --since "$day" --until "$day"
  capture "${prefix}.report.json" \
    report --json --no-summarize --since "$day" --until "$day"
done

for model in "${MODELS[@]}"; do
  # URI encoding keeps provider/model IDs in one filename without collisions.
  model_filename="$(jq -rn --arg model "$model" '$model | @uri')"
  price_day="$(date -u +%Y-%m-%d)"
  capture "${OUTPUT_DIR}/golden-${price_day}-tokscale-${VERSION}.pricing.${model_filename}.json" \
    pricing --json -- "$model"
done

printf '[✔] Captured graph, full-history report, %s daily model/report pairs, and %s prices in %s\n' \
  "${#DAYS[@]}" "${#MODELS[@]}" "$OUTPUT_DIR"
