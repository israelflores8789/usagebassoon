#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only
set -euo pipefail

# Configuration
VERSION="4.15.1"
OUTPUT_DIR="."

# Define your list of dates (YYYY-MM-DD)
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
)

# Optional: ensure destination directory exists
mkdir -p "$OUTPUT_DIR"

for day in "${DAYS[@]}"; do
  output_file="${OUTPUT_DIR}/golden-${day}-tokscale-${VERSION}.daily.json"
  echo "[+] Exporting tokscale data for ${day} -> ${output_file}..."

  # Pin tokscale version to avoid package cache drift
  bunx "tokscale@${VERSION}" models \
    --json \
    --group-by client,session,model \
    --since "${day}" \
    --until "${day}" > "${output_file}"
done

echo "[✔] Completed exports for ${#DAYS[@]} day(s)."
