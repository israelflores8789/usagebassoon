# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

# UsageBassoon justfile recipes

set shell := ["bash", "-euo", "pipefail", "-c"]

py := "uv run python"
pytest := "uv run pytest"
src_dir := "src"
test_dir := "tests"

install-dev:
    @echo "Installing core + development dependencies..."
    @uv sync --all-groups

install-motherduck:
    @echo "Installing motherduck 🦆..."
    @curl -s https://install.motherduck.com | sh


# ---- build & publish ----

# Remember to set the UV_PUBLISH_TOKEN environment variable.

build:
    @echo "Building the project..."
    rm -rf dist/
    uv build --no-sources

# Validate artifacts before any upload.
check-dist: build
    uv run twine check dist/*

publish-test: check-dist
    uv publish \
        --publish-url https://test.pypi.org/legacy/

publish *args: check-dist
    uv publish {{args}}


# ---- hygiene ----

spell:
    typos

spell-diff:
    typos --diff

spell-fix:
    typos --write-changes

test *args:
    {{pytest}} -v -s {{args}}

test-bq-live test-name reset="0":
    @mkdir -p .test_logs
    @bash -o pipefail -c '{ \
        echo "[just] $(date -u +%FT%TZ) starting pytest for {{test-name}}"; \
        PYTHONUNBUFFERED=1 USAGEBASSOON_BIGQUERY_LIVE=1 USAGEBASSOON_BIGQUERY_LIVE_RESET={{reset}} uv run pytest -vv \
        --tb=short \
        --color=no \
        "{{test-name}}" < /dev/null; \
    } 2>&1 | tee .test_logs/pytest-bq-live.log'

test-gcs-live test-name:
    @mkdir -p .test_logs
    @bash -o pipefail -c '{ \
        echo "[just] $(date -u +%FT%TZ) starting pytest for {{test-name}}"; \
        PYTHONUNBUFFERED=1 USAGEBASSOON_GCS_LIVE=1 uv run pytest -vv \
        --tb=short \
        --color=no \
        "{{test-name}}" < /dev/null; \
    } 2>&1 | tee .test_logs/pytest-gcs-live.log'

# Run tests with coverage reporting
coverage *args:
    {{pytest}} --cov=src --cov-report=term-missing {{args}}

lint:
    #!/usr/bin/env bash
    set -u
    status=0

    uv run ruff check {{src_dir}} {{test_dir}} || status=1
    uv run ruff format --check {{src_dir}} {{test_dir}} || status=1

    exit "$status"

lint-fix:
    uv run ruff check --fix {{src_dir}} {{test_dir}}
    uv run ruff format {{src_dir}} {{test_dir}}

typecheck:
    uv run pyrefly check {{src_dir}} {{test_dir}}

clean:
    rm -rf dist/ build/ .pytest_cache/ .mypy_cache/ .ruff_cache/
    find . -type d -name __pycache__ -prune -exec rm -rf {} +

check-justfile:
    just --fmt --check

check-license:
    #!/usr/bin/env bash
    uvx --from 'reuse[charset-normalizer]' reuse lint
    if [[ ! -f LICENSES/AGPL-3.0-only.txt ]]; then
        echo "LICENSES/AGPL-3.0-only.txt missing — downloading..."
        uvx --from 'reuse[charset-normalizer]' reuse download AGPL-3.0-only
    else
        echo "LICENSES/AGPL-3.0-only.txt present."
    fi


# ---- full CI gate ----

ci: test lint typecheck

# --- CD / release ---

release-build:
    uv build --no-sources

release-check:
    uv run twine check dist/*

release-test:
    uv publish --publish-url https://test.pypi.org/legacy/

# Rotate the changelog locally before opening the release PR.
release version:
    #!/usr/bin/env bash
    set -euo pipefail

    CHANGELOG="CHANGELOG.md"
    VERSION="{{ version }}"

    if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([A-Za-z0-9.-]*)$ ]]; then
        echo "error: version must look like 1.2.3 or 1.2.3rc1" >&2
        exit 1
    fi

    if [[ ! -f "$CHANGELOG" ]]; then
        echo "error: $CHANGELOG is missing" >&2
        exit 1
    fi

    if [[ -n "$(git status --porcelain)" ]]; then
        echo "error: working tree is dirty; commit or stash first" >&2
        exit 1
    fi

    unreleased_count="$(awk '$0 == "## [Unreleased]" { count += 1 } END { print count + 0 }' "$CHANGELOG")"
    if [[ "$unreleased_count" -ne 1 ]]; then
        echo "error: expected exactly one '## [Unreleased]' heading, found $unreleased_count" >&2
        exit 1
    fi

    if awk -v heading="## [$VERSION]" '
        index($0, heading) == 1 &&
          (length($0) == length(heading) ||
           substr($0, length(heading) + 1, 2) == " -") {
            found=1
            exit
        }
        END { exit !found }
    ' "$CHANGELOG"; then
        echo "error: $CHANGELOG already has a [$VERSION] section" >&2
        exit 1
    fi

    bullets="$(awk '
        $0 == "## [Unreleased]" { inside=1; next }
        inside && /^## \[/ { inside=0 }
        inside && /^[[:space:]]*[-*] / { count += 1 }
        END { print count + 0 }
    ' "$CHANGELOG")"
    if [[ "$bullets" -eq 0 ]]; then
        echo "error: [Unreleased] has no bullet entries; write release notes first" >&2
        exit 1
    fi

    today="$(date -u +%Y-%m-%d)"
    tmp="$(mktemp "${CHANGELOG}.tmp.XXXXXX")"
    trap 'rm -f "$tmp"' EXIT

    awk -v version="$VERSION" -v date="$today" '
        $0 == "## [Unreleased]" {
            print
            print ""
            print "## [" version "] - " date
            next
        }
        { print }
    ' "$CHANGELOG" > "$tmp"

    target="## [$VERSION] - $today"
    if ! awk -v target="$target" '$0 == target { found=1 } END { exit !found }' "$tmp"; then
        echo "error: changelog rewrite failed validation; file untouched" >&2
        exit 1
    fi

    mv "$tmp" "$CHANGELOG"
    trap - EXIT

    echo "Prepared $CHANGELOG for v$VERSION. Review and commit it, then merge the PR."
    echo "After merging, tag the resulting main commit:"
    echo "  git tag -a v$VERSION -m \"v$VERSION\""
    echo "  git push origin v$VERSION"


# --- tokscale canonical commands ---

# Canonical tokscale commands for raw JSON data input.
# These commands are the raw interface for usagebassoon.
# Use the versioned golden JSON fixtures in tests/fixtures/ for testing.
# Updating the golden fixtures requires a dedicated PR.

tokscale-report:
    tokscale report --json --no-summarize

tokscale-models:
    tokscale models --json --group-by client,session,model --merge-worktrees

tokscale-graph:
    tokscale graph

tokscale-pricing:
    tokscale pricing <model-id> --json
