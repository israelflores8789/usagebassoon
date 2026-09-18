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
    @mkdir -p .codex_logs
    @bash -o pipefail -c '{ \
        echo "[just] $(date -u +%FT%TZ) starting pytest for {{test-name}}"; \
        PYTHONUNBUFFERED=1 USAGEBASSOON_BIGQUERY_LIVE=1 USAGEBASSOON_BIGQUERY_LIVE_RESET={{reset}} uv run pytest -vv \
        --tb=short \
        --color=no \
        "{{test-name}}" < /dev/null; \
    } 2>&1 | tee .codex_logs/pytest-bq-live.log'

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

release:
    uv publish


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
