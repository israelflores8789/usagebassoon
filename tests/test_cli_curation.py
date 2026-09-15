# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_curation.py — Typer command tests for curation operations."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.config import CONFIG_PATH_ENV_VAR

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _write_config(path: Path, database: Path) -> None:
    """Write a local-DuckDB configuration for CLI integration tests.

    Args:
        path: Config path to create.
        database: DuckDB file selected by that config.
    """
    path.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\ndatabase = "{database}"\n'
    )


def test_curation_commands_use_only_the_explicit_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assert --config wins and no command-level backend override exists."""
    selected_config = tmp_path / "selected.toml"
    selected_database = tmp_path / "selected.duckdb"
    environment_config = tmp_path / "environment.toml"
    _write_config(selected_config, selected_database)
    _write_config(environment_config, tmp_path / "environment.duckdb")
    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(environment_config))
    runner = CliRunner()

    client_result = runner.invoke(
        app,
        [
            "tag",
            "project-alpha",
            "--client",
            "codex",
            "--config",
            str(selected_config),
        ],
    )
    workspace_result = runner.invoke(
        app,
        [
            "tag",
            "shared-workspace",
            "--workspace",
            "/repo",
            "--config",
            str(selected_config),
        ],
    )
    note_result = runner.invoke(
        app,
        [
            "note",
            "Track this session",
            "--client",
            "codex",
            "--session",
            "ses_123",
            "--config",
            str(selected_config),
        ],
    )

    assert client_result.exit_code == 0
    assert workspace_result.exit_code == 0
    assert note_result.exit_code == 0
    backend = DuckDBBackend(selected_database)
    try:
        assert backend.query(
            "SELECT source_id, scope, client, workspace, tag FROM tags ORDER BY tag"
        ).to_pylist() == [
            {
                "source_id": SOURCE_ID,
                "scope": "client",
                "client": "codex",
                "workspace": "",
                "tag": "project-alpha",
            },
            {
                "source_id": SOURCE_ID,
                "scope": "workspace",
                "client": "",
                "workspace": "/repo",
                "tag": "shared-workspace",
            },
        ]
        assert backend.query("SELECT note FROM notes").to_pylist() == [
            {"note": "Track this session"}
        ]
    finally:
        backend.close()


def test_tag_command_rejects_non_peer_scope_mix(tmp_path: Path) -> None:
    """Assert a workspace tag cannot also name a client or session."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usage.duckdb")
    result = CliRunner().invoke(
        app,
        [
            "tag",
            "project-alpha",
            "--client",
            "codex",
            "--workspace",
            "/repo",
            "--session",
            "ses_123",
            "--config",
            str(config),
        ],
    )
    assert result.exit_code != 0
    assert "--workspace cannot be combined" in result.output
