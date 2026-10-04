# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_curation.py — Typer command tests for curation operations."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.config import CONFIG_PATH_ENV_VAR

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _write_config(path: Path, database: Path) -> None:
    """Write a local-DuckDB configuration for CLI integration tests."""
    path.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend.provider = "duckdb"\n'
        f'backend.duckdb.database = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    try:
        backend.apply_ddl()
    finally:
        backend.close()


def _command(config: Path, *parts: str) -> list[str]:
    """Return one curation command with its explicit configuration path."""
    return [*parts, "--config", str(config)]


def test_curation_commands_use_only_the_explicit_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Assert every curation subcommand honors its explicit configuration."""
    selected_config = tmp_path / "selected.toml"
    selected_database = tmp_path / "selected.duckdb"
    environment_config = tmp_path / "environment.toml"
    _write_config(selected_config, selected_database)
    _write_config(environment_config, tmp_path / "environment.duckdb")
    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(environment_config))
    runner = CliRunner()

    tag_result = runner.invoke(
        app,
        _command(selected_config, "tag", "add", "project-alpha", "--client", "codex"),
    )
    note_result = runner.invoke(
        app,
        _command(
            selected_config,
            "note",
            "set",
            "Track this session",
            "--client",
            "codex",
            "--session",
            "ses_123",
        ),
    )

    assert tag_result.exit_code == 0
    assert note_result.exit_code == 0
    backend = DuckDBBackend(selected_database)
    try:
        assert backend.query("SELECT tag FROM tags").to_pylist() == [
            {"tag": "project-alpha"}
        ]
        assert backend.query("SELECT note FROM notes").to_pylist() == [
            {"note": "Track this session"}
        ]
    finally:
        backend.close()


def test_curation_help_documents_the_supported_operations() -> None:
    """Expose each supported tag and note operation in command help."""
    runner = CliRunner()
    for command, operations in (
        ("tag", ("add", "rename", "remove")),
        ("note", ("set", "edit", "remove")),
    ):
        result = runner.invoke(app, [command, "--help"])
        assert result.exit_code == 0
        output = plain_cli_output(result.stdout)
        for operation in operations:
            assert operation in output.split()


def test_tag_commands_support_each_scope_rename_and_remove(tmp_path: Path) -> None:
    """Assert tags mutate only at their declared complete target scope."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    runner = CliRunner()
    client = _command(config, "tag", "add", "client-tag", "--client", "codex")
    workspace = _command(config, "tag", "add", "workspace-tag", "--workspace", "/repo")
    session = _command(
        config,
        "tag",
        "add",
        "session-tag",
        "--client",
        "codex",
        "--session",
        "ses_123",
    )
    assert runner.invoke(app, client).exit_code == 0
    assert runner.invoke(app, workspace).exit_code == 0
    assert runner.invoke(app, session).exit_code == 0
    renamed = runner.invoke(
        app,
        _command(
            config,
            "tag",
            "rename",
            "session-tag",
            "renamed",
            "--client",
            "codex",
            "--session",
            "ses_123",
        ),
    )
    removed = runner.invoke(
        app, _command(config, "tag", "remove", "workspace-tag", "--workspace", "/repo")
    )
    assert renamed.exit_code == 0
    assert removed.exit_code == 0
    backend = DuckDBBackend(database)
    try:
        assert backend.query("SELECT tag FROM tags ORDER BY tag").to_pylist() == [
            {"tag": "client-tag"},
            {"tag": "renamed"},
        ]
    finally:
        backend.close()


def test_note_edit_uses_editor_and_does_not_write_on_invalid_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Assert editor success, blank content, and failures preserve note safety."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usage.duckdb"
    _write_config(config, database)
    runner = CliRunner()
    target = ["--client", "codex", "--session", "ses_123"]
    assert (
        runner.invoke(app, _command(config, "note", "set", "first", *target)).exit_code
        == 0
    )
    backend = DuckDBBackend(database)
    try:
        original = backend.query(
            "SELECT created_at, collected_at FROM notes"
        ).to_pylist()[0]
    finally:
        backend.close()

    editor = tmp_path / "editor.py"
    editor.write_text(
        "from pathlib import Path\nimport sys\n"
        "Path(sys.argv[-1]).write_text(sys.argv[1])\n"
        "sys.exit(1 if sys.argv[1] == 'failed content' else 0)\n"
    )
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.setenv("EDITOR", f"{sys.executable} {editor} edited")
    edited = runner.invoke(app, _command(config, "note", "edit", *target))
    assert edited.exit_code == 0
    assert "Updated note" in plain_cli_output(edited.output)

    backend = DuckDBBackend(database)
    try:
        revised = backend.query(
            "SELECT note, created_at, collected_at FROM notes"
        ).to_pylist()[0]
        assert revised["note"] == "edited"
        assert revised["created_at"] == original["created_at"]
        assert revised["collected_at"] > original["collected_at"]
    finally:
        backend.close()

    for content in ("   ", "failed content"):
        monkeypatch.setenv("EDITOR", f"{sys.executable} {editor} '{content}'")
        rejected = runner.invoke(app, _command(config, "note", "edit", *target))
        assert rejected.exit_code != 0
        if not content.strip():
            assert "note remove" in plain_cli_output(rejected.output)
        backend = DuckDBBackend(database)
        try:
            assert backend.query(
                "SELECT note, created_at, collected_at FROM notes"
            ).to_pylist() == [revised]
        finally:
            backend.close()


def test_note_edit_requires_a_configured_editor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Assert edit directs users to set when no editor has been configured."""
    config = tmp_path / "config.toml"
    _write_config(config, tmp_path / "usage.duckdb")
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.delenv("EDITOR", raising=False)
    result = CliRunner().invoke(
        app,
        _command(config, "note", "edit", "--client", "codex", "--session", "ses_123"),
    )
    assert result.exit_code != 0
    assert "note set" in plain_cli_output(result.output)
