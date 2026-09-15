# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_restore.py — Typer integration tests for snapshot restoration."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _write_config(path: Path, database: Path) -> None:
    """Write a local-DuckDB configuration for restore command tests."""
    path.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\ndatabase = "{database}"\n'
    )


def _seed_session(database: Path, session_id: str) -> None:
    """Insert one snapshot-worthy session into an initialized warehouse."""
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.connection.execute(
        "INSERT INTO sessions "
        "(source_id, client, session_id, first_seen_at, last_seen_at, last_updated_at) "
        "VALUES (?, ?, ?, NOW(), NOW(), NOW())",
        [SOURCE_ID, "codex", session_id],
    )
    backend.close()


def test_restore_rehydrates_an_empty_warehouse_from_latest_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Restore actual Parquet snapshot data through both public CLI commands."""
    monkeypatch.setenv("HOME", str(tmp_path))
    source_config = tmp_path / "source.toml"
    source_database = tmp_path / "source.duckdb"
    _write_config(source_config, source_database)
    _seed_session(source_database, "ses_source")
    runner = CliRunner()

    snapshot_result = runner.invoke(app, ["snapshot", "--config", str(source_config)])
    destination_config = tmp_path / "destination.toml"
    destination_database = tmp_path / "destination.duckdb"
    _write_config(destination_config, destination_database)
    restore_result = runner.invoke(
        app, ["restore", "--config", str(destination_config)]
    )

    assert snapshot_result.exit_code == 0
    assert restore_result.exit_code == 0
    assert "sessions=1" in restore_result.output
    backend = DuckDBBackend(destination_database)
    try:
        assert backend.query("SELECT session_id FROM sessions").to_pylist() == [
            {"session_id": "ses_source"}
        ]
    finally:
        backend.close()


def test_restore_rejects_a_populated_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Protect current usage data from an accidental restore overwrite."""
    monkeypatch.setenv("HOME", str(tmp_path))
    source_config = tmp_path / "source.toml"
    source_database = tmp_path / "source.duckdb"
    _write_config(source_config, source_database)
    _seed_session(source_database, "ses_source")
    runner = CliRunner()
    assert (
        runner.invoke(app, ["snapshot", "--config", str(source_config)]).exit_code == 0
    )

    destination_config = tmp_path / "destination.toml"
    destination_database = tmp_path / "destination.duckdb"
    _write_config(destination_config, destination_database)
    _seed_session(destination_database, "ses_existing")
    result = runner.invoke(app, ["restore", "--config", str(destination_config)])

    assert result.exit_code != 0
    assert "restore requires an empty warehouse" in result.output
    backend = DuckDBBackend(destination_database)
    try:
        assert backend.query("SELECT session_id FROM sessions").to_pylist() == [
            {"session_id": "ses_existing"}
        ]
    finally:
        backend.close()
