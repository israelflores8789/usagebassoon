# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_query_export.py — Privacy and read-only command integration tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create a populated DuckDB configuration for command tests.

    Args:
        tmp_path: Per-test temporary directory.

    Returns:
        Explicit config path and its initialized backend.
    """
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\ndatabase = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.append(
        "notes",
        pa.table(
            {
                "source_id": [SOURCE_ID],
                "client": ["codex"],
                "session_id": ["ses_private"],
                "note": ["Call Ada at example@private.test"],
                "created_at": [datetime(2026, 9, 15, 12, tzinfo=UTC)],
                "updated_at": [datetime(2026, 9, 15, 12, tzinfo=UTC)],
            }
        ),
    )
    return config, backend


def test_query_warns_and_rejects_mutating_sql(tmp_path: Path) -> None:
    """Keep query raw but prevent it from changing the configured warehouse."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    runner = CliRunner()

    query_result = runner.invoke(
        app,
        [
            "query",
            "SELECT client, session_id FROM notes",
            "--config",
            str(config),
        ],
    )
    delete_result = runner.invoke(
        app,
        ["query", "DELETE FROM notes", "--config", str(config)],
    )

    assert query_result.exit_code == 0
    assert "ses_private" in query_result.output
    assert "returns raw data" in query_result.stderr
    assert delete_result.exit_code != 0
    assert "read-only SELECT or WITH" in delete_result.output


def test_export_obfuscates_by_default_and_can_export_raw(tmp_path: Path) -> None:
    """Make share-safe export the default while preserving an explicit raw path."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    sanitized_path = tmp_path / "sanitized.json"
    raw_path = tmp_path / "raw.json"
    runner = CliRunner()

    sanitized = runner.invoke(
        app,
        [
            "export",
            "notes",
            str(sanitized_path),
            "--format",
            "json",
            "--config",
            str(config),
        ],
    )
    raw = runner.invoke(
        app,
        [
            "export",
            "notes",
            str(raw_path),
            "--format",
            "json",
            "--raw",
            "--config",
            str(config),
        ],
    )

    sanitized_row = json.loads(sanitized_path.read_text())[0]
    raw_row = json.loads(raw_path.read_text())[0]
    assert sanitized.exit_code == 0
    assert "obfuscated by default" in sanitized.stderr
    assert sanitized_row["client"] == "codex"
    assert sanitized_row["session_id"] == "session-alpha"
    assert sanitized_row["note"] == "[redacted]"
    assert raw.exit_code == 0
    assert raw_row["session_id"] == "ses_private"
    assert raw_row["note"] == "Call Ada at example@private.test"


def test_doctor_sanitizes_config_location_unless_raw(tmp_path: Path) -> None:
    """Make doctor issue-ready by default while warning on raw diagnostics."""
    missing = tmp_path / "private" / "config.toml"
    runner = CliRunner()

    sanitized = runner.invoke(app, ["doctor", "--config", str(missing)])
    raw = runner.invoke(app, ["doctor", "--raw", "--config", str(missing)])

    assert sanitized.exit_code == 1
    assert str(missing) not in sanitized.output
    assert "<config-path>" in sanitized.output
    assert raw.exit_code == 1
    assert str(missing) in raw.output.replace("\n", "")
    assert "raw doctor output may contain" in raw.stderr
