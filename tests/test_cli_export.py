# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_export.py — Typer integration tests for share-safe exports."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create a populated local warehouse for export command tests."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\n'
        f'local_database = "{database}"\n'
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


def test_export_supports_csv_and_parquet_and_rejects_unknown_relations(
    tmp_path: Path,
) -> None:
    """Exercise every binary or delimited file path at the CLI boundary."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    csv_path = tmp_path / "notes.csv"
    parquet_path = tmp_path / "notes.parquet"
    runner = CliRunner()

    csv_result = runner.invoke(
        app,
        [
            "export",
            "notes",
            str(csv_path),
            "--format",
            "csv",
            "--config",
            str(config),
        ],
    )
    parquet_result = runner.invoke(
        app,
        [
            "export",
            "notes",
            str(parquet_path),
            "--format",
            "parquet",
            "--raw",
            "--config",
            str(config),
        ],
    )
    invalid = runner.invoke(
        app,
        [
            "export",
            "not_a_relation",
            str(tmp_path / "bad.json"),
            "--config",
            str(config),
        ],
    )

    assert csv_result.exit_code == 0
    assert "session-alpha" in csv_path.read_text()
    assert parquet_result.exit_code == 0
    assert pq.read_table(parquet_path).to_pylist()[0]["session_id"] == "ses_private"
    assert invalid.exit_code != 0
    assert "target must be one of" in invalid.output
