# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_query.py — Typer integration tests for bounded relation queries."""

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
    """Create a populated local warehouse for query command tests."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\ndatabase = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.append(
        "sessions",
        pa.table(
            {
                "source_id": [SOURCE_ID],
                "client": ["codex"],
                "session_id": ["ses_private"],
                "workspace": ["/project-alpha"],
                "workspace_label": ["project-alpha"],
                "created_at": [datetime(2026, 9, 15, 12, tzinfo=UTC)],
                "last_active": [datetime(2026, 9, 15, 12, tzinfo=UTC)],
                "duration_minutes": [1],
                "message_count": [1],
                "tokscale_cost_usd": [0.0],
                "models_used": [["gpt-5"]],
                "session_label": ["project-alpha · 2026-09-15 · ses_private"],
                "first_seen_at": [datetime(2026, 9, 15, 12, tzinfo=UTC)],
                "last_seen_at": [datetime(2026, 9, 15, 12, tzinfo=UTC)],
                "updated_at": [datetime(2026, 9, 15, 12, tzinfo=UTC)],
            }
        ),
    )
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


def test_query_warns_and_rejects_non_allowlisted_relations(tmp_path: Path) -> None:
    """Expose raw relation output without accepting arbitrary warehouse SQL."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    runner = CliRunner()

    query_result = runner.invoke(
        app,
        [
            "query",
            "session_notes",
            "--filter",
            "client=codex",
            "--limit",
            "1",
            "--config",
            str(config),
        ],
    )
    delete_result = runner.invoke(
        app,
        ["query", "notes", "--config", str(config)],
    )

    assert query_result.exit_code == 0
    assert "ses_private" in query_result.output
    assert "returns raw data" in query_result.stderr
    assert delete_result.exit_code != 0
    assert "not supported" in delete_result.output


def test_query_writes_json_csv_and_parquet_formats(tmp_path: Path) -> None:
    """Serialize a permitted bounded relation query in every file format."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    json_path = tmp_path / "notes.json"
    csv_path = tmp_path / "notes.csv"
    parquet_path = tmp_path / "notes.parquet"
    runner = CliRunner()
    json_result = runner.invoke(
        app,
        [
            "query",
            "session_notes",
            "--filter",
            "client=codex",
            "--limit",
            "1",
            "--format",
            "json",
            "--output",
            str(json_path),
            "--config",
            str(config),
        ],
    )
    csv_result = runner.invoke(
        app,
        [
            "query",
            "session_notes",
            "--filter",
            "client=codex",
            "--limit",
            "1",
            "--format",
            "csv",
            "--output",
            str(csv_path),
            "--config",
            str(config),
        ],
    )
    parquet_result = runner.invoke(
        app,
        [
            "query",
            "session_notes",
            "--filter",
            "client=codex",
            "--limit",
            "1",
            "--format",
            "parquet",
            "--output",
            str(parquet_path),
            "--config",
            str(config),
        ],
    )

    assert json_result.exit_code == 0
    created_at = json.loads(json_path.read_text())[0]["created_at"]
    assert created_at.startswith("2026-09-15T12:00:00")
    assert csv_result.exit_code == 0
    assert "ses_private" in csv_path.read_text()
    assert parquet_result.exit_code == 0
    assert pq.read_table(parquet_path).num_rows == 1


def test_query_requires_output_only_for_file_formats(tmp_path: Path) -> None:
    """Reject incompatible output combinations before they can surprise users."""
    config, backend = _configured_store(tmp_path)
    backend.close()
    runner = CliRunner()

    table_output = runner.invoke(
        app,
        [
            "query",
            "session_notes",
            "--output",
            str(tmp_path / "out"),
            "--config",
            str(config),
        ],
    )
    parquet_without_output = runner.invoke(
        app,
        [
            "query",
            "session_notes",
            "--format",
            "parquet",
            "--config",
            str(config),
        ],
    )

    assert table_output.exit_code != 0
    assert "--output requires csv, json, or parquet" in table_output.output
    assert parquet_without_output.exit_code != 0
    assert "--output is required for parquet format" in parquet_without_output.output
