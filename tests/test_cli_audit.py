# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_audit.py — Typer integration tests for audit history."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create an initialized local warehouse for an audit command test."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\nlocal_database = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    return config, backend


def test_audit_orders_runs_newest_first_and_honors_limit(tmp_path: Path) -> None:
    """Render only the requested newest audit records."""
    config, backend = _configured_store(tmp_path)
    backend.append(
        "ingest_runs",
        pa.table(
            {
                "run_id": ["old-run", "new-run"],
                "source_id": [SOURCE_ID, SOURCE_ID],
                "started_at": [
                    datetime(2026, 9, 14, tzinfo=UTC),
                    datetime(2026, 9, 15, tzinfo=UTC),
                ],
                "finished_at": [
                    datetime(2026, 9, 14, 1, tzinfo=UTC),
                    datetime(2026, 9, 15, 1, tzinfo=UTC),
                ],
                "status": ["ok", "partial"],
                "rows_in": [1, 2],
                "rows_inserted": [1, 1],
                "rows_updated": [0, 1],
                "drift_events": [0, 1],
            }
        ),
    )
    backend.close()

    result = CliRunner().invoke(app, ["audit", "--limit", "1", "--config", str(config)])

    assert result.exit_code == 0
    assert "new-run" in result.output
    assert "old-run" not in result.output


def test_audit_rejects_a_non_positive_limit(tmp_path: Path) -> None:
    """Let Typer reject audit limits outside the declared option range."""
    config, backend = _configured_store(tmp_path)
    backend.close()

    result = CliRunner().invoke(app, ["audit", "--limit", "0", "--config", str(config)])

    assert result.exit_code != 0
    assert "Invalid value" in result.output
