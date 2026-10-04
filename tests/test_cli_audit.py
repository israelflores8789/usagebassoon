# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_audit.py — Typer integration tests for audit history."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from tests._observations import observations
from usagebassoon.archiver import SnapshotArchiver
from usagebassoon.audit import audit_sources
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create an initialized local warehouse for an audit command test."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\n'
        f'local_database = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    return config, backend


def test_audit_orders_runs_newest_first_and_honors_limit(tmp_path: Path) -> None:
    """Render only the requested newest audit records."""
    config, backend = _configured_store(tmp_path)
    backend.append(
        "collection_ledger",
        observations(
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
                }
            )
        ),
    )
    backend.close()

    result = CliRunner().invoke(
        app, ["audit", "runs", "--limit", "1", "--config", str(config)]
    )

    assert result.exit_code == 0
    assert "new-run" in plain_cli_output(result.output)
    assert "old-run" not in plain_cli_output(result.output)


def test_audit_rejects_a_non_positive_limit(tmp_path: Path) -> None:
    """Let Typer reject audit limits outside the declared option range."""
    config, backend = _configured_store(tmp_path)
    backend.close()

    result = CliRunner().invoke(
        app, ["audit", "runs", "--limit", "0", "--config", str(config)]
    )

    assert result.exit_code != 0
    assert "Invalid value" in plain_cli_output(result.output)


def test_source_audit_uses_latest_run_metadata_and_includes_ledgerless_sources(
    tmp_path: Path,
) -> None:
    """Recover source evidence from archived records even after configuration loss."""
    _, backend = _configured_store(tmp_path)
    older, newer = datetime(2026, 9, 14, tzinfo=UTC), datetime(2026, 9, 15, tzinfo=UTC)
    backend.append(
        "collection_ledger",
        observations(
            pa.table(
                {
                    "run_id": ["old", "new"],
                    "source_id": [SOURCE_ID, SOURCE_ID],
                    "started_at": [older, newer],
                    "finished_at": [older, newer],
                    "status": ["ok", "partial"],
                    "host": ["old-host", "new-host"],
                    "cpu_count": [64, 2],
                }
            )
        ),
    )
    other = "22222222-2222-4222-8222-222222222222"
    backend.append(
        "notes",
        observations(
            pa.table(
                {
                    "source_id": [other],
                    "client": ["codex"],
                    "session_id": ["session"],
                    "note": ["curation only"],
                    "created_at": [newer],
                    "collected_at": [newer],
                }
            )
        ),
    )
    try:
        rows = audit_sources(backend)
        assert (
            backend.query(
                "SELECT * FROM audit_sources "
                "ORDER BY last_activity DESC NULLS LAST, source_id DESC"
            ).to_pylist()
            == rows
        )
        assert rows[0]["source_id"] == SOURCE_ID
        assert rows[0]["run_count"] == 2
        assert rows[0]["host"] == "new-host" and rows[0]["cpu_count"] == 2
        assert rows[1]["source_id"] == other and rows[1]["last_activity"] is None
        store = SnapshotArchiver(str(tmp_path / "archive"))
        uri = store.write(backend, run_id="audit", manual=True, pin=True)
        assert uri is not None
    finally:
        backend.close()
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "audit",
            "sources",
            "--from-snapshot",
            uri,
            "--json",
            "--config",
            str(tmp_path / "lost.toml"),
        ],
    )
    assert result.exit_code == 0, plain_cli_output(result.output)
    sources = json.loads(plain_cli_output(result.stdout))
    expected = json.loads(json.dumps(rows, default=lambda value: value.isoformat()))
    assert sources == expected
    assert sources[0]["source_id"] == SOURCE_ID and sources[0]["host"] == "new-host"
    assert sources[1]["source_id"] == other and sources[1]["run_count"] == 0
    runs = runner.invoke(
        app,
        [
            "audit",
            "runs",
            "--from-snapshot",
            uri,
            "--json",
            "--limit",
            "1",
            "--config",
            str(tmp_path / "lost.toml"),
        ],
    )
    assert runs.exit_code == 0
    assert [row["run_id"] for row in json.loads(plain_cli_output(runs.stdout))] == [
        "new"
    ]
