# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_export.py — Typer integration tests for share-safe exports."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from tests._observations import observations
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.config import UsageBassoonConfig
from usagebassoon.sql_safety import PUBLIC_RELATIONS
from usagebassoon.storage_model import EVENT_KEYS, STATE_KEYS

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def _configured_store(tmp_path: Path) -> tuple[Path, DuckDBBackend]:
    """Create a populated local warehouse for export command tests."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend.provider = "duckdb"\n'
        f'backend.duckdb.database = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.append(
        "notes",
        observations(
            pa.table(
                {
                    "source_id": [SOURCE_ID],
                    "client": ["codex"],
                    "session_id": ["ses_private"],
                    "note": ["Call Ada at example@private.test"],
                    "created_at": [datetime(2026, 9, 15, 12, tzinfo=UTC)],
                    "collected_at": [datetime(2026, 9, 15, 12, tzinfo=UTC)],
                }
            )
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
    assert "obfuscated by default" in plain_cli_output(sanitized.stderr)
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
    assert "target must be one of" in plain_cli_output(invalid.output)


@pytest.mark.parametrize("target", sorted(STATE_KEYS | EVENT_KEYS))
def test_export_reads_canonical_views(
    target: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Export logical state through views, including accepted uncompacted raw data."""
    from unittest.mock import MagicMock

    config = tmp_path / "config.toml"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\n'
        f'backend.provider = "duckdb"\n'
        f'backend.duckdb.database = ":memory:"\n'
    )
    publication = pa.table({"source_id": [SOURCE_ID], "value": ["accepted raw data"]})
    client = MagicMock()
    client.query.return_value = publication

    def open_backend(_config: UsageBassoonConfig) -> MagicMock:
        """Return a recording backend with already accepted observations."""
        return client

    monkeypatch.setattr("usagebassoon.cli.export.open_backend", open_backend)
    output = tmp_path / "export.parquet"
    result = CliRunner().invoke(
        app,
        ["export", target, str(output), "--raw", "--config", str(config)],
    )
    assert result.exit_code == 0
    client.query.assert_called_once_with(f"SELECT * FROM current_{target}")
    assert pq.read_table(output).to_pylist() == publication.to_pylist()
    assert target not in PUBLIC_RELATIONS
