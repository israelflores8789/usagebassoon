# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_snapshot.py — Typer integration tests for private snapshots."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.snapshots import SnapshotStore

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


def test_snapshot_writes_a_manual_run_manifest_for_an_uncollected_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the documented manual marker before any collection run exists."""
    monkeypatch.setenv("HOME", str(tmp_path))
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\ndatabase = "{database}"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.connection.execute(
        "INSERT INTO sessions "
        "(source_id, client, session_id, first_seen_at, last_seen_at, last_updated_at) "
        "VALUES (?, ?, ?, NOW(), NOW(), NOW())",
        [SOURCE_ID, "codex", "ses_1"],
    )
    backend.close()

    result = CliRunner().invoke(app, ["snapshot", "--config", str(config)])

    store = SnapshotStore(f"file://{tmp_path}/.usagebassoon/snapshots")
    stamps = store.list_snapshots()
    fs, root = store._fs_root()
    with fs.open(f"{root}/{stamps[0]}/manifest.json") as handle:
        manifest = json.load(handle)
    assert result.exit_code == 0
    assert "Created private raw snapshot at" in result.output
    assert manifest["run_id"] == "manual"
    assert manifest["tables"] == {"sessions": 1}
