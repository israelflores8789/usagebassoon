# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_cli_snapshot.py — Typer integration tests for private snapshots."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon.archiver import SNAPSHOT_TABLES
from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.config import default_snapshot_directory

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


@pytest.mark.parametrize("location", ["default", "path", "file_uri"])
def test_snapshot_writes_a_manual_run_manifest_for_an_uncollected_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    location: str,
) -> None:
    """Use the documented manual marker before any collection run exists."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "AppData" / "Local"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / ".local" / "share"))
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    snapshot_directory = default_snapshot_directory()
    snapshot_settings = ""
    if location != "default":
        value = str(snapshot_directory)
        if location == "file_uri":
            value = f"file://{value}"
        snapshot_settings = f"[snapshots.local]\npath = '{value}'\n"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\n'
        f'backend.provider = "duckdb"\nbackend.duckdb.database = "{database}"\n'
        + snapshot_settings
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.connection.execute(
        "INSERT INTO sessions "
        "(event_id, source_id, client, session_id, first_seen_at, "
        "last_seen_at, collected_at) "
        "VALUES (UUID(), ?, ?, ?, NOW(), NOW(), NOW())",
        [SOURCE_ID, "codex", "ses_1"],
    )
    backend.close()

    result = CliRunner().invoke(app, ["snapshot", "--config", str(config)])
    assert result.exit_code == 0, plain_cli_output(result.output)

    store = SnapshotStore(str(snapshot_directory))
    stamps = store.list_snapshots()
    with (snapshot_directory / stamps[0] / "manifest.json").open() as handle:
        manifest = json.load(handle)
    assert result.exit_code == 0
    assert "Created private raw snapshot at" in plain_cli_output(result.output)
    assert manifest["run_id"] == "manual"
    assert manifest["tables"]["sessions"]["rows"] == 1
    assert set(manifest["tables"]) == set(SNAPSHOT_TABLES)


def test_snapshot_management_and_audit_aliases_share_structured_results(
    tmp_path: Path,
) -> None:
    """Use one shared integrity handler without requiring a destination config."""
    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()
    store = SnapshotStore(str(tmp_path / "archive"))
    try:
        uri = store.write(backend, run_id="manual", manual=True, pin=True)
        assert uri is not None
    finally:
        backend.close()
    runner = CliRunner()
    for command in (
        ["snapshot", "audit"],
        ["audit", "snapshot"],
        ["audit", "snapshots"],
    ):
        result = runner.invoke(
            app,
            [
                *command,
                "--from-snapshot",
                uri,
                "--json",
                "--config",
                str(tmp_path / "lost.toml"),
            ],
            input="y\n",
        )
        assert result.exit_code == 0, plain_cli_output(result.output)
        payload = json.loads(plain_cli_output(result.stdout))
        assert payload["id"] == Path(uri).name
        assert payload["valid"] is True
        assert set(payload["rows"]) == set(SNAPSHOT_TABLES)
    listed = runner.invoke(
        app,
        [
            "snapshot",
            "list",
            "--from-snapshot",
            str(tmp_path / "archive"),
            "--json",
            "--config",
            str(tmp_path / "lost.toml"),
        ],
        input="y\n",
    )
    assert listed.exit_code == 0
    record = json.loads(plain_cli_output(listed.stdout))["snapshots"][0]
    assert record["pinned"] is True and record["latest"] is True
    assert all(
        record[field] is not None
        for field in (
            "usagebassoon_version",
            "data_schema_version",
            "snapshot_format_version",
            "backend_schema_version",
            "backend_schema_hash",
        )
    )
    refused = runner.invoke(
        app,
        ["snapshot", "delete", uri, "--config", str(tmp_path / "lost.toml")],
        input="no\n",
    )
    assert refused.exit_code != 0
    assert (Path(uri) / "COMPLETE").exists()
    deleted = runner.invoke(
        app,
        ["snapshot", "delete", uri, "--config", str(tmp_path / "lost.toml")],
        input="DELETE\n",
    )
    assert deleted.exit_code == 0, plain_cli_output(deleted.output)
    assert not (Path(uri) / "COMPLETE").exists()


def test_snapshot_delete_does_not_include_copies_created_during_confirmation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Delete only the locations displayed before the operator types DELETE."""
    import typer

    from tests.test_bucket_gcs import MemoryGcsArchive
    from usagebassoon.buckets.local import LocalSnapshotBucket

    source = DuckDBBackend(":memory:")
    source.apply_ddl()
    cloud = MemoryGcsArchive()
    local = tmp_path / "archive"
    store = SnapshotStore(destination_uris=(str(local), cloud.uri), buckets=(cloud,))
    try:
        uri = SnapshotStore(str(local)).write(
            source, run_id="manual", manual=True, pin=True
        )
        assert uri is not None
    finally:
        source.close()
    identifier = Path(uri).name

    def confirm(_text: str, *, default: str, show_default: bool) -> str:
        del default, show_default
        bucket = LocalSnapshotBucket(str(local))
        for obj in bucket.list(identifier):
            cloud.write_bytes(
                obj.name, bucket.read_bytes(obj.name, version=obj.version)
            )
        return "DELETE"

    def archiver_for_config(_config: Path | None) -> SnapshotStore:
        return store

    monkeypatch.setattr("usagebassoon.cli.snapshot.read_archiver", archiver_for_config)
    monkeypatch.setattr(typer, "prompt", confirm)
    result = CliRunner().invoke(app, ["snapshot", "delete", identifier])
    assert result.exit_code == 0, plain_cli_output(result.output)
    assert not (local / identifier / "COMPLETE").exists()
    assert f"archive/{identifier}/COMPLETE" in cloud.objects


def test_inspect_exposes_verified_producer_metadata_for_unsupported_contract(
    tmp_path: Path,
) -> None:
    """An unsupported reader does not hide the producer needed for manual recovery."""
    from hashlib import sha256

    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()
    try:
        uri = SnapshotStore(str(tmp_path / "archive")).write(
            backend, run_id="future", manual=True
        )
        assert uri is not None
    finally:
        backend.close()
    path = Path(uri) / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["data_schema_version"] = 999
    raw = json.dumps(manifest).encode()
    path.write_bytes(raw)
    (Path(uri) / "COMPLETE").write_text(
        json.dumps(
            {"snapshot_id": Path(uri).name, "manifest_sha256": sha256(raw).hexdigest()}
        )
    )
    result = CliRunner().invoke(
        app,
        [
            "snapshot",
            "inspect",
            "--from-snapshot",
            uri,
            "--json",
            "--config",
            str(tmp_path / "lost.toml"),
        ],
        input="y\n",
    )
    assert result.exit_code == 0, plain_cli_output(result.output)
    payload = json.loads(plain_cli_output(result.stdout))
    assert payload["manifest"]["data_schema_version"] == 999
    assert payload["manifest"]["usagebassoon_version"]
    assert payload["compatibility"]["supported"] is False


def test_inspect_archive_root_uses_immutables_when_controls_are_corrupt(
    tmp_path: Path,
) -> None:
    """Latest provenance remains inspectable without usable lifecycle metadata."""
    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()
    root = tmp_path / "archive"
    uri = SnapshotStore(str(root)).write(backend, run_id="inspect", manual=True)
    backend.close()
    assert uri is not None
    (root / "control.json").write_text("damaged")
    (root / "catalog.json").write_text("damaged")
    (Path(uri) / "state.json").write_text("damaged")
    result = CliRunner().invoke(
        app,
        [
            "snapshot",
            "inspect",
            "--from-snapshot",
            str(root),
            "--config",
            str(tmp_path / "lost.toml"),
            "--json",
        ],
        input="y\n",
    )
    assert result.exit_code == 0, plain_cli_output(result.output)
    assert (
        json.loads(plain_cli_output(result.stdout))["manifest"]["snapshot_id"]
        == Path(uri).name
    )


def test_declining_disabled_archive_access_aborts_without_audit_output(
    tmp_path: Path,
) -> None:
    """A negative permission answer is cancellation, not an invalid parameter."""
    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()
    uri = SnapshotStore(str(tmp_path / "archive")).write(
        backend, run_id="decline", manual=True
    )
    backend.close()
    assert uri is not None
    result = CliRunner().invoke(
        app,
        [
            "snapshot",
            "audit",
            "--from-snapshot",
            uri,
            "--config",
            str(tmp_path / "lost.toml"),
            "--json",
        ],
        input="n\n",
    )
    assert result.exit_code != 0
    assert "Aborted" in plain_cli_output(result.output)
    assert '"valid": true' not in plain_cli_output(result.stdout)
    assert (Path(uri) / "COMPLETE").exists()


def test_partial_snapshot_failure_reports_retained_copy_and_exits_unsuccessfully(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed destination must not turn a retained healthy copy into CLI success."""
    config = tmp_path / "config.toml"
    database = tmp_path / "usagebassoon.duckdb"
    root = tmp_path / "archive"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\n'
        f'backend.provider = "duckdb"\nbackend.duckdb.database = "{database}"\n'
        f'[snapshots.local]\nenable = true\npath = "{root}"\n'
        '[snapshots.gcs]\nenable = true\nproject = "test"\nuri = "gs://unavailable/archive"\n'
    )
    backend = DuckDBBackend(database)
    backend.apply_ddl()
    backend.close()

    def unavailable(
        uri: str, *, project: str | None = None, credentials_file: Path | None = None
    ) -> None:
        """Fail cloud adapter construction before any network request."""
        del uri, project, credentials_file
        raise OSError("destination unavailable")

    monkeypatch.setattr("usagebassoon.buckets.gcs.GcsSnapshotBucket", unavailable)
    result = CliRunner().invoke(app, ["snapshot", "--config", str(config)])
    output = " ".join(plain_cli_output(result.output).replace("│", "").split())
    assert result.exit_code != 0
    assert "Snapshot failed at gs://unavailable/archive" in output
    assert "verified copies retained at:" in output
    assert "Created private raw snapshot" not in output
    store = SnapshotStore(str(root))
    with store.reader.prepare() as prepared:
        assert prepared.candidate.bucket.uri == str(root)
        assert prepared.manifest["run_id"] == "manual"
