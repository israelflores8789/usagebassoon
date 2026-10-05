# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_motherduck_live.py — Opt-in MotherDuck integration tests.

Run with ``just test-md-live`` against the disposable ``usagebassoon_it`` database.
The recipe enables live access and resets the dedicated integration schema.

Raw developer invocation, with ``MOTHERDUCK_TOKEN`` already set::

    export USAGEBASSOON_MOTHERDUCK_LIVE=1
    export USAGEBASSOON_MOTHERDUCK_LIVE_RESET=1
    uv run pytest -m motherduck_live tests/test_backend_motherduck_live.py

Both live-access and reset variables must equal ``1``; the module resets the
dedicated database at setup and cleanup. ``MOTHERDUCK_TOKEN`` supplies credentials.
The test database is fixed to ``usagebassoon_it`` and has no environment override.

Scenarios cover configured credentials/CLI/library access, publication and retries,
source identity and curation, consistent reads, and portable atomic recovery.
"""

from __future__ import annotations

import json
import logging
import os
import signal
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import duckdb
import pyarrow as pa
import pytest
from sqlglot import exp
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from tests._observations import observations
from tests._sql_parity import (
    assert_view_results_match,
    normalized_records,
    seed_synthetic_data,
    statements,
    view_names,
)
from tests.conftest import (
    EXPECTED_DAILY_STATS_ROWS,
    EXPECTED_REPORT_ROWS,
)
from usagebassoon.api import connect, query_arrow
from usagebassoon.archiver import SnapshotArchiver
from usagebassoon.backends.base import (
    CurrentStateWrite,
    PersistenceBatch,
    StorageBackend,
    is_simple_identifier,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.backends.motherduck import MotherDuckBackend
from usagebassoon.cli.app import app
from usagebassoon.config import ConfigurationManager, UsageBassoonConfig
from usagebassoon.curation import NoteAssignment, TagAssignment, add_tag, set_note
from usagebassoon.diagnostics import run_doctor
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import NormalizedBundle, normalize
from usagebassoon.persistence import PersistSummary, persist_run, persist_with_retries
from usagebassoon.storage_model import SNAPSHOT_TABLES, STATE_KEYS

pytestmark = [pytest.mark.motherduck_live, pytest.mark.usefixtures("live_settings")]

_DATABASE = "usagebassoon_it"
_LIVE_TEST_TIMEOUT_SECONDS = 300
_LOG = logging.getLogger("usagebassoon-motherduck-live")


@pytest.fixture(autouse=True)
def _bound_live_test_duration() -> Iterator[None]:
    """Fail a blocked live test within the per-test time limit."""
    if not hasattr(signal, "SIGALRM"):
        yield
        return

    def fail_timeout(_: int, __: object) -> None:
        """Raise from the main pytest thread when a live call stalls."""
        raise TimeoutError("live MotherDuck test exceeded 300 seconds")

    previous_handler = signal.signal(signal.SIGALRM, fail_timeout)
    signal.setitimer(signal.ITIMER_REAL, _LIVE_TEST_TIMEOUT_SECONDS)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


@dataclass(frozen=True, slots=True)
class LiveSettings:
    """Configuration for one isolated run in the dedicated test database."""

    source_id: str
    config_path: Path


def _require_live_access() -> None:
    """Require explicit access and reset authorization before remote writes."""
    if os.environ.get("USAGEBASSOON_MOTHERDUCK_LIVE") != "1":
        pytest.skip("set USAGEBASSOON_MOTHERDUCK_LIVE=1 for live MotherDuck tests")
    if os.environ.get("USAGEBASSOON_MOTHERDUCK_LIVE_RESET") != "1":
        pytest.fail("set USAGEBASSOON_MOTHERDUCK_LIVE_RESET=1 for schema reset")
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        pytest.fail("MOTHERDUCK_TOKEN is required for live MotherDuck tests")


def _reset_test_schema(backend: MotherDuckBackend) -> None:
    """Drop only packaged relations in the verified disposable database."""
    current = backend.query("SELECT current_database() AS name").to_pylist()
    if current != [{"name": _DATABASE}]:
        pytest.fail(f"refusing schema reset outside {_DATABASE!r}: {current!r}")

    for name in reversed(view_names()):
        if not is_simple_identifier(name):
            pytest.fail(f"invalid packaged view name {name!r}")
        backend.connection.execute(f'DROP VIEW IF EXISTS "{name}"')
    for statement in reversed(statements("duckdb", "ddl.sql")):
        if not isinstance(statement, exp.Create) or statement.kind != "TABLE":
            pytest.fail("DuckDB DDL must contain only CREATE TABLE statements")
        table = statement.this.this.name
        if not is_simple_identifier(table):
            pytest.fail(f"invalid packaged table name {table!r}")
        backend.connection.execute(f'DROP TABLE IF EXISTS "{table}"')
    # This database is exclusively disposable: older prerelease table shapes
    # must not survive a reset and masquerade as unexpected recovery data.
    for schema, table in backend.connection.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_type = 'BASE TABLE'"
    ).fetchall():
        quoted_schema = '"' + str(schema).replace('"', '""') + '"'
        quoted_table = '"' + str(table).replace('"', '""') + '"'
        backend.connection.execute(f"DROP TABLE {quoted_schema}.{quoted_table}")


@pytest.fixture(scope="module")
def live_settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[LiveSettings]:
    """Reset the dedicated schema and write a token-free backend config."""
    _require_live_access()
    source_id = str(uuid4())
    config_path = tmp_path_factory.mktemp("motherduck-live") / "config.toml"
    log_directory = config_path.parent / "logs"
    config_path.write_text(
        f'source_id = "{source_id}"\n'
        'backend.provider = "motherduck"\n'
        "[backend.motherduck]\n"
        f'database = "{_DATABASE}"\n'
        "[collection]\n"
        "max_retries = 3\n"
        "retry_initial_seconds = 0.1\n"
        "[logging]\n"
        f'directory = "{log_directory}"\n'
    )
    backend = MotherDuckBackend(_DATABASE)
    try:
        _reset_test_schema(backend)
        backend.apply_ddl()
    finally:
        backend.close()
    try:
        yield LiveSettings(source_id, config_path)
    finally:
        cleanup = MotherDuckBackend(_DATABASE)
        try:
            _reset_test_schema(cleanup)
        finally:
            cleanup.close()


def _backend() -> MotherDuckBackend:
    """Open a separate service-account connection to the test database."""
    return MotherDuckBackend(_DATABASE)


def _normalized_bundle(
    collection_bundle: CollectionBundle, source_id: str
) -> NormalizedBundle:
    """Give golden fixture facts a unique run and source identity."""
    return normalize(
        replace(collection_bundle, run_id=str(uuid4()), source_id=source_id)
    )


def _local_snapshot_config(
    settings: LiveSettings, directory: Path, source_id: str
) -> Path:
    """Configure an explicit source and private local archive for a live scenario."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"config-{source_id}.toml"
    path.write_text(
        settings.config_path.read_text().replace(
            f'source_id = "{settings.source_id}"', f'source_id = "{source_id}"', 1
        )
        + f'\n[snapshots.local]\nenable = true\npath = "{directory / "snapshots"}"\n'
    )
    return path


def _rows_for_source(
    backend: StorageBackend, table: str, source_id: str
) -> list[dict[str, object]]:
    """Read comparison-safe current-state rows for one source."""
    if not is_simple_identifier(table):
        raise ValueError(f"invalid table name {table!r}")
    return normalized_records(
        backend.query(
            f'SELECT * FROM "{table}" WHERE source_id = :source_id',
            {"source_id": source_id},
        )
    )


@pytest.fixture(autouse=True)
def empty_live_warehouse(live_settings: LiveSettings) -> None:
    """Isolate each case's rows in the dedicated initialized integration database.

    These tests must remain serialized because they share usagebassoon_it.
    """
    configuration = ConfigurationManager(live_settings.config_path).load()
    assert configuration.motherduck is not None
    backend = MotherDuckBackend(configuration.motherduck.database)
    try:
        with backend.transaction():
            for table in SNAPSHOT_TABLES:
                backend.connection.execute(f'DELETE FROM "{table}"')
    finally:
        backend.close()


def test_live_synthetic_views_match_duckdb() -> None:
    """Replay every shipped DuckDB view on the MotherDuck service."""
    local = DuckDBBackend(":memory:")
    remote = _backend()
    try:
        local.apply_ddl()
        seed_synthetic_data(local)
        seed_synthetic_data(remote)
        assert_view_results_match(local, remote, right_label="MotherDuck")
    finally:
        local.close()
        remote.close()


def test_live_batch_matches_duckdb_and_retries_idempotently(
    live_settings: LiveSettings, collection_bundle: CollectionBundle
) -> None:
    """Compare inserts, later/stale updates, retained history, and run replay."""
    normalized = _normalized_bundle(collection_bundle, live_settings.source_id)
    configuration = ConfigurationManager(live_settings.config_path).load()
    local = DuckDBBackend(":memory:")
    remote = _backend()
    expected_inserted = (
        EXPECTED_REPORT_ROWS
        + EXPECTED_DAILY_STATS_ROWS
        + sum(len(prices) for prices in collection_bundle.pricing_by_day.values())
    )
    try:
        local.apply_ddl()
        expected = persist_run(local, normalized)
        actual = persist_with_retries(configuration, normalized, _LOG)
        assert actual == expected
        assert (actual.inserted, actual.updated) == (expected_inserted, 0)
        for table in (
            "sessions",
            "daily_stats",
            "price_versions",
            "collection_status",
        ):
            assert _rows_for_source(remote, table, live_settings.source_id) == (
                _rows_for_source(local, table, live_settings.source_id)
            ), table
        retried = persist_with_retries(configuration, normalized, _LOG)
        assert (retried.inserted, retried.updated) == (0, 0)
        assert remote.query(
            "SELECT count(*) AS n FROM collection_runs WHERE run_id = :run_id",
            {"run_id": normalized.run_id},
        ).to_pylist() == [{"n": 1}]

        later = _normalized_bundle(
            replace(
                collection_bundle,
                started_at=collection_bundle.started_at + timedelta(hours=1),
                finished_at=collection_bundle.finished_at + timedelta(hours=1),
            ),
            live_settings.source_id,
        )
        prices = cast(
            list[dict[str, object]], later.tables["price_versions"].to_pylist()
        )
        price = next(row for row in prices if row["price_output_per_token"] is not None)
        daily_rows = cast(
            list[dict[str, object]], later.tables["daily_stats"].to_pylist()
        )
        daily = next(
            row
            for row in daily_rows
            if row["day"] == price["day"] and row["model"] == price["model"]
        )
        sessions = cast(list[dict[str, object]], later.tables["sessions"].to_pylist())
        session = next(
            row
            for row in sessions
            if row["client"] == daily["client"]
            and row["session_id"] == daily["session_id"]
        )
        targets = {"sessions": session, "daily_stats": daily, "price_versions": price}

        def selected(bundle: NormalizedBundle) -> NormalizedBundle:
            """Reobserve one key per domain, leaving other historical facts absent."""
            tables = dict(bundle.tables)
            for table, target in targets.items():
                keys = STATE_KEYS[table]
                records = cast(list[dict[str, object]], tables[table].to_pylist())
                matching = [
                    row
                    for row in records
                    if all(row[key] == target[key] for key in keys)
                ]
                assert len(matching) == 1
                tables[table] = pa.Table.from_pylist(
                    matching, schema=tables[table].schema
                )
            return replace(bundle, tables=tables)

        later = selected(later)
        input_tokens, total_tokens = daily["input_tokens"], daily["total_tokens"]
        output_rate = price["price_output_per_token"]
        assert isinstance(input_tokens, int) and isinstance(total_tokens, int)
        assert isinstance(output_rate, float)
        changed = {
            "daily_stats": {
                **daily,
                "input_tokens": input_tokens + 10,
                "total_tokens": total_tokens + 10,
            },
            "price_versions": {**price, "price_output_per_token": output_rate + 0.0001},
        }
        later = replace(
            later,
            tables={
                **later.tables,
                **{
                    table: pa.Table.from_pylist(
                        [row], schema=later.tables[table].schema
                    )
                    for table, row in changed.items()
                },
            },
        )
        first_seen = normalized.tables["sessions"].to_pylist()
        original_session = next(
            row
            for row in first_seen
            if row["client"] == daily["client"]
            and row["session_id"] == daily["session_id"]
        )
        expected_update = persist_run(local, later)
        actual_update = persist_with_retries(configuration, later, _LOG)
        assert actual_update == expected_update
        assert actual_update.inserted == 0 and actual_update.updated > 0
        assert (
            remote.query("SELECT * FROM daily_stats").num_rows
            == normalized.tables["daily_stats"].num_rows
        )
        assert remote.query(
            "SELECT first_seen_at, last_seen_at FROM sessions "
            "WHERE source_id = :source_id AND client = :client "
            "AND session_id = :session_id",
            {
                "source_id": live_settings.source_id,
                "client": str(daily["client"]),
                "session_id": str(daily["session_id"]),
            },
        ).to_pylist() == [
            {
                "first_seen_at": original_session["first_seen_at"],
                "last_seen_at": session["last_seen_at"],
            }
        ]
        for table in (*targets, "daily_cost", "collection_status", "collection_ledger"):
            assert _rows_for_source(
                remote, table, live_settings.source_id
            ) == _rows_for_source(local, table, live_settings.source_id), table
        before_stale = {
            table: _rows_for_source(remote, table, live_settings.source_id)
            for table in targets
        }
        stale = selected(_normalized_bundle(collection_bundle, live_settings.source_id))
        stale_result = persist_with_retries(configuration, stale, _LOG)
        assert (stale_result.inserted, stale_result.updated) == (0, 0)
        for table, before in before_stale.items():
            assert _rows_for_source(remote, table, live_settings.source_id) == before
        later_retry = persist_with_retries(configuration, later, _LOG)
        assert (later_retry.inserted, later_retry.updated) == (0, 0)
        assert remote.query(
            "SELECT count(*) AS n FROM collection_runs WHERE source_id = :source_id",
            {"source_id": live_settings.source_id},
        ).to_pylist() == [{"n": 3}]
    finally:
        local.close()
        remote.close()


def test_live_concurrent_sources_persist_independently(
    live_settings: LiveSettings, collection_bundle: CollectionBundle
) -> None:
    """Commit two source namespaces through concurrent remote transactions."""
    first_source = str(uuid4())
    second_source = str(uuid4())
    first = _normalized_bundle(collection_bundle, first_source)
    second = _normalized_bundle(collection_bundle, second_source)
    configuration = ConfigurationManager(live_settings.config_path).load()
    configs: tuple[UsageBassoonConfig, UsageBassoonConfig] = (
        replace(configuration, source_id=first_source),
        replace(configuration, source_id=second_source),
    )

    def persist(item: tuple[UsageBassoonConfig, NormalizedBundle]) -> PersistSummary:
        """Persist one source using its own configured identity."""
        return persist_with_retries(item[0], item[1], _LOG)

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(
            executor.map(
                persist,
                zip(configs, (first, second), strict=True),
            )
        )
    assert all(outcome.inserted > 0 for outcome in outcomes)
    backend = _backend()
    try:
        assert backend.query(
            "SELECT count(*) AS n FROM collection_runs "
            "WHERE run_id IN (:first_run, :second_run)",
            {"first_run": first.run_id, "second_run": second.run_id},
        ).to_pylist() == [{"n": 2}]
        assert backend.query(
            "SELECT count(DISTINCT source_id) AS n FROM sessions "
            "WHERE source_id IN (:first_source, :second_source)",
            {"first_source": first_source, "second_source": second_source},
        ).to_pylist() == [{"n": 2}]
    finally:
        backend.close()


def test_live_batch_rolls_back_after_late_failure(
    collection_bundle: CollectionBundle,
) -> None:
    bundle = _normalized_bundle(collection_bundle, str(uuid4()))
    backend = _backend()
    try:
        batch = PersistenceBatch(
            bundle.run_id,
            (
                CurrentStateWrite(
                    "daily_stats",
                    bundle.tables["daily_stats"],
                    ("source_id", "day", "client", "session_id", "model"),
                    ("total_tokens",),
                ),
            ),
            {"missing_history": bundle.tables["collection_ledger"]},
            bundle.tables["collection_ledger"],
        )
        with pytest.raises(duckdb.Error, match="missing_history"):
            backend.persist_batch(batch)
        assert not backend.has_committed_run(bundle.run_id)
        assert (
            backend.query(
                "SELECT * FROM daily_stats WHERE source_id = :source_id",
                {"source_id": bundle.tables["daily_stats"]["source_id"][0].as_py()},
            ).num_rows
            == 0
        )
    finally:
        backend.close()


def test_live_snapshot_stream_reads_one_transaction_state() -> None:
    """Exclude a remote commit between lazy table reads and release the read scope."""
    reader = _backend()
    writer = _backend()
    source_id = str(uuid4())
    try:
        with reader.stream_snapshot(("sessions", "notes")) as snapshot:
            assert snapshot.captured_at.tzinfo is not None
            list(snapshot.tables["sessions"])
            stamp = datetime.now(UTC)
            writer.append(
                "notes",
                observations(
                    pa.table(
                        {
                            "source_id": [source_id],
                            "client": ["codex"],
                            "session_id": ["between-streams"],
                            "note": ["committed after capture"],
                            "created_at": [stamp],
                            "collected_at": [stamp],
                        }
                    )
                ),
            )
            assert writer.query(
                "SELECT count(*) AS n FROM notes WHERE source_id = :source_id",
                {"source_id": source_id},
            ).to_pylist() == [{"n": 1}]
            assert all(
                row["source_id"] != source_id
                for batch in snapshot.tables["notes"]
                for row in batch.to_pylist()
            )
        assert reader.query(
            "SELECT count(*) AS n FROM notes WHERE source_id = :source_id",
            {"source_id": source_id},
        ).to_pylist() == [{"n": 1}]
    finally:
        reader.close()
        writer.close()


def test_live_doctor_keeps_first_query_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exclude drift committed after doctor's first query until its next report."""
    reader = _backend()
    writer = _backend()
    source_id = str(uuid4())
    original_query = reader.query
    committed = False

    def read_then_commit_drift(
        sql: str, parameters: Mapping[str, str] | None = None
    ) -> pa.Table:
        """Commit through another connection once the diagnostic snapshot exists."""
        nonlocal committed
        result = original_query(sql, parameters)
        if sql == "SELECT 1 AS doctor_ok" and not committed:
            stamp = datetime.now(UTC)
            with writer.transaction():
                writer.append(
                    "schema_drift_events",
                    observations(
                        pa.table(
                            {
                                "source_id": [source_id],
                                "run_id": [str(uuid4())],
                                "domain": ["models"],
                                "tokscale_ver": ["4.15.1"],
                                "drift_key": ["unknown_field:entries[].extra"],
                                "drift_kind": ["unknown_field"],
                                "path": ["entries[].extra"],
                                "detail": ["committed after first doctor query"],
                                "contract_tokscale_ver": ["4.15.1"],
                                "created_at": [stamp],
                                "collected_at": [stamp],
                                "resolved": [False],
                                "observation_count": [1],
                            }
                        )
                    ),
                )
            committed = True
        return result

    monkeypatch.setattr(reader, "query", read_then_commit_drift)
    try:
        first_report = run_doctor(
            reader,
            backend_name="motherduck",
            database=_DATABASE,
            snapshot_enabled=False,
        )
        assert committed
        assert first_report.status == "ok", first_report.checks
        first_drift = next(
            check for check in first_report.checks if check.name == "schema_drift"
        )
        assert first_drift.status == "ok"
        assert first_drift.message == "no unresolved events"
        assert reader.query(
            "SELECT count(*) AS n FROM open_schema_drift_events "
            "WHERE source_id = :source_id",
            {"source_id": source_id},
        ).to_pylist() == [{"n": 1}]
        next_report = run_doctor(
            reader,
            backend_name="motherduck",
            database=_DATABASE,
            snapshot_enabled=False,
        )
        assert not next_report.errors, next_report.checks
        next_drift = next(
            check for check in next_report.checks if check.name == "schema_drift"
        )
        assert next_drift.status == "warning"
        assert next_drift.message == "1 unresolved event(s)"
    finally:
        reader.close()
        writer.close()


def test_live_configured_clients_and_curation_preserve_source_identity(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Exercise ambient auth, real CLI/library reads, global tags, and scoped notes."""
    source_a, source_b = live_settings.source_id, str(uuid4())
    config_a = _local_snapshot_config(live_settings, tmp_path, source_a)
    config_b = _local_snapshot_config(live_settings, tmp_path, source_b)
    remote = _backend()
    session = collection_bundle.report_rows[0]
    target = {"client": session.client, "session_id": session.session_id}
    options = ("--client", session.client, "--session", session.session_id)
    tag = "motherduck-" + str(uuid4())
    renamed, reserved = tag + "-renamed", tag + "-reserved"
    runner = CliRunner()

    def invoke(*parts: str, config: Path = config_a) -> str:
        """Open the configured remote backend through the real command path."""
        result = runner.invoke(app, [*parts, "--config", str(config)])
        output = plain_cli_output(result.output)
        assert result.exit_code == 0, f"{parts!r}: {output}\n{result.exception!r}"
        return output

    def preflight(_configuration: object) -> tuple[tuple[str, ...], str]:
        """Keep backend coverage independent of the installed tokscale binary."""
        return ("tokscale",), collection_bundle.graph.meta.version

    monkeypatch.setattr("usagebassoon.cli.doctor.preflight_tokscale", preflight)
    try:
        persist_run(remote, _normalized_bundle(collection_bundle, source_a))
        persist_run(remote, _normalized_bundle(collection_bundle, source_b))
        facts_before = normalized_records(remote.query("SELECT * FROM daily_stats"))
        assert "Initialized motherduck schema" in invoke("init")
        assert normalized_records(remote.query("SELECT * FROM daily_stats")) == (
            facts_before
        )
        library = connect(config_a)
        try:
            assert isinstance(library, MotherDuckBackend)
            assert library.query("SELECT current_database() AS name").to_pylist() == [
                {"name": _DATABASE}
            ]
        finally:
            library.close()
        result = query_arrow(
            "SELECT source_id, session_id FROM report_daily_usage "
            f"WHERE source_id = '{source_a}' LIMIT 1000",
            config=config_a,
        )
        assert result.num_rows > 0
        assert set(result.column("source_id").to_pylist()) == {source_a}
        query_path = tmp_path / "query.json"
        invoke(
            "query",
            "report_daily_usage",
            "--filter",
            f"source_id={source_a}",
            "--format",
            "json",
            "--output",
            str(query_path),
        )
        queried = cast(list[dict[str, object]], json.loads(query_path.read_text()))
        assert len(queried) == result.num_rows
        assert {row["source_id"] for row in queried} == {source_a}
        assert "Model Token Usage" in invoke("report", "models", "--source", source_a)
        export_path = tmp_path / "sessions.json"
        invoke("export", "sessions", str(export_path), "--format", "json", "--raw")
        exported = cast(list[dict[str, object]], json.loads(export_path.read_text()))
        assert {row["source_id"] for row in exported} == {source_a, source_b}

        invoke("tag", "add", tag, *options)
        first_tag = remote.query(
            "SELECT source_id, created_at FROM current_tags "
            "WHERE client = :client AND session_id = :session_id AND tag = :tag",
            {**target, "tag": tag},
        ).to_pylist()[0]
        assert first_tag["source_id"] == source_a
        assert "Already present" in invoke("tag", "add", tag, *options, config=config_b)
        invoke("tag", "add", reserved, *options)
        assert "already assigned" in invoke(
            "tag", "rename", tag, reserved, *options, config=config_b
        )
        assert remote.query(
            "SELECT tag FROM current_tags "
            "WHERE client = :client AND session_id = :session_id ORDER BY tag",
            target,
        ).to_pylist() == [{"tag": tag}, {"tag": reserved}]
        assert "Renamed" in invoke(
            "tag", "rename", tag, renamed, *options, config=config_b
        )
        assert remote.query(
            "SELECT source_id, created_at FROM current_tags "
            "WHERE client = :client AND session_id = :session_id AND tag = :tag",
            {**target, "tag": renamed},
        ).to_pylist() == [
            {"source_id": source_b, "created_at": first_tag["created_at"]}
        ]
        assert remote.query(
            "SELECT source_id FROM session_tags "
            "WHERE client = :client AND session_id = :session_id AND tag = :tag "
            "ORDER BY source_id",
            {**target, "tag": renamed},
        ).to_pylist() == [
            {"source_id": value} for value in sorted((source_a, source_b))
        ]

        invoke("note", "set", "first source note", *options)
        invoke("note", "set", "second source note", *options, config=config_b)
        invoke("note", "set", "edited first source note", *options)
        expected_notes = {
            source_a: "edited first source note",
            source_b: "second source note",
        }
        assert {
            row["source_id"]: row["note"]
            for row in remote.query(
                "SELECT source_id, note FROM noted_sessions "
                "WHERE client = :client AND session_id = :session_id",
                target,
            ).to_pylist()
        } == expected_notes
        invoke("doctor")
        invoke("audit", "runs")
        invoke("note", "remove", *options)
        assert remote.query(
            "SELECT source_id, note FROM session_notes "
            "WHERE client = :client AND session_id = :session_id",
            target,
        ).to_pylist() == [{"source_id": source_b, "note": "second source note"}]
        invoke("note", "remove", *options, config=config_b)
        invoke("tag", "remove", renamed, *options)
        invoke("tag", "remove", reserved, *options, config=config_b)
        assert (
            remote.query(
                "SELECT * FROM session_notes "
                "WHERE client = :client AND session_id = :session_id",
                target,
            ).num_rows
            == 0
        )
        assert (
            remote.query(
                "SELECT * FROM session_tags "
                "WHERE client = :client AND session_id = :session_id",
                target,
            ).num_rows
            == 0
        )
        with monkeypatch.context() as missing_credentials:
            missing_credentials.delenv("MOTHERDUCK_TOKEN")
            rejected = runner.invoke(
                app, ["query", "report_summary", "--config", str(config_a)]
            )
        assert rejected.exit_code != 0
        assert "MOTHERDUCK_TOKEN" in plain_cli_output(rejected.output)
        assert normalized_records(remote.query("SELECT * FROM daily_stats")) == (
            facts_before
        )
    finally:
        remote.close()


def test_live_restore_receipt_survives_lost_reply_and_reconnection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolve a real committed restore and refuse replay through a new connection."""
    source = DuckDBBackend(":memory:")
    remote = _backend()
    attempts: list[str] = []
    original = remote.restore_snapshot

    def lost_reply(
        files: Mapping[str, Path], *, operation_id: str, snapshot_id: str
    ) -> None:
        original(files, operation_id=operation_id, snapshot_id=snapshot_id)
        attempts.append(operation_id)
        raise OSError("MotherDuck restore acknowledgement lost")

    def forbidden_replay(*_args: object, **_kwargs: object) -> None:
        pytest.fail("a committed restore was replayed after reconnecting")

    try:
        source.apply_ddl()
        note = NoteAssignment(str(uuid4()), "codex", "receipt-session", "durable note")
        set_note(source, note)
        expected = source.query("SELECT * FROM current_notes").to_pylist()
        store = SnapshotArchiver(str(tmp_path / "archive"))
        uri = store.write(source, run_id="receipt", manual=True)
        assert uri is not None
        notices: list[str] = []
        monkeypatch.setattr(remote, "restore_snapshot", lost_reply)
        assert store.restore(remote, uri, notice=notices.append)["notes"] == 1
        assert any("acknowledgement was interrupted" in notice for notice in notices)
        assert len(attempts) == 1
        remote.close()
        remote = _backend()
        monkeypatch.setattr(remote, "restore_snapshot", forbidden_replay)
        assert remote.restore_committed(attempts[0])
        notices.clear()
        assert store.restore(remote, uri, notice=notices.append)["notes"] == 1
        assert any("already committed" in notice for notice in notices)
        assert remote.query("SELECT * FROM current_notes").to_pylist() == expected
        assert remote.query(
            "SELECT count(*) AS n FROM restore_receipts "
            "WHERE operation_id = :operation",
            {"operation": attempts[0]},
        ).to_pylist() == [{"n": 1}]
    finally:
        remote.close()
        source.close()


def test_live_portable_snapshots_and_atomic_restore(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify both snapshot directions, restore safety, and transaction rollback."""
    source_a, source_b = live_settings.source_id, str(uuid4())
    capture_config = _local_snapshot_config(live_settings, tmp_path, source_a)
    restore_config = _local_snapshot_config(
        live_settings, tmp_path / "restore", source_a
    )
    captured = SnapshotArchiver.from_config(ConfigurationManager(capture_config).load())
    portable = SnapshotArchiver.from_config(ConfigurationManager(restore_config).load())
    remote = _backend()
    local = DuckDBBackend(":memory:")
    runner = CliRunner()
    session = collection_bundle.report_rows[0]

    def rows(backend: StorageBackend) -> dict[str, list[dict[str, object]]]:
        """Compare every persisted fact, curation row, and audit/debug stream."""
        return {
            table: normalized_records(backend.query(f'SELECT * FROM "{table}"'))
            for table in SNAPSHOT_TABLES
        }

    try:
        local.apply_ddl()
        persist_run(remote, _normalized_bundle(collection_bundle, source_a))
        persist_run(remote, _normalized_bundle(collection_bundle, source_b))
        add_tag(
            remote,
            TagAssignment(
                "session",
                source_a,
                "restored-tag",
                client=session.client,
                session_id=session.session_id,
            ),
        )
        for source_id in (source_a, source_b):
            set_note(
                remote,
                NoteAssignment(
                    source_id,
                    session.client,
                    session.session_id,
                    f"note for {source_id}",
                ),
            )
        stamp = datetime.now(UTC)
        debug_common: dict[str, list[object]] = {
            "source_id": [source_a],
            "run_id": [str(uuid4())],
            "created_at": [stamp],
            "collected_at": [stamp],
            "resolved": [False],
            "observation_count": [1],
        }
        debug_payloads: dict[str, dict[str, list[object]]] = {
            "schema_drift_events": {
                "domain": ["models"],
                "tokscale_ver": [collection_bundle.graph.meta.version],
                "drift_key": ["unknown_field:entries[].archive"],
                "drift_kind": ["unknown_field"],
                "path": ["entries[].archive"],
                "detail": ["live archive drift"],
                "contract_tokscale_ver": [collection_bundle.graph.meta.version],
            },
            "reconciliation_issues": {
                "check_name": ["tokens"],
                "issue_key": ["archive-check"],
                "message": ["live archive reconciliation"],
            },
        }
        for table, payload in debug_payloads.items():
            remote.append(table, observations(pa.table({**debug_common, **payload})))
        expected = rows(remote)
        snapshot = runner.invoke(app, ["snapshot", "--config", str(capture_config)])
        assert snapshot.exit_code == 0, plain_cli_output(snapshot.output)
        assert len(captured.list_snapshots()) == 1
        captured.restore(local)
        assert rows(local) == expected
        assert portable.write(local, run_id=str(uuid4())) is not None

        command = ["restore", "--config", str(restore_config)]
        rejected = runner.invoke(app, command, input="y\n")
        assert rejected.exit_code != 0
        assert "restore requires an empty warehouse" in plain_cli_output(
            rejected.output
        )
        assert rows(remote) == expected

        # All test writers are stopped before resetting the dedicated destination.
        _reset_test_schema(remote)
        uninitialized = runner.invoke(
            app, ["query", "report_summary", "--config", str(restore_config)]
        )
        assert uninitialized.exit_code != 0
        assert "not initialized" in plain_cli_output(uninitialized.output)
        initialized = runner.invoke(
            app, ["init", "--restore", "--config", str(restore_config)]
        )
        assert initialized.exit_code == 0, plain_cli_output(initialized.output)
        declined = runner.invoke(app, command, input="n\n")
        assert declined.exit_code != 0
        assert "Aborted" in plain_cli_output(declined.output)
        assert all(not records for records in rows(remote).values())
        original_append = remote.append
        written: list[str] = []

        def fail_after_first_table(table: str, data: pa.Table) -> None:
            """Inject a late failure after a real remote table has been restored."""
            if written:
                raise RuntimeError("injected restore failure")
            original_append(table, data)
            written.append(table)
            assert remote.query(f'SELECT * FROM "{table}"').num_rows == data.num_rows

        with monkeypatch.context() as failed_restore:
            failed_restore.setattr(remote, "append", fail_after_first_table)
            with pytest.raises(RuntimeError, match="injected restore failure"):
                portable.restore(remote)
        assert len(written) == 1
        assert all(not records for records in rows(remote).values())
        restored = runner.invoke(app, command, input="y\n")
        assert restored.exit_code == 0, (
            f"{plain_cli_output(restored.output)}\n{restored.exception!r}"
        )
        assert "Restored" in plain_cli_output(restored.output)
        assert rows(remote) == expected
        assert remote.query(
            "SELECT source_id, note FROM noted_sessions "
            "WHERE client = :client AND session_id = :session_id ORDER BY source_id",
            {"client": session.client, "session_id": session.session_id},
        ).to_pylist() == [
            {"source_id": value, "note": f"note for {value}"}
            for value in sorted((source_a, source_b))
        ]
        from usagebassoon.audit import audit_sources

        assert {row["source_id"] for row in audit_sources(remote)} == {
            source_a,
            source_b,
        }
    finally:
        remote.close()
        local.close()
