# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_bigquery_live.py — Opt-in live BigQuery integration tests.

Run with ``USAGEBASSOON_BIGQUERY_LIVE=1 uv run pytest -m bigquery_live``.
The suite accepts only the dedicated ``usagebassoon_it`` dataset. Set
``USAGEBASSOON_BIGQUERY_LIVE_RESET=1`` to enable the final snapshot/restore
test, which deletes and recreates tables in that disposable dataset.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from typer.testing import CliRunner

from tests.conftest import (
    EXPECTED_DAILY_STATS_ROWS,
    EXPECTED_DAYS,
    EXPECTED_REPORT_ROWS,
)
from usagebassoon.backends.base import CurrentStateWrite, PersistenceBatch
from usagebassoon.backends.bigquery import BigQueryBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.collector import _persist_with_retries
from usagebassoon.config import ConfigurationManager
from usagebassoon.merge import PersistSummary, persist_run
from usagebassoon.normalizer import CollectionBundle, NormalizedBundle, normalize
from usagebassoon.snapshots import SnapshotStore

pytestmark = pytest.mark.bigquery_live

_DATASET = "usagebassoon_it"
_LOCATION = "US"


@dataclass(frozen=True, slots=True)
class LiveSettings:
    """Connection and configuration details for the disposable test dataset."""

    project: str
    location: str
    source_id: str
    config_path: Path

    @property
    def dataset_id(self) -> str:
        """Return the fully qualified integration dataset identifier."""
        return f"{self.project}.{_DATASET}"


def _require_live_access() -> None:
    """Skip remote calls unless the caller explicitly enables them."""
    if os.environ.get("USAGEBASSOON_BIGQUERY_LIVE") != "1":
        pytest.skip("set USAGEBASSOON_BIGQUERY_LIVE=1 to run live BigQuery tests")
    dataset = os.environ.get("USAGEBASSOON_BIGQUERY_DATASET", _DATASET)
    if dataset != _DATASET:
        pytest.fail(
            "live BigQuery tests may run only against the dedicated "
            f"{_DATASET!r} dataset"
        )


def _reset_test_schema(client: bigquery.Client, dataset_id: str) -> None:
    """Remove leftover relations from the dedicated integration dataset.

    Args:
        client: Authenticated BigQuery client for the test project.
        dataset_id: Fully qualified dedicated integration dataset identifier.
    """
    try:
        relations = list(client.list_tables(dataset_id))
    except NotFound:
        return
    for relation in relations:
        if relation.table_type in {"VIEW", "MATERIALIZED_VIEW"}:
            client.delete_table(relation.reference, not_found_ok=True)
    for relation in relations:
        if relation.table_type not in {"VIEW", "MATERIALIZED_VIEW"}:
            client.delete_table(relation.reference, not_found_ok=True)


@pytest.fixture(scope="module")
def live_settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[LiveSettings]:
    """Create an explicit ADC-backed config for the disposable dataset."""
    _require_live_access()
    location = os.environ.get("USAGEBASSOON_BIGQUERY_LOCATION", _LOCATION)
    client = bigquery.Client(
        project=os.environ.get("USAGEBASSOON_BIGQUERY_PROJECT"),
        location=location,
    )
    try:
        project = client.project
        if not project:
            pytest.fail("BigQuery ADC did not resolve a project")
        source_id = str(uuid4())
        config_path = tmp_path_factory.mktemp("bigquery-live") / "config.toml"
        log_directory = config_path.parent / "logs"
        config_path.write_text(
            f'source_id = "{source_id}"\n'
            'backend = "bigquery"\n'
            f'database = "{_DATASET}"\n'
            "[bigquery]\n"
            f'project = "{project}"\n'
            f'location = "{location}"\n'
            "[collection]\n"
            "max_retries = 3\n"
            "retry_initial_seconds = 0.1\n"
            "[logging]\n"
            f'directory = "{log_directory}"\n'
        )
        backend = BigQueryBackend(project, _DATASET, location=location)
        try:
            _reset_test_schema(client, f"{project}.{_DATASET}")
            backend.apply_ddl()
            dataset = backend.client.get_dataset(f"{project}.{_DATASET}")
            assert isinstance(dataset.location, str)
            assert dataset.location.casefold() == location.casefold()
        finally:
            backend.close()
        yield LiveSettings(project, location, source_id, config_path)
    finally:
        client.close()


def _backend(settings: LiveSettings) -> BigQueryBackend:
    """Open a fresh BigQuery backend for one live assertion."""
    return BigQueryBackend(settings.project, _DATASET, location=settings.location)


def _normalized_bundle(
    settings: LiveSettings,
    collection_bundle: CollectionBundle,
    *,
    source_id: str | None = None,
) -> NormalizedBundle:
    """Create a collision-free golden-fixture batch for the live dataset."""
    return normalize(
        replace(
            collection_bundle,
            run_id=str(uuid4()),
            source_id=source_id or settings.source_id,
        )
    )


def _rows_for_source(
    backend: BigQueryBackend | DuckDBBackend,
    table: str,
    source_id: str,
    order_by: str,
) -> list[dict[str, object]]:
    """Return deterministic current-state rows for one source namespace."""
    return backend.query(
        f"SELECT * FROM {table} WHERE source_id = '{source_id}' ORDER BY {order_by}"
    ).to_pylist()


def _stage_tables(
    client: bigquery.Client,
    dataset_id: str,
    *,
    run_id: str | None = None,
) -> list[str]:
    """List leftover remote staging tables for one dedicated test run."""
    suffix = run_id.replace("-", "") if run_id else ""
    return sorted(
        item.table_id
        for item in client.list_tables(dataset_id)
        if item.table_id.startswith("_stage_") and item.table_id.endswith(suffix)
    )


def test_live_batch_matches_duckdb_and_retries_idempotently(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
) -> None:
    """Persist the full golden batch and compare BigQuery current state to DuckDB."""
    normalized = _normalized_bundle(live_settings, collection_bundle)
    duckdb_backend = DuckDBBackend(":memory:")
    bigquery_backend = _backend(live_settings)
    expected_inserted = (
        EXPECTED_REPORT_ROWS
        + EXPECTED_DAILY_STATS_ROWS
        + EXPECTED_DAYS
        + sum(len(prices) for prices in collection_bundle.pricing_by_day.values())
        + len(collection_bundle.processed_targets)
    )
    try:
        duckdb_backend.apply_ddl()
        bigquery_backend.apply_ddl()
        expected = persist_run(duckdb_backend, normalized)
        actual = persist_run(bigquery_backend, normalized)

        assert (actual.inserted, actual.updated) == (
            expected_inserted,
            0,
        )
        assert actual == expected
        for table, order_by in (
            ("sessions", "client, session_id"),
            ("session_model_stats", "client, session_id, model"),
            ("daily_stats", "day, client, session_id, model"),
            ("daily_activity", "day"),
            ("price_versions", "day, model"),
            ("daily_processed_state", "day, target"),
        ):
            assert _rows_for_source(
                bigquery_backend,
                table,
                normalized.tables["ingest_runs"].column("source_id").to_pylist()[0],
                order_by,
            ) == _rows_for_source(
                duckdb_backend,
                table,
                normalized.tables["ingest_runs"].column("source_id").to_pylist()[0],
                order_by,
            )
        assert bigquery_backend.query(
            "SELECT rows_inserted, rows_updated FROM ingest_runs "
            f"WHERE run_id = '{normalized.run_id}'"
        ).to_pylist() == [{"rows_inserted": expected_inserted, "rows_updated": 0}]
        price_count = sum(
            len(prices) for prices in collection_bundle.pricing_by_day.values()
        )
        assert bigquery_backend.query(
            "SELECT count(*) AS n FROM price_versions "
            f"WHERE source_id = '{live_settings.source_id}'"
        ).to_pylist() == [{"n": price_count}]

        retried = persist_run(bigquery_backend, normalized)
        assert (retried.inserted, retried.updated) == (0, 0)
        assert all(
            result.inserted == 0 and result.updated == 0
            for result in retried.per_table.values()
        )
        assert bigquery_backend.query(
            "SELECT count(*) AS n FROM ingest_runs "
            f"WHERE run_id = '{normalized.run_id}'"
        ).to_pylist() == [{"n": 1}]
        assert (
            _stage_tables(
                bigquery_backend.client,
                live_settings.dataset_id,
                run_id=normalized.run_id,
            )
            == []
        )

        sessions_schema = bigquery_backend.client.get_table(
            f"{live_settings.dataset_id}.sessions"
        ).schema
        schema_by_name = {field.name: field for field in sessions_schema}
        assert schema_by_name["models_used"].field_type == "STRING"
        assert schema_by_name["models_used"].mode == "REPEATED"
        assert schema_by_name["created_at"].field_type == "TIMESTAMP"
        daily_schema = bigquery_backend.client.get_table(
            f"{live_settings.dataset_id}.daily_stats"
        ).schema
        assert {field.name: field.field_type for field in daily_schema}["day"] == "DATE"
    finally:
        duckdb_backend.close()
        bigquery_backend.close()


def test_live_batch_rolls_back_after_staging_and_logs_cleanup(
    live_settings: LiveSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Rollback user writes when a later staged append target does not exist."""
    backend = _backend(live_settings)
    run_id = str(uuid4())
    source_id = str(uuid4())
    current = pa.table(
        {
            "source_id": [source_id],
            "day": [date(2026, 9, 16)],
            "intensity": [1],
            "active_time_ms": [100],
            "last_updated_at": [datetime(2026, 9, 16, tzinfo=UTC)],
        }
    )
    batch = PersistenceBatch(
        run_id=run_id,
        current_state=(
            CurrentStateWrite(
                "daily_activity",
                current,
                ("source_id", "day"),
                ("intensity", "active_time_ms"),
            ),
        ),
        append_only={"missing_history": pa.table({"run_id": [run_id]})},
        ingest_runs=pa.table({"run_id": [run_id]}),
    )
    try:
        backend.apply_ddl()
        with (
            caplog.at_level(logging.INFO, logger="usagebassoon"),
            pytest.raises(Exception, match="missing_history"),
        ):
            backend.persist_batch(batch)
        assert backend.query(
            f"SELECT count(*) AS n FROM daily_activity WHERE source_id = '{source_id}'"
        ).to_pylist() == [{"n": 0}]
        assert backend.query(
            f"SELECT count(*) AS n FROM ingest_runs WHERE run_id = '{run_id}'"
        ).to_pylist() == [{"n": 0}]
        assert (
            _stage_tables(
                backend.client,
                live_settings.dataset_id,
                run_id=run_id,
            )
            == []
        )
        assert any(
            "removed BigQuery staging table" in record.message
            for record in caplog.records
        )
    finally:
        backend.close()


def test_live_location_and_credential_errors_are_actionable(
    live_settings: LiveSettings,
    tmp_path: Path,
) -> None:
    """Keep location mismatch and unusable credential diagnostics predictable."""
    mismatched = BigQueryBackend(live_settings.project, _DATASET, location="EU")
    try:
        with pytest.raises(
            ValueError,
            match="does not match existing dataset location",
        ):
            mismatched.apply_ddl()
    finally:
        mismatched.close()
    with pytest.raises(RuntimeError, match="credentials file could not be loaded"):
        BigQueryBackend(
            live_settings.project,
            _DATASET,
            credentials_file=tmp_path / "missing-service-account.json",
        )


def test_live_concurrent_sources_use_distinct_stages_and_retry(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
) -> None:
    """Persist independent source namespaces concurrently through the retry path."""
    first_source_id = str(uuid4())
    second_source_id = str(uuid4())
    first = _normalized_bundle(
        live_settings, collection_bundle, source_id=first_source_id
    )
    second = _normalized_bundle(
        live_settings, collection_bundle, source_id=second_source_id
    )
    configuration = ConfigurationManager(live_settings.config_path).load()

    def persist(normalized: NormalizedBundle) -> PersistSummary:
        """Persist one run through the same bounded retry path as collection."""
        return _persist_with_retries(
            configuration,
            normalized,
            logging.getLogger("usagebassoon-bigquery-live"),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(persist, (first, second)))
    assert all(outcome.inserted >= 0 for outcome in outcomes)

    backend = _backend(live_settings)
    try:
        assert backend.query(
            "SELECT count(*) AS n FROM ingest_runs "
            f"WHERE run_id IN ('{first.run_id}', '{second.run_id}')"
        ).to_pylist() == [{"n": 2}]
        assert backend.query(
            "SELECT count(DISTINCT source_id) AS n FROM sessions "
            f"WHERE source_id IN ('{first_source_id}', '{second_source_id}')"
        ).to_pylist() == [{"n": 2}]
        assert _stage_tables(backend.client, live_settings.dataset_id) == []
    finally:
        backend.close()


def test_live_cli_commands_except_report(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Exercise configured BigQuery CLI commands other than out-of-scope report."""
    normalized = _normalized_bundle(live_settings, collection_bundle)
    backend = _backend(live_settings)
    try:
        backend.apply_ddl()
        persist_run(backend, normalized)
    finally:
        backend.close()

    session = collection_bundle.report_rows[0]
    runner = CliRunner()
    export_path = tmp_path / "sessions.json"
    monkeypatch.setenv("HOME", str(tmp_path))
    commands = (
        ["init", "--config", str(live_settings.config_path)],
        [
            "query",
            "SELECT count(*) AS n FROM sessions",
            "--config",
            str(live_settings.config_path),
        ],
        [
            "export",
            "sessions",
            str(export_path),
            "--format",
            "json",
            "--config",
            str(live_settings.config_path),
        ],
        [
            "tag",
            "live-test",
            "--client",
            session.client,
            "--config",
            str(live_settings.config_path),
        ],
        [
            "note",
            "live integration note",
            "--client",
            session.client,
            "--session",
            session.session_id,
            "--config",
            str(live_settings.config_path),
        ],
        ["doctor", "--config", str(live_settings.config_path)],
        ["audit", "--config", str(live_settings.config_path)],
        ["snapshot", "--config", str(live_settings.config_path)],
    )
    results = [runner.invoke(app, command) for command in commands]

    assert all(result.exit_code == 0 for result in results)
    assert export_path.is_file()
    snapshots = SnapshotStore(f"file://{tmp_path}/.usagebassoon/snapshots")
    assert snapshots.list_snapshots()


def test_live_restore_requires_an_explicit_disposable_reset(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Restore a BigQuery snapshot only after an explicit test-dataset reset."""
    if os.environ.get("USAGEBASSOON_BIGQUERY_LIVE_RESET") != "1":
        pytest.skip("set USAGEBASSOON_BIGQUERY_LIVE_RESET=1 to run live restore")

    normalized = _normalized_bundle(live_settings, collection_bundle)
    backend = _backend(live_settings)
    try:
        backend.apply_ddl()
        persist_run(backend, normalized)
    finally:
        backend.close()

    monkeypatch.setenv("HOME", str(tmp_path))
    runner = CliRunner()
    snapshot = runner.invoke(
        app,
        ["snapshot", "--config", str(live_settings.config_path)],
    )
    assert snapshot.exit_code == 0

    client = bigquery.Client(
        project=live_settings.project,
        location=live_settings.location,
    )
    try:
        for item in client.list_tables(live_settings.dataset_id):
            if item.table_type == "TABLE":
                client.delete_table(item.reference, not_found_ok=True)
    finally:
        client.close()

    restored = runner.invoke(
        app,
        ["restore", "--config", str(live_settings.config_path)],
    )
    assert restored.exit_code == 0
    source_id = normalized.tables["ingest_runs"].column("source_id").to_pylist()[0]
    backend = _backend(live_settings)
    try:
        assert backend.query(
            f"SELECT count(*) AS n FROM sessions WHERE source_id = '{source_id}'"
        ).to_pylist() == [{"n": EXPECTED_REPORT_ROWS}]
    finally:
        backend.close()
