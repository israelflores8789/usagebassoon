# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_bigquery_live.py — Opt-in live BigQuery integration tests.

Run with ``just test-bq-live`` against the disposable ``usagebassoon_it`` dataset.
Or, run with ``USAGEBASSOON_BIGQUERY_LIVE=1 uv run pytest -m bigquery_live``.

The ``bigquery_live`` marker selects these tests, and ``USAGEBASSOON_BIGQUERY_LIVE=1``
enables access to a preconfigured BigQuery dataset called ``usagebassoon_it``.

Set ``USAGEBASSOON_BIGQUERY_LIVE_RESET=1`` to enable the final snapshot/restore
test, which deletes and recreates tables in the preconfigured, disposable dataset.
"""

from __future__ import annotations

import logging
import os
import signal
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pyarrow as pa
import pytest
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from typer.testing import CliRunner

from tests._sql_parity import assert_view_results_match, seed_synthetic_data
from tests.conftest import (
    EXPECTED_DAILY_STATS_ROWS,
    EXPECTED_DAYS,
    EXPECTED_REPORT_ROWS,
)
from usagebassoon import persistence as persistence_module
from usagebassoon.archiver import SnapshotArchiver as SnapshotStore
from usagebassoon.backends.base import (
    CurrentStateWrite,
    PersistenceBatch,
    SourceLeaseBusy,
    SourceLeaseToken,
)
from usagebassoon.backends.bigquery import BigQueryBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.config import ConfigurationManager
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import NormalizedBundle, normalize
from usagebassoon.persistence import PersistSummary, persist_run, persist_with_retries
from usagebassoon.source_leases import source_lease

pytestmark = pytest.mark.bigquery_live

_DATASET = "usagebassoon_it"
_LOCATION = "US"
_LIVE_TEST_TIMEOUT_SECONDS = 300


@pytest.fixture(autouse=True)
def _bound_live_test_duration() -> Iterator[None]:
    """Fail a live test instead of leaving a CI worker blocked indefinitely."""
    if not hasattr(signal, "SIGALRM"):
        yield
        return

    def fail_timeout(_: int, __: object) -> None:
        """Raise a visible failure from the main pytest thread."""
        raise TimeoutError(
            "live BigQuery test exceeded "
            f"{_LIVE_TEST_TIMEOUT_SECONDS} seconds; inspect BigQuery jobs and retries"
        )

    previous_handler = signal.signal(signal.SIGALRM, fail_timeout)
    signal.setitimer(signal.ITIMER_REAL, _LIVE_TEST_TIMEOUT_SECONDS)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


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
            "[bigquery]\n"
            f'project = "{project}"\n'
            f'dataset = "{_DATASET}"\n'
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


def _assert_rows_match(
    table: str,
    actual: list[dict[str, object]],
    expected: list[dict[str, object]],
) -> None:
    """Assert row parity and report the first differing fields clearly."""
    if actual == expected:
        return
    for index, (actual_row, expected_row) in enumerate(
        zip(actual, expected, strict=False)
    ):
        differences = {
            field: (actual_row.get(field), expected_row.get(field))
            for field in actual_row.keys() | expected_row.keys()
            if actual_row.get(field) != expected_row.get(field)
        }
        if differences:
            pytest.fail(f"{table} row {index} differs by field: {differences!r}")
    pytest.fail(
        f"{table} row count differs: BigQuery={len(actual)}, DuckDB={len(expected)}"
    )


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


def _audit_only_batch(
    source_id: str,
    run_id: str,
    *,
    lease: SourceLeaseToken | None = None,
) -> PersistenceBatch:
    """Build a valid minimal batch for live lease and ledger assertions."""
    return PersistenceBatch(
        run_id=run_id,
        current_state=(),
        append_only={},
        ingest_runs=pa.table(
            {
                "run_id": [run_id],
                "source_id": [source_id],
                "started_at": [datetime.now(UTC)],
                "rows_inserted": [0],
                "rows_updated": [0],
            }
        ),
        lease=lease,
    )


def test_live_synthetic_views_match_duckdb(live_settings: LiveSettings) -> None:
    """Compare every view using purpose-built rows in native BigQuery."""
    duckdb_backend = DuckDBBackend(":memory:")
    bigquery_backend = _backend(live_settings)
    try:
        duckdb_backend.apply_ddl()
        bigquery_backend.apply_ddl()
        seed_synthetic_data(duckdb_backend)
        seed_synthetic_data(bigquery_backend)
        assert_view_results_match(duckdb_backend, bigquery_backend)
    finally:
        duckdb_backend.close()
        bigquery_backend.close()


def test_live_batch_matches_duckdb_and_retries_idempotently(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
) -> None:
    """Persist the full golden batch and compare BigQuery current state to DuckDB."""
    normalized = _normalized_bundle(live_settings, collection_bundle)
    duckdb_backend = DuckDBBackend(":memory:")
    bigquery_backend = _backend(live_settings)
    configuration = ConfigurationManager(live_settings.config_path).load()
    expected_inserted = (
        EXPECTED_REPORT_ROWS
        + EXPECTED_DAILY_STATS_ROWS
        + EXPECTED_DAYS
        + sum(len(prices) for prices in collection_bundle.pricing_by_day.values())
        + len(collection_bundle.ingest_status)
    )
    try:
        duckdb_backend.apply_ddl()
        bigquery_backend.apply_ddl()
        expected = persist_run(duckdb_backend, normalized)
        actual = persist_with_retries(
            configuration,
            normalized,
            logging.getLogger("usagebassoon-bigquery-live"),
        )

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
            ("ingest_status", "day, domain"),
        ):
            bigquery_rows = _rows_for_source(
                bigquery_backend,
                table,
                normalized.tables["ingest_runs"].column("source_id").to_pylist()[0],
                order_by,
            )
            duckdb_rows = _rows_for_source(
                duckdb_backend,
                table,
                normalized.tables["ingest_runs"].column("source_id").to_pylist()[0],
                order_by,
            )
            _assert_rows_match(
                table,
                bigquery_rows,
                duckdb_rows,
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

        retried = persist_with_retries(
            configuration,
            normalized,
            logging.getLogger("usagebassoon-bigquery-live"),
        )
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
        daily_types = {field.name: field.field_type for field in daily_schema}
        assert daily_types["day"] == "DATE"
        assert {
            name: daily_types[name]
            for name in (
                "perf_duration_ms",
                "perf_timed_tokens",
                "perf_sample_count",
                "perf_token_coverage",
                "tokscale_ms_per_1k_tokens",
            )
        } == {
            "perf_duration_ms": "INTEGER",
            "perf_timed_tokens": "INTEGER",
            "perf_sample_count": "INTEGER",
            "perf_token_coverage": "FLOAT",
            "tokscale_ms_per_1k_tokens": "FLOAT",
        }
    finally:
        duckdb_backend.close()
        bigquery_backend.close()


def test_live_source_lease_takeover_fences_stale_batches(
    live_settings: LiveSettings,
) -> None:
    """Reclaim an expired source and reject its former owner's batch."""
    first_backend = _backend(live_settings)
    second_backend = _backend(live_settings)
    source_id = str(uuid4())
    first_run = str(uuid4())
    first: SourceLeaseToken | None = None
    successor: SourceLeaseToken | None = None
    try:
        first_backend.ensure_source_lease(source_id)
        second_backend.ensure_source_lease(source_id)
        assert first_backend.query(
            "SELECT COUNT(*) AS n FROM source_leases WHERE source_id = @source_id",
            {"source_id": source_id},
        ).to_pylist() == [{"n": 1}]
        first = first_backend.claim_source_lease(source_id, first_run, str(uuid4()))
        assert first is not None
        assert (
            second_backend.claim_source_lease(source_id, str(uuid4()), str(uuid4()))
            is None
        )
        assert first_backend.renew_source_lease(first)

        other_source = str(uuid4())
        second_backend.ensure_source_lease(other_source)
        independent = second_backend.claim_source_lease(
            other_source, str(uuid4()), str(uuid4())
        )
        assert independent is not None
        second_backend.release_source_lease(independent)

        first_backend._lease_rows(
            f"UPDATE {first_backend._table_ref('source_leases')} "
            "SET lease_expires_at = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), "
            "INTERVAL 1 SECOND) WHERE source_id = @source_id AND fence = @fence",
            [
                bigquery.ScalarQueryParameter("source_id", "STRING", source_id),
                bigquery.ScalarQueryParameter("fence", "INT64", first.fence),
            ],
        )
        successor = second_backend.claim_source_lease(
            source_id, str(uuid4()), str(uuid4())
        )
        assert successor is not None
        assert successor.fence == first.fence + 1
        assert not first_backend.renew_source_lease(first)
        first_backend.release_source_lease(first)
        assert second_backend.renew_source_lease(successor)

        with pytest.raises(BadRequest, match="collection source lease was lost"):
            first_backend.persist_batch(
                _audit_only_batch(source_id, first_run, lease=first)
            )
        assert first_backend.query(
            "SELECT COUNT(*) AS n FROM ingest_runs WHERE run_id = @run_id",
            {"run_id": first_run},
        ).to_pylist() == [{"n": 0}]
        assert (
            _stage_tables(
                first_backend.client, live_settings.dataset_id, run_id=first_run
            )
            == []
        )
    finally:
        if first is not None:
            first_backend.release_source_lease(first)
        if successor is not None:
            second_backend.release_source_lease(successor)
        first_backend.close()
        second_backend.close()


def test_live_simultaneous_source_claims_have_one_owner(
    live_settings: LiveSettings,
) -> None:
    """Allow only one of two simultaneous claims on the same source row."""
    first_backend = _backend(live_settings)
    second_backend = _backend(live_settings)
    source_id = str(uuid4())
    start = Barrier(2)
    winner: SourceLeaseToken | None = None

    def claim(backend: BigQueryBackend) -> SourceLeaseToken | None:
        """Start a contender only after both BigQuery clients are ready."""
        start.wait()
        return backend.claim_source_lease(source_id, str(uuid4()), str(uuid4()))

    try:
        first_backend.ensure_source_lease(source_id)
        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(claim, (first_backend, second_backend)))
        owned = [token for token in claims if token is not None]
        assert len(owned) == 1
        winner = owned[0]
        assert first_backend.query(
            "SELECT fence, owner_id FROM source_leases WHERE source_id = @source_id",
            {"source_id": source_id},
        ).to_pylist() == [{"fence": winner.fence, "owner_id": winner.owner_id}]
        assert winner.fence == 1
    finally:
        if winner is not None:
            first_backend.release_source_lease(winner)
        first_backend.close()
        second_backend.close()


def test_live_same_run_lease_and_ledger_prevent_duplicate_audit_rows(
    live_settings: LiveSettings,
) -> None:
    """Block a competing run and make a later retry an idempotent no-op."""
    first_backend = _backend(live_settings)
    second_backend = _backend(live_settings)
    source_id = str(uuid4())
    run_id = str(uuid4())
    token: SourceLeaseToken | None = None
    try:
        first_backend.ensure_source_lease(source_id)
        token = first_backend.claim_source_lease(source_id, run_id, str(uuid4()))
        assert token is not None
        batch = _audit_only_batch(source_id, run_id)
        with pytest.raises(SourceLeaseBusy):
            second_backend.persist_batch(batch)
        committed = first_backend.persist_batch(replace(batch, lease=token))
        assert not committed.already_committed
        first_backend.release_source_lease(token)
        retried = second_backend.persist_batch(batch)
        assert retried.already_committed
        assert second_backend.query(
            "SELECT COUNT(*) AS n FROM ingest_runs WHERE run_id = @run_id",
            {"run_id": run_id},
        ).to_pylist() == [{"n": 1}]
        assert (
            _stage_tables(
                second_backend.client, live_settings.dataset_id, run_id=run_id
            )
            == []
        )
    finally:
        if token is not None:
            first_backend.release_source_lease(token)
        first_backend.close()
        second_backend.close()


def test_live_ambiguous_commit_retry_keeps_one_audit_row(
    live_settings: LiveSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry a committed run after its acknowledgement is lost."""
    run_id = str(uuid4())
    source_id = str(uuid4())
    bundle = NormalizedBundle(
        run_id, {"ingest_runs": _audit_only_batch(source_id, run_id).ingest_runs}
    )
    configuration = ConfigurationManager(live_settings.config_path).load()
    actual_persist = persistence_module.persist_run
    attempts = 0

    def lose_first_acknowledgement(
        backend: BigQueryBackend, normalized: NormalizedBundle
    ) -> PersistSummary:
        """Commit remotely, then simulate losing the first client response."""
        nonlocal attempts
        result = actual_persist(backend, normalized)
        attempts += 1
        if attempts == 1:
            raise BadRequest("transaction is aborted due to concurrent update")
        return result

    monkeypatch.setattr(persistence_module, "persist_run", lose_first_acknowledgement)
    summary = persistence_module.persist_with_retries(
        configuration,
        bundle,
        logging.getLogger("usagebassoon-bigquery-live"),
    )
    assert attempts == 2
    assert summary == PersistSummary(0, 0, {})
    backend = _backend(live_settings)
    try:
        assert backend.query(
            "SELECT COUNT(*) AS n FROM ingest_runs WHERE run_id = @run_id",
            {"run_id": run_id},
        ).to_pylist() == [{"n": 1}]
        assert (
            _stage_tables(backend.client, live_settings.dataset_id, run_id=run_id) == []
        )
    finally:
        backend.close()


def test_live_source_lease_context_releases_for_next_collection(
    live_settings: LiveSettings,
) -> None:
    """Hold the collection source from planning until context exit."""
    configuration = ConfigurationManager(live_settings.config_path).load()
    logger = logging.getLogger("usagebassoon-bigquery-live")
    contender = _backend(live_settings)
    try:
        with source_lease(configuration, str(uuid4()), logger) as active:
            active.check()
            assert (
                contender.claim_source_lease(
                    configuration.source_id, str(uuid4()), str(uuid4())
                )
                is None
            )
        next_token = contender.claim_source_lease(
            configuration.source_id, str(uuid4()), str(uuid4())
        )
        assert next_token is not None
        assert next_token.fence > active.token.fence
        contender.release_source_lease(next_token)
    finally:
        contender.close()


def test_live_snapshot_reads_tables_at_one_bigquery_timestamp(
    live_settings: LiveSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exclude a note committed between two historical table reads."""
    reader = _backend(live_settings)
    writer = _backend(live_settings)
    source_id = str(uuid4())
    original = reader._read_query_arrow
    reads = 0

    def read_then_write(job: bigquery.job.QueryJob) -> pa.Table:
        """Commit a note after the first snapshot table materializes."""
        nonlocal reads
        data = original(job)
        reads += 1
        if reads == 1:
            stamp = datetime.now(UTC)
            writer.append(
                "notes",
                pa.table(
                    {
                        "source_id": [source_id],
                        "client": ["codex"],
                        "session_id": ["between-reads"],
                        "note": ["committed after capture"],
                        "created_at": [stamp],
                        "updated_at": [stamp],
                    }
                ),
            )
        return data

    monkeypatch.setattr(reader, "_read_query_arrow", read_then_write)
    try:
        snapshot = reader.read_snapshot_tables(("sessions", "notes"))
        assert snapshot.captured_at.tzinfo is not None
        assert reads == 2
        assert all(
            row["source_id"] != source_id
            for row in snapshot.tables["notes"].to_pylist()
        )
        assert writer.query(
            "SELECT COUNT(*) AS n FROM notes WHERE source_id = @source_id",
            {"source_id": source_id},
        ).to_pylist() == [{"n": 1}]
    finally:
        reader.close()
        writer.close()


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
            "updated_at": [datetime(2026, 9, 16, tzinfo=UTC)],
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
        ingest_runs=pa.table({"run_id": [run_id], "source_id": [source_id]}),
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
        return persist_with_retries(
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


def test_live_cli_commands_including_models_report(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Exercise configured BigQuery CLI commands, including the models report."""
    normalized = _normalized_bundle(live_settings, collection_bundle)
    configuration = ConfigurationManager(live_settings.config_path).load()
    persist_with_retries(
        configuration,
        normalized,
        logging.getLogger("usagebassoon-bigquery-live"),
    )

    def fake_tokscale_preflight(_configuration: object) -> tuple[tuple[str, ...], str]:
        """Keep BigQuery CLI coverage independent of an installed tokscale binary."""
        return ("tokscale",), "4.15.1"

    monkeypatch.setattr(
        "usagebassoon.cli.doctor.preflight_tokscale", fake_tokscale_preflight
    )

    session = collection_bundle.report_rows[0]
    runner = CliRunner()
    export_path = tmp_path / "sessions.json"
    monkeypatch.setenv("HOME", str(tmp_path))
    commands = (
        ["init", "--config", str(live_settings.config_path)],
        [
            "query",
            "report_summary",
            "--limit",
            "1",
            "--config",
            str(live_settings.config_path),
        ],
        ["report", "models", "--config", str(live_settings.config_path)],
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
            "add",
            "live-test",
            "--client",
            session.client,
            "--config",
            str(live_settings.config_path),
        ],
        [
            "note",
            "set",
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

    failures = [
        f"{command!r} exited with {result.exit_code}:\n{result.output}"
        f"\nexception: {result.exception!r}"
        for command, result in zip(commands, results, strict=True)
        if result.exit_code != 0
    ]
    assert not failures, "CLI command failures:\n" + "\n".join(failures)
    renamed = runner.invoke(
        app,
        [
            "tag",
            "rename",
            "live-test",
            "live-test-renamed",
            "--client",
            session.client,
            "--config",
            str(live_settings.config_path),
        ],
    )
    removed_tag = runner.invoke(
        app,
        [
            "tag",
            "remove",
            "live-test-renamed",
            "--client",
            session.client,
            "--config",
            str(live_settings.config_path),
        ],
    )
    removed_note = runner.invoke(
        app,
        [
            "note",
            "remove",
            "--client",
            session.client,
            "--session",
            session.session_id,
            "--config",
            str(live_settings.config_path),
        ],
    )
    assert renamed.exit_code == 0
    assert removed_tag.exit_code == 0
    assert removed_note.exit_code == 0
    assert export_path.is_file()
    snapshots = SnapshotStore.from_config(configuration)
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
