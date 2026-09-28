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

import os
import signal
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from importlib import resources
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from tests._sql_parity import normalized_records, seed_synthetic_data, view_names
from usagebassoon.archiver import SnapshotArchiver
from usagebassoon.backends.bigquery import BigQueryBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS, normalize
from usagebassoon.persistence import persist_run
from usagebassoon.storage_model import DEBUG_TABLES, SNAPSHOT_TABLES, STATE_KEYS

pytestmark = pytest.mark.bigquery_live

_DATASET = "usagebassoon_it"
_LOCATION = "US"
_LIVE_TEST_TIMEOUT_SECONDS = 600


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
        _reset_test_schema(client, f"{client.project}.{_DATASET}")
        client.close()


def _backend(settings: LiveSettings) -> BigQueryBackend:
    """Open a fresh BigQuery backend for one live assertion."""
    return BigQueryBackend(settings.project, _DATASET, location=settings.location)


def test_live_synthetic_views_use_the_shared_logical_contract(
    live_settings: LiveSettings,
) -> None:
    """Compare native BigQuery views with DuckDB, including duplicate raw writes."""
    local = DuckDBBackend(":memory:")
    remote = _backend(live_settings)
    try:
        local.apply_ddl()
        seed_synthetic_data(local)
        seed_synthetic_data(remote)
        seed_synthetic_data(remote)
        names = view_names()
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                name: executor.submit(remote.query, f"SELECT * FROM {name}")
                for name in names
            }
            for name, future in futures.items():
                actual = future.result()
                expected = local.query(f"SELECT * FROM {name}")
                assert expected.column_names == actual.column_names, name
                preserve_order = name == "report_summary_models"
                assert normalized_records(expected, preserve_order=preserve_order) == (
                    normalized_records(actual, preserve_order=preserve_order)
                ), name
        # Clear test observations while retaining the initialized schema.
        physical = {"raw_" + name for name in set(STATE_KEYS) | DEBUG_TABLES}
        physical.add("collection_ledger")
        script = (
            "BEGIN TRANSACTION;\n"
            + "\n".join(
                f"DELETE FROM {remote._table_ref(name)} WHERE TRUE;"
                for name in sorted(physical)
            )
            + "\nCOMMIT TRANSACTION;"
        )
        remote._wait_for_job(
            remote.client.query(
                script,
                job_config=remote._query_config(),
                location=remote.location,
            )
        )
    finally:
        remote.close()
        local.close()


def test_live_append_publication_and_snapshot_portability(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Check raw visibility, replay dedup, latency, and BigQuery→DuckDB restore."""
    source_id = live_settings.source_id
    bundle = normalize(
        replace(collection_bundle, source_id=source_id, run_id=str(uuid4()))
    )
    remote = _backend(live_settings)
    local = DuckDBBackend(":memory:")
    local.apply_ddl()
    try:
        remote.preflight()
        started = time.monotonic()
        persist_run(remote, bundle)
        elapsed = time.monotonic() - started
        with capsys.disabled():
            print(f"BigQuery publication: {elapsed:.3f}s", flush=True)
        assert elapsed < 15.0
        assert remote.query("SELECT * FROM daily_stats").num_rows == 0
        persist_run(remote, bundle)
        assert remote.query("SELECT * FROM collection_runs").num_rows == 1
        archive = SnapshotArchiver(str(tmp_path / "portable"))
        archive.write(remote, run_id=bundle.run_id)
        archive.restore(local)
        for table in SNAPSHOT_TABLES:
            canonical = "current_" + table
            assert (
                local.query(f"SELECT * FROM {canonical}").num_rows
                == remote.query(f"SELECT * FROM {canonical}").num_rows
            )
        assert (
            local.query("SELECT SUM(total_tokens) AS n FROM daily_stats").to_pylist()
            == remote.query(
                "SELECT SUM(total_tokens) AS n FROM current_daily_stats"
            ).to_pylist()
        )
        # The approved nightly script is exercised as production SQL, with no
        # partition-decorator prototype or alternate controller.
        sql = (
            resources.files("usagebassoon.sql.bigquery")
            .joinpath("compaction.sql")
            .read_text()
        )
        remote._wait_for_job(
            remote.client.query(
                remote._qualify_view_sql(sql),
                job_config=remote._query_config(),
                location=remote.location,
            )
        )
        assert (
            remote.query("SELECT * FROM daily_stats").num_rows
            == bundle.tables["daily_stats"].num_rows
        )
        before = remote.query(
            "SELECT SUM(total_tokens) AS n FROM current_daily_stats"
        ).to_pylist()
        with pytest.raises(Exception, match="empty warehouse"):
            archive.restore(remote)
        assert (
            remote.query(
                "SELECT SUM(total_tokens) AS n FROM current_daily_stats"
            ).to_pylist()
            == before
        )
        # Bootstrap directly to gold and reproduce an equivalent local snapshot.
        _reset_test_schema(remote.client, remote.dataset_ref)
        remote.apply_ddl()
        local_archive = SnapshotArchiver(str(tmp_path / "local"))
        local_archive.write(local, run_id=str(uuid4()))
        local_archive.restore(remote)
        assert (
            remote.query("SELECT * FROM daily_stats").num_rows
            == bundle.tables["daily_stats"].num_rows
        )
        assert remote.query("SELECT * FROM raw_daily_stats").num_rows == 0
        assert (
            remote.query(
                "SELECT SUM(total_tokens) AS n FROM current_daily_stats"
            ).to_pylist()
            == before
        )
    finally:
        remote.close()
        local.close()


def test_live_compaction_freezes_inputs_and_preserves_tombstones(
    live_settings: LiveSettings,
) -> None:
    """Publish during compaction; preserve backfills and reject raw resurrection."""
    remote = _backend(live_settings)
    source_id = str(uuid4())
    stamp = datetime.now(UTC)
    usage_day = date(2021, 1, 2)
    schema = CANONICAL_TABLE_SCHEMAS["daily_stats"]
    initial = {
        "event_id": str(uuid4()),
        "source_id": source_id,
        "day": usage_day,
        "client": "codex",
        "session_id": "backfill",
        "model": "test-model",
        "input_tokens": 100,
        "output_tokens": 0,
        "cache_read": 0,
        "cache_write": 0,
        "reasoning": 0,
        "total_tokens": 100,
        "collected_at": stamp,
    }
    tag = {
        "event_id": str(uuid4()),
        "source_id": source_id,
        "scope": "session",
        "client": "codex",
        "workspace": "",
        "session_id": "backfill",
        "tag": "temporary",
        "created_at": stamp,
        "collected_at": stamp,
        "is_deleted": False,
    }
    try:
        # An expired arrival bucket's larger count must not mask a new bucket.
        remote._wait_for_job(
            remote.client.query(
                f"INSERT INTO {remote._table_ref('compaction_ledger')} VALUES "
                f"('{source_id}', GENERATE_UUID(), GENERATE_UUID(), 'daily_stats', "
                "DATE '2021-01-02', DATE_SUB(CURRENT_DATE(), INTERVAL 91 DAY), "
                "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 9999)",
                job_config=remote._query_config(),
                location=remote.location,
            )
        )
        remote.append("daily_stats", pa.Table.from_pylist([initial], schema=schema))
        remote.append(
            "tags",
            pa.Table.from_pylist(
                [
                    tag,
                    {
                        **tag,
                        "event_id": str(uuid4()),
                        "is_deleted": True,
                        "collected_at": stamp + timedelta(seconds=1),
                    },
                ],
                schema=CANONICAL_TABLE_SCHEMAS["tags"],
            ),
        )
        sql = remote._qualify_view_sql(
            resources.files("usagebassoon.sql.bigquery")
            .joinpath("compaction.sql")
            .read_text()
        )
        job = remote.client.query(
            sql,
            job_config=remote._query_config(),
            location=remote.location,
        )
        deadline = time.monotonic() + 30
        while job.started is None and time.monotonic() < deadline:
            job.reload()
            time.sleep(0.1)
        assert job.started is not None and job.state != "DONE"
        latest = {
            **initial,
            "event_id": str(uuid4()),
            "total_tokens": 200,
            "input_tokens": 200,
        }
        remote.append("daily_stats", pa.Table.from_pylist([latest], schema=schema))
        remote._wait_for_job(job)
        where = f" WHERE source_id = '{source_id}'"
        assert remote.query(
            "SELECT total_tokens FROM daily_stats" + where
        ).to_pylist() == [{"total_tokens": 100}]
        assert remote.query(
            "SELECT total_tokens FROM current_daily_stats" + where
        ).to_pylist() == [{"total_tokens": 200}]
        progress = remote.query(
            "SELECT raw_rows_processed FROM compaction_ledger"
            + where
            + " AND domain = 'daily_stats' AND arrival_day = CURRENT_DATE()"
        ).to_pylist()
        assert progress == [{"raw_rows_processed": 1}]
        assert remote.query("SELECT is_deleted FROM tags" + where).to_pylist() == [
            {"is_deleted": True}
        ]
        remote._wait_for_job(
            remote.client.query(
                sql,
                job_config=remote._query_config(),
                location=remote.location,
            )
        )
        # Simulate raw retention after successful compaction.
        remote._wait_for_job(
            remote.client.query(
                "BEGIN TRANSACTION;\n"
                f"DELETE FROM {remote._table_ref('raw_tags')}" + where + ";\n"
                f"DELETE FROM {remote._table_ref('raw_daily_stats')}" + where + ";\n"
                "COMMIT TRANSACTION;",
                job_config=remote._query_config(),
                location=remote.location,
            )
        )
        assert remote.query(
            "SELECT total_tokens FROM current_daily_stats" + where
        ).to_pylist() == [{"total_tokens": 200}]
        remote.append(
            "tags", pa.Table.from_pylist([tag], schema=CANONICAL_TABLE_SCHEMAS["tags"])
        )
        assert remote.query("SELECT * FROM current_tags" + where).num_rows == 0
        assert (
            remote.query(
                "SELECT * FROM compaction_backlog WHERE age_days >= 2"
            ).num_rows
            == 0
        )
    finally:
        remote.close()
