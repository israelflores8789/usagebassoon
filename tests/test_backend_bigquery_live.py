# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_bigquery_live.py — Opt-in live BigQuery integration tests.

Raw developer invocation::

    export USAGEBASSOON_BIGQUERY_LIVE=1
    export USAGEBASSOON_BIGQUERY_LIVE_RESET=1
    uv run pytest -m bigquery_live tests/test_backend_bigquery_live.py

``just test-bq-live tests/test_backend_bigquery_live.py 1`` is a convenience
wrapper that also retains partial logs after timeouts.

Set ``USAGEBASSOON_BIGQUERY_LIVE=1`` to enable these ``bigquery_live`` tests against
the dedicated ``usagebassoon_it`` dataset.

Set ``USAGEBASSOON_BIGQUERY_LIVE_RESET=1`` to enable the restore test that deletes
and recreates the initialized integration schema. The module fixture provisions
and cleans up the dedicated dataset schema for every live run.

``USAGEBASSOON_BIGQUERY_PROJECT`` selects the ADC-backed project, and
``USAGEBASSOON_BIGQUERY_LOCATION`` selects its dataset location (default ``US``).
``USAGEBASSOON_BIGQUERY_DATASET`` must be ``usagebassoon_it`` when provided.

Credentials use application default authentication, including
``GOOGLE_APPLICATION_CREDENTIALS`` when configured.
"""

from __future__ import annotations

import os
import signal
import time
from collections.abc import Generator, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from importlib import resources
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, bigquery_datatransfer
from google.protobuf.field_mask_pb2 import FieldMask
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from tests._snapshot_fakes import seed_recovery_data
from tests._sql_parity import normalized_records, seed_synthetic_data, view_names
from usagebassoon.archiver import SnapshotArchiver
from usagebassoon.backends.bigquery import BigQueryBackend
from usagebassoon.backends.bigquery_compaction import (
    BigQueryBackendCompaction,
    install_compaction,
)
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.config import ConfigurationManager
from usagebassoon.curation import (
    NoteAssignment,
    TagAssignment,
    add_tag,
    remove_note,
    remove_tag,
    rename_tag,
    set_note,
)
from usagebassoon.ingest import CollectionBundle
from usagebassoon.normalizer import CANONICAL_TABLE_SCHEMAS, NormalizedBundle, normalize
from usagebassoon.persistence import persist_run
from usagebassoon.storage_model import DEBUG_TABLES, SNAPSHOT_TABLES, STATE_KEYS

pytestmark = pytest.mark.bigquery_live

_DATASET = "usagebassoon_it"
_LOCATION = "US"
_LIVE_TEST_TIMEOUT_SECONDS = 600


@contextmanager
def _phase(name: str) -> Generator[None, None, None]:
    """Print phase wall time, including when a live check raises an error."""
    started = time.monotonic()
    try:
        yield
    finally:
        print(f"BigQuery {name}: {time.monotonic() - started:.3f}s", flush=True)


def _compact(backend: BigQueryBackend) -> None:
    """Run the packaged compaction transaction and report its wall time."""
    sql = backend._qualify_view_sql(
        resources.files("usagebassoon.sql.bigquery")
        .joinpath("compaction.sql")
        .read_text()
    )
    with _phase("compaction"):
        backend._wait_for_job(
            backend.client.query(
                sql, job_config=backend._query_config(), location=backend.location
            )
        )


def _normalized_bundle(
    collection_bundle: CollectionBundle,
) -> tuple[str, NormalizedBundle]:
    """Return an independent source identity and normalized collection run."""
    source_id = str(uuid4())
    return source_id, normalize(
        replace(collection_bundle, source_id=source_id, run_id=str(uuid4()))
    )


def _local_snapshot_config(
    settings: LiveSettings, directory: Path, source_id: str
) -> Path:
    """Use the test's exact source namespace and an explicit private archive."""
    configuration = directory / "config.toml"
    content = settings.config_path.read_text().replace(
        f'source_id = "{settings.source_id}"', f'source_id = "{source_id}"', 1
    )
    configuration.write_text(
        content
        + f'\n[snapshots.local]\nenable = true\npath = "{directory / "snapshots"}"\n'
    )
    return configuration


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


def _wait_for_compaction_jobs(
    client: bigquery.Client,
    dataset_id: str,
    *,
    timeout_seconds: float = _LIVE_TEST_TIMEOUT_SECONDS,
) -> None:
    """Wait for pending and running dataset queries before destructive cleanup.

    Args:
        client: Authenticated integration client with its configured location.
        dataset_id: Dedicated integration dataset to make quiescent.
        timeout_seconds: Maximum time to wait without mutating any tables.

    Raises:
        TimeoutError: If background work does not finish within the bound.
    """
    project, dataset = dataset_id.split(".")
    if dataset != _DATASET:
        raise ValueError("compaction waits require the dedicated integration dataset")
    location = client.location or _LOCATION
    deadline = time.monotonic() + timeout_seconds
    sql = (
        "SELECT job_id FROM "
        f"`{project}.region-{location.lower()}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` "
        "WHERE state IN ('PENDING', 'RUNNING') "
        "AND (STRPOS(query, @dataset_reference) > 0 "
        "OR (destination_table.project_id = @project "
        "AND destination_table.dataset_id = @dataset))"
    )
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "dataset_reference", "STRING", f"`{dataset_id}."
            ),
            bigquery.ScalarQueryParameter("project", "STRING", project),
            bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
        ]
    )
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("background BigQuery jobs did not finish before cleanup")
        jobs = list(
            client.query(sql, job_config=config, location=location).result(
                timeout=remaining
            )
        )
        if not jobs:
            return
        print(
            "Waiting for BigQuery jobs before cleanup: "
            + ", ".join(str(row["job_id"]) for row in jobs),
            flush=True,
        )
        time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))


def _drain_compaction_schedule(
    client: bigquery_datatransfer.DataTransferServiceClient,
    name: str,
    *,
    timeout_seconds: float = _LIVE_TEST_TIMEOUT_SECONDS,
) -> None:
    """Disable new launches and await queued/running transfers before deletion."""
    client.update_transfer_config(
        transfer_config=bigquery_datatransfer.TransferConfig(name=name, disabled=True),
        update_mask=FieldMask(paths=["disabled"]),
    )
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("scheduled compaction did not finish before cleanup")
        runs = list(
            client.list_transfer_runs(
                request=bigquery_datatransfer.ListTransferRunsRequest(
                    parent=name,
                    states=[
                        bigquery_datatransfer.TransferState.PENDING,
                        bigquery_datatransfer.TransferState.RUNNING,
                    ],
                ),
                timeout=remaining,
            )
        )
        if not runs:
            return
        print(f"Waiting for {len(runs)} scheduled compaction run(s)", flush=True)
        time.sleep(min(10.0, max(0.0, deadline - time.monotonic())))


def _reset_test_schema(client: bigquery.Client, dataset_id: str) -> None:
    """Remove leftover relations from the dedicated integration dataset.

    Args:
        client: Authenticated BigQuery client for the test project.
        dataset_id: Fully qualified dedicated integration dataset identifier.
    """
    _wait_for_compaction_jobs(client, dataset_id)
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
            'backend.provider = "bigquery"\n'
            "[backend.bigquery]\n"
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
            with _phase("schema provisioning"):
                _reset_test_schema(client, f"{project}.{_DATASET}")
                backend.apply_ddl()
            dataset = backend.client.get_dataset(f"{project}.{_DATASET}")
            assert isinstance(dataset.location, str)
            assert dataset.location.casefold() == location.casefold()
        finally:
            backend.close()
        yield LiveSettings(project, location, source_id, config_path)
    finally:
        with _phase("schema cleanup"):
            _reset_test_schema(client, f"{client.project}.{_DATASET}")
        client.close()


@pytest.fixture
def managed_compaction_schedule(
    live_settings: LiveSettings, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    """Remove only schedules created by a live CLI initialization test."""
    parent = (
        f"projects/{live_settings.project}/locations/{live_settings.location.lower()}"
    )
    with bigquery_datatransfer.DataTransferServiceClient() as client:
        existing = {
            config.name for config in client.list_transfer_configs(parent=parent)
        }
        created: set[str] = set()

        def install(
            backend: BigQueryBackendCompaction, *, enabled: bool | None = True
        ) -> str:
            """Run real provisioning and track any newly created configuration."""
            name = install_compaction(backend, enabled=enabled)
            if name not in existing:
                created.add(name)
            return name

        monkeypatch.setattr(
            "usagebassoon.backends.bigquery_compaction.install_compaction", install
        )
        try:
            yield
        finally:
            for name in created:
                _drain_compaction_schedule(client, name)
                with bigquery.Client(
                    project=live_settings.project, location=live_settings.location
                ) as query_client:
                    _wait_for_compaction_jobs(query_client, live_settings.dataset_id)
                client.delete_transfer_config(name=name)


def _backend(settings: LiveSettings) -> BigQueryBackend:
    """Open a fresh BigQuery backend for one live assertion."""
    return BigQueryBackend(settings.project, _DATASET, location=settings.location)


@pytest.fixture(autouse=True)
def empty_live_warehouse(live_settings: LiveSettings) -> None:
    """Start every case with empty usage tables in the dedicated warehouse.

    These tests must remain serialized because they share usagebassoon_it.
    """
    backend = _backend(live_settings)
    physical = set(STATE_KEYS) | {
        "raw_" + name for name in set(STATE_KEYS) | DEBUG_TABLES
    }
    physical.add("collection_ledger")
    script = (
        "BEGIN TRANSACTION;\n"
        + "\n".join(
            f"TRUNCATE TABLE {backend._table_ref(name)};" for name in sorted(physical)
        )
        + f"\nDELETE FROM {backend._table_ref('compaction_ledger')} "
        "WHERE domain != '__lock__';\nCOMMIT TRANSACTION;"
    )
    try:
        with _phase("test row cleanup"):
            _wait_for_compaction_jobs(backend.client, live_settings.dataset_id)
            backend._wait_for_job(
                backend.client.query(
                    script,
                    job_config=backend._query_config(),
                    location=backend.location,
                )
            )
    finally:
        backend.close()


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
    finally:
        remote.close()
        local.close()


@pytest.mark.usefixtures("managed_compaction_schedule")
def test_live_cli_commands_including_models_report(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Exercise configured CLI reads, curation, export, init, and snapshots."""
    source_id, bundle = _normalized_bundle(collection_bundle)
    backend = _backend(live_settings)
    try:
        persist_run(backend, bundle)
    finally:
        backend.close()
    config_path = _local_snapshot_config(live_settings, tmp_path, source_id)
    configuration = ConfigurationManager(config_path).load()

    def preflight(_configuration: object) -> tuple[tuple[str, ...], str]:
        """Keep warehouse CLI checks independent of an installed tokscale binary."""
        return ("tokscale",), collection_bundle.graph.meta.version

    monkeypatch.setattr("usagebassoon.cli.doctor.preflight_tokscale", preflight)
    session = collection_bundle.report_rows[0]
    runner = CliRunner()
    tag = "cli-" + str(uuid4())
    export_path = tmp_path / "sessions.json"

    def invoke(*parts: str) -> str:
        """Run one real configured CLI command and report its wall time."""
        with _phase("CLI " + " ".join(parts[:2])):
            result = runner.invoke(app, [*parts, "--config", str(config_path)])
        output = plain_cli_output(result.output)
        assert result.exit_code == 0, f"{parts!r}: {output}\n{result.exception!r}"
        return output

    assert "Initialized bigquery schema" in invoke("init")
    invoke("query", "report_summary", "--limit", "1", "--format", "json")
    assert "Model Token Usage" in invoke("report", "models", "--source", source_id)
    invoke("export", "sessions", str(export_path), "--format", "json")
    invoke("tag", "add", tag, "--client", session.client)
    invoke(
        "note",
        "set",
        "live integration note",
        "--client",
        session.client,
        "--session",
        session.session_id,
    )
    invoke("doctor")
    invoke("audit", "runs")
    invoke("snapshot")
    invoke("tag", "rename", tag, tag + "-renamed", "--client", session.client)
    invoke("tag", "remove", tag + "-renamed", "--client", session.client)
    invoke(
        "note", "remove", "--client", session.client, "--session", session.session_id
    )
    assert export_path.is_file()
    assert SnapshotArchiver.from_config(configuration).list_snapshots()


@pytest.mark.usefixtures("managed_compaction_schedule")
def test_live_doctor_schedule_lifecycle(
    live_settings: LiveSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drain compaction launched by init before the following test cleans rows."""

    def preflight(_configuration: object) -> tuple[tuple[str, ...], str]:
        """Keep the read/scheduler regression independent of local tokscale."""
        return ("tokscale",), "4.15.1"

    monkeypatch.setattr("usagebassoon.cli.doctor.preflight_tokscale", preflight)
    runner = CliRunner()
    for command in ("init", "doctor"):
        with _phase("CLI " + command):
            result = runner.invoke(
                app, [command, "--config", str(live_settings.config_path)]
            )
        assert result.exit_code == 0, plain_cli_output(result.output)


def test_live_location_and_credential_errors_are_actionable(
    live_settings: LiveSettings, tmp_path: Path
) -> None:
    """Reject mismatched dataset locations and unusable credential files clearly."""
    location = "US" if live_settings.location.casefold() == "eu" else "EU"
    backend = BigQueryBackend(live_settings.project, _DATASET, location=location)
    try:
        with pytest.raises(
            ValueError, match="configured BigQuery location does not match the dataset"
        ):
            backend.apply_ddl()
    finally:
        backend.close()
    with pytest.raises(RuntimeError, match="credentials file could not be loaded"):
        BigQueryBackend(
            live_settings.project,
            _DATASET,
            credentials_file=tmp_path / "missing-service-account.json",
        )


def test_live_append_publication_is_visible_and_replay_safe(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Require fast collection publication, immediate visibility, and safe replay."""
    source_id, bundle = _normalized_bundle(collection_bundle)
    remote = _backend(live_settings)
    try:
        remote.preflight()
        parameters = {"source_id": source_id}
        for attempt in ("initial", "replay"):
            started = time.monotonic()
            persist_run(remote, bundle)
            elapsed = time.monotonic() - started
            with capsys.disabled():
                print(f"BigQuery {attempt} publication: {elapsed:.3f}s", flush=True)
            # REQUIRED collection performance contract: publication must finish in 15s.
            # Do not remove, relax, skip, or replace this assertion with timing logs
            # without explicit maintainer approval. Slow collect-time queries/appends
            # make collection unusable; both initial publication and replay must pass.
            # Time persistence only; provisioning and diagnostic reads are excluded.
            assert elapsed < 15.0, (
                f"BigQuery {attempt} collection publication took {elapsed:.3f}s; "
                "required threshold is 15 seconds"
            )
            assert (
                remote.query(
                    "SELECT * FROM daily_stats WHERE source_id = :source_id", parameters
                ).num_rows
                == 0
            )
            for table in ("sessions", "daily_stats", "price_versions"):
                assert (
                    remote.query(
                        f"SELECT * FROM current_{table} WHERE source_id = :source_id",
                        parameters,
                    ).num_rows
                    == bundle.tables[table].num_rows
                )
            assert (
                remote.query(
                    "SELECT * FROM collection_runs WHERE run_id = :run_id",
                    {"run_id": bundle.run_id},
                ).num_rows
                == 1
            )
    finally:
        remote.close()


def test_live_snapshot_portability_includes_raw_and_compacted_facts(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    tmp_path: Path,
) -> None:
    """Restore complete BigQuery state into DuckDB and reject populated targets."""
    source_id, bundle = _normalized_bundle(collection_bundle)
    remote = _backend(live_settings)
    local = DuckDBBackend(":memory:")
    local.apply_ddl()
    try:
        persist_run(remote, bundle)
        archive = SnapshotArchiver(str(tmp_path / "portable"))
        with _phase("snapshot capture"):
            assert archive.write(remote, run_id=bundle.run_id) is not None
        with _phase("DuckDB restore"):
            archive.restore(local)
        for table in SNAPSHOT_TABLES:
            canonical = "current_" + table
            assert normalized_records(local.query(f"SELECT * FROM {canonical}")) == (
                normalized_records(remote.query(f"SELECT * FROM {canonical}"))
            ), table
        assert (
            local.query("SELECT SUM(total_tokens) AS n FROM daily_stats").to_pylist()
            == remote.query(
                "SELECT SUM(total_tokens) AS n FROM current_daily_stats"
            ).to_pylist()
        )
        _compact(remote)
        assert (
            remote.query(
                "SELECT * FROM daily_stats WHERE source_id = :source_id",
                {"source_id": source_id},
            ).num_rows
            == bundle.tables["daily_stats"].num_rows
        )
        for table in SNAPSHOT_TABLES:
            local.connection.execute(f"DELETE FROM {table}")
        with _phase("compacted snapshot capture"):
            assert archive.write(remote, run_id=str(uuid4())) is not None
        archive.restore(local)
        for table in SNAPSHOT_TABLES:
            assert normalized_records(
                local.query(f"SELECT * FROM current_{table}")
            ) == (normalized_records(remote.query(f"SELECT * FROM current_{table}"))), (
                table
            )
        before = remote.query(
            "SELECT SUM(total_tokens) AS n FROM current_daily_stats"
        ).to_pylist()
        with (
            _phase("populated BigQuery restore rejection"),
            pytest.raises(ValueError, match="empty warehouse"),
        ):
            archive.restore(remote)
        assert (
            remote.query(
                "SELECT SUM(total_tokens) AS n FROM current_daily_stats"
            ).to_pylist()
            == before
        )
    finally:
        remote.close()
        local.close()


def test_live_compaction_cutoff_preserves_late_appends_and_gold_only_keys(
    live_settings: LiveSettings,
) -> None:
    """Exclude post-cutoff appends; preserve gold-only facts and tombstones."""
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
        "op": "upsert",
        "updated_at": stamp,
        "op_id": str(uuid4()),
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
                        "op": "delete",
                        "updated_at": stamp + timedelta(seconds=1),
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
        cutoff = remote.query("SELECT CURRENT_TIMESTAMP() AS cutoff").to_pylist()[0][
            "cutoff"
        ]
        assert isinstance(cutoff, datetime)
        sql = sql.replace(
            "SET cutoff = CURRENT_TIMESTAMP();",
            f"SET cutoff = TIMESTAMP '{cutoff.isoformat()}';",
        )
        compaction_started = time.monotonic()
        job = remote.client.query(
            sql,
            job_config=remote._query_config(),
            location=remote.location,
        )
        deadline = time.monotonic() + 30
        while job.started is None and time.monotonic() < deadline:
            job.reload()
            time.sleep(0.1)
        assert job.started is not None
        latest = {
            **initial,
            "event_id": str(uuid4()),
            "total_tokens": 200,
            "input_tokens": 200,
        }
        remote.append("daily_stats", pa.Table.from_pylist([latest], schema=schema))
        remote._wait_for_job(job)
        print(
            "BigQuery compaction with concurrent append: "
            f"{time.monotonic() - compaction_started:.3f}s",
            flush=True,
        )
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
        assert remote.query("SELECT op FROM tags" + where).to_pylist() == [
            {"op": "delete"}
        ]
        _compact(remote)
        # Simulate expiry of an arrival bucket while retaining its old progress.
        remote._wait_for_job(
            remote.client.query(
                "BEGIN TRANSACTION;\n"
                f"DELETE FROM {remote._table_ref('raw_tags')}" + where + ";\n"
                f"DELETE FROM {remote._table_ref('raw_daily_stats')}" + where + ";\n"
                f"UPDATE {remote._table_ref('compaction_ledger')} "
                "SET arrival_day = DATE_SUB(CURRENT_DATE(), INTERVAL 91 DAY)"
                + where
                + " AND arrival_day = CURRENT_DATE();\n"
                "COMMIT TRANSACTION;",
                job_config=remote._query_config(),
                location=remote.location,
            )
        )
        assert remote.query(
            "SELECT total_tokens FROM current_daily_stats" + where
        ).to_pylist() == [{"total_tokens": 200}]
        distinct = {**initial, "event_id": str(uuid4()), "session_id": "late-key"}
        remote.append("daily_stats", pa.Table.from_pylist([distinct], schema=schema))
        _compact(remote)
        assert remote.query(
            "SELECT session_id, total_tokens FROM daily_stats"
            + where
            + " ORDER BY session_id"
        ).to_pylist() == [
            {"session_id": "backfill", "total_tokens": 200},
            {"session_id": "late-key", "total_tokens": 100},
        ]
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


def test_live_global_tags_and_source_scoped_notes_survive_compaction_and_snapshot(
    live_settings: LiveSettings, tmp_path: Path
) -> None:
    """Preserve global tag lifetimes and independent source-scoped session notes."""
    remote = _backend(live_settings)
    local = DuckDBBackend(":memory:")
    source_a, source_b = str(uuid4()), str(uuid4())
    client = "curation-" + str(uuid4())
    session_id = str(uuid4())
    created = datetime.now(UTC) - timedelta(days=2)
    changed = created + timedelta(days=1)
    tag = TagAssignment(
        "session", source_a, "old", client=client, session_id=session_id
    )

    def current(table: str, note_source: str | None = None) -> dict[str, object]:
        """Read a global tag or the note for one exact source/session identity."""
        parameters = {"client": client, "session_id": session_id}
        scope = ""
        if table == "notes":
            parameters["source_id"] = note_source or source_a
            scope = " AND source_id = :source_id"
        rows = remote.query(
            f"SELECT * FROM current_{table} "
            "WHERE client = :client AND session_id = :session_id" + scope,
            parameters,
        ).to_pylist()
        assert len(rows) == 1
        return rows[0]

    try:
        local.apply_ddl()
        add_tag(remote, tag, at=created)
        set_note(
            remote, NoteAssignment(source_a, client, session_id, "first"), at=created
        )
        _compact(remote)
        other_tag = replace(tag, source_id=source_b)
        assert add_tag(remote, other_tag, at=changed).affected == 0
        assert rename_tag(remote, other_tag, "new", at=changed).renamed
        revised_note = NoteAssignment(source_a, client, session_id, "edited")
        set_note(remote, revised_note, at=changed)
        set_note(
            remote,
            NoteAssignment(source_b, client, session_id, "independent"),
            at=changed,
        )
        before = {table: current(table) for table in ("tags", "notes")}
        _compact(remote)
        for table, row in before.items():
            assert current(table) == row
            assert row["source_id"] == (source_b if table == "tags" else source_a)
            assert row["created_at"] == created
            assert row["updated_at"] == changed
        assert current("notes", source_b)["note"] == "independent"
        assert current("notes", source_b)["created_at"] == changed
        rename_events = remote.query(
            "SELECT op, op_id, event_id FROM raw_tags "
            "WHERE client = :client AND source_id = :source_id ORDER BY op",
            {"client": client, "source_id": source_b},
        ).to_pylist()
        assert [row["op"] for row in rename_events] == ["delete", "upsert"]
        assert rename_events[0]["op_id"] == rename_events[1]["op_id"]
        assert rename_events[0]["event_id"] != rename_events[1]["event_id"]
        renamed_tag = replace(other_tag, tag="new")
        assert remove_tag(remote, renamed_tag) == 1
        assert remove_note(remote, revised_note) == 1
        _compact(remote)
        for table in ("tags", "notes"):
            parameters = {"client": client}
            scope = ""
            if table == "notes":
                parameters["source_id"] = source_a
                scope = " AND source_id = :source_id"
            assert (
                remote.query(
                    f"SELECT * FROM current_{table} WHERE client = :client" + scope,
                    parameters,
                ).num_rows
                == 0
            )
        assert current("notes", source_b)["note"] == "independent"
        recreated = datetime.now(UTC)
        add_tag(remote, replace(renamed_tag, source_id=source_a), at=recreated)
        set_note(remote, replace(revised_note, source_id=source_a), at=recreated)
        for phase in range(2):
            if phase:
                _compact(remote)
            store = SnapshotArchiver(str(tmp_path / f"archive-{phase}"))
            with _phase("curation snapshot capture"):
                assert store.write(remote, run_id=str(uuid4())) is not None
            with _phase("curation DuckDB restore"):
                store.restore(local)
            for table in ("tags", "notes"):
                expected = current(table)
                assert expected["created_at"] == recreated
                assert expected["updated_at"] == recreated
                parameters = {"client": client}
                scope = ""
                if table == "notes":
                    parameters["source_id"] = source_a
                    scope = " AND source_id = :source_id"
                restored = local.query(
                    f"SELECT * FROM current_{table} WHERE client = :client" + scope,
                    parameters,
                ).to_pylist()
                assert restored == [expected]
            assert local.query(
                "SELECT note, created_at FROM current_notes "
                "WHERE source_id = :source_id AND client = :client",
                {"source_id": source_b, "client": client},
            ).to_pylist() == [{"note": "independent", "created_at": changed}]
            # Each restore must target an initialized, empty warehouse.
            for table in SNAPSHOT_TABLES:
                local.connection.execute(f"DELETE FROM {table}")
    finally:
        remote.close()
        local.close()


@pytest.mark.usefixtures("managed_compaction_schedule")
def test_live_restore_requires_an_explicit_disposable_reset(
    live_settings: LiveSettings,
    collection_bundle: CollectionBundle,
    tmp_path: Path,
) -> None:
    """Restore a DuckDB snapshot into BigQuery after an opted-in schema reset."""
    if os.environ.get("USAGEBASSOON_BIGQUERY_LIVE_RESET") != "1":
        pytest.skip("set USAGEBASSOON_BIGQUERY_LIVE_RESET=1 to run live restore")
    source_id, bundle = _normalized_bundle(collection_bundle)
    remote = _backend(live_settings)
    local = DuckDBBackend(":memory:")
    local.apply_ddl()
    config_path = _local_snapshot_config(live_settings, tmp_path, source_id)
    configuration = ConfigurationManager(config_path).load()
    archive = SnapshotArchiver.from_config(configuration)
    runner = CliRunner()
    command = ["restore", "--config", str(config_path)]
    parameters = {"source_id": source_id}
    try:
        sources = (source_id, str(uuid4()))
        expected = seed_recovery_data(local, collection_bundle, sources)
        persist_run(remote, bundle)
        captured = archive.write(local, run_id=bundle.run_id, manual=True, pin=True)
        assert captured is not None
        relocated_root = tmp_path / "relocated"
        copied = archive.copy(captured, str(relocated_root))
        (relocated_root / "catalog.json").unlink()
        (Path(copied) / "state.json").write_text("damaged lifecycle metadata")
        command.extend(["--from-snapshot", copied])
        before = normalized_records(
            remote.query(
                "SELECT * FROM current_daily_stats WHERE source_id = :source_id",
                parameters,
            )
        )
        with _phase("populated BigQuery CLI restore rejection"):
            rejected = runner.invoke(app, command, input="y\n")
        assert rejected.exit_code != 0
        assert "restore requires an empty warehouse" in plain_cli_output(
            rejected.output
        )
        assert (
            normalized_records(
                remote.query(
                    "SELECT * FROM current_daily_stats WHERE source_id = :source_id",
                    parameters,
                )
            )
            == before
        )
        with _phase("explicit disposable schema reset"):
            _reset_test_schema(remote.client, remote.dataset_ref)
            remote.apply_ddl()
        initialized = runner.invoke(
            app, ["init", "--restore", "--config", str(config_path)]
        )
        assert initialized.exit_code == 0, plain_cli_output(initialized.output)
        state = remote.maintenance_status()
        assert state is not None and not state[0]
        initialized = runner.invoke(app, ["init", "--config", str(config_path)])
        assert initialized.exit_code == 0, plain_cli_output(initialized.output)
        state = remote.maintenance_status()
        assert state is not None and state[0]
        declined = runner.invoke(app, command, input="y\nn\n")
        assert declined.exit_code != 0
        assert "Aborted" in plain_cli_output(declined.output)
        # Simulate a crashed attempt with a populated, nonexpired owned stage.
        from usagebassoon.backends.bigquery import _schema_from_arrow
        from usagebassoon.snapshot.restore import restore_operation_id

        with archive.reader.prepare(copied) as prepared:
            operation = restore_operation_id(prepared)
        stage = bigquery.Table(
            f"{remote.dataset_ref}._stage_notes_{uuid4().hex}",
            schema=_schema_from_arrow(local.query("SELECT * FROM notes")),
        )
        stage.labels = {
            "usagebassoon_kind": "restore_stage",
            "usagebassoon_restore": operation,
        }
        stage.expires = datetime.now(UTC) + timedelta(days=1)
        remote.client.create_table(stage)
        remote._load(
            local.query("SELECT * FROM notes"),
            str(stage.reference),
            disposition="WRITE_APPEND",
        )
        with _phase("BigQuery CLI restore after interrupted attempt"):
            restored = runner.invoke(app, command, input="y\ny\n")
        assert restored.exit_code == 0, (
            f"{plain_cli_output(restored.output)}\n{restored.exception!r}"
        )
        assert "Restored" in plain_cli_output(restored.output)
        for table in SNAPSHOT_TABLES:
            relation = ("replay_" if table in DEBUG_TABLES else "current_") + table
            assert (
                normalized_records(remote.query(f"SELECT * FROM {relation}"))
                == expected[table]
            ), table
        assert remote.query("SELECT * FROM daily_stats").num_rows == (
            len(expected["daily_stats"])
        )
        assert remote.query("SELECT * FROM raw_daily_stats").num_rows == 0
        state = remote.maintenance_status()
        assert state is not None and not state[0]
        initialized = runner.invoke(app, ["init", "--config", str(config_path)])
        assert initialized.exit_code == 0, plain_cli_output(initialized.output)
        state = remote.maintenance_status()
        assert state is not None and state[0]
        assert remote.restore_stages() == []
        totals = remote.query(
            "SELECT SUM(total_tokens) AS total FROM current_daily_stats"
        ).to_pylist()
        persist_run(
            remote,
            normalize(
                replace(collection_bundle, source_id=source_id, run_id=str(uuid4()))
            ),
        )
        assert remote.query(
            "SELECT COUNT(*) AS count FROM current_daily_stats"
        ).to_pylist() == [{"count": len(expected["daily_stats"])}]
        assert set(
            remote.query("SELECT DISTINCT source_id FROM current_daily_stats")
            .column("source_id")
            .to_pylist()
        ) == set(sources)
        _compact(remote)
        assert (
            remote.query(
                "SELECT SUM(total_tokens) AS total FROM current_daily_stats"
            ).to_pylist()
            == totals
        )
        assert remote.query(
            "SELECT COUNT(*) AS count FROM daily_stats"
        ).to_pylist() == [{"count": len(expected["daily_stats"])}]
    finally:
        remote.close()
        local.close()


def test_live_consistent_read_preserves_state_across_compaction_and_late_append(
    live_settings: LiveSettings,
) -> None:
    """Keep gold/raw joins and doctor ledger reads at the first query's instant."""
    remote = _backend(live_settings)

    def append_observations() -> None:
        """Seed fixture observations directly, independent of publication latency."""
        source_id = str(uuid4())
        script = (
            f"INSERT INTO {remote._table_ref('raw_daily_stats')} "
            "(event_id, source_id, day, client, session_id, model, input_tokens, "
            "output_tokens, cache_read, cache_write, reasoning, "
            "total_tokens, collected_at) "
            "VALUES (GENERATE_UUID(), @source_id, DATE '2026-09-10', "
            "'test', 'session', "
            "'model', 10, 20, 0, 0, 0, 30, CURRENT_TIMESTAMP()); "
            f"INSERT INTO {remote._table_ref('raw_price_versions')} "
            "(event_id, source_id, day, model, source, price_input_per_token, "
            "price_output_per_token, collected_at) "
            "VALUES (GENERATE_UUID(), @source_id, DATE '2026-09-10', 'model', 'test', "
            "0.001, 0.002, CURRENT_TIMESTAMP()); "
            f"INSERT INTO {remote._table_ref('collection_ledger')} "
            "(event_id, source_id, run_id, day, domain, collected_at, "
            "started_at, finished_at, status) "
            "VALUES (GENERATE_UUID(), @source_id, GENERATE_UUID(), DATE '2026-09-10', "
            "'collection', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), "
            "CURRENT_TIMESTAMP(), 'failed');"
        )
        remote._wait_for_job(
            remote.client.query(
                script,
                job_config=remote._query_config(
                    parameters=[
                        bigquery.ScalarQueryParameter("source_id", "STRING", source_id)
                    ]
                ),
                location=remote.location,
            )
        )

    try:
        append_observations()
        usage_sql = (
            "SELECT source_id, day, client, session_id, model, total_tokens, cost_usd "
            "FROM daily_cost ORDER BY source_id, day, client, session_id, model"
        )
        ledger_sql = "SELECT run_id, status FROM collection_runs ORDER BY run_id"
        with remote.consistent_read() as read:
            usage_before = read.query(usage_sql)
            ledger_before = read.query(ledger_sql)
            assert usage_before.num_rows == 1
            assert ledger_before.num_rows == 1
            _compact(remote)
            append_observations()
            assert read.query(usage_sql).equals(usage_before)
            assert read.query(ledger_sql).equals(ledger_before)
            assert remote.query(usage_sql).num_rows == 2
            assert remote.query(ledger_sql).num_rows == 2
    finally:
        remote.close()


def test_live_snapshot_reads_gold_only_and_uncompacted_keys(
    live_settings: LiveSettings, tmp_path: Path
) -> None:
    """Capture native durable-only facts together with accepted raw arrivals."""
    remote = _backend(live_settings)
    target = DuckDBBackend(":memory:")
    source_id = str(uuid4())
    try:
        target.apply_ddl()
        # Seed durable state directly; collection itself must never write gold.
        remote._wait_for_job(
            remote.client.query(
                f"INSERT INTO {remote._table_ref('daily_stats')} "
                "(event_id, source_id, day, client, session_id, model, input_tokens, "
                "output_tokens, cache_read, cache_write, reasoning, total_tokens, "
                "collected_at) VALUES (GENERATE_UUID(), @source_id, DATE '2021-01-02', "
                "'codex', 'gold-only', 'test-model', 100, 0, 0, 0, 0, 100, "
                "CURRENT_TIMESTAMP())",
                job_config=remote._query_config(
                    parameters=[
                        bigquery.ScalarQueryParameter("source_id", "STRING", source_id)
                    ]
                ),
                location=remote.location,
            )
        )
        remote.append(
            "daily_stats",
            pa.Table.from_pylist(
                [
                    {
                        "event_id": str(uuid4()),
                        "source_id": source_id,
                        "day": date(2021, 1, 2),
                        "client": "codex",
                        "session_id": "raw-only",
                        "model": "test-model",
                        "input_tokens": 200,
                        "output_tokens": 0,
                        "cache_read": 0,
                        "cache_write": 0,
                        "reasoning": 0,
                        "total_tokens": 200,
                        "collected_at": datetime.now(UTC),
                    }
                ],
                schema=CANONICAL_TABLE_SCHEMAS["daily_stats"],
            ),
        )
        assert remote.query("SELECT session_id FROM daily_stats").to_pylist() == [
            {"session_id": "gold-only"}
        ]
        assert remote.query("SELECT session_id FROM raw_daily_stats").to_pylist() == [
            {"session_id": "raw-only"}
        ]
        store = SnapshotArchiver(str(tmp_path / "gold-and-raw"))
        uri = store.write(remote, run_id="gold-and-raw", manual=True)
        assert uri is not None
        assert store.restore(target, uri)["daily_stats"] == 2
        assert target.query(
            "SELECT source_id, session_id, total_tokens FROM current_daily_stats "
            "ORDER BY session_id"
        ).to_pylist() == [
            {"source_id": source_id, "session_id": "gold-only", "total_tokens": 100},
            {"source_id": source_id, "session_id": "raw-only", "total_tokens": 200},
        ]
    finally:
        remote.close()
        target.close()


def test_live_source_audit_aggregates_history_with_one_summary_per_source(
    live_settings: LiveSettings, collection_bundle: CollectionBundle
) -> None:
    """Validate portable aggregation SQL against native BigQuery audit views."""
    from usagebassoon.audit import audit_sources

    remote = _backend(live_settings)
    sources = (str(uuid4()), str(uuid4()))
    try:
        for source_id in sources:
            persist_run(
                remote,
                normalize(
                    replace(collection_bundle, source_id=source_id, run_id=str(uuid4()))
                ),
            )
        rows = {str(row["source_id"]): row for row in audit_sources(remote)}
        assert set(sources) <= set(rows)
        assert all(rows[source]["run_count"] == 1 for source in sources)
        assert all(rows[source]["last_activity"] is not None for source in sources)
    finally:
        remote.close()
