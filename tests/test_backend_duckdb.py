# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_backend_duckdb.py — Tests for local DuckDB and offline MotherDuck validation."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from tests._sql_parity import normalized_records
from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.backends.factory import StorageBackendRegistry, open_backend
from usagebassoon.backends.motherduck import MotherDuckBackend
from usagebassoon.config import (
    BackendName,
    BigQueryConfig,
    MotherDuckConfig,
    UsageBassoonConfig,
)
from usagebassoon.ingest import CollectionBundle


@pytest.mark.parametrize("provider", ["duckdb", "motherduck", "bigquery"])
@pytest.mark.parametrize("initialize", [False, True])
def test_backend_factory_applies_settings_and_enforces_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider: BackendName,
    initialize: bool,
) -> None:
    """Every provider receives its settings without implicit schema provisioning."""
    configuration = UsageBassoonConfig(
        tmp_path / "config.toml",
        "11111111-1111-4111-8111-111111111111",
        provider,
        local_database=tmp_path / "configured.duckdb",
        motherduck=MotherDuckConfig("usagebassoon_it"),
        bigquery=BigQueryConfig(
            "test-project",
            "usagebassoon_it",
            location="EU",
            credentials_file=tmp_path / "credentials.json",
            maximum_bytes_billed=123456,
            timeout_seconds=17.0,
        ),
    )
    backend = DuckDBBackend(":memory:")
    constructors: list[tuple[tuple[object, ...], dict[str, object]]] = []
    readiness: list[str] = []

    def construct(*args: object, **kwargs: object) -> StorageBackend:
        """Record provider construction without contacting a remote service."""
        constructors.append((args, kwargs))
        return backend

    def preflight() -> None:
        readiness.append("preflight")

    def forbidden_ddl() -> None:
        raise AssertionError("backend construction must not issue DDL")

    constructor = {
        "duckdb": "usagebassoon.backends.duckdb_local.DuckDBBackend",
        "motherduck": "usagebassoon.backends.motherduck.MotherDuckBackend",
        "bigquery": "usagebassoon.backends.bigquery.BigQueryBackend",
    }[provider]
    monkeypatch.setattr(constructor, construct)
    monkeypatch.setattr(backend, "preflight", preflight)
    monkeypatch.setattr(backend, "apply_ddl", forbidden_ddl)
    try:
        assert open_backend(configuration, initialize=initialize) is backend
        assert readiness == ([] if initialize else ["preflight"])
        expected: tuple[tuple[object, ...], dict[str, object]]
        if provider == "duckdb":
            expected = ((configuration.local_database,), {})
        elif provider == "motherduck":
            expected = (("usagebassoon_it",), {})
        else:
            expected = (
                ("test-project", "usagebassoon_it"),
                {
                    "location": "EU",
                    "credentials_file": tmp_path / "credentials.json",
                    "maximum_bytes_billed": 123456,
                    "timeout_seconds": 17.0,
                },
            )
        assert constructors == [expected]
    finally:
        backend.close()


def test_registered_backend_uses_shared_open_and_snapshot_contract(
    tmp_path: Path,
) -> None:
    """An additional provider participates through the complete backend protocol."""
    from usagebassoon.archiver import SnapshotArchiver

    backend = DuckDBBackend(":memory:")
    configuration = UsageBassoonConfig(
        tmp_path / "config.toml",
        "11111111-1111-4111-8111-111111111111",
        cast(BackendName, "additional-provider"),
    )
    opened: list[UsageBassoonConfig] = []

    def construct(config: UsageBassoonConfig) -> StorageBackend:
        opened.append(config)
        return backend

    registry = StorageBackendRegistry(factories={"additional-provider": construct})
    try:
        assert opened == []
        provision = open_backend(configuration, initialize=True, registry=registry)
        assert provision is backend
        provision.apply_ddl()
        assert open_backend(configuration, registry=registry) is backend
        archive = SnapshotArchiver(str(tmp_path / "archive"))
        snapshot = archive.write(backend, run_id="registered", manual=True, pin=True)
        assert snapshot is not None
        with archive.reader.prepare(snapshot) as prepared:
            assert prepared.manifest["source_backend"] == "duckdb"
        assert opened == [configuration, configuration]
    finally:
        backend.close()
    with pytest.raises(ValueError, match="unsupported storage backend provider"):
        open_backend(configuration)


@pytest.mark.parametrize("provider", ["duckdb", "motherduck", "bigquery"])
def test_backend_factory_rejects_missing_selected_settings(
    tmp_path: Path, provider: BackendName
) -> None:
    """Missing provider settings fail before attempting connection or authentication."""
    configuration = UsageBassoonConfig(
        tmp_path / "config.toml",
        "11111111-1111-4111-8111-111111111111",
        provider,
    )
    with pytest.raises(ValueError, match="missing"):
        open_backend(configuration)


def test_local_backend_applies_current_duckdb_schema(tmp_path: Path) -> None:
    from usagebassoon.storage_model import SNAPSHOT_TABLES

    backend = DuckDBBackend(tmp_path / "deep" / "stats.duckdb")
    try:
        backend.apply_ddl()
        backend.apply_ddl()
        backend.preflight()
        tables = set(
            backend.query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE'"
            )
            .column("table_name")
            .to_pylist()
        )
        assert tables == set(SNAPSHOT_TABLES) | {
            "schema_marker",
            "schema_migrations",
            "restore_receipts",
        }
    finally:
        backend.close()


def test_local_backend_merges_current_state_in_place(
    collection_bundle: CollectionBundle,
) -> None:
    from datetime import timedelta
    from uuid import uuid4

    from usagebassoon.normalizer import normalize
    from usagebassoon.persistence import persist_run

    backend = DuckDBBackend(":memory:")
    try:
        backend.apply_ddl()
        first = normalize(collection_bundle)
        persist_run(backend, first)
        later = normalize(
            replace(
                collection_bundle,
                run_id=str(uuid4()),
                started_at=collection_bundle.started_at + timedelta(hours=1),
            )
        )
        daily = later.tables["daily_stats"]
        changed = daily.to_pylist()
        changed[0]["input_tokens"] += 10
        changed[0]["total_tokens"] += 10
        refreshed = pa.Table.from_pylist(changed, schema=daily.schema)
        later = replace(later, tables={**later.tables, "daily_stats": refreshed})
        persist_run(backend, later)
        actual = backend.query("SELECT * FROM current_daily_stats")
        assert normalized_records(actual) == normalized_records(refreshed)
        assert (
            backend.query("SELECT * FROM daily_stats").num_rows
            == first.tables["daily_stats"].num_rows
        )
    finally:
        backend.close()


def test_motherduck_rejects_invalid_database_name() -> None:
    """Assert MotherDuck rejects empty and already-prefixed database names."""
    with pytest.raises(ValueError, match="database name"):
        MotherDuckBackend("")
    with pytest.raises(ValueError, match="database name"):
        MotherDuckBackend("md:usagebassoon")


def test_motherduck_requires_token_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assert a missing MotherDuck token fails without contacting the service."""
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="MOTHERDUCK_TOKEN"):
        MotherDuckBackend("usagebassoon")


@pytest.mark.parametrize("backend_type", [DuckDBBackend, MotherDuckBackend])
def test_transactional_read_session_preserves_snapshot_after_external_commit(
    backend_type: type[DuckDBBackend] | type[MotherDuckBackend],
) -> None:
    """Verify both providers' explicit read scopes with the shared DuckDB engine."""
    connection_backend = DuckDBBackend(":memory:")
    # Avoid MotherDuck credentials while exercising its explicit scope implementation.
    backend = object.__new__(backend_type)
    backend.connection = connection_backend.connection
    writer = backend.connection.cursor()
    try:
        backend.apply_ddl()
        backend.query("CREATE TABLE values_at_read (value INTEGER)")
        backend.query("INSERT INTO values_at_read VALUES (1)")
        with backend.consistent_read() as read:
            assert read.query("SELECT * FROM values_at_read").num_rows == 1
            writer.execute("INSERT INTO values_at_read VALUES (2)")
            assert read.query("SELECT * FROM values_at_read").num_rows == 1
        assert backend.query("SELECT * FROM values_at_read").num_rows == 2
    finally:
        writer.close()
        backend.close()


def test_registered_migration_chain_rolls_back_and_retries_without_legacy_assets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise future migration rollback, chaining and ledger idempotency."""
    import usagebassoon.backends.duckdb_local as module
    import usagebassoon.schema_assets as assets

    backend = DuckDBBackend(":memory:")
    backend.apply_ddl()
    final_hash = assets.schema_hash("duckdb")
    steps = (
        assets.SchemaMigration(
            2,
            {"duckdb": "original", "bigquery": "original"},
            {"duckdb": "middle", "bigquery": "middle"},
            {"duckdb": "step2.sql", "bigquery": "step2.sql"},
        ),
        assets.SchemaMigration(
            3,
            {"duckdb": "middle", "bigquery": "middle"},
            {"duckdb": final_hash, "bigquery": assets.schema_hash("bigquery")},
            {"duckdb": "step3.sql", "bigquery": "step3.sql"},
        ),
    )
    monkeypatch.setattr(assets, "SCHEMA_VERSION", 3)
    monkeypatch.setattr(module, "SCHEMA_VERSION", 3)
    monkeypatch.setattr(assets, "SCHEMA_MIGRATIONS", steps)
    backend.connection.execute(
        "UPDATE schema_marker SET version = 1, schema_hash = 'original'"
    )
    fail = [True]

    def sql(step: assets.SchemaMigration, _dialect: str) -> str:
        if step.version == 3 and fail[0]:
            raise RuntimeError("interrupted migration")
        return f"CREATE TABLE migration_{step.version} (source_id VARCHAR NOT NULL)"

    monkeypatch.setattr(assets.SchemaMigration, "sql", sql)
    try:
        with pytest.raises(RuntimeError, match="interrupted migration"):
            backend.preflight()
        assert backend.query("SELECT version FROM schema_marker").to_pylist() == [
            {"version": 1}
        ]
        assert backend.query("SELECT * FROM schema_migrations").num_rows == 0
        assert (
            backend.query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = 'migration_2'"
            ).num_rows
            == 0
        )
        fail[0] = False
        backend.preflight()
        backend.preflight()
        assert backend.query("SELECT version FROM schema_marker").to_pylist() == [
            {"version": 3}
        ]
        assert backend.query(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).to_pylist() == [{"version": 2}, {"version": 3}]
    finally:
        backend.close()


@pytest.mark.parametrize(
    "failure", [RuntimeError("primary failure"), KeyboardInterrupt()]
)
def test_transaction_preserves_failure_when_rollback_fails(
    failure: BaseException,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Rollback failure cannot replace an operation failure or cancellation."""
    import logging

    backend = DuckDBBackend(":memory:")
    connection = backend.connection
    calls: list[str] = []

    class BrokenRollback:
        """Fail only the rollback performed during exception unwinding."""

        def execute(self, sql: str) -> None:
            calls.append(sql)
            if sql == "ROLLBACK":
                raise RuntimeError("rollback failure")

    monkeypatch.setattr(backend, "connection", BrokenRollback())
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("usagebassoon"), "disabled", False)
    try:
        with pytest.raises(type(failure)) as caught, backend.transaction():
            raise failure
        assert caught.value is failure
        assert calls == ["BEGIN TRANSACTION", "ROLLBACK"]
        assert "could not roll back" in caplog.text
        assert "rollback failure" in caplog.text
    finally:
        connection.close()
