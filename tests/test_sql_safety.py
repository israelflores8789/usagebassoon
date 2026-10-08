# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_sql_safety.py — Public relation-query boundary tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from tests._cli import plain_cli_output
from usagebassoon import query, query_arrow
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
from usagebassoon.curation import NoteAssignment, set_note
from usagebassoon.sql_safety import build_relation_query, validate_read_only_sql

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


@pytest.fixture
def initialized_config(tmp_path: Path) -> Path:
    """Create an initialized local configuration for public-query tests."""
    database = tmp_path / "usage.duckdb"
    backend = DuckDBBackend(database)
    try:
        backend.apply_ddl()
    finally:
        backend.close()
    config = tmp_path / "config.toml"
    config.write_text(
        f'source_id = "{SOURCE_ID}"\nbackend.provider = "duckdb"\n'
        f'backend.duckdb.database = "{database}"\n'
    )
    return config


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM read_csv_auto('/tmp/secret.csv') LIMIT 1",
        "SELECT * FROM 'https://example.test/data.parquet' LIMIT 1",
        "SELECT * FROM other_dataset.daily_cost LIMIT 1",
        "SELECT count(*) FROM daily_cost LIMIT 1",
        "SELECT * FROM (SELECT * FROM daily_cost) AS nested LIMIT 1",
        "SELECT * FROM daily_cost LIMIT 1; SELECT * FROM report_models LIMIT 1",
        "DELETE FROM daily_cost",
        "WITH facts AS (SELECT * FROM daily_cost) SELECT * FROM facts LIMIT 1",
        "SELECT * FROM daily_cost JOIN report_models USING (model) LIMIT 1",
        "SELECT * FROM daily_cost UNION ALL SELECT * FROM daily_cost LIMIT 1",
        "SELECT * FROM daily_cost ORDER BY day LIMIT 1",
        "SELECT * FROM daily_cost",
        "SELECT * FROM daily_cost LIMIT 0",
        "SELECT * FROM daily_cost LIMIT 1000001",
        "SELECT * FROM daily_cost LIMIT :limit",
        "SELECT * FROM session_model_stats_current LIMIT 1",
    ],
)
@pytest.mark.parametrize("dialect", ["duckdb", "bigquery"])
def test_public_sql_validator_rejects_external_or_expressive_sql(
    sql: str, dialect: str
) -> None:
    """Reject external data access and SQL features outside relation queries."""
    with pytest.raises(ValueError):
        validate_read_only_sql(sql, dialect=dialect)


def test_public_sql_validator_accepts_one_allowlisted_bounded_relation() -> None:
    """Permit a simple allowlisted relation with a bound filter parameter."""
    sql, parameters = build_relation_query(
        "daily_cost",
        filters={"client": "codex"},
        limit=25,
    )

    validate_read_only_sql(sql, dialect="duckdb")

    assert parameters == {"filter_0": "codex"}


def test_public_sql_validator_uses_bigquery_identifier_quoting() -> None:
    """Generate SQL that BigQuery accepts for an allowlisted relation."""
    sql, parameters = build_relation_query(
        "report_summary",
        limit=1,
        dialect="bigquery",
    )

    validate_read_only_sql(sql, dialect="bigquery")

    assert sql == "SELECT * FROM `report_summary` LIMIT 1"
    assert parameters == {}


@pytest.mark.parametrize("api", ["arrow", "dataframe"])
def test_python_query_apis_apply_the_public_validator(
    initialized_config: Path, api: str
) -> None:
    """Guard both Arrow and dataframe APIs before backend execution."""
    read = query_arrow if api == "arrow" else query
    allowed = query_arrow(
        "SELECT * FROM report_models LIMIT 1", config=initialized_config
    )
    assert allowed.num_rows == 0
    with pytest.raises(ValueError):
        read(
            "SELECT * FROM read_csv_auto('/tmp/secret.csv') LIMIT 1",
            config=initialized_config,
        )
    with pytest.raises(ValueError):
        read("SELECT * FROM daily_cost", config=initialized_config)


def test_public_query_preserves_colon_containing_session_identity(
    initialized_config: Path,
) -> None:
    """Return the exact stored session rather than a rewritten literal."""
    backend = DuckDBBackend(initialized_config.parent / "usage.duckdb")
    try:
        set_note(
            backend, NoteAssignment(SOURCE_ID, "codex", "provider:session", "note")
        )
    finally:
        backend.close()
    result = query_arrow(
        "SELECT session_id FROM session_notes "
        "WHERE session_id = 'provider:session' /* :comment */ LIMIT 1",
        config=initialized_config,
    )
    assert result.to_pylist() == [{"session_id": "provider:session"}]


@pytest.mark.parametrize(
    "relation",
    [
        "read_csv_auto",
        "https://example.test/private.parquet",
        "daily_cost; DELETE FROM notes",
    ],
)
def test_cli_query_rejects_external_readers_and_sql_text(
    initialized_config: Path, relation: str
) -> None:
    """Reject executable relation arguments at the CLI boundary."""
    runner = CliRunner()
    rejected = runner.invoke(
        app,
        [
            "query",
            relation,
            "--filter",
            "path=/tmp/secret.csv",
            "--config",
            str(initialized_config),
        ],
    )
    assert rejected.exit_code != 0
    assert "not supported" in plain_cli_output(rejected.output)
