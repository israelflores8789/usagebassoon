# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_sql_safety.py — Public relation-query boundary tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from usagebassoon import query, query_arrow
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.cli.app import app
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
        f'source_id = "{SOURCE_ID}"\nbackend = "duckdb"\ndatabase = "{database}"\n'
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
    ],
)
def test_public_sql_validator_rejects_external_or_expressive_sql(sql: str) -> None:
    """Reject external data access and SQL features outside relation queries."""
    with pytest.raises(ValueError):
        validate_read_only_sql(sql, dialect="duckdb")


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


def test_python_query_apis_apply_the_public_validator(
    initialized_config: Path,
) -> None:
    """Guard both Arrow and dataframe APIs before backend execution."""
    allowed = query_arrow(
        "SELECT * FROM report_models LIMIT 1", config=initialized_config
    )
    assert allowed.num_rows == 0
    with pytest.raises(ValueError):
        query_arrow(
            "SELECT * FROM read_csv_auto('/tmp/secret.csv') LIMIT 1",
            config=initialized_config,
        )
    with pytest.raises(ValueError):
        query("SELECT * FROM daily_cost", config=initialized_config)


def test_cli_query_only_accepts_an_allowlisted_relation(
    initialized_config: Path,
) -> None:
    """Generate a bounded relation query rather than accepting arbitrary SQL."""
    runner = CliRunner()

    allowed = runner.invoke(
        app,
        ["query", "report_models", "--limit", "1", "--config", str(initialized_config)],
    )
    rejected = runner.invoke(
        app,
        [
            "query",
            "read_csv_auto",
            "--filter",
            "path=/tmp/secret.csv",
            "--config",
            str(initialized_config),
        ],
    )

    assert allowed.exit_code == 0
    assert rejected.exit_code != 0
    assert "not supported" in rejected.output
