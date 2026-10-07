# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""schema_assets.py — Packaged schema assets and named native query templates."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from importlib import resources

# Fresh installations use one baseline. Future upgrades register explicit steps.
SCHEMA_VERSION = 1
RUNTIME_SCHEMA_ASSETS = ("ddl.sql", "views.sql")
PARITY_SCHEMA_ASSETS = RUNTIME_SCHEMA_ASSETS
QUERY_ASSET = "queries.sql"


@cache
def _query_templates(dialect: str) -> dict[str, str]:
    """Index packaged queries separated by explicit ``-- name:`` markers."""
    if dialect not in {"duckdb", "bigquery"}:
        raise ValueError(f"unsupported SQL dialect: {dialect}")
    sql = (
        resources.files(f"usagebassoon.sql.{dialect}").joinpath(QUERY_ASSET).read_text()
    )
    queries: dict[str, list[str]] = {}
    current: str | None = None
    for line in sql.splitlines():
        if line.startswith("-- name:"):
            name = line.removeprefix("-- name:").strip()
            if not name.isascii() or not name.isidentifier():
                raise ValueError(f"invalid packaged query name: {name!r}")
            if name in queries:
                raise ValueError(f"duplicate packaged query: {name}")
            queries[name] = []
            current = name
        elif current is not None:
            queries[current].append(line)
    result = {name: "\n".join(lines).strip() for name, lines in queries.items()}
    if not result or any(not sql for sql in result.values()):
        raise ValueError("packaged queries must contain nonempty named SQL")
    return result


def query_sql(dialect: str, name: str) -> str:
    """Read one named native query template from the dialect's queries.sql.

    Callers insert only trusted SQL fragments and bind values separately. Query
    templates are not installed schema objects or part of the schema hash.

    Args:
        dialect: SQL installation dialect; MotherDuck uses DuckDB.
        name: Exact logical query name from a ``-- name:`` marker.

    Returns:
        Packaged SQL, including placeholders for controlled query construction.

    Raises:
        ValueError: If the dialect, query name, or packaged markers are invalid.
    """
    try:
        return _query_templates(dialect)[name]
    except KeyError as error:
        raise ValueError(f"packaged query is not defined: {name}") from error


def schema_hash(dialect: str) -> str:
    """Hash packaged schema assets and compaction without an application version."""
    from hashlib import sha256

    package = resources.files(f"usagebassoon.sql.{dialect}")
    names = RUNTIME_SCHEMA_ASSETS + (
        ("compaction.sql",) if dialect == "bigquery" else ()
    )
    return sha256(
        "\n".join(package.joinpath(name).read_text() for name in names).encode()
    ).hexdigest()[:63]


@dataclass(frozen=True, slots=True)
class SchemaMigration:
    """A hash-gated future upgrade; BigQuery SQL must survive replay after a crash."""

    version: int
    previous_hashes: dict[str, str]
    target_hashes: dict[str, str]
    assets: dict[str, str]

    def sql(self, dialect: str) -> str:
        """Read the registered native SQL step for one backend dialect."""
        return (
            resources.files(f"usagebassoon.sql.{dialect}")
            .joinpath(self.assets[dialect])
            .read_text()
        )


# There is no legacy baseline to migrate. Register actual steps after release.
SCHEMA_MIGRATIONS: tuple[SchemaMigration, ...] = ()


def validate_migration_registry() -> None:
    """Require native, hash-gated upgrades for every supported SQL installation."""
    dialects = {"duckdb", "bigquery"}
    versions: set[int] = set()
    previous = 0
    for step in SCHEMA_MIGRATIONS:
        if (
            step.version in versions
            or step.version <= previous
            or step.version > SCHEMA_VERSION
        ):
            raise RuntimeError("schema migration versions must be unique and ordered")
        if any(
            set(mapping) != dialects
            for mapping in (step.previous_hashes, step.target_hashes, step.assets)
        ):
            raise RuntimeError(
                "schema migrations must explicitly cover every SQL dialect"
            )
        if any(
            not value
            for mapping in (step.previous_hashes, step.target_hashes)
            for value in mapping.values()
        ):
            raise RuntimeError("schema migrations require original and target hashes")
        if any(
            name in {*RUNTIME_SCHEMA_ASSETS, "compaction.sql"}
            or not name.endswith(".sql")
            or ".." in name
            or name.startswith("/")
            for name in step.assets.values()
        ):
            raise RuntimeError("schema migrations require dedicated forward SQL assets")
        versions.add(step.version)
        previous = step.version


def pending_migrations(
    version: int, stored_hash: str, dialect: str
) -> tuple[SchemaMigration, ...]:
    """Validate the marker and return only registered, contiguous upgrade steps."""
    if version > SCHEMA_VERSION:
        raise RuntimeError("this warehouse was migrated by a newer UsageBassoon")
    if version == SCHEMA_VERSION:
        if stored_hash != schema_hash(dialect):
            raise RuntimeError("warehouse schema hash does not match this package")
        return ()
    validate_migration_registry()
    steps = tuple(step for step in SCHEMA_MIGRATIONS if step.version > version)
    for step in steps:
        if step.version != version + 1 or step.previous_hashes[dialect] != stored_hash:
            raise RuntimeError("no safe migration path exists for this warehouse")
        version = step.version
        stored_hash = step.target_hashes[dialect]
    if version != SCHEMA_VERSION or stored_hash != schema_hash(dialect):
        raise RuntimeError(
            "no safe migration path exists; initialize a fresh warehouse"
        )
    return steps


def view_sql(dialect: str, name: str) -> str:
    """Return one packaged native view definition for private query connections.

    Args:
        dialect: SQL installation whose view definition is needed.
        name: Exact name of the packaged view.

    Returns:
        The native CREATE VIEW statement.

    Raises:
        ValueError: If the requested dialect or view is not supported.
    """
    import sqlglot
    from sqlglot import exp

    if dialect not in {"duckdb", "bigquery"}:
        raise ValueError(f"unsupported SQL dialect: {dialect}")
    sql = (
        resources.files(f"usagebassoon.sql.{dialect}").joinpath("views.sql").read_text()
    )
    for statement in sqlglot.parse(sql, read=dialect):
        if (
            isinstance(statement, exp.Create)
            and statement.kind == "VIEW"
            and statement.this.name == name
        ):
            return statement.sql(dialect=dialect)
    raise ValueError(f"packaged view is not defined: {name}")
