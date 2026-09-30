# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""schema_assets.py — Ordered packaged SQL assets for schema initialization."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources

# Fresh installations use one baseline. Future upgrades register explicit steps.
SCHEMA_VERSION = 1
RUNTIME_SCHEMA_ASSETS = ("ddl.sql", "views.sql")
PARITY_SCHEMA_ASSETS = RUNTIME_SCHEMA_ASSETS


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
