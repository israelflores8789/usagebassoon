# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""format.py — Immutable snapshot contracts and forward transformation registry."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from usagebassoon.deadlines import remaining_seconds
from usagebassoon.storage_model import CANONICAL_TABLE_SCHEMAS, DATA_SCHEMA_VERSION

FORMAT_VERSION = 1
LEASE_SECONDS = 300
CATALOG_NAME = "catalog.json"
DATA_CONTRACTS: dict[int, Mapping[str, pa.Schema]] = {
    DATA_SCHEMA_VERSION: CANONICAL_TABLE_SCHEMAS,
}


def timestamp(value: object) -> datetime:
    """Parse an aware archive timestamp into UTC."""
    if not isinstance(value, str):
        raise ValueError("snapshot timestamp must be text")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("snapshot timestamp must include a timezone")
    return parsed.astimezone(UTC)


def snapshot_id(value: object) -> str:
    """Validate an identifier that names exactly one snapshot directory."""
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
        raise ValueError(f"invalid snapshot identifier: {value!r}")
    return value


def encode(document: Mapping[str, object]) -> bytes:
    """Serialize a document deterministically."""
    return (json.dumps(dict(document), sort_keys=True, indent=2) + "\n").encode()


def _current_reader(document: dict[str, object]) -> dict[str, object]:
    """Decode the current packaging contract without changing its contents."""
    return dict(document)


FORMAT_READERS: dict[int, Callable[[dict[str, object]], dict[str, object]]] = {
    FORMAT_VERSION: _current_reader,
}


def decode_manifest(document: dict[str, object]) -> dict[str, object]:
    """Dispatch a verified original manifest to a registered format reader."""
    version = document.get("snapshot_format_version")
    if (
        not isinstance(version, int)
        or isinstance(version, bool)
        or version not in FORMAT_READERS
    ):
        raise ValueError("unsupported snapshot format; use a compatible UsageBassoon")
    decoded = FORMAT_READERS[version](document)
    if version != FORMAT_VERSION:
        decoded["original_snapshot_format_version"] = version
    return decoded


def digest(path: Path) -> str:
    """Hash a file without materializing its contents."""
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            remaining_seconds()
            checksum.update(chunk)
    return checksum.hexdigest()


def validate_manifest(document: dict[str, object], identifier: str) -> None:
    """Validate completeness and version provenance before reading tables."""
    if document.get("snapshot_id") != identifier:
        raise ValueError("snapshot manifest identity does not match")
    if document.get("snapshot_format_version") != FORMAT_VERSION:
        raise ValueError("unsupported snapshot format; use a compatible UsageBassoon")
    version = document.get("data_schema_version")
    if not isinstance(version, int) or isinstance(version, bool):
        raise ValueError("snapshot data schema version is invalid")
    if version > DATA_SCHEMA_VERSION:
        raise ValueError("snapshot needs a newer UsageBassoon data schema reader")
    if version < DATA_SCHEMA_VERSION and not transformation_path(version):
        raise ValueError("no registered recovery transformation for this data schema")
    timestamp(document.get("captured_at"))
    for field in ("usagebassoon_version", "source_backend", "backend_schema_hash"):
        if not isinstance(document.get(field), str) or not document[field]:
            raise ValueError(f"snapshot has no {field}")
    if not isinstance(document.get("backend_schema_version"), int):
        raise ValueError("snapshot has no backend schema version")
    contract = DATA_CONTRACTS.get(version)
    if contract is None:
        raise ValueError("snapshot data contract reader is not registered")
    tables = document.get("tables")
    if not isinstance(tables, dict) or set(tables) != set(contract):
        raise ValueError("snapshot must contain every expected table")
    if any(
        not isinstance(t, dict) or t.get("status") != "complete"
        for t in tables.values()
    ):
        raise ValueError("snapshot contains an incomplete table")


@dataclass(frozen=True, slots=True)
class SnapshotTransformation:
    """One registered file-based, identity-preserving data-contract upgrade."""

    from_version: int
    to_version: int
    transform: Callable[[Mapping[str, Path], Path], Mapping[str, Path]]
    validate: Callable[[Mapping[str, Path], Mapping[str, Path]], None]


# Prerelease archives have no legacy support obligation. Released contracts add
# immutable recovery examples and explicit transformations here.
TRANSFORMATIONS: tuple[SnapshotTransformation, ...] = ()


def transformation_path(version: int) -> tuple[SnapshotTransformation, ...]:
    """Resolve a contiguous forward path, rejecting gaps and ambiguous steps."""
    steps: list[SnapshotTransformation] = []
    while version < DATA_SCHEMA_VERSION:
        matches = [step for step in TRANSFORMATIONS if step.from_version == version]
        if len(matches) != 1 or matches[0].to_version != version + 1:
            raise ValueError("no safe snapshot transformation path")
        step = matches[0]
        if (
            step.from_version not in DATA_CONTRACTS
            or step.to_version not in DATA_CONTRACTS
        ):
            raise ValueError("snapshot transformation contracts are not registered")
        steps.append(step)
        version = step.to_version
    if version != DATA_SCHEMA_VERSION:
        raise ValueError("snapshot transformation overshoots the current contract")
    return tuple(steps)
