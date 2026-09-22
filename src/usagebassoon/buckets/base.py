# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""base.py — Contracts shared by portable snapshot object-storage providers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

type SnapshotVersion = int | str


def validate_relative_name(value: str, *, allow_empty: bool = False) -> str:
    """Validate a portable snapshot-root-relative object name.

    Args:
        value: POSIX-style object name relative to a snapshot bucket root.
        allow_empty: Whether an empty name is valid for prefix listing.

    Returns:
        The validated object name.

    Raises:
        ValueError: If the name is not a safe relative object name.
    """
    if not isinstance(value, str):
        raise ValueError("snapshot object name must be a string")
    if not value:
        if allow_empty:
            return value
        raise ValueError("snapshot object name must not be empty")
    if value.startswith(("/", "\\\\")) or "\\" in value or "\x00" in value:
        raise ValueError(f"snapshot object name is not relative: {value!r}")
    components = value.split("/")
    if any(component in {"", ".", ".."} for component in components):
        raise ValueError(f"snapshot object name has an unsafe component: {value!r}")
    if len(value) >= 2 and value[0].isalpha() and value[1] == ":":
        raise ValueError(f"snapshot object name is not relative: {value!r}")
    return value


class SnapshotPreconditionError(RuntimeError):
    """Raised when a version-conditional bucket operation loses a race."""


@dataclass(frozen=True, slots=True)
class SnapshotObject:
    """Immutable metadata for one published snapshot object.

    Attributes:
        name: Snapshot-root-relative object name.
        version: Provider-specific immutable version at publication time.
        size: Object size in bytes.
        checksum: Provider checksum when available.
    """

    name: str
    version: SnapshotVersion
    size: int
    checksum: str | None


class SnapshotBucket(Protocol):
    """Provider-neutral object storage for snapshot catalogs and Parquet artifacts.

    Implementations guarantee version-aware reads, compare-and-swap catalog
    publication, and exact-version deletion. They do not define snapshot
    semantics; :class:`usagebassoon.archiver.SnapshotArchiver` does that.
    """

    uri: str

    def read_json(
        self, relative_name: str
    ) -> tuple[dict[str, object] | None, SnapshotVersion | None]:
        """Read one JSON object and its current version, if present."""
        ...

    def write_json_cas(
        self,
        relative_name: str,
        payload: dict[str, object],
        *,
        expected_version: SnapshotVersion | None,
    ) -> SnapshotObject:
        """Atomically create or replace JSON when its version still matches."""
        ...

    def write_bytes(
        self,
        relative_name: str,
        payload: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> SnapshotObject:
        """Write bytes and return their immutable publication metadata."""
        ...

    def read_bytes(self, relative_name: str, *, version: SnapshotVersion) -> bytes:
        """Read the exact published version of one object."""
        ...

    def delete(self, relative_name: str, *, version: SnapshotVersion) -> None:
        """Delete exactly one published object version."""
        ...

    def list(self, relative_prefix: str) -> tuple[SnapshotObject, ...]:
        """List immutable metadata below one snapshot-root-relative prefix."""
        ...

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Report provider lifecycle settings that could remove snapshots."""
        ...
