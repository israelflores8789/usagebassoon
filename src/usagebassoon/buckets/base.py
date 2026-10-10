# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""base.py — Contracts shared by portable snapshot object-storage providers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit

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

    def upload_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Upload a file with bounded memory and create-only semantics."""
        ...

    def download_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Download the current stable revision into a file with bounded memory."""
        ...

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


def bucket_scheme(uri: str) -> str:
    """Identify a URI scheme while preserving ordinary filesystem paths."""
    return urlsplit(uri).scheme.lower() if "://" in uri else ""


def bucket_uri(uri: str) -> str:
    """Normalize archive roots without constructing or authenticating adapters."""
    if not uri.strip():
        raise ValueError("snapshot bucket URI must not be empty")
    scheme = bucket_scheme(uri)
    if scheme not in {"", "file"}:
        return f"{scheme}://{uri.partition('://')[2]}".rstrip("/")
    path = uri.partition("://")[2] if scheme == "file" else uri
    if not path:
        raise ValueError("snapshot bucket path must not be empty")
    return str(Path(path).expanduser().resolve())


class ScopedSnapshotBucket:
    """Expose a nested archive root through any existing bucket adapter."""

    def __init__(self, bucket: SnapshotBucket, prefix: str) -> None:
        """Reuse authentication and provider operations for a confined subroot."""
        self._bucket = bucket
        self._prefix = validate_relative_name(prefix)
        self.uri = f"{bucket_uri(bucket.uri)}/{self._prefix}"

    def _name(self, name: str, *, allow_empty: bool = False) -> str:
        """Translate a portable object name into the parent archive namespace."""
        relative = validate_relative_name(name, allow_empty=allow_empty)
        return f"{self._prefix}/{relative}"

    def _object(self, obj: SnapshotObject) -> SnapshotObject:
        """Keep provider versions while returning names relative to this root."""
        if not obj.name.startswith(self._prefix + "/"):
            raise ValueError("provider returned an object outside the scoped root")
        return SnapshotObject(
            obj.name.removeprefix(self._prefix + "/"),
            obj.version,
            obj.size,
            obj.checksum,
        )

    def upload_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Publish a bounded file under this archive root."""
        return self._object(self._bucket.upload_file(self._name(relative_name), path))

    def download_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Download one parent-provider revision with relative metadata."""
        return self._object(self._bucket.download_file(self._name(relative_name), path))

    def read_json(
        self, relative_name: str
    ) -> tuple[dict[str, object] | None, SnapshotVersion | None]:
        """Read JSON metadata from this root."""
        return self._bucket.read_json(self._name(relative_name))

    def write_json_cas(
        self,
        relative_name: str,
        payload: dict[str, object],
        *,
        expected_version: SnapshotVersion | None,
    ) -> SnapshotObject:
        """Delegate native compare-and-swap without weakening its precondition."""
        return self._object(
            self._bucket.write_json_cas(
                self._name(relative_name), payload, expected_version=expected_version
            )
        )

    def write_bytes(
        self,
        relative_name: str,
        payload: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> SnapshotObject:
        """Write one scoped object through the parent provider."""
        return self._object(
            self._bucket.write_bytes(
                self._name(relative_name), payload, content_type=content_type
            )
        )

    def read_bytes(self, relative_name: str, *, version: SnapshotVersion) -> bytes:
        """Read exactly the requested parent-provider revision."""
        return self._bucket.read_bytes(self._name(relative_name), version=version)

    def delete(self, relative_name: str, *, version: SnapshotVersion) -> None:
        """Delete only the scoped name at its observed parent-provider revision."""
        self._bucket.delete(self._name(relative_name), version=version)

    def list(self, relative_prefix: str) -> tuple[SnapshotObject, ...]:
        """List scoped names, excluding neighboring archive prefixes."""
        prefix = self._name(relative_prefix, allow_empty=True)
        return tuple(
            self._object(obj)
            for obj in self._bucket.list(prefix.rstrip("/"))
            if obj.name.startswith(self._prefix + "/")
        )

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Retain the underlying provider's lifecycle inspection."""
        return self._bucket.lifecycle_warnings()
