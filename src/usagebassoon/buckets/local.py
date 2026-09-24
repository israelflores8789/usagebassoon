# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""local.py — Filesystem-backed implementation of the snapshot bucket contract."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from usagebassoon.buckets.base import (
    SnapshotObject,
    SnapshotPreconditionError,
    SnapshotVersion,
    validate_relative_name,
)


@contextmanager
def _catalog_lock(path: Path) -> Generator[None]:
    """Hold an OS lock on one local catalog across processes and threads.

    Yields:
        No value while the lock is held.
    """
    with path.open("a+b") as lock_file:
        if os.name == "nt":
            import msvcrt

            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


class LocalSnapshotBucket:
    """Store portable snapshot objects below one safely confined local root."""

    def __init__(self, uri: str) -> None:
        """Create a bucket rooted at a filesystem path or ``file://`` URI."""
        self.uri = uri.rstrip("/")
        self._root = Path(self.uri.removeprefix("file://")).expanduser().resolve()

    def _path(self, relative_name: str) -> Path:
        """Resolve one safe snapshot object path below the configured root."""
        relative = validate_relative_name(relative_name)
        candidate = (self._root / relative).resolve()
        try:
            candidate.relative_to(self._root)
        except ValueError as error:
            raise ValueError(
                f"snapshot object escapes local bucket root: {relative!r}"
            ) from error
        return candidate

    @staticmethod
    def _version(payload: bytes) -> str:
        """Return the content-addressed local version for one object."""
        return hashlib.sha256(payload).hexdigest()

    def read_json(
        self, relative_name: str
    ) -> tuple[dict[str, object] | None, SnapshotVersion | None]:
        """Read a JSON object and its content-addressed version."""
        path = self._path(relative_name)
        if not path.exists():
            return None, None
        raw = path.read_bytes()
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError(
                f"snapshot JSON object {relative_name!r} must be an object"
            )
        return payload, self._version(raw)

    def write_json_cas(
        self,
        relative_name: str,
        payload: dict[str, object],
        *,
        expected_version: SnapshotVersion | None,
    ) -> SnapshotObject:
        """Atomically compare and replace JSON.

        Verifies the local content version and writes under a
        process-shared filesystem lock.
        """
        path = self._path(relative_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = path.with_name(f".{path.name}.lock")
        with _catalog_lock(lock_path):
            current = path.read_bytes() if path.exists() else None
            actual_version = None if current is None else self._version(current)
            if actual_version != expected_version:
                raise SnapshotPreconditionError("local snapshot object version changed")
            raw = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode()
            temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
            try:
                temporary.write_bytes(raw)
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
        version = self._version(raw)
        return SnapshotObject(
            name=validate_relative_name(relative_name),
            version=version,
            size=len(raw),
            checksum=version,
        )

    def write_bytes(
        self,
        relative_name: str,
        payload: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> SnapshotObject:
        """Write one local snapshot object and return its immutable metadata."""
        del content_type
        path = self._path(relative_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        version = self._version(payload)
        return SnapshotObject(
            name=validate_relative_name(relative_name),
            version=version,
            size=len(payload),
            checksum=version,
        )

    def read_bytes(self, relative_name: str, *, version: SnapshotVersion) -> bytes:
        """Read one local object; integrity is verified by the archiver hash."""
        del version
        return self._path(relative_name).read_bytes()

    def delete(self, relative_name: str, *, version: SnapshotVersion) -> None:
        """Delete a local object only when its content version still matches."""
        path = self._path(relative_name)
        if not path.exists():
            raise SnapshotPreconditionError("local snapshot object is absent")
        if self._version(path.read_bytes()) != version:
            raise SnapshotPreconditionError("local snapshot object version changed")
        path.unlink()

    def list(self, relative_prefix: str) -> tuple[SnapshotObject, ...]:
        """List local snapshot objects below one safe relative prefix."""
        prefix = validate_relative_name(relative_prefix, allow_empty=True)
        root = self._root if not prefix else self._path(prefix)
        if not root.exists():
            return ()
        objects: list[SnapshotObject] = []
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            raw = path.read_bytes()
            version = self._version(raw)
            objects.append(
                SnapshotObject(
                    name=path.relative_to(self._root).as_posix(),
                    version=version,
                    size=len(raw),
                    checksum=version,
                )
            )
        return tuple(objects)

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Return no provider lifecycle warnings for a local filesystem."""
        return ()
