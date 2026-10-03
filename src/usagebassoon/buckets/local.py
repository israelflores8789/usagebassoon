# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""local.py — Filesystem-backed implementation of the snapshot bucket contract."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
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


def sync_directory(path: Path) -> None:
    """Sync directory entries on platforms that support directory descriptors."""
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            os.fsync(descriptor)
        except OSError as error:
            if error.errno not in {errno.EINVAL, errno.ENOTSUP}:
                raise
    finally:
        os.close(descriptor)


def durable_directory(path: Path) -> None:
    """Persist links for newly created directory ancestors before publishing files."""
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        directory.mkdir(exist_ok=True)
        sync_directory(directory.parent)
        sync_directory(directory)


def durable_replace(path: Path, payload: bytes) -> None:
    """Flush a same-filesystem temporary object before atomic replacement."""
    durable_directory(path.parent)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


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
        durable_directory(path.parent)
        lock_path = path.with_name(f".{path.name}.lock")
        with _catalog_lock(lock_path):
            current = path.read_bytes() if path.exists() else None
            actual_version = None if current is None else self._version(current)
            if actual_version != expected_version:
                raise SnapshotPreconditionError("local snapshot object version changed")
            raw = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode()
            durable_replace(path, raw)
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
        durable_directory(path.parent)
        durable_replace(path, payload)
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

    def upload_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Durably publish a file without buffering its complete contents."""
        target = self._path(relative_name)
        durable_directory(target.parent)
        temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
        try:
            with path.open("rb") as source, temporary.open("xb") as destination:
                shutil.copyfileobj(source, destination, length=1024 * 1024)
                destination.flush()
                os.fsync(destination.fileno())
            # Hard-link publication is atomic and fails if the identity exists.
            os.link(temporary, target)
            sync_directory(target.parent)
        finally:
            temporary.unlink(missing_ok=True)
        with target.open("rb") as stream:
            version = hashlib.file_digest(stream, "sha256").hexdigest()
        return SnapshotObject(relative_name, version, target.stat().st_size, version)

    def download_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Copy from one open revision, retaining bounded memory."""
        with self._path(relative_name).open("rb") as source, path.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
        with path.open("rb") as stream:
            version = hashlib.file_digest(stream, "sha256").hexdigest()
        return SnapshotObject(relative_name, version, path.stat().st_size, version)

    def delete(self, relative_name: str, *, version: SnapshotVersion) -> None:
        """Delete a local object only when its content version still matches."""
        path = self._path(relative_name)
        if not path.exists():
            raise SnapshotPreconditionError("local snapshot object is absent")
        with _catalog_lock(path.with_name(f".{path.name}.lock")):
            with path.open("rb") as stream:
                actual = hashlib.file_digest(stream, "sha256").hexdigest()
            if actual != version:
                raise SnapshotPreconditionError("local snapshot object version changed")
            path.unlink()
            sync_directory(path.parent)

    def list(self, relative_prefix: str) -> tuple[SnapshotObject, ...]:
        """List local snapshot objects below one safe relative prefix."""
        prefix = validate_relative_name(relative_prefix, allow_empty=True)
        root = self._root if not prefix else self._path(prefix)
        if not root.exists():
            return ()
        objects: list[SnapshotObject] = []
        for path in (root,) if root.is_file() else root.rglob("*"):
            if not path.is_file():
                continue
            if path.name.startswith("."):
                continue
            with path.open("rb") as stream:
                version = hashlib.file_digest(stream, "sha256").hexdigest()
            objects.append(
                SnapshotObject(
                    name=path.relative_to(self._root).as_posix(),
                    version=version,
                    size=path.stat().st_size,
                    checksum=version,
                )
            )
        return tuple(objects)

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Return no provider lifecycle warnings for a local filesystem."""
        return ()
