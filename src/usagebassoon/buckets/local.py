# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""local.py — Filesystem-backed implementation of the snapshot bucket contract."""

from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
import time
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
from usagebassoon.deadlines import checked, remaining_seconds

_LOG = logging.getLogger("usagebassoon")


def _file_metadata(path: Path) -> tuple[str, os.stat_result]:
    """Hash bounded chunks and identify the same open file revision."""
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            remaining_seconds()
            checksum.update(chunk)
        return checksum.hexdigest(), os.fstat(stream.fileno())


def _remove_temporary(path: Path) -> None:
    """Remove a disposable file without masking publication or failure."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        _LOG.warning("could not remove temporary snapshot file %s", path, exc_info=True)


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
        directory.mkdir(mode=0o700, exist_ok=True)
        sync_directory(directory.parent)
        sync_directory(directory)


def durable_replace(path: Path, payload: bytes) -> None:
    """Flush a same-filesystem temporary object before atomic replacement."""
    durable_directory(path.parent)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with os.fdopen(
            os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb"
        ) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        sync_directory(path.parent)
    finally:
        _remove_temporary(temporary)


@contextmanager
def _catalog_lock(path: Path) -> Generator[None]:
    """Hold an OS lock on one local catalog across processes and threads.

    Yields:
        No value while the lock is held.
    """
    with os.fdopen(os.open(path, os.O_RDWR | os.O_CREAT, 0o600), "a+b") as lock_file:
        if os.name == "nt":
            import msvcrt

            lock_file.seek(0)
            while True:
                remaining_seconds()
                try:
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    time.sleep(min(0.05, remaining_seconds()))
            try:
                yield
            finally:
                try:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    _LOG.warning(
                        "could not unlock snapshot catalog; closing handle",
                        exc_info=True,
                    )
        else:
            import fcntl

            while True:
                remaining_seconds()
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    time.sleep(min(0.05, remaining_seconds()))
            try:
                yield
            finally:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                except OSError:
                    _LOG.warning(
                        "could not unlock snapshot catalog; closing handle",
                        exc_info=True,
                    )


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
    def _version(stat: os.stat_result) -> str:
        """Identify a physical file revision independently of its content digest."""
        return (
            f"{stat.st_dev}:{stat.st_ino}:{stat.st_ctime_ns}:"
            f"{stat.st_mtime_ns}:{stat.st_size}"
        )

    def _lock(self, relative_name: str) -> Path:
        """Use bounded, stable lock stripes that survive snapshot deletion."""
        durable_directory(self._root)
        identity = os.path.normcase(str(self._path(relative_name)))
        stripe = hashlib.sha256(identity.encode()).hexdigest()[:2]
        return self._root / f".snapshot-lock-{stripe}"

    @checked
    def read_json(
        self, relative_name: str
    ) -> tuple[dict[str, object] | None, SnapshotVersion | None]:
        """Read JSON and the identity of the same open file revision."""
        path = self._path(relative_name)
        if not path.exists():
            return None, None
        with path.open("rb") as stream:
            raw = stream.read()
            version = self._version(os.fstat(stream.fileno()))
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError(
                f"snapshot JSON object {relative_name!r} must be an object"
            )
        return payload, version

    @checked
    def write_json_cas(
        self,
        relative_name: str,
        payload: dict[str, object],
        *,
        expected_version: SnapshotVersion | None,
    ) -> SnapshotObject:
        """Atomically compare and replace JSON.

        Verifies the physical file revision and writes under a
        process-shared filesystem lock.
        """
        path = self._path(relative_name)
        durable_directory(path.parent)
        lock_path = self._lock(relative_name)
        with _catalog_lock(lock_path):
            actual_version = self._version(path.stat()) if path.exists() else None
            if actual_version != expected_version:
                raise SnapshotPreconditionError("local snapshot object version changed")
            raw = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode()
            durable_replace(path, raw)
            version = self._version(path.stat())
        return SnapshotObject(
            name=validate_relative_name(relative_name),
            version=version,
            size=len(raw),
            checksum=hashlib.sha256(raw).hexdigest(),
        )

    @checked
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
        with _catalog_lock(self._lock(relative_name)):
            durable_replace(path, payload)
            version = self._version(path.stat())
        return SnapshotObject(
            name=validate_relative_name(relative_name),
            version=version,
            size=len(payload),
            checksum=hashlib.sha256(payload).hexdigest(),
        )

    @checked
    def read_bytes(self, relative_name: str, *, version: SnapshotVersion) -> bytes:
        """Read the requested physical revision from one open descriptor."""
        with self._path(relative_name).open("rb") as stream:
            if self._version(os.fstat(stream.fileno())) != version:
                raise SnapshotPreconditionError("local snapshot object version changed")
            return stream.read()

    @checked
    def upload_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Durably publish a file without buffering its complete contents."""
        target = self._path(relative_name)
        durable_directory(target.parent)
        temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
        try:
            with (
                path.open("rb") as source,
                os.fdopen(
                    os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
                    "wb",
                ) as destination,
            ):
                while chunk := source.read(1024 * 1024):
                    remaining_seconds()
                    destination.write(chunk)
                destination.flush()
                os.fsync(destination.fileno())
            # Hard-link publication is atomic and fails if the identity exists.
            with _catalog_lock(self._lock(relative_name)):
                os.link(temporary, target)
                temporary.unlink()
                sync_directory(target.parent)
                checksum, stat = _file_metadata(target)
                version = self._version(stat)
        finally:
            _remove_temporary(temporary)
        return SnapshotObject(relative_name, version, stat.st_size, checksum)

    @checked
    def download_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Copy from one open revision, retaining bounded memory."""
        with self._path(relative_name).open("rb") as source, path.open("wb") as target:
            while chunk := source.read(1024 * 1024):
                remaining_seconds()
                target.write(chunk)
            version = self._version(os.fstat(source.fileno()))
        checksum, stat = _file_metadata(path)
        return SnapshotObject(relative_name, version, stat.st_size, checksum)

    @checked
    def delete(self, relative_name: str, *, version: SnapshotVersion) -> None:
        """Delete only the requested incarnation under the publication lock."""
        path = self._path(relative_name)
        if not path.exists():
            raise SnapshotPreconditionError("local snapshot object is absent")
        with _catalog_lock(self._lock(relative_name)):
            actual = self._version(path.stat())
            if actual != version:
                raise SnapshotPreconditionError("local snapshot object version changed")
            path.unlink()
            sync_directory(path.parent)
            parent = path.parent
            while parent != self._root:
                try:
                    parent.rmdir()
                except OSError as error:
                    if error.errno not in {errno.ENOTEMPTY, errno.EEXIST, errno.ENOENT}:
                        _LOG.warning(
                            "could not remove empty snapshot directory %s",
                            parent,
                            exc_info=True,
                        )
                    break
                sync_directory(parent.parent)
                parent = parent.parent

    @checked
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
            checksum, stat = _file_metadata(path)
            version = self._version(stat)
            objects.append(
                SnapshotObject(
                    name=path.relative_to(self._root).as_posix(),
                    version=version,
                    size=stat.st_size,
                    checksum=checksum,
                )
            )
        return tuple(objects)

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Return no provider lifecycle warnings for a local filesystem."""
        return ()
