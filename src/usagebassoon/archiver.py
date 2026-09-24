# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""archiver.py — Catalog-published portable Parquet snapshot publication and restore."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
from base64 import b64decode, b64encode
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from typing import TYPE_CHECKING
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from usagebassoon.backends.base import StorageBackend
from usagebassoon.buckets.base import (
    SnapshotBucket,
    SnapshotObject,
    SnapshotPreconditionError,
    SnapshotVersion,
    validate_relative_name,
)
from usagebassoon.buckets.local import LocalSnapshotBucket
from usagebassoon.config import default_snapshot_directory, parse_interval

if TYPE_CHECKING:
    from usagebassoon.config import UsageBassoonConfig

SNAPSHOT_TABLES: tuple[str, ...] = (
    "sessions",
    "daily_stats",
    "daily_activity",
    "price_versions",
    "ingest_status",
    "reconciliation_issues",
    "schema_drift_events",
    "ingest_runs",
    "tags",
    "notes",
)
_CATALOG_NAME = "catalog.json"
_FORMAT_VERSION = 1
_LEASE_SECONDS = 300
_SNAPSHOT_ID = re.compile(r"[A-Za-z0-9_-]+\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_LOG = logging.getLogger("usagebassoon")


def _now() -> datetime:
    """Return the current UTC instant."""
    return datetime.now(UTC)


def _timestamp(value: str) -> datetime:
    """Parse a timezone-aware snapshot timestamp."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("snapshot timestamp must include a timezone")
    return parsed.astimezone(UTC)


def _empty_catalog() -> dict[str, object]:
    """Return the initial archive catalog document."""
    return {
        "version": _FORMAT_VERSION,
        "entries": [],
        "reservation": None,
        "fence": 0,
    }


def _validate_snapshot_id(value: object) -> str:
    """Validate one generated snapshot identifier.

    Args:
        value: Untrusted catalog or command-line snapshot identifier.

    Returns:
        The validated identifier.

    Raises:
        ValueError: If the identifier cannot name one snapshot directory.
    """
    if not isinstance(value, str) or _SNAPSHOT_ID.fullmatch(value) is None:
        raise ValueError(f"invalid snapshot identifier: {value!r}")
    return value


def _reference_metadata(
    reference: object, *, expected_name: str
) -> tuple[str, SnapshotVersion, int, str]:
    """Validate one checksum-protected archive object reference.

    Args:
        reference: Untrusted catalog or manifest object reference.
        expected_name: Exact archive-relative name expected for the object.

    Returns:
        Object name, generation, size, and SHA-256 digest.

    Raises:
        ValueError: If the reference does not have the expected safe shape.
    """
    if not isinstance(reference, dict):
        raise ValueError("snapshot object reference must be an object")
    name = reference.get("name")
    version = reference.get("version")
    size = reference.get("size")
    sha256 = reference.get("sha256")
    if (
        not isinstance(name, str)
        or validate_relative_name(name) != expected_name
        or not isinstance(version, (int, str))
        or isinstance(version, bool)
        or (isinstance(version, int) and version < 0)
        or (isinstance(version, str) and not version)
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size < 0
        or not isinstance(sha256, str)
        or _SHA256.fullmatch(sha256) is None
    ):
        raise ValueError("snapshot object reference is invalid")
    return name, version, size, sha256


@dataclass(frozen=True, slots=True)
class _CapturedTable:
    """One serialized table captured once for all snapshot destinations."""

    rows: int
    schema: str
    schema_ipc: str
    completed_at: str
    payload: bytes | None


class _ReservationHeartbeat:
    """Keep snapshot reservations valid while a capture is in progress."""

    def __init__(
        self,
        archiver: SnapshotArchiver,
        owner: str,
        claims: tuple[tuple[SnapshotBucket, int], ...],
    ) -> None:
        """Prepare periodic renewal for all claimed destinations."""
        self._archiver = archiver
        self._owner = owner
        self._claims = claims
        self._stop = Event()
        self._failure: Exception | None = None
        self._thread = Thread(target=self._run, name="snapshot-lease", daemon=True)

    def _run(self) -> None:
        """Renew reservations until stopped or a claim is lost."""
        while not self._stop.wait(_LEASE_SECONDS / 3):
            try:
                if not self._archiver._renew_claims(self._owner, self._claims, _now()):
                    raise RuntimeError("snapshot publication reservation was lost")
            except Exception as error:
                self._failure = error
                self._stop.set()
                return

    def start(self) -> None:
        """Start the background renewal loop."""
        self._thread.start()

    def stop(self) -> None:
        """Stop renewal before catalog publication or failure cleanup."""
        self._stop.set()
        self._thread.join()

    def check(self) -> None:
        """Raise if renewal failed during the capture."""
        if self._failure is not None:
            raise RuntimeError("snapshot reservation renewal failed") from self._failure


class SnapshotArchiver:
    """Publish and restore complete catalog-backed normalized-table snapshots.

    The archiver owns snapshot format, publication, retention, and restore
    semantics. Snapshot buckets only provide version-aware object storage.
    """

    def __init__(
        self,
        uri: str | None = None,
        *,
        file_uri: str | None = None,
        gcs_archive_uri: str | None = None,
        max_snapshots: int = 3,
        interval: str | None = None,
        gcs_bucket: SnapshotBucket | None = None,
    ) -> None:
        """Configure one or two snapshot destinations.

        Args:
            uri: Single local or cloud snapshot archive URI.
            file_uri: Optional local archive URI.
            gcs_archive_uri: Optional GCS archive URI.
            max_snapshots: Positive published-snapshot retention ceiling.
            interval: Optional publication cadence.
            gcs_bucket: Injectable cloud bucket for isolated tests.
        """
        if max_snapshots < 1:
            raise ValueError("max_snapshots must be positive")
        if uri is not None and (file_uri is not None or gcs_archive_uri is not None):
            raise ValueError("uri cannot be combined with file_uri or gcs_archive_uri")
        locations = (
            [(uri, gcs_bucket)]
            if uri is not None
            else [
                *([(file_uri, None)] if file_uri is not None else []),
                *(
                    [(gcs_archive_uri, gcs_bucket)]
                    if gcs_archive_uri is not None
                    else []
                ),
            ]
        )
        if not locations:
            raise ValueError("at least one snapshot destination is required")
        if file_uri is not None and file_uri.startswith("gs://"):
            raise ValueError("file_uri must be a local path or file:// URI")
        if gcs_archive_uri is not None and not gcs_archive_uri.startswith("gs://"):
            raise ValueError("gcs_archive_uri must be a gs:// URI")
        self._archives = tuple(
            archive if archive is not None else LocalSnapshotBucket(location)
            for location, archive in locations
        )
        self._primary = self._archives[0]
        self.uri = self._primary.uri
        self.max_snapshots = max_snapshots
        self.interval = parse_interval(interval)

    @classmethod
    def from_config(cls, configuration: UsageBassoonConfig) -> SnapshotArchiver:
        """Create an archiver from validated application configuration."""
        from usagebassoon.buckets.gcs import GcsSnapshotBucket

        settings = configuration.snapshots
        gcs = configuration.gcs
        file_uri = settings.file_uri if settings is not None else None
        gcs_archive_uri = gcs.uri if gcs is not None else None
        gcs_bucket = (
            GcsSnapshotBucket(
                gcs.uri,
                project=gcs.project,
                credentials_file=gcs.credentials_file,
                timeout_seconds=gcs.timeout_seconds,
            )
            if gcs is not None
            else None
        )
        if file_uri is None and gcs_archive_uri is None:
            file_uri = str(default_snapshot_directory())
        return cls(
            file_uri=file_uri,
            gcs_archive_uri=gcs_archive_uri,
            max_snapshots=settings.max_snapshots if settings else 3,
            interval=settings.interval if settings else None,
            gcs_bucket=gcs_bucket,
        )

    @property
    def is_gcs(self) -> bool:
        """Return whether any configured destination uses GCS."""
        return any(archive.uri.startswith("gs://") for archive in self._archives)

    @property
    def destination_uris(self) -> tuple[str, ...]:
        """Return configured snapshot destination URIs in publication order."""
        return tuple(archive.uri for archive in self._archives)

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Return advisory lifecycle warnings for configured GCS archives."""
        return tuple(
            warning
            for archive in self._archives
            for warning in archive.lifecycle_warnings()
        )

    def _read_catalog_for(
        self, archive: SnapshotBucket
    ) -> tuple[dict[str, object], SnapshotVersion | None]:
        """Read one destination catalog."""
        catalog, version = archive.read_json(_CATALOG_NAME)
        return _empty_catalog() if catalog is None else catalog, version

    def _read_catalog(self) -> tuple[dict[str, object], SnapshotVersion | None]:
        """Read the primary destination catalog."""
        return self._read_catalog_for(self._primary)

    def _write_catalog_for(
        self,
        archive: SnapshotBucket,
        catalog: dict[str, object],
        generation: SnapshotVersion | None,
    ) -> None:
        """Write one destination catalog."""
        archive.write_json_cas(_CATALOG_NAME, catalog, expected_version=generation)

    def _write_catalog(
        self, catalog: dict[str, object], generation: SnapshotVersion | None
    ) -> None:
        """Write the primary destination catalog."""
        self._write_catalog_for(self._primary, catalog, generation)

    @staticmethod
    def _entries(catalog: dict[str, object]) -> list[dict[str, object]]:
        """Validate and return catalog entries in publication order."""
        if catalog.get("version") != _FORMAT_VERSION:
            raise ValueError("snapshot catalog must use format version 2")
        entries = catalog.get("entries", [])
        if not isinstance(entries, list) or not all(
            isinstance(item, dict) for item in entries
        ):
            raise ValueError("snapshot catalog entries must be objects")
        result: list[dict[str, object]] = []
        for item in entries:
            snapshot_id = _validate_snapshot_id(item.get("snapshot_id"))
            if item.get("prefix") != snapshot_id:
                raise ValueError("snapshot catalog entry prefix is invalid")
            _reference_metadata(
                item.get("manifest"),
                expected_name=f"{snapshot_id}/manifest.json",
            )
            result.append(dict(item))
        return result

    def _due(self, catalog: dict[str, object], now: datetime) -> bool:
        """Return whether this instance's configured cadence permits capture."""
        if self.interval is None:
            return True
        entries = self._entries(catalog)
        if not entries:
            return True
        published = entries[-1].get("published_at")
        if not isinstance(published, str):
            raise ValueError("snapshot catalog entry has no published_at")
        return now >= _timestamp(published) + self.interval

    def _claim_for(
        self,
        archive: SnapshotBucket,
        now: datetime,
        owner: str,
    ) -> int | None:
        """Acquire one destination reservation."""
        for _ in range(5):
            catalog, generation = self._read_catalog_for(archive)
            if not self._due(catalog, now):
                return None
            reservation = catalog.get("reservation")
            if isinstance(reservation, dict):
                expires = reservation.get("expires_at")
                if isinstance(expires, str) and _timestamp(expires) > now:
                    return None
            previous_fence = catalog.get("fence", 0)
            if not isinstance(previous_fence, int):
                raise ValueError("snapshot catalog fence must be an integer")
            fence = previous_fence + 1
            catalog["fence"] = fence
            catalog["reservation"] = {
                "owner": owner,
                "fence": fence,
                "expires_at": (now + timedelta(seconds=_LEASE_SECONDS)).isoformat(),
            }
            try:
                self._write_catalog_for(archive, catalog, generation)
            except SnapshotPreconditionError:
                _LOG.warning(
                    "snapshot reservation claim raced with another writer for %s",
                    archive.uri,
                    exc_info=True,
                )
                continue
            return fence
        return None

    def _renew_for(
        self, archive: SnapshotBucket, owner: str, fence: int, now: datetime
    ) -> bool:
        """Extend one unexpired reservation without changing its fence."""
        for _ in range(5):
            catalog, generation = self._read_catalog_for(archive)
            reservation = catalog.get("reservation")
            if (
                not isinstance(reservation, dict)
                or reservation.get("owner") != owner
                or reservation.get("fence") != fence
            ):
                return False
            expires = reservation.get("expires_at")
            if not isinstance(expires, str) or _timestamp(expires) <= now:
                return False
            catalog["reservation"] = {
                **reservation,
                "expires_at": (now + timedelta(seconds=_LEASE_SECONDS)).isoformat(),
            }
            try:
                self._write_catalog_for(archive, catalog, generation)
            except SnapshotPreconditionError:
                continue
            return True
        return False

    def _renew_claims(
        self,
        owner: str,
        claims: tuple[tuple[SnapshotBucket, int], ...],
        now: datetime,
    ) -> bool:
        """Extend every destination reservation held by this capture."""
        return all(
            self._renew_for(archive, owner, fence, now) for archive, fence in claims
        )

    def _claim(self, now: datetime) -> tuple[str, int] | None:
        """Acquire a primary destination reservation for compatibility."""
        owner = uuid4().hex
        fence = self._claim_for(self._primary, now, owner)
        return None if fence is None else (owner, fence)

    def _claim_all(
        self, now: datetime
    ) -> tuple[str, tuple[tuple[SnapshotBucket, int], ...]] | None:
        """Reserve every configured destination before capturing any data."""
        owner = uuid4().hex
        claims: list[tuple[SnapshotBucket, int]] = []
        try:
            for archive in self._archives:
                fence = self._claim_for(archive, now, owner)
                if fence is None:
                    self._release_claims(owner, claims)
                    return None
                claims.append((archive, fence))
        except Exception:
            self._release_claims(owner, claims)
            raise
        return owner, tuple(claims)

    def _release_claims(
        self, owner: str, claims: list[tuple[SnapshotBucket, int]]
    ) -> None:
        """Release reservations still owned by one failed capture."""
        for archive, fence in claims:
            try:
                catalog, generation = self._read_catalog_for(archive)
                reservation = catalog.get("reservation")
                if (
                    isinstance(reservation, dict)
                    and reservation.get("owner") == owner
                    and reservation.get("fence") == fence
                ):
                    catalog["reservation"] = None
                    self._write_catalog_for(archive, catalog, generation)
            except SnapshotPreconditionError:
                _LOG.warning(
                    "snapshot reservation release raced with another writer for %s",
                    archive.uri,
                    exc_info=True,
                )

    @staticmethod
    def _schema(data: pa.Table) -> str:
        """Return a stable Arrow schema representation for manifests."""
        return str(data.schema)

    @staticmethod
    def _serialized_schema(data: pa.Table) -> str:
        """Return an Arrow IPC schema suitable for compatibility checks."""
        return b64encode(data.schema.serialize().to_pybytes()).decode()

    @staticmethod
    def _serialize_table(data: pa.Table) -> bytes:
        """Serialize one canonical Arrow table to Parquet bytes."""
        sink = io.BytesIO()
        pq.write_table(data, sink)
        return sink.getvalue()

    def _capture_table(self, backend: StorageBackend, table: str) -> _CapturedTable:
        """Read and serialize one expected table exactly once."""
        data = backend.query(f"SELECT * FROM {table}")
        if not isinstance(data, pa.Table):
            raise TypeError(f"snapshot query for {table} did not return an Arrow table")
        return _CapturedTable(
            rows=data.num_rows,
            schema=self._schema(data),
            schema_ipc=self._serialized_schema(data),
            completed_at=_now().isoformat(),
            payload=self._serialize_table(data) if data.num_rows else None,
        )

    @staticmethod
    def _object_reference(
        object_ref: SnapshotObject, payload: bytes
    ) -> dict[str, object]:
        """Convert one archive object to a manifest reference."""
        return {
            "name": object_ref.name,
            "version": object_ref.version,
            "size": object_ref.size,
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    def _write_target(
        self,
        archive: SnapshotBucket,
        snapshot_id: str,
        captures: dict[str, _CapturedTable],
        manifest_base: dict[str, object],
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        """Write one complete archive and return its entry and object references."""
        written: list[dict[str, object]] = []
        try:
            tables: dict[str, object] = {}
            for table in SNAPSHOT_TABLES:
                captured = captures[table]
                objects: list[dict[str, object]] = []
                if captured.payload is not None:
                    object_ref = archive.write_bytes(
                        f"{snapshot_id}/{table}.parquet",
                        captured.payload,
                    )
                    object_value = self._object_reference(
                        object_ref,
                        captured.payload,
                    )
                    objects.append(object_value)
                    written.append(object_value)
                tables[table] = {
                    "status": "complete",
                    "completed_at": captured.completed_at,
                    "rows": captured.rows,
                    "schema": captured.schema,
                    "schema_ipc": captured.schema_ipc,
                    "objects": objects,
                }
            manifest = {**manifest_base, "tables": tables}
            relative = f"{snapshot_id}/manifest.json"
            raw = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
            manifest_ref = archive.write_bytes(
                relative,
                raw,
                content_type="application/json",
            )
            manifest_value = self._object_reference(manifest_ref, raw)
            written.append(manifest_value)
            return {
                "snapshot_id": snapshot_id,
                "prefix": snapshot_id,
                "manifest": manifest_value,
            }, written
        except Exception:
            self._delete_objects(archive, written)
            raise

    def _delete_objects(
        self, archive: SnapshotBucket, objects: list[dict[str, object]]
    ) -> None:
        """Delete newly written objects without crossing generations."""
        for object_ref in objects:
            name, version = object_ref.get("name"), object_ref.get("version")
            if isinstance(name, str) and isinstance(version, (int, str)):
                try:
                    archive.delete(name, version=version)
                except SnapshotPreconditionError:
                    _LOG.warning(
                        "snapshot object cleanup raced with another writer for %s",
                        archive.uri,
                        exc_info=True,
                    )

    def _publish_for(
        self,
        archive: SnapshotBucket,
        entry: dict[str, object],
        *,
        owner: str,
        fence: int,
        now: datetime,
    ) -> bool | None:
        """Publish one destination entry under its active reservation."""
        for _ in range(5):
            catalog, generation = self._read_catalog_for(archive)
            reservation = catalog.get("reservation")
            if (
                not isinstance(reservation, dict)
                or reservation.get("owner") != owner
                or reservation.get("fence") != fence
            ):
                return None
            expires = reservation.get("expires_at")
            if (
                not isinstance(expires, str)
                or _timestamp(expires) <= now
                or not self._due(catalog, now)
            ):
                return None
            published_entry = {**entry, "published_at": now.isoformat()}
            all_entries = [*self._entries(catalog), published_entry]
            catalog["entries"] = all_entries
            catalog["reservation"] = None
            try:
                self._write_catalog_for(archive, catalog, generation)
            except SnapshotPreconditionError:
                _LOG.warning(
                    "snapshot publication raced with another writer for %s",
                    archive.uri,
                    exc_info=True,
                )
                continue
            return True
        return None

    def _rotate_for(self, archive: SnapshotBucket) -> list[dict[str, object]]:
        """Apply retention after every destination has published."""
        for _ in range(5):
            catalog, generation = self._read_catalog_for(archive)
            entries = self._entries(catalog)
            if len(entries) <= self.max_snapshots:
                return []
            retired = entries[: -self.max_snapshots]
            catalog["entries"] = entries[-self.max_snapshots :]
            try:
                self._write_catalog_for(archive, catalog, generation)
            except SnapshotPreconditionError:
                _LOG.warning(
                    "snapshot retention update raced with another writer for %s",
                    archive.uri,
                    exc_info=True,
                )
                continue
            return retired
        raise RuntimeError(f"could not rotate snapshot catalog for {archive.uri}")

    def _publish(
        self, entry: dict[str, object], *, owner: str, fence: int, now: datetime
    ) -> list[dict[str, object]] | None:
        """Publish the primary destination entry for compatibility."""
        published = self._publish_for(
            self._primary,
            entry,
            owner=owner,
            fence=fence,
            now=now,
        )
        return None if published is None else self._rotate_for(self._primary)

    def _remove_entry(self, archive: SnapshotBucket, snapshot_id: str) -> None:
        """Remove one just-published entry during dual-publication rollback."""
        for _ in range(5):
            catalog, generation = self._read_catalog_for(archive)
            entries = self._entries(catalog)
            filtered = [
                entry for entry in entries if entry.get("snapshot_id") != snapshot_id
            ]
            if len(filtered) == len(entries):
                return
            catalog["entries"] = filtered
            try:
                self._write_catalog_for(archive, catalog, generation)
            except SnapshotPreconditionError:
                _LOG.warning(
                    "snapshot rollback raced with another writer for %s",
                    archive.uri,
                    exc_info=True,
                )
                continue
            return

    def _load_manifest(
        self, entry: dict[str, object], archive: SnapshotBucket | None = None
    ) -> dict[str, object]:
        """Load an exact catalog-referenced manifest and validate completeness."""
        destination = self._primary if archive is None else archive
        reference, snapshot_id = entry.get("manifest"), entry.get("snapshot_id")
        if not isinstance(reference, dict):
            raise ValueError("snapshot catalog entry is incomplete")
        validated_snapshot_id = _validate_snapshot_id(snapshot_id)
        raw = self._read_verified_reference(
            destination,
            reference,
            expected_name=f"{validated_snapshot_id}/manifest.json",
        )
        manifest = json.loads(raw)
        if (
            not isinstance(manifest, dict)
            or manifest.get("version") != _FORMAT_VERSION
            or manifest.get("snapshot_id") != validated_snapshot_id
        ):
            raise ValueError("snapshot manifest does not match its catalog entry")
        tables = manifest.get("tables")
        if (
            not isinstance(tables, dict)
            or set(tables) != set(SNAPSHOT_TABLES)
            or any(
                not isinstance(value, dict) or value.get("status") != "complete"
                for value in tables.values()
            )
        ):
            raise ValueError(
                "snapshot manifest does not contain every complete expected table"
            )
        return manifest

    @staticmethod
    def _read_verified_reference(
        archive: SnapshotBucket,
        reference: object,
        *,
        expected_name: str,
    ) -> bytes:
        """Read one exact object and verify its manifest SHA-256 before parsing.

        Args:
            archive: Snapshot destination that owns the object.
            reference: Untrusted object metadata.
            expected_name: Exact archive-relative name expected for the object.

        Returns:
            Verified raw object bytes.

        Raises:
            ValueError: If metadata, size, or SHA-256 verification fails.
        """
        name, version, size, sha256 = _reference_metadata(
            reference,
            expected_name=expected_name,
        )
        raw = archive.read_bytes(name, version=version)
        if len(raw) != size:
            raise ValueError(f"snapshot object size does not match: {name!r}")
        if hashlib.sha256(raw).hexdigest() != sha256:
            raise ValueError(f"snapshot object SHA-256 does not match: {name!r}")
        return raw

    def _cleanup_entry(
        self, entry: dict[str, object], archive: SnapshotBucket | None = None
    ) -> None:
        """Remove unretained published objects without crossing generations."""
        destination = self._primary if archive is None else archive
        snapshot_id = entry.get("snapshot_id")
        validated_snapshot_id = _validate_snapshot_id(snapshot_id)
        if entry.get("prefix") != validated_snapshot_id:
            raise ValueError("snapshot catalog entry prefix is invalid")
        _reference_metadata(
            entry.get("manifest"),
            expected_name=f"{validated_snapshot_id}/manifest.json",
        )
        manifest = self._load_manifest(entry, destination)
        tables = manifest["tables"]
        assert isinstance(tables, dict)
        objects: list[dict[str, object]] = []
        for table in tables.values():
            assert isinstance(table, dict)
            values = table.get("objects", [])
            if isinstance(values, list):
                objects.extend(value for value in values if isinstance(value, dict))
        manifest_ref = entry.get("manifest")
        if isinstance(manifest_ref, dict):
            objects.append(manifest_ref)
        self._delete_objects(destination, objects)

    def _cleanup_stale_gcs_staging(
        self, archive: SnapshotBucket, now: datetime
    ) -> None:
        """Retry safe deletion of expired unreferenced GCS staging prefixes."""
        catalog, _ = self._read_catalog_for(archive)
        retained = frozenset(
            entry.get("snapshot_id")
            for entry in self._entries(catalog)
            if isinstance(entry.get("snapshot_id"), str)
        )
        cutoff = now - timedelta(seconds=_LEASE_SECONDS)
        for object_ref in archive.list(""):
            relative = validate_relative_name(object_ref.name)
            snapshot_id = relative.split("/", 1)[0]
            if (
                _SNAPSHOT_ID.fullmatch(snapshot_id) is None
                or snapshot_id in retained
                or "_" not in snapshot_id
            ):
                continue
            stamp = snapshot_id.split("_", 1)[0]
            try:
                created = datetime.strptime(stamp, "%Y-%m-%dT%H%M%SZ").replace(
                    tzinfo=UTC
                )
            except ValueError:
                _LOG.warning(
                    "ignoring stale GCS object with invalid snapshot timestamp %s",
                    snapshot_id,
                    exc_info=True,
                )
                continue
            if created >= cutoff:
                continue
            try:
                archive.delete(relative, version=object_ref.version)
            except SnapshotPreconditionError:
                _LOG.warning(
                    "stale GCS snapshot cleanup raced with another writer for %s",
                    archive.uri,
                    exc_info=True,
                )

    def write(self, backend: StorageBackend, *, run_id: str) -> str | None:
        """Capture once, publish to every destination, and rotate snapshots."""
        created = _now()
        claimed = self._claim_all(created)
        if claimed is None:
            return None
        owner, claims = claimed
        heartbeat = _ReservationHeartbeat(self, owner, claims)
        heartbeat.start()
        snapshot_id = f"{created.strftime('%Y-%m-%dT%H%M%SZ')}_{uuid4().hex}"
        written: dict[SnapshotBucket, list[dict[str, object]]] = {}
        entries: dict[SnapshotBucket, dict[str, object]] = {}
        published: list[SnapshotBucket] = []
        try:
            captures = {
                table: self._capture_table(backend, table) for table in SNAPSHOT_TABLES
            }
            fingerprint = hashlib.sha256(
                json.dumps(
                    {table: captures[table].schema for table in SNAPSHOT_TABLES},
                    sort_keys=True,
                ).encode()
            ).hexdigest()
            manifest_base: dict[str, object] = {
                "version": _FORMAT_VERSION,
                "snapshot_id": snapshot_id,
                "created_at": created.isoformat(),
                "source_backend": getattr(backend, "dialect", type(backend).__name__),
                "run_id": run_id,
                "schema_fingerprint": fingerprint,
            }
            for archive in self._archives:
                entry, objects = self._write_target(
                    archive,
                    snapshot_id,
                    captures,
                    manifest_base,
                )
                entries[archive] = entry
                written[archive] = objects
            heartbeat.stop()
            heartbeat.check()
            published_at = _now()
            if not self._renew_claims(owner, claims, published_at):
                raise RuntimeError("snapshot publication reservation was lost")
            for archive, fence in claims:
                published_target = self._publish_for(
                    archive,
                    entries[archive],
                    owner=owner,
                    fence=fence,
                    now=published_at,
                )
                if published_target is None:
                    raise RuntimeError(
                        f"snapshot publication reservation was lost for {archive.uri}"
                    )
                published.append(archive)
        except Exception:
            heartbeat.stop()
            for archive in published:
                self._remove_entry(archive, snapshot_id)
            self._release_claims(owner, list(claims))
            for archive, objects in written.items():
                self._delete_objects(archive, objects)
            raise
        finally:
            heartbeat.stop()
        for archive in published:
            retired = self._rotate_for(archive)
            for old in retired:
                self._cleanup_entry(old, archive)
            self._cleanup_stale_gcs_staging(archive, _now())
        return f"{self.uri}/{snapshot_id}"

    def list_snapshots(self) -> list[str]:
        """List complete catalog-published snapshots from the primary archive."""
        catalog, _ = self._read_catalog()
        values: list[str] = []
        for entry in self._entries(catalog):
            values.append(_validate_snapshot_id(entry.get("snapshot_id")))
        return values

    def _restore_entry(self, snapshot: str) -> tuple[SnapshotBucket, dict[str, object]]:
        """Select the first destination containing the requested publication."""
        if snapshot != "latest":
            _validate_snapshot_id(snapshot)
        any_entries = False
        for archive in self._archives:
            catalog, _ = self._read_catalog_for(archive)
            entries = self._entries(catalog)
            if not entries:
                continue
            any_entries = True
            entry = (
                entries[-1]
                if snapshot == "latest"
                else next(
                    (
                        value
                        for value in entries
                        if value.get("snapshot_id") == snapshot
                    ),
                    None,
                )
            )
            if entry is not None:
                return archive, entry
        if any_entries:
            raise ValueError(f"unknown published snapshot {snapshot!r}")
        raise ValueError(f"no published snapshots under {self.uri}")

    def restore(
        self, backend: StorageBackend, snapshot: str = "latest"
    ) -> dict[str, int]:
        """Validate then append a complete snapshot into an empty backend."""
        self._ensure_empty(backend)
        archive, entry = self._restore_entry(snapshot)
        manifest = self._load_manifest(entry, archive)
        tables = manifest["tables"]
        assert isinstance(tables, dict)
        loaded: dict[str, pa.Table] = {}
        for table in SNAPSHOT_TABLES:
            spec = tables[table]
            assert isinstance(spec, dict)
            schema_ipc = spec.get("schema_ipc")
            if not isinstance(schema_ipc, str):
                raise ValueError(f"snapshot table {table} has no serialized schema")
            expected_schema = pa.ipc.read_schema(pa.BufferReader(b64decode(schema_ipc)))
            destination = backend.query(f"SELECT * FROM {table} LIMIT 0")
            if not destination.schema.equals(expected_schema, check_metadata=True):
                raise ValueError(
                    f"snapshot table {table} is incompatible with destination schema"
                )
            object_refs = spec.get("objects", [])
            if not isinstance(object_refs, list):
                raise ValueError(f"snapshot table {table} has invalid object metadata")
            rows = spec.get("rows")
            if (
                not isinstance(rows, int)
                or isinstance(rows, bool)
                or rows < 0
                or (rows == 0 and object_refs)
                or (rows > 0 and len(object_refs) != 1)
            ):
                raise ValueError(f"snapshot table {table} has invalid row metadata")
            parts: list[pa.Table] = []
            for reference in object_refs:
                raw = self._read_verified_reference(
                    archive,
                    reference,
                    expected_name=f"{entry['snapshot_id']}/{table}.parquet",
                )
                parts.append(pq.read_table(pa.BufferReader(raw)))
            if parts:
                data = pa.concat_tables(parts)
                if data.num_rows != rows:
                    raise ValueError(f"snapshot table {table} row count does not match")
                loaded[table] = data
        restored: dict[str, int] = {}
        for table in SNAPSHOT_TABLES:
            data = loaded.get(table)
            rows = 0 if data is None else data.num_rows
            if data is not None and rows:
                backend.append(table, data)
            restored[table] = rows
        return restored

    @staticmethod
    def _ensure_empty(backend: StorageBackend) -> None:
        """Require every snapshot table in the destination to be empty.

        Args:
            backend: Initialized destination warehouse.

        Raises:
            ValueError: If the destination contains any restorable data.
        """
        populated: list[str] = []
        for table in SNAPSHOT_TABLES:
            rows = backend.query(f"SELECT count(*) AS count FROM {table}").to_pylist()
            if len(rows) != 1 or not isinstance(rows[0].get("count"), int):
                raise ValueError(f"could not count destination snapshot table {table}")
            if rows[0]["count"]:
                populated.append(table)
        if populated:
            raise ValueError(
                "restore requires an empty warehouse; populated tables: "
                + ", ".join(populated)
            )
