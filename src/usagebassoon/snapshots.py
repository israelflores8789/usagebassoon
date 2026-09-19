# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""snapshots.py — Catalog-published portable Parquet restoration snapshots."""

from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
from base64 import b64decode, b64encode
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.gcs import GcsObject

if TYPE_CHECKING:
    from usagebassoon.config import UsageBassoonConfig

SNAPSHOT_TABLES: tuple[str, ...] = (
    "sessions",
    "daily_stats",
    "daily_activity",
    "price_versions",
    "daily_processed_state",
    "run_metrics",
    "reconciliation_issues",
    "schema_drift",
    "ingest_runs",
    "tags",
    "notes",
)
_CATALOG_NAME = "catalog.json"
_LEASE_SECONDS = 300
_DURATION = re.compile(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>[smhdw])\Z", re.I)


class GcsArchive(Protocol):
    """The GCS object operations used by a snapshot archive."""

    def read_json(
        self, relative_name: str
    ) -> tuple[dict[str, object] | None, int | None]:
        """Read JSON and its generation."""
        ...

    def write_json_cas(
        self,
        relative_name: str,
        payload: dict[str, object],
        *,
        generation: int | None,
    ) -> GcsObject:
        """Write JSON conditionally."""
        ...

    def write_bytes(
        self,
        relative_name: str,
        payload: bytes,
        *,
        if_generation_match: int | None = None,
        content_type: str = "application/octet-stream",
    ) -> GcsObject:
        """Write bytes and return immutable metadata."""
        ...

    def read_bytes(self, relative_name: str, *, generation: int) -> bytes:
        """Read an exact object generation."""
        ...

    def delete(self, relative_name: str, *, generation: int) -> None:
        """Delete an exact object generation."""
        ...

    def list(self, relative_prefix: str) -> tuple[GcsObject, ...]:
        """List immutable object metadata under an archive-relative prefix."""
        ...

    def relative(self, object_name: str) -> str:
        """Make an object name relative to the archive root."""
        ...

    def lifecycle_delete_warnings(self) -> tuple[str, ...]:
        """Inspect lifecycle rules for matching deletes."""
        ...


def parse_interval(value: str | None) -> timedelta | None:
    """Parse a positive compact snapshot cadence duration.

    Args:
        value: ``<number><s|m|h|d|w>`` duration, or ``None``.

    Returns:
        Parsed duration, or ``None`` when cadence is disabled.

    Raises:
        ValueError: If the value is not a positive supported duration.
    """
    if value is None:
        return None
    match = _DURATION.fullmatch(value.strip())
    if match is None:
        raise ValueError("snapshots.interval must be like 30m, 12h, or 7d")
    amount = float(match["value"])
    if amount <= 0:
        raise ValueError("snapshots.interval must be positive")
    return timedelta(
        seconds=amount
        * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[match["unit"].lower()]
    )


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
    return {"version": 1, "entries": [], "reservation": None, "fence": 0}


class _SnapshotArchive:
    """Uniform object and catalog operations for one snapshot destination."""

    def __init__(self, uri: str, gcs_archive: GcsArchive | None = None) -> None:
        """Open a local or GCS archive destination."""
        self.uri = uri.rstrip("/")
        self.gcs: GcsArchive | None = None
        self.local: Path | None = None
        if self.uri.startswith("gs://"):
            if gcs_archive is None:
                from usagebassoon.backends.gcs import GcsArchive as RealGcsArchive

                gcs_archive = RealGcsArchive(self.uri)
            self.gcs = gcs_archive
            self.local = None
        else:
            self.gcs = None
            self.local = Path(self.uri.removeprefix("file://")).expanduser()

    @property
    def is_gcs(self) -> bool:
        """Return whether this destination uses Google Cloud Storage."""
        return self.gcs is not None

    def read_catalog(self) -> tuple[dict[str, object], int | None]:
        """Read the destination catalog and its optional generation."""
        if self.gcs is not None:
            catalog, generation = self.gcs.read_json(_CATALOG_NAME)
            return _empty_catalog() if catalog is None else catalog, generation
        assert self.local is not None
        path = self.local / _CATALOG_NAME
        if not path.exists():
            return _empty_catalog(), None
        payload = json.loads(path.read_text())
        if not isinstance(payload, dict):
            raise ValueError("snapshot catalog must be a JSON object")
        return payload, None

    def write_catalog(self, catalog: dict[str, object], generation: int | None) -> None:
        """Persist the destination catalog atomically or with GCS CAS."""
        if self.gcs is not None:
            self.gcs.write_json_cas(_CATALOG_NAME, catalog, generation=generation)
            return
        assert self.local is not None
        self.local.mkdir(parents=True, exist_ok=True)
        temporary = self.local / f".{_CATALOG_NAME}.{uuid4().hex}.tmp"
        temporary.write_text(json.dumps(catalog, sort_keys=True, indent=2) + "\n")
        temporary.replace(self.local / _CATALOG_NAME)

    def write_bytes(
        self,
        relative_name: str,
        payload: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> GcsObject:
        """Write one snapshot object."""
        if self.gcs is not None:
            return self.gcs.write_bytes(
                relative_name,
                payload,
                content_type=content_type,
            )
        del content_type
        assert self.local is not None
        path = self.local / relative_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return GcsObject(
            name=relative_name,
            generation=0,
            size=len(payload),
            checksum=hashlib.sha256(payload).hexdigest(),
        )

    def read_bytes(self, relative_name: str, *, generation: int) -> bytes:
        """Read one exact snapshot object."""
        if self.gcs is not None:
            return self.gcs.read_bytes(relative_name, generation=generation)
        del generation
        assert self.local is not None
        return (self.local / relative_name).read_bytes()

    def delete(self, relative_name: str, *, generation: int) -> None:
        """Delete one snapshot object without crossing a GCS generation."""
        if self.gcs is not None:
            self.gcs.delete(relative_name, generation=generation)
            return
        del generation
        assert self.local is not None
        (self.local / relative_name).unlink(missing_ok=True)

    def relative(self, object_name: str) -> str:
        """Return a destination-relative object name."""
        return self.gcs.relative(object_name) if self.gcs is not None else object_name

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Return advisory lifecycle warnings for this destination."""
        return () if self.gcs is None else self.gcs.lifecycle_delete_warnings()

    def remove_snapshot_tree(self, snapshot_id: str) -> None:
        """Remove a local snapshot prefix during retention or rollback."""
        if self.local is not None:
            shutil.rmtree(self.local / snapshot_id, ignore_errors=True)


@dataclass(frozen=True, slots=True)
class _CapturedTable:
    """One serialized table captured once for all snapshot destinations."""

    rows: int
    schema: str
    schema_ipc: str
    completed_at: str
    payload: bytes | None


class SnapshotStore:
    """A catalog-backed local, GCS, or dual-destination snapshot archive."""

    def __init__(
        self,
        uri: str | None = None,
        *,
        file_uri: str | None = None,
        gcs_archive_uri: str | None = None,
        max_snapshots: int = 3,
        interval: str | None = None,
        gcs_archive: GcsArchive | None = None,
    ) -> None:
        """Configure one or two snapshot destinations.

        Args:
            uri: Backward-compatible single local or GCS archive URI.
            file_uri: Optional local archive URI.
            gcs_archive_uri: Optional GCS archive URI.
            max_snapshots: Positive published-snapshot retention ceiling.
            interval: Optional publication cadence.
            gcs_archive: Injectable GCS adapter for isolated tests.
        """
        if max_snapshots < 1:
            raise ValueError("max_snapshots must be positive")
        if uri is not None and (file_uri is not None or gcs_archive_uri is not None):
            raise ValueError("uri cannot be combined with file_uri or gcs_archive_uri")
        locations = (
            [(uri, gcs_archive)]
            if uri is not None
            else [
                *([(file_uri, None)] if file_uri is not None else []),
                *(
                    [(gcs_archive_uri, gcs_archive)]
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
            _SnapshotArchive(location, archive) for location, archive in locations
        )
        self._primary = self._archives[0]
        self.uri = self._primary.uri
        self.max_snapshots = max_snapshots
        self.interval = parse_interval(interval)

    @classmethod
    def from_config(cls, configuration: UsageBassoonConfig) -> SnapshotStore:
        """Create a snapshot store from validated application configuration."""
        from usagebassoon.backends.gcs import GcsArchive

        settings = configuration.snapshots
        gcs = configuration.gcs
        file_uri = settings.file_uri if settings is not None else None
        gcs_archive_uri = gcs.uri if gcs is not None else None
        gcs_archive = (
            GcsArchive(
                gcs.uri,
                project=gcs.project,
                location=gcs.location,
                credentials_file=gcs.credentials_file,
            )
            if gcs is not None
            else None
        )
        if file_uri is None and gcs_archive_uri is None:
            file_uri = f"file://{Path('~/.usagebassoon/snapshots').expanduser()}"
        return cls(
            file_uri=file_uri,
            gcs_archive_uri=gcs_archive_uri,
            max_snapshots=settings.max_snapshots if settings else 3,
            interval=settings.interval if settings else None,
            gcs_archive=gcs_archive,
        )

    @property
    def is_gcs(self) -> bool:
        """Return whether any configured destination uses GCS."""
        return any(archive.is_gcs for archive in self._archives)

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
        self, archive: _SnapshotArchive
    ) -> tuple[dict[str, object], int | None]:
        """Read one destination catalog."""
        return archive.read_catalog()

    def _read_catalog(self) -> tuple[dict[str, object], int | None]:
        """Read the primary destination catalog."""
        return self._read_catalog_for(self._primary)

    def _write_catalog_for(
        self,
        archive: _SnapshotArchive,
        catalog: dict[str, object],
        generation: int | None,
    ) -> None:
        """Write one destination catalog."""
        archive.write_catalog(catalog, generation)

    def _write_catalog(
        self, catalog: dict[str, object], generation: int | None
    ) -> None:
        """Write the primary destination catalog."""
        self._write_catalog_for(self._primary, catalog, generation)

    @staticmethod
    def _entries(catalog: dict[str, object]) -> list[dict[str, object]]:
        """Validate and return catalog entries in publication order."""
        entries = catalog.get("entries", [])
        if not isinstance(entries, list) or not all(
            isinstance(item, dict) for item in entries
        ):
            raise ValueError("snapshot catalog entries must be objects")
        return [dict(item) for item in entries]

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
        archive: _SnapshotArchive,
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
            except RuntimeError as error:
                if error.__class__.__name__ != "GcsPreconditionError":
                    raise
                continue
            return fence
        return None

    def _claim(self, now: datetime) -> tuple[str, int] | None:
        """Acquire a primary destination reservation for compatibility."""
        owner = uuid4().hex
        fence = self._claim_for(self._primary, now, owner)
        return None if fence is None else (owner, fence)

    def _claim_all(
        self, now: datetime
    ) -> tuple[str, tuple[tuple[_SnapshotArchive, int], ...]] | None:
        """Reserve every configured destination before capturing any data."""
        owner = uuid4().hex
        claims: list[tuple[_SnapshotArchive, int]] = []
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
        self, owner: str, claims: list[tuple[_SnapshotArchive, int]]
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
            except RuntimeError as error:
                if error.__class__.__name__ != "GcsPreconditionError":
                    raise

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
        archive: _SnapshotArchive, object_ref: GcsObject
    ) -> dict[str, object]:
        """Convert one archive object to a manifest reference."""
        return {
            "name": archive.relative(object_ref.name),
            "generation": object_ref.generation,
            "size": object_ref.size,
            "checksum": object_ref.checksum,
        }

    def _write_target(
        self,
        archive: _SnapshotArchive,
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
                    object_value = self._object_reference(archive, object_ref)
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
            manifest_value: dict[str, object] = {
                "name": archive.relative(manifest_ref.name),
                "generation": manifest_ref.generation,
            }
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
        self, archive: _SnapshotArchive, objects: list[dict[str, object]]
    ) -> None:
        """Delete newly written objects without crossing generations."""
        for object_ref in objects:
            name, generation = object_ref.get("name"), object_ref.get("generation")
            if isinstance(name, str) and isinstance(generation, int):
                try:
                    archive.delete(name, generation=generation)
                except RuntimeError as error:
                    if error.__class__.__name__ != "GcsPreconditionError":
                        raise

    def _publish_for(
        self,
        archive: _SnapshotArchive,
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
            except RuntimeError as error:
                if error.__class__.__name__ != "GcsPreconditionError":
                    raise
                continue
            return True
        return None

    def _rotate_for(self, archive: _SnapshotArchive) -> list[dict[str, object]]:
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
            except RuntimeError as error:
                if error.__class__.__name__ != "GcsPreconditionError":
                    raise
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

    def _remove_entry(self, archive: _SnapshotArchive, snapshot_id: str) -> None:
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
            except RuntimeError as error:
                if error.__class__.__name__ != "GcsPreconditionError":
                    raise
                continue
            return

    def _load_manifest(
        self, entry: dict[str, object], archive: _SnapshotArchive | None = None
    ) -> dict[str, object]:
        """Load an exact catalog-referenced manifest and validate completeness."""
        destination = self._primary if archive is None else archive
        reference, snapshot_id = entry.get("manifest"), entry.get("snapshot_id")
        if not isinstance(reference, dict) or not isinstance(snapshot_id, str):
            raise ValueError("snapshot catalog entry is incomplete")
        name, generation = reference.get("name"), reference.get("generation")
        if not isinstance(name, str) or not isinstance(generation, int):
            raise ValueError("snapshot catalog manifest reference is invalid")
        manifest = json.loads(destination.read_bytes(name, generation=generation))
        if not isinstance(manifest, dict) or manifest.get("snapshot_id") != snapshot_id:
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

    def _cleanup_entry(
        self, entry: dict[str, object], archive: _SnapshotArchive | None = None
    ) -> None:
        """Remove unretained published objects without crossing generations."""
        destination = self._primary if archive is None else archive
        snapshot_id = entry.get("snapshot_id")
        if not isinstance(snapshot_id, str):
            return
        if not destination.is_gcs:
            destination.remove_snapshot_tree(snapshot_id)
            return
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
        self, archive: _SnapshotArchive, now: datetime
    ) -> None:
        """Retry safe deletion of expired unreferenced GCS staging prefixes."""
        if archive.gcs is None:
            return
        catalog, _ = self._read_catalog_for(archive)
        retained = frozenset(
            entry.get("snapshot_id")
            for entry in self._entries(catalog)
            if isinstance(entry.get("snapshot_id"), str)
        )
        cutoff = now - timedelta(seconds=_LEASE_SECONDS)
        for object_ref in archive.gcs.list(""):
            relative = archive.gcs.relative(object_ref.name)
            snapshot_id = relative.split("/", 1)[0]
            if snapshot_id in retained or "_" not in snapshot_id:
                continue
            stamp = snapshot_id.split("_", 1)[0]
            try:
                created = datetime.strptime(stamp, "%Y-%m-%dT%H%M%SZ").replace(
                    tzinfo=UTC
                )
            except ValueError:
                continue
            if created >= cutoff:
                continue
            try:
                archive.gcs.delete(relative, generation=object_ref.generation)
            except RuntimeError as error:
                if error.__class__.__name__ != "GcsPreconditionError":
                    raise

    def write(self, backend: StorageBackend, *, run_id: str) -> str | None:
        """Capture once, publish to every destination, and rotate snapshots."""
        created = _now()
        claimed = self._claim_all(created)
        if claimed is None:
            return None
        owner, claims = claimed
        snapshot_id = f"{created.strftime('%Y-%m-%dT%H%M%SZ')}_{uuid4().hex}"
        written: dict[_SnapshotArchive, list[dict[str, object]]] = {}
        entries: dict[_SnapshotArchive, dict[str, object]] = {}
        published: list[_SnapshotArchive] = []
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
                "version": 1,
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
            published_at = _now()
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
            for archive in published:
                self._remove_entry(archive, snapshot_id)
            self._release_claims(owner, list(claims))
            for archive, objects in written.items():
                self._delete_objects(archive, objects)
            raise
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
            snapshot_id = entry.get("snapshot_id")
            if not isinstance(snapshot_id, str):
                raise ValueError("snapshot catalog entry has no snapshot_id")
            values.append(snapshot_id)
        return values

    def _restore_entry(
        self, snapshot: str
    ) -> tuple[_SnapshotArchive, dict[str, object]]:
        """Select the first destination containing the requested publication."""
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
            parts: list[pa.Table] = []
            for reference in object_refs:
                if (
                    not isinstance(reference, dict)
                    or not isinstance(reference.get("name"), str)
                    or not isinstance(reference.get("generation"), int)
                ):
                    raise ValueError(
                        f"snapshot table {table} has invalid object reference"
                    )
                raw = archive.read_bytes(
                    reference["name"], generation=reference["generation"]
                )
                parts.append(pq.read_table(pa.BufferReader(raw)))
            if parts:
                data = pa.concat_tables(parts)
                loaded[table] = data
        restored: dict[str, int] = {}
        for table in SNAPSHOT_TABLES:
            data = loaded.get(table)
            rows = 0 if data is None else data.num_rows
            if data is not None and rows:
                backend.append(table, data)
            restored[table] = rows
        return restored
