# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""snapshots.py — Catalog-published portable Parquet restoration snapshots."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from base64 import b64decode, b64encode
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.gcs import GcsObject

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


class SnapshotStore:
    """A catalog-backed local or GCS Parquet snapshot archive.

    Every backend is read through canonical Arrow tables. A snapshot becomes
    restorable only after its complete manifest is published in the catalog.
    """

    def __init__(
        self,
        uri: str,
        *,
        max_snapshots: int = 3,
        interval: str | None = None,
        gcs_archive: GcsArchive | None = None,
    ) -> None:
        """Configure one archive.

        Args:
            uri: Local path, ``file://`` URI, or GCS archive root.
            max_snapshots: Positive published-snapshot retention ceiling.
            interval: Optional publication cadence.
            gcs_archive: Injectable GCS adapter for isolated tests.
        """
        if max_snapshots < 1:
            raise ValueError("max_snapshots must be positive")
        self.uri = uri.rstrip("/")
        self.max_snapshots = max_snapshots
        self.interval = parse_interval(interval)
        self._gcs = gcs_archive
        if self.uri.startswith("gs://") and self._gcs is None:
            from usagebassoon.backends.gcs import GcsArchive

            self._gcs = GcsArchive(self.uri)
        self._local = (
            None
            if self.uri.startswith("gs://")
            else Path(self.uri.removeprefix("file://")).expanduser()
        )

    @property
    def is_gcs(self) -> bool:
        """Return whether this archive uses Google Cloud Storage."""
        return self._gcs is not None

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Return advisory lifecycle-delete warnings for GCS archives."""
        return () if self._gcs is None else self._gcs.lifecycle_delete_warnings()

    def _read_catalog(self) -> tuple[dict[str, object], int | None]:
        """Read the complete-snapshot catalog and its GCS generation."""
        if self._gcs is not None:
            catalog, generation = self._gcs.read_json(_CATALOG_NAME)
            return _empty_catalog() if catalog is None else catalog, generation
        assert self._local is not None
        path = self._local / _CATALOG_NAME
        if not path.exists():
            return _empty_catalog(), None
        payload = json.loads(path.read_text())
        if not isinstance(payload, dict):
            raise ValueError("snapshot catalog must be a JSON object")
        return payload, None

    def _write_catalog(
        self, catalog: dict[str, object], generation: int | None
    ) -> None:
        """Persist a catalog atomically, using GCS CAS when configured."""
        if self._gcs is not None:
            self._gcs.write_json_cas(_CATALOG_NAME, catalog, generation=generation)
            return
        assert self._local is not None
        self._local.mkdir(parents=True, exist_ok=True)
        temporary = self._local / f".{_CATALOG_NAME}.{uuid4().hex}.tmp"
        temporary.write_text(json.dumps(catalog, sort_keys=True, indent=2) + "\n")
        temporary.replace(self._local / _CATALOG_NAME)

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

    def _claim(self, now: datetime) -> tuple[str, int] | None:
        """Acquire a short catalog reservation, avoiding redundant captures."""
        owner = uuid4().hex
        for _ in range(5):
            catalog, generation = self._read_catalog()
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
                self._write_catalog(catalog, generation)
            except RuntimeError as error:
                if error.__class__.__name__ != "GcsPreconditionError":
                    raise
                continue
            return owner, fence
        return None

    @staticmethod
    def _schema(data: pa.Table) -> str:
        """Return a stable Arrow schema representation for manifests."""
        return str(data.schema)

    @staticmethod
    def _serialized_schema(data: pa.Table) -> str:
        """Return an Arrow IPC schema suitable for empty-table compatibility checks."""
        return b64encode(data.schema.serialize().to_pybytes()).decode()

    def _write_table(self, relative: str, data: pa.Table) -> list[dict[str, object]]:
        """Write one deterministic Parquet object from a canonical Arrow table."""
        import io

        sink = io.BytesIO()
        pq.write_table(data, sink)
        raw = sink.getvalue()
        if self._gcs is not None:
            object_ref = self._gcs.write_bytes(relative, raw)
            return [
                {
                    "name": self._gcs.relative(object_ref.name),
                    "generation": object_ref.generation,
                    "size": object_ref.size,
                    "checksum": object_ref.checksum,
                }
            ]
        assert self._local is not None
        path = self._local / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return [
            {
                "name": relative,
                "generation": 0,
                "size": len(raw),
                "checksum": hashlib.sha256(raw).hexdigest(),
            }
        ]

    def _capture_table(
        self, backend: StorageBackend, table: str, snapshot_id: str
    ) -> dict[str, object]:
        """Capture one expected table or fail before manifest publication."""
        data = backend.query(f"SELECT * FROM {table}")
        if not isinstance(data, pa.Table):
            raise TypeError(f"snapshot query for {table} did not return an Arrow table")
        objects: list[dict[str, object]] = []
        if data.num_rows:
            objects = self._write_table(f"{snapshot_id}/{table}.parquet", data)
        return {
            "status": "complete",
            "completed_at": _now().isoformat(),
            "rows": data.num_rows,
            "schema": self._schema(data),
            "schema_ipc": self._serialized_schema(data),
            "objects": objects,
        }

    def _write_manifest(
        self, snapshot_id: str, manifest: dict[str, object]
    ) -> dict[str, object]:
        """Write the complete manifest after all expected captures succeed."""
        relative = f"{snapshot_id}/manifest.json"
        raw = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
        if self._gcs is not None:
            object_ref = self._gcs.write_bytes(
                relative, raw, content_type="application/json"
            )
            return {
                "name": self._gcs.relative(object_ref.name),
                "generation": object_ref.generation,
            }
        assert self._local is not None
        path = self._local / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return {"name": relative, "generation": 0}

    def _publish(
        self, entry: dict[str, object], *, owner: str, fence: int, now: datetime
    ) -> list[dict[str, object]] | None:
        """Publish an entry under its active fence and compute retired entries."""
        for _ in range(5):
            catalog, generation = self._read_catalog()
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
            all_entries = [*self._entries(catalog), entry]
            entry["published_at"] = now.isoformat()
            catalog["entries"] = all_entries[-self.max_snapshots :]
            catalog["reservation"] = None
            try:
                self._write_catalog(catalog, generation)
            except RuntimeError as error:
                if error.__class__.__name__ != "GcsPreconditionError":
                    raise
                continue
            return all_entries[: -self.max_snapshots]
        return None

    def _load_manifest(self, entry: dict[str, object]) -> dict[str, object]:
        """Load an exact catalog-referenced manifest and validate completeness."""
        reference, snapshot_id = entry.get("manifest"), entry.get("snapshot_id")
        if not isinstance(reference, dict) or not isinstance(snapshot_id, str):
            raise ValueError("snapshot catalog entry is incomplete")
        name, generation = reference.get("name"), reference.get("generation")
        if not isinstance(name, str) or not isinstance(generation, int):
            raise ValueError("snapshot catalog manifest reference is invalid")
        if self._gcs is not None:
            raw = self._gcs.read_bytes(name, generation=generation)
        else:
            assert self._local is not None
            raw = (self._local / name).read_bytes()
        manifest = json.loads(raw)
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

    def _cleanup_entry(self, entry: dict[str, object]) -> None:
        """Remove unretained published objects without affecting newer generations."""
        snapshot_id = entry.get("snapshot_id")
        if not isinstance(snapshot_id, str):
            return
        if self._gcs is None:
            assert self._local is not None
            shutil.rmtree(self._local / snapshot_id, ignore_errors=True)
            return
        manifest = self._load_manifest(entry)
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
        for object_ref in objects:
            name, generation = object_ref.get("name"), object_ref.get("generation")
            if isinstance(name, str) and isinstance(generation, int):
                try:
                    self._gcs.delete(name, generation=generation)
                except RuntimeError as error:
                    if error.__class__.__name__ != "GcsPreconditionError":
                        raise

    def _cleanup_stale_gcs_staging(self, now: datetime) -> None:
        """Retry safe deletion of expired unreferenced GCS staging prefixes."""
        if self._gcs is None:
            return
        retained = frozenset(self.list_snapshots())
        cutoff = now - timedelta(seconds=_LEASE_SECONDS)
        for object_ref in self._gcs.list(""):
            relative = self._gcs.relative(object_ref.name)
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
                self._gcs.delete(relative, generation=object_ref.generation)
            except RuntimeError as error:
                if error.__class__.__name__ != "GcsPreconditionError":
                    raise

    def write(self, backend: StorageBackend, *, run_id: str) -> str | None:
        """Capture, publish, and rotate a snapshot, or skip when not due."""
        created = _now()
        claim = self._claim(created)
        if claim is None:
            return None
        owner, fence = claim
        snapshot_id = f"{created.strftime('%Y-%m-%dT%H%M%SZ')}_{uuid4().hex}"
        tables = {
            table: self._capture_table(backend, table, snapshot_id)
            for table in SNAPSHOT_TABLES
        }
        fingerprint = hashlib.sha256(
            json.dumps(
                {table: tables[table]["schema"] for table in SNAPSHOT_TABLES},
                sort_keys=True,
            ).encode()
        ).hexdigest()
        manifest: dict[str, object] = {
            "version": 1,
            "snapshot_id": snapshot_id,
            "created_at": created.isoformat(),
            "source_backend": getattr(backend, "dialect", type(backend).__name__),
            "run_id": run_id,
            "schema_fingerprint": fingerprint,
            "tables": tables,
        }
        entry: dict[str, object] = {
            "snapshot_id": snapshot_id,
            "prefix": snapshot_id,
            "manifest": self._write_manifest(snapshot_id, manifest),
        }
        retired = self._publish(entry, owner=owner, fence=fence, now=_now())
        if retired is None:
            return None
        for old in retired:
            self._cleanup_entry(old)
        self._cleanup_stale_gcs_staging(_now())
        return f"{self.uri}/{snapshot_id}"

    def list_snapshots(self) -> list[str]:
        """List only complete catalog-published snapshots, oldest first."""
        catalog, _ = self._read_catalog()
        values: list[str] = []
        for entry in self._entries(catalog):
            snapshot_id = entry.get("snapshot_id")
            if not isinstance(snapshot_id, str):
                raise ValueError("snapshot catalog entry has no snapshot_id")
            values.append(snapshot_id)
        return values

    def restore(
        self, backend: StorageBackend, snapshot: str = "latest"
    ) -> dict[str, int]:
        """Validate then append a complete published snapshot into an empty backend."""
        catalog, _ = self._read_catalog()
        entries = self._entries(catalog)
        if not entries:
            raise ValueError(f"no published snapshots under {self.uri}")
        entry = (
            entries[-1]
            if snapshot == "latest"
            else next(
                (value for value in entries if value.get("snapshot_id") == snapshot),
                None,
            )
        )
        if entry is None:
            raise ValueError(f"unknown published snapshot {snapshot!r}")
        manifest = self._load_manifest(entry)
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
                if self._gcs is not None:
                    raw = self._gcs.read_bytes(
                        reference["name"], generation=reference["generation"]
                    )
                else:
                    assert self._local is not None
                    raw = (self._local / reference["name"]).read_bytes()
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
