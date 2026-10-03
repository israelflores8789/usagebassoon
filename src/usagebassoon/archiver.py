# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""archiver.py — Public orchestration of portable snapshots and recovery."""

from __future__ import annotations

import hashlib
import logging
import tomllib
from base64 import b64encode
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from uuid import uuid4

import pyarrow.parquet as pq

from usagebassoon.backends.base import StorageBackend
from usagebassoon.buckets.base import SnapshotBucket, SnapshotObject
from usagebassoon.buckets.local import LocalSnapshotBucket
from usagebassoon.config import (
    GcsConfig,
    _gcs_config,
    _snapshot_config,
    default_snapshot_directory,
    parse_interval,
)
from usagebassoon.snapshot.catalog import ArchiveBusy, Catalog
from usagebassoon.snapshot.format import FORMAT_VERSION, digest, encode, timestamp
from usagebassoon.snapshot.reader import Candidate, SnapshotReader
from usagebassoon.snapshot.restore import restore_prepared
from usagebassoon.storage_model import (
    CANONICAL_TABLE_SCHEMAS,
    DATA_SCHEMA_VERSION,
    SNAPSHOT_TABLES,
)
from usagebassoon.version import __version__

if TYPE_CHECKING:
    from usagebassoon.config import UsageBassoonConfig

_LOG = logging.getLogger("usagebassoon")


def _now() -> datetime:
    """Return the current UTC instant."""
    return datetime.now(UTC)


class SnapshotArchiver:
    """Coordinate canonical Arrow capture, archive lifecycle, and recovery."""

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
        """Configure one or two snapshot destinations."""
        if max_snapshots < 1:
            raise ValueError("max_snapshots must be positive")
        if uri is not None and (file_uri is not None or gcs_archive_uri is not None):
            raise ValueError("uri cannot be combined with file_uri or gcs_archive_uri")
        if file_uri is not None and file_uri.startswith("gs://"):
            raise ValueError("file_uri must be a local path or file:// URI")
        if gcs_archive_uri is not None and not gcs_archive_uri.startswith("gs://"):
            raise ValueError("gcs_archive_uri must be a gs:// URI")
        self._configuration: UsageBassoonConfig | None = None
        self._cloud_settings: GcsConfig | None = None
        locations = (
            [uri] if uri else [value for value in (file_uri, gcs_archive_uri) if value]
        )
        if not locations:
            raise ValueError("at least one snapshot destination is required")
        self._archives: tuple[SnapshotBucket, ...] = tuple(
            gcs_bucket
            if value.startswith("gs://") and gcs_bucket is not None
            else self._bucket(value)
            for value in locations
        )
        self.uri = self._archives[0].uri
        self.max_snapshots = max_snapshots
        self.interval = parse_interval(interval)
        self._weekly: set[str] = set()

    def _bucket(self, uri: str) -> SnapshotBucket:
        """Resolve an explicit URI without redirecting it through configuration."""
        if not uri.startswith("gs://"):
            return LocalSnapshotBucket(uri)
        from usagebassoon.buckets.gcs import GcsSnapshotBucket

        settings = self._cloud_settings
        return GcsSnapshotBucket(
            uri,
            project=settings.project if settings else None,
            credentials_file=settings.credentials_file if settings else None,
            timeout_seconds=settings.timeout_seconds if settings else 60.0,
        )

    @classmethod
    def from_config(cls, configuration: UsageBassoonConfig) -> SnapshotArchiver:
        """Create an authenticated archiver with explicit weekly destination policy."""
        from usagebassoon.buckets.gcs import GcsSnapshotBucket

        settings, gcs = configuration.snapshots, configuration.gcs
        file_uri = settings.file_uri if settings else None
        cloud = (
            GcsSnapshotBucket(
                gcs.uri,
                project=gcs.project,
                credentials_file=gcs.credentials_file,
                timeout_seconds=gcs.timeout_seconds,
            )
            if gcs
            else None
        )
        result = cls(
            file_uri=file_uri
            or (str(default_snapshot_directory()) if not gcs else None),
            gcs_archive_uri=gcs.uri if gcs else None,
            gcs_bucket=cloud,
            max_snapshots=settings.max_snapshots if settings else 3,
            interval=settings.interval if settings else None,
        )
        result._configuration = configuration
        result._cloud_settings = gcs
        if file_uri and settings and not settings.disable_weekly_snapshots:
            result._weekly.add(file_uri.rstrip("/"))
        if gcs and not gcs.disable_weekly_snapshots:
            result._weekly.add(gcs.uri.rstrip("/"))
        return result

    @classmethod
    def for_read(cls, path: Path) -> SnapshotArchiver:
        """Read archive settings without requiring a valid destination or source ID."""
        if not path.exists():
            return cls(str(default_snapshot_directory()))
        from usagebassoon.buckets.gcs import GcsSnapshotBucket

        payload = tomllib.loads(path.read_text(encoding="utf-8"))
        settings = _snapshot_config(payload.get("snapshots"))
        gcs = _gcs_config(payload.get("gcs"))
        cloud = (
            GcsSnapshotBucket(
                gcs.uri,
                project=gcs.project,
                credentials_file=gcs.credentials_file,
                timeout_seconds=gcs.timeout_seconds,
            )
            if gcs
            else None
        )
        result = cls(
            file_uri=settings.file_uri
            if settings and settings.file_uri
            else (None if gcs else str(default_snapshot_directory())),
            gcs_archive_uri=gcs.uri if gcs else None,
            gcs_bucket=cloud,
            max_snapshots=settings.max_snapshots if settings else 3,
        )
        result._cloud_settings = gcs
        return result

    @property
    def reader(self) -> SnapshotReader:
        """Return the shared reader used by restore, inspection, and audit."""
        return SnapshotReader(self._archives, self._bucket)

    @property
    def is_gcs(self) -> bool:
        """Return whether a configured archive uses GCS."""
        return any(a.uri.startswith("gs://") for a in self._archives)

    @property
    def destination_uris(self) -> tuple[str, ...]:
        """Return the configured archive locations."""
        return tuple(a.uri for a in self._archives)

    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Return provider lifecycle risks affecting configured archives."""
        return tuple(w for a in self._archives for w in a.lifecycle_warnings())

    def _roles(self, catalog: Catalog, now: datetime, *, manual: bool) -> list[str]:
        """Determine due obligations from successful destination publications."""
        if manual:
            return ["manual"]
        roles: list[str] = []
        scheduled: list[datetime] = []
        week = now.strftime("%G-W%V")
        has_week = False
        for entry in catalog.entries():
            state, _ = catalog.bucket.read_json(f"{entry['snapshot_id']}/state.json")
            if state is None or state.get("retired") is not False:
                continue
            memberships = state.get("roles", [])
            if isinstance(memberships, list) and "scheduled" in memberships:
                scheduled.append(timestamp(entry.get("captured_at")))
            if (
                isinstance(memberships, list)
                and "weekly" in memberships
                and state.get("weekly_slot") == week
            ):
                has_week = True
        if (
            self.interval is not None
            and (not scheduled or now >= max(scheduled) + self.interval)
        ) or (self._configuration is None and self.interval is None):
            roles.append("scheduled")
        if catalog.bucket.uri in self._weekly and not has_week:
            roles.append("weekly")
        return roles

    def write(
        self,
        backend: StorageBackend,
        *,
        run_id: str,
        manual: bool = False,
        pin: bool = False,
    ) -> str | None:
        """Capture once and publish due destinations under live reservations."""
        created = _now()
        identifier = f"{created.strftime('%Y-%m-%dT%H%M%SZ')}_{uuid4().hex}"
        catalogs = [
            Catalog(a, self.max_snapshots)
            for a in sorted(self._archives, key=_bucket_uri)
        ]
        due = [(c, self._roles(c, created, manual=manual)) for c in catalogs]
        due = [(c, roles) for c, roles in due if roles]
        if not due:
            return None
        written: dict[SnapshotBucket, list[SnapshotObject]] = {}
        try:
            with (
                ExitStack() as stack,
                TemporaryDirectory(prefix="usagebassoon-snapshot-") as temporary,
            ):
                for catalog, _ in due:
                    stack.enter_context(catalog.hold(enforce_policy=True))
                due = [(c, self._roles(c, created, manual=manual)) for c, _ in due]
                due = [(c, roles) for c, roles in due if roles]
                if not due:
                    return None
                try:
                    for catalog, memberships in due:
                        catalog.check()
                        initial_state: dict[str, object] = {
                            "kind": "snapshot_stage",
                            "owner": catalog.owner,
                            "fence": catalog.fence,
                            "created_at": created.isoformat(),
                            "pinned": pin,
                            "retired": False,
                            "published": False,
                            "roles": memberships,
                            "weekly_slot": created.strftime("%G-W%V")
                            if "weekly" in memberships
                            else None,
                        }
                        written[catalog.bucket] = [
                            catalog.bucket.write_json_cas(
                                f"{identifier}/state.json",
                                initial_state,
                                expected_version=None,
                            )
                        ]
                    directory = Path(temporary)
                    specifications: dict[str, object] = {}
                    files: dict[str, Path] = {}
                    with backend.stream_snapshot(SNAPSHOT_TABLES) as stream:
                        captured_at = stream.captured_at.isoformat()
                        for table in SNAPSHOT_TABLES:
                            schema = CANONICAL_TABLE_SCHEMAS[table]
                            path = directory / f"{table}.parquet"
                            rows = 0
                            with pq.ParquetWriter(path, schema) as writer:
                                for batch in stream.tables[table]:
                                    batch = batch.cast(schema)
                                    writer.write_batch(batch)
                                    rows += batch.num_rows
                            refs: list[dict[str, object]] = []
                            if rows:
                                files[table] = path
                                refs.append(
                                    {
                                        "name": path.name,
                                        "size": path.stat().st_size,
                                        "sha256": digest(path),
                                    }
                                )
                            specifications[table] = {
                                "status": "complete",
                                "rows": rows,
                                "schema_ipc": b64encode(
                                    schema.serialize().to_pybytes()
                                ).decode(),
                                "objects": refs,
                            }
                    roles = sorted(
                        {role for _, memberships in due for role in memberships}
                    )
                    manifest: dict[str, object] = {
                        "snapshot_id": identifier,
                        "snapshot_format_version": FORMAT_VERSION,
                        "data_schema_version": DATA_SCHEMA_VERSION,
                        "usagebassoon_version": __version__,
                        **backend.snapshot_provenance(),
                        "created_at": created.isoformat(),
                        "captured_at": captured_at,
                        "run_id": run_id,
                        "cadence": {
                            "trigger": "manual" if manual else "automatic",
                            "roles": roles,
                            "interval_seconds": self.interval.total_seconds()
                            if self.interval
                            else None,
                            "weekly_slot": created.strftime("%G-W%V")
                            if "weekly" in roles
                            else None,
                        },
                        "tables": specifications,
                    }
                    raw = encode(manifest)
                    manifest_path = directory / "manifest.json"
                    manifest_path.write_bytes(raw)
                    completion_path = directory / "COMPLETE"
                    completion_path.write_bytes(
                        encode(
                            {
                                "snapshot_id": identifier,
                                "manifest_sha256": hashlib.sha256(raw).hexdigest(),
                            }
                        )
                    )
                    for catalog, _memberships in due:
                        catalog.check()
                        archive = catalog.bucket
                        owned = written[archive]
                        for path in [*files.values(), manifest_path]:
                            catalog.check()
                            owned.append(
                                archive.upload_file(f"{identifier}/{path.name}", path)
                            )
                        state, version = archive.read_json(f"{identifier}/state.json")
                        assert state is not None
                        archive.write_json_cas(
                            f"{identifier}/state.json",
                            {**state, "captured_at": captured_at},
                            expected_version=version,
                        )
                        catalog.check()
                        catalog.publish(identifier, captured_at)
                        owned.append(
                            archive.upload_file(
                                f"{identifier}/COMPLETE", completion_path
                            )
                        )
                        verified = directory / f"verify-{catalog.owner}"
                        verified.mkdir()
                        self.reader.download(
                            Candidate(archive, identifier, captured_at),
                            verified,
                            SNAPSHOT_TABLES,
                            allow_staging=True,
                        )
                        state, version = archive.read_json(f"{identifier}/state.json")
                        assert state is not None
                        catalog.check()
                        archive.write_json_cas(
                            f"{identifier}/state.json",
                            {
                                **state,
                                "published": True,
                                "verified_at": _now().isoformat(),
                            },
                            expected_version=version,
                        )
                    for catalog, _ in due:
                        catalog.record_outcome(identifier)
                        try:
                            catalog.rotate()
                        except Exception:
                            _LOG.exception(
                                "Snapshot publication succeeded; "
                                "retention cleanup failed at %s",
                                catalog.bucket.uri,
                            )
                    return f"{due[0][0].bucket.uri}/{identifier}"
                except Exception as error:
                    self._rollback(due, identifier, written, error=str(error))
                    raise

        except ArchiveBusy:
            if not written:
                return None
            raise

    def _rollback(
        self,
        due: list[tuple[Catalog, list[str]]],
        identifier: str,
        written: dict[SnapshotBucket, list[SnapshotObject]],
        *,
        error: str,
    ) -> None:
        """Retire owned partial copies while their destination claims remain live."""
        for catalog, _ in due:
            if catalog.bucket not in written:
                continue
            try:
                catalog.check()
                catalog.record_outcome(identifier, error=error)
                catalog.retire(identifier)
            except Exception:
                _LOG.exception(
                    "Snapshot rollback could not clean owned objects at %s",
                    catalog.bucket.uri,
                )

    def list_snapshots(self) -> list[str]:
        """List unique snapshot identities across configured destinations."""
        candidates, _ = self.reader.candidates(allow_empty=True)
        return list(dict.fromkeys(c.identifier for c in reversed(candidates)))

    def restore(
        self,
        backend: StorageBackend,
        snapshot: str = "latest",
        *,
        notice: Callable[[str], None] | None = None,
    ) -> dict[str, int]:
        """Validate the selected archive then restore with all writers stopped."""
        with self.reader.prepare(snapshot, warning=notice) as prepared:
            return restore_prepared(backend, prepared, notice=notice)

    def pin(self, selection: str) -> None:
        """Pin every exact selected copy under protected archive mutations."""
        candidates, automatic = self.reader.candidates(selection)
        if automatic:
            raise ValueError("pin requires an exact snapshot ID or directory")
        for candidate in sorted(candidates, key=_candidate_uri):
            with Catalog(candidate.bucket, self.max_snapshots).hold() as catalog:
                self.reader.manifest(candidate)
                catalog.pin(candidate.identifier)

    def delete(self, selection: str) -> None:
        """Retire exact selected copies; callers own deletion confirmation."""
        candidates = self.reader.lifecycle_candidates(selection)
        self.delete_candidates(candidates)

    def delete_candidates(self, candidates: Sequence[Candidate]) -> None:
        """Delete exactly the copies a caller enumerated for confirmation."""
        completed: list[str] = []
        for candidate in sorted(candidates, key=_candidate_uri):
            try:
                with Catalog(candidate.bucket, self.max_snapshots).hold() as catalog:
                    catalog.retire(candidate.identifier)
                completed.append(candidate.uri)
            except Exception as error:
                raise RuntimeError(
                    "Snapshot deletion completed at "
                    f"{', '.join(completed) or 'no locations'}; "
                    f"cleanup failed at {candidate.uri}: {error}. "
                    "Retired copies remain retired; retry the same selection."
                ) from error

    def copy(self, selection: str, destination: str) -> str:
        """Relocate a verified snapshot without rewriting its identity."""
        bucket = self._bucket(destination)
        with (
            self.reader.prepare(selection, transform=False) as prepared,
            Catalog(bucket, self.max_snapshots).hold() as catalog,
        ):
            identifier = prepared.candidate.identifier
            existing_state, _ = bucket.read_json(f"{identifier}/state.json")
            if existing_state is not None or any(
                e["snapshot_id"] == identifier for e in catalog.entries()
            ):
                raise ValueError("destination already contains this snapshot")
            state, _ = prepared.candidate.bucket.read_json(f"{identifier}/state.json")
            if state is None or state.get("retired") is not False:
                raise ValueError("snapshot lifecycle state is missing or retired")
            catalog.check()
            bucket.write_json_cas(
                f"{identifier}/state.json",
                {
                    **state,
                    "kind": "snapshot_stage",
                    "owner": catalog.owner,
                    "fence": catalog.fence,
                    "published": False,
                },
                expected_version=None,
            )
            try:
                for table, path in prepared.files.items():
                    catalog.check()
                    bucket.upload_file(f"{identifier}/{table}.parquet", path)
                with TemporaryDirectory(prefix="usagebassoon-copy-") as temporary:
                    directory = Path(temporary)
                    manifest_path = directory / "manifest.json"
                    manifest_path.write_bytes(prepared.manifest_bytes)
                    bucket.upload_file(f"{identifier}/manifest.json", manifest_path)
                    catalog.publish(identifier, prepared.candidate.captured_at)
                    catalog.check()
                    bucket.write_json_cas(
                        f"{identifier}/COMPLETE",
                        {
                            "snapshot_id": identifier,
                            "manifest_sha256": hashlib.sha256(
                                prepared.manifest_bytes
                            ).hexdigest(),
                        },
                        expected_version=None,
                    )
                    self.reader.download(
                        Candidate(bucket, identifier, prepared.candidate.captured_at),
                        directory,
                        SNAPSHOT_TABLES,
                        allow_staging=True,
                        transform=False,
                    )
                current, version = bucket.read_json(f"{identifier}/state.json")
                assert current is not None
                catalog.check()
                bucket.write_json_cas(
                    f"{identifier}/state.json",
                    {**current, "published": True, "verified_at": _now().isoformat()},
                    expected_version=version,
                )
            except Exception:
                try:
                    catalog.check()
                    catalog.retire(identifier)
                except Exception:
                    _LOG.exception(
                        "Copy cleanup failed at %s; incomplete objects remain owned",
                        bucket.uri,
                    )
                raise
        return f"{bucket.uri}/{identifier}"


def _bucket_uri(bucket: SnapshotBucket) -> str:
    """Order archive claims deterministically."""
    return bucket.uri


def _candidate_uri(candidate: Candidate) -> str:
    """Order lifecycle operations deterministically."""
    return candidate.uri
