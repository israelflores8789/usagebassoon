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
from uuid import uuid4

import pyarrow.parquet as pq

from usagebassoon.backends.base import StorageBackend
from usagebassoon.buckets.base import (
    ScopedSnapshotBucket,
    SnapshotBucket,
    SnapshotObject,
    bucket_uri,
)
from usagebassoon.buckets.factory import BucketFactory, SnapshotBucketRegistry
from usagebassoon.config import (
    LoggingConfig,
    SnapshotsConfig,
    UsageBassoonConfig,
    _logging_config,
    _snapshots_config,
    default_snapshot_directory,
    parse_interval,
)
from usagebassoon.logger import configure as configure_logging
from usagebassoon.snapshot.catalog import ArchiveBusy, Catalog
from usagebassoon.snapshot.format import FORMAT_VERSION, digest, encode, timestamp
from usagebassoon.snapshot.reader import Candidate, SnapshotReader, snapshot_location
from usagebassoon.snapshot.restore import restore_prepared
from usagebassoon.storage_model import (
    CANONICAL_TABLE_SCHEMAS,
    DATA_SCHEMA_VERSION,
    SNAPSHOT_TABLES,
)
from usagebassoon.version import __version__

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
        destination_uris: Sequence[str] = (),
        max_snapshots: int = 3,
        interval: str | None = None,
        buckets: Sequence[SnapshotBucket] = (),
        bucket_factory: BucketFactory | None = None,
    ) -> None:
        """Configure destinations with optional protocol adapters or factories."""
        if max_snapshots < 1:
            raise ValueError("max_snapshots must be positive")
        if uri is not None and destination_uris:
            raise ValueError("uri cannot be combined with destination_uris")
        self._configuration: UsageBassoonConfig | None = None
        locations = (uri,) if uri is not None else tuple(destination_uris)
        if not locations:
            locations = tuple(bucket.uri for bucket in buckets)
        self._locations = tuple(dict.fromkeys(bucket_uri(value) for value in locations))
        self._read_locations = self._locations
        self._buckets: dict[str, SnapshotBucket] = {}
        for bucket in buckets:
            location = bucket_uri(bucket.uri)
            if location in self._buckets:
                raise ValueError("duplicate injected snapshot bucket location")
            self._buckets[location] = bucket
        self._bucket_factory = bucket_factory or SnapshotBucketRegistry().resolve
        self.uri = self._locations[0] if self._locations else ""
        self.max_snapshots = max_snapshots
        self.interval = parse_interval(interval)
        self._weekly: set[str] = set()

    def _bucket(self, uri: str) -> SnapshotBucket:
        """Resolve an explicit URI without redirecting it through configuration."""
        location = bucket_uri(uri)
        if location not in self._buckets:
            parents = [
                root for root in self._buckets if location.startswith(root + "/")
            ]
            if parents:
                root = max(parents, key=len)
                bucket: SnapshotBucket = ScopedSnapshotBucket(
                    self._buckets[root], location[len(root) + 1 :]
                )
            else:
                bucket = self._bucket_factory(location)
            if bucket_uri(bucket.uri) != location:
                raise ValueError("snapshot bucket factory redirected the requested URI")
            self._buckets[location] = bucket
        return self._buckets[location]

    @classmethod
    def from_config(cls, configuration: UsageBassoonConfig) -> SnapshotArchiver:
        """Create an authenticated archiver with explicit weekly destination policy."""
        result = cls._from_settings(configuration.snapshots)
        result._configuration = configuration
        return result

    @classmethod
    def _from_settings(cls, settings: SnapshotsConfig) -> SnapshotArchiver:
        """Construct enabled archives from backend-independent snapshot settings."""
        registry = SnapshotBucketRegistry.from_settings(settings)
        result = cls(
            destination_uris=[d.uri for d in registry.destinations if d.enabled],
            max_snapshots=settings.max_snapshots,
            interval=settings.schedule.interval,
            bucket_factory=registry.resolve,
        )
        result._read_locations = tuple(d.uri for d in registry.destinations)
        result._weekly = {
            d.uri for d in registry.destinations if d.enabled and d.weekly
        }
        return result

    @property
    def _archives(self) -> tuple[SnapshotBucket, ...]:
        """Construct configured adapters only when their locations are needed."""
        return tuple(self._bucket(uri) for uri in self._locations)

    def selection_enabled(self, selection: str) -> bool:
        """Determine whether an explicit location belongs to an enabled archive."""
        if selection == "latest" or not (
            "/" in selection
            or selection.startswith((".", "~"))
            or Path(selection).exists()
        ):
            return True
        location = snapshot_location(selection)
        for configured in self._locations:
            root = bucket_uri(configured)
            if location == root or location.startswith(root + "/"):
                return True
        return False

    @classmethod
    def for_read(cls, path: Path) -> SnapshotArchiver:
        """Read archive settings without requiring a valid backend or source ID."""
        if not path.exists():
            configure_logging(LoggingConfig())
            result = cls()
            result._read_locations = (str(default_snapshot_directory()),)
            return result
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
        configure_logging(_logging_config(payload.get("logging")))
        settings = _snapshots_config(payload.get("snapshots"))
        return cls._from_settings(settings)

    @property
    def reader(self) -> SnapshotReader:
        """Return the shared reader used by restore, inspection, and audit."""
        return SnapshotReader(
            lambda: tuple(self._bucket(uri) for uri in self._read_locations),
            self._bucket,
            locations=self._read_locations,
        )

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
            state = catalog.state(str(entry["snapshot_id"]))
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
        if not self._locations:
            if not manual:
                return None
            raise ValueError("at least one snapshot destination is required")
        identifier = f"{created.strftime('%Y-%m-%dT%H%M%SZ')}_{uuid4().hex}"
        catalogs = [
            Catalog(a, self.max_snapshots)
            for a in sorted(self._archives, key=_bucket_uri)
        ]
        due = [(c, self._roles(c, created, manual=manual)) for c in catalogs]
        due = [(c, roles) for c, roles in due if roles]
        if not due:
            return None
        written: dict[str, list[SnapshotObject]] = {}
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
                        catalog.stage(identifier, initial_state)
                        written[bucket_uri(catalog.bucket.uri)] = []
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
                        owned = written[bucket_uri(archive.uri)]
                        for path in [*files.values(), manifest_path]:
                            catalog.check()
                            owned.append(
                                archive.upload_file(f"{identifier}/{path.name}", path)
                            )
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
                        catalog.publish(identifier, captured_at)
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
            _LOG.warning(
                "Snapshot attempt stopped after contention or ownership loss; "
                "next cadence will retry"
            )
            if not manual:
                return None
            raise

    def _rollback(
        self,
        due: list[tuple[Catalog, list[str]]],
        identifier: str,
        written: dict[str, list[SnapshotObject]],
        *,
        error: str,
    ) -> None:
        """Retire owned partial copies while their destination claims remain live."""
        for catalog, _ in due:
            if bucket_uri(catalog.bucket.uri) not in written:
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
            try:
                state = Catalog(prepared.candidate.bucket).state(identifier)
            except (ValueError, OSError, RuntimeError):
                _LOG.warning(
                    "Snapshot copy could not read lifecycle state at %s; "
                    "preserving a pin",
                    prepared.candidate.uri,
                    exc_info=True,
                )
                state = None
            if state is None:
                state = {"pinned": True, "roles": list[str](), "retired": False}
            # Cleanup revisions describe the original provider's objects only.
            state.pop("cleanup", None)
            catalog.stage(
                identifier,
                {
                    **state,
                    "kind": "snapshot_stage",
                    "published": False,
                    "retired": False,
                },
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
                catalog.publish(identifier, prepared.candidate.captured_at)
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
