# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""reader.py — Global snapshot selection and verified disk-backed Arrow access."""

from __future__ import annotations

import errno
import hashlib
import json
import logging
import re
from base64 import b64decode
from collections.abc import Callable, Generator, Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from usagebassoon.buckets.base import SnapshotBucket, validate_relative_name
from usagebassoon.snapshot.catalog import Catalog
from usagebassoon.snapshot.format import (
    DATA_CONTRACTS,
    decode_manifest,
    digest,
    snapshot_id,
    timestamp,
    transformation_path,
    validate_manifest,
)
from usagebassoon.storage_model import (
    CANONICAL_TABLE_SCHEMAS,
    DATA_SCHEMA_VERSION,
    SNAPSHOT_TABLES,
)

_LOG = logging.getLogger("usagebassoon")


@dataclass(frozen=True, slots=True)
class Candidate:
    """One available copy of an immutable snapshot."""

    bucket: SnapshotBucket
    identifier: str
    captured_at: str

    @property
    def uri(self) -> str:
        """Return the portable directory URI."""
        return f"{self.bucket.uri}/{self.identifier}"


@dataclass(frozen=True, slots=True)
class PreparedSnapshot:
    """Validated local Parquet files owned by the reader's temporary context."""

    candidate: Candidate
    manifest: dict[str, object]
    files: dict[str, Path]
    rows: dict[str, int]
    manifest_bytes: bytes
    warnings: tuple[str, ...] = ()

    def batches(self, table: str) -> Iterable[pa.RecordBatch]:
        """Read a verified table in canonical bounded batches."""
        if table not in self.files:
            return
        schema = CANONICAL_TABLE_SCHEMAS[table]
        for batch in pq.ParquetFile(self.files[table]).iter_batches(batch_size=65536):
            yield batch.cast(schema)


class SnapshotReader:
    """Resolve locations and validate candidates before any destination write."""

    def __init__(
        self,
        buckets: Sequence[SnapshotBucket],
        resolver: Callable[[str], SnapshotBucket],
    ) -> None:
        """Bind configured locations and an authenticated URI resolver."""
        self.buckets = tuple(buckets)
        self.resolver = resolver

    def candidates(
        self,
        selection: str = "latest",
        *,
        allow_empty: bool = False,
        warning: Callable[[str], None] | None = None,
    ) -> tuple[list[Candidate], bool]:
        """Resolve a latest/root request or an exact ID/path request."""
        buckets = self.buckets
        exact: str | None = None
        automatic = selection == "latest"
        if selection != "latest":
            if (
                "/" in selection
                or selection.startswith((".", "~"))
                or Path(selection).exists()
            ):
                location = selection.rstrip("/").removesuffix("/manifest.json")
                bucket = self.resolver(location)
                direct = any(
                    obj.name == "manifest.json" for obj in bucket.list("manifest.json")
                )
                if direct:
                    parent, leaf = (
                        location.rsplit("/", 1) if "/" in location else (".", location)
                    )
                    exact = snapshot_id(leaf)
                    buckets = (self.resolver(parent),)
                else:
                    buckets = (bucket,)
                    automatic = True
            else:
                exact = snapshot_id(selection)
        candidates: list[Candidate] = []
        failures: list[str] = []
        for bucket in buckets:
            try:
                entries: list[dict[str, object]] = []
                if exact is not None:
                    if not any(
                        obj.name == f"{exact}/manifest.json"
                        for obj in bucket.list(f"{exact}/manifest.json")
                    ):
                        continue
                    manifest = self.manifest(Candidate(bucket, exact, ""))
                    entries.append(
                        {
                            "snapshot_id": exact,
                            "captured_at": manifest.get("captured_at"),
                        }
                    )
                else:
                    entries = Catalog(bucket).entries()
                candidates.extend(
                    Candidate(bucket, str(e["snapshot_id"]), str(e["captured_at"]))
                    for e in entries
                )
            except Exception as error:
                message = f"Cannot discover snapshots at {bucket.uri}: {error}"
                if not automatic:
                    raise ValueError(message) from error
                failures.append(message)
                _LOG.warning(message)
                if warning:
                    warning(message)
        candidates.sort(
            key=_candidate_order,
            reverse=True,
        )
        if not candidates and not allow_empty:
            raise ValueError(
                "no complete snapshots found"
                + (": " + "; ".join(failures) if failures else "")
            )
        return candidates, automatic

    @staticmethod
    def manifest(candidate: Candidate) -> dict[str, object]:
        """Return verified immutable manifest metadata."""
        return SnapshotReader.manifest_document(candidate)[0]

    @staticmethod
    def manifest_document(
        candidate: Candidate, *, allow_staging: bool = False
    ) -> tuple[dict[str, object], bytes]:
        """Verify immutable completion evidence and portable manifest metadata."""
        prefix = candidate.identifier
        state, _ = candidate.bucket.read_json(f"{prefix}/state.json")
        if state is not None and state.get("retired") is True:
            raise ValueError("snapshot is retired")
        if not allow_staging and state is not None and state.get("published") is False:
            raise ValueError("snapshot publication has not completed")
        complete, _ = candidate.bucket.read_json(f"{prefix}/COMPLETE")
        if complete is None or complete.get("snapshot_id") != prefix:
            raise ValueError("snapshot has no valid completion record")
        refs = candidate.bucket.list(f"{prefix}/manifest.json")
        reference = next((r for r in refs if r.name == f"{prefix}/manifest.json"), None)
        if reference is None:
            raise ValueError("snapshot manifest is missing")
        raw = candidate.bucket.read_bytes(reference.name, version=reference.version)
        if hashlib.sha256(raw).hexdigest() != complete.get("manifest_sha256"):
            raise ValueError("snapshot manifest SHA-256 does not match completion")
        value: object = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("snapshot manifest must be an object")
        value = decode_manifest(value)
        validate_manifest(value, prefix)
        if candidate.captured_at and timestamp(value.get("captured_at")) != timestamp(
            candidate.captured_at
        ):
            raise ValueError("snapshot capture point does not match its catalog")
        return value, raw

    def listing(self, selection: str = "latest") -> list[dict[str, object]]:
        """List every copy with metadata, without implying full object verification."""
        candidates, _ = self.candidates(selection, allow_empty=True)
        if not candidates:
            return []
        latest = candidates[0].identifier
        result: list[dict[str, object]] = []
        for candidate in candidates:
            try:
                manifest = self.manifest(candidate)
                state, _ = candidate.bucket.read_json(
                    f"{candidate.identifier}/state.json"
                )
                result.append(
                    {
                        "id": candidate.identifier,
                        "captured_at": candidate.captured_at,
                        "latest": candidate.identifier == latest,
                        "location": "GCS"
                        if candidate.bucket.uri.startswith("gs://")
                        else "Local",
                        "uri": candidate.uri,
                        "pinned": state.get("pinned") if state else None,
                        "roles": state.get("roles") if state else None,
                        "weekly_slot": state.get("weekly_slot") if state else None,
                        "verified_at": state.get("verified_at") if state else None,
                        **{
                            k: manifest.get(k)
                            for k in (
                                "usagebassoon_version",
                                "data_schema_version",
                                "snapshot_format_version",
                                "backend_schema_version",
                                "backend_schema_hash",
                                "cadence",
                            )
                        },
                    }
                )
            except Exception as error:
                result.append(
                    {
                        "id": candidate.identifier,
                        "uri": candidate.uri,
                        "captured_at": candidate.captured_at,
                        "error": str(error),
                    }
                )
        return result

    def lifecycle_candidates(self, selection: str) -> list[Candidate]:
        """Resolve exact lifecycle targets, including cleanup of retired copies."""
        if selection == "latest":
            raise ValueError("lifecycle changes require an exact snapshot ID or URI")
        buckets = self.buckets
        if (
            "/" in selection
            or selection.startswith((".", "~"))
            or Path(selection).exists()
        ):
            location = selection.rstrip("/").removesuffix("/manifest.json")
            parent, leaf = (
                location.rsplit("/", 1) if "/" in location else (".", location)
            )
            identifier = snapshot_id(leaf)
            buckets = (self.resolver(parent),)
        else:
            identifier = snapshot_id(selection)
        targets: list[Candidate] = []
        for bucket in buckets:
            state, _ = bucket.read_json(f"{identifier}/state.json")
            if state is not None:
                targets.append(
                    Candidate(bucket, identifier, str(state.get("captured_at") or ""))
                )
        if not targets:
            raise ValueError(
                "snapshot lifecycle state is missing; refusing destructive action"
            )
        return targets

    def download(
        self,
        candidate: Candidate,
        directory: Path,
        tables: Sequence[str],
        *,
        allow_staging: bool = False,
        transform: bool = True,
    ) -> PreparedSnapshot:
        """Download and validate only the requested tables into private files."""
        manifest, manifest_bytes = self.manifest_document(
            candidate, allow_staging=allow_staging
        )
        version = manifest["data_schema_version"]
        assert isinstance(version, int)
        contract = DATA_CONTRACTS[version]
        specifications = manifest["tables"]
        assert isinstance(specifications, dict)
        files: dict[str, Path] = {}
        counts: dict[str, int] = {}
        selected = tuple(contract) if version != DATA_SCHEMA_VERSION else tables
        for table in selected:
            if table not in contract:
                raise ValueError(f"unsupported snapshot table: {table}")
            spec = specifications[table]
            if not isinstance(spec, dict):
                raise ValueError("invalid snapshot table metadata")
            rows = spec.get("rows")
            schema_ipc = spec.get("schema_ipc")
            objects = spec.get("objects")
            if (
                not isinstance(rows, int)
                or isinstance(rows, bool)
                or rows < 0
                or not isinstance(objects, list)
                or not isinstance(schema_ipc, str)
            ):
                raise ValueError(f"invalid table metadata: {table}")
            schema = pa.ipc.read_schema(
                pa.BufferReader(b64decode(schema_ipc, validate=True))
            )
            if not schema.equals(contract[table], check_metadata=True):
                raise ValueError(
                    f"snapshot table {table} is incompatible with destination schema"
                )
            if len(objects) != (1 if rows else 0):
                raise ValueError(f"invalid object count: {table}")
            counts[table] = rows
            for ref in objects:
                if not isinstance(ref, dict):
                    raise ValueError("invalid snapshot object reference")
                name, size, sha = ref.get("name"), ref.get("size"), ref.get("sha256")
                if (
                    not isinstance(name, str)
                    or validate_relative_name(name) != f"{table}.parquet"
                    or not isinstance(size, int)
                    or isinstance(size, bool)
                    or size < 0
                    or not isinstance(sha, str)
                    or re.fullmatch(r"[0-9a-f]{64}", sha) is None
                ):
                    raise ValueError("invalid portable object reference")
                path = directory / name
                candidate.bucket.download_file(f"{candidate.identifier}/{name}", path)
                if path.stat().st_size != size or digest(path) != sha:
                    raise ValueError(
                        f"snapshot object SHA-256 or size does not match: {name}"
                    )
                parquet = pq.ParquetFile(path)
                if parquet.metadata.num_rows != rows or not parquet.schema_arrow.equals(
                    schema, check_metadata=False
                ):
                    raise ValueError(
                        f"snapshot table {table} row count or schema does not match"
                    )
                for batch in parquet.iter_batches(batch_size=65536):
                    batch.validate(full=True)
                    for field in schema:
                        if (
                            not field.nullable
                            and batch.column(
                                batch.schema.get_field_index(field.name)
                            ).null_count
                        ):
                            raise ValueError(
                                "snapshot has null required field: "
                                f"{table}.{field.name}"
                            )
                files[table] = path
        steps = transformation_path(version) if transform else ()
        for step in steps:
            output = directory / f"contract-{step.to_version}"
            output.mkdir()
            transformed = step.transform(files, output)
            if any(
                path.resolve() in {p.resolve() for p in files.values()}
                for path in transformed.values()
            ):
                raise ValueError(
                    "snapshot transformations must preserve original files"
                )
            step.validate(files, transformed)
            files = dict(transformed)
        if steps:
            counts = {}
            if set(files) != set(SNAPSHOT_TABLES):
                raise ValueError(
                    "snapshot transformation did not produce every current table"
                )
            for table, path in files.items():
                parquet = pq.ParquetFile(path)
                if not parquet.schema_arrow.equals(
                    CANONICAL_TABLE_SCHEMAS[table], check_metadata=False
                ):
                    raise ValueError(
                        "snapshot transformation produced an invalid current schema"
                    )
                for batch in parquet.iter_batches(batch_size=65536):
                    batch.validate(full=True)
                counts[table] = parquet.metadata.num_rows
        return PreparedSnapshot(candidate, manifest, files, counts, manifest_bytes)

    @contextmanager
    def prepare(
        self,
        selection: str = "latest",
        *,
        tables: Sequence[str] = SNAPSHOT_TABLES,
        warning: Callable[[str], None] | None = None,
        transform: bool = True,
    ) -> Generator[PreparedSnapshot]:
        """Select and fully validate before yielding; never retry destination writes.

        Yields:
            Verified files until this context exits.
        """
        notices: list[str] = []

        def discovery_warning(message: str) -> None:
            """Keep discovery failures in recovery metadata and stderr notices."""
            notices.append(message)
            if warning:
                warning(message)

        candidates, automatic = self.candidates(selection, warning=discovery_warning)
        with TemporaryDirectory(prefix="usagebassoon-restore-") as temporary:
            directory = Path(temporary)
            prepared: PreparedSnapshot | None = None
            for candidate in candidates:
                downloaded: PreparedSnapshot | None = None
                try:
                    # A download reservation prevents concurrent rotation. Read-only
                    # archives can still recover through stable provider revisions.
                    catalog = Catalog(candidate.bucket)
                    try:
                        with catalog.hold():
                            downloaded = self.download(
                                candidate, directory, tables, transform=transform
                            )
                    except Exception as error:
                        if (
                            not isinstance(
                                error, (PermissionError, json.JSONDecodeError)
                            )
                            and error.__class__.__name__ != "Forbidden"
                            and "invalid archive catalog" not in str(error)
                        ):
                            raise
                        downloaded = self.download(
                            candidate, directory, tables, transform=transform
                        )
                except Exception as error:
                    if isinstance(error, OSError) and error.errno in {
                        errno.ENOSPC,
                        errno.EDQUOT,
                    }:
                        raise RuntimeError(
                            "Insufficient temporary disk space to validate the "
                            "snapshot; free space and retry. "
                            "Destination data was not changed."
                        ) from error
                    message = f"Snapshot {candidate.uri} cannot be read: {error}"
                    if not automatic:
                        raise ValueError(message) from error
                    notices.append(message)
                    _LOG.warning(message)
                    if warning:
                        warning(message)
                    continue
                prepared = downloaded
                break
            if prepared is None:
                raise ValueError("no usable snapshot: " + "; ".join(notices))
            if notices:
                message = (
                    f"Recovery selected {prepared.candidate.uri}, "
                    f"captured {prepared.candidate.captured_at}; "
                    "newer candidates were unavailable."
                )
                _LOG.warning(message)
                if warning:
                    warning(message)
            # Destination exceptions occur outside the fallback loop.
            yield PreparedSnapshot(
                prepared.candidate,
                prepared.manifest,
                prepared.files,
                prepared.rows,
                prepared.manifest_bytes,
                tuple(notices),
            )


def _candidate_order(candidate: Candidate) -> tuple[datetime, str, str]:
    """Order copies globally with deterministic ties."""
    return timestamp(candidate.captured_at), candidate.identifier, candidate.bucket.uri
