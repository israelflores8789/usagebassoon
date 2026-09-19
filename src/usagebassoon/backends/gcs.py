# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""gcs.py — Generation-safe Google Cloud Storage archive access."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast
from urllib.parse import urlparse


class GcsPreconditionError(RuntimeError):
    """Raised when a generation-conditional GCS operation loses a race."""


class GcsBlob(Protocol):
    """The subset of the official Blob API used by the archive adapter."""

    name: str
    generation: int | str | None
    size: int | None
    crc32c: str | None

    def reload(self) -> None:
        """Refresh object metadata."""
        ...

    def exists(self) -> bool:
        """Return whether the object exists."""
        ...

    def download_as_bytes(self, *, if_generation_match: int | None = None) -> bytes:
        """Download object bytes with an optional generation precondition."""
        ...

    def download_as_text(self) -> str:
        """Download object text."""
        ...

    def upload_from_string(
        self,
        payload: bytes,
        *,
        content_type: str,
        if_generation_match: int | None,
    ) -> None:
        """Upload object bytes with an optional generation precondition."""
        ...

    def delete(self, *, if_generation_match: int) -> None:
        """Delete with an exact generation precondition."""
        ...


class GcsBucket(Protocol):
    """The bucket operations needed by :class:`GcsArchive`."""

    lifecycle_rules: Iterable[Mapping[str, Mapping[str, object]]]

    def blob(self, name: str, generation: int | None = None) -> GcsBlob:
        """Return one blob handle."""
        ...

    def reload(self) -> None:
        """Refresh bucket metadata."""
        ...


class GcsClient(Protocol):
    """The client operations needed by :class:`GcsArchive`."""

    def bucket(self, bucket_name: str) -> GcsBucket:
        """Return a bucket handle."""
        ...

    def list_blobs(self, bucket: GcsBucket, *, prefix: str) -> Iterable[GcsBlob]:
        """List blob handles below one prefix."""
        ...


@dataclass(frozen=True, slots=True)
class GcsObject:
    """Immutable metadata required to read or remove one GCS object safely.

    Attributes:
        name: Bucket-relative object name.
        generation: Object generation at publication time.
        size: Object size in bytes.
        checksum: Server-provided CRC32C checksum when available.
    """

    name: str
    generation: int
    size: int
    checksum: str | None


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    """Split a GCS archive URI into bucket name and normalized object prefix.

    Args:
        uri: ``gs://bucket/optional/prefix`` archive URI.

    Returns:
        Bucket name and slash-free archive prefix.

    Raises:
        ValueError: If the URI is not a bucket-qualified GCS URI.
    """
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc or parsed.params or parsed.query:
        raise ValueError(f"invalid GCS archive URI: {uri!r}")
    return parsed.netloc, parsed.path.strip("/")


class GcsArchive:
    """Small adapter for generation-aware archive object operations."""

    def __init__(
        self,
        uri: str,
        *,
        project: str | None = None,
        location: str | None = None,
        credentials_file: Path | None = None,
        client: GcsClient | None = None,
    ) -> None:
        """Open an archive bucket through the optional official GCS client.

        Args:
            uri: GCS archive root.
            project: Optional GCP project identifier.
            location: Configured bucket location metadata.
            credentials_file: Optional service-account credential file.
            client: Injectable ``google.cloud.storage.Client`` for tests.

        Raises:
            RuntimeError: If the optional GCS dependency is unavailable.
        """
        self.uri = uri.rstrip("/")
        self.bucket_name, self.prefix = parse_gcs_uri(self.uri)
        self.project = project
        self.location = location
        self.credentials_file = credentials_file
        if client is None:
            try:
                from google.cloud import storage
            except ImportError as error:
                raise RuntimeError(
                    "GCS snapshots require the usagebassoon[gcs] extra"
                ) from error
            resolved_credentials = None
            if credentials_file is not None:
                try:
                    from google.auth.exceptions import GoogleAuthError
                    from google.oauth2 import service_account
                except ImportError as error:
                    raise RuntimeError(
                        "GCS snapshots require the usagebassoon[gcs] extra"
                    ) from error
                try:
                    resolved_credentials = (
                        service_account.Credentials.from_service_account_file(
                            str(credentials_file)
                        )
                    )
                except (GoogleAuthError, OSError, ValueError) as error:
                    raise RuntimeError(
                        "GCS credentials file could not be loaded"
                    ) from error
            try:
                client_value = cast(
                    GcsClient,
                    storage.Client(
                        project=project,
                        credentials=resolved_credentials,
                    ),
                )
            except Exception as error:
                if error.__class__.__name__ in {
                    "DefaultCredentialsError",
                    "GoogleAuthError",
                }:
                    raise RuntimeError(
                        "GCS authentication failed; configure ADC or credentials_file"
                    ) from error
                raise
        else:
            client_value = client
        self.client = client_value
        self.bucket = client_value.bucket(self.bucket_name)

    def key(self, relative_name: str) -> str:
        """Return an archive-root-relative name as a bucket object name."""
        relative = relative_name.strip("/")
        return f"{self.prefix}/{relative}" if self.prefix else relative

    def relative(self, object_name: str) -> str:
        """Return an object name relative to this archive root."""
        if not self.prefix:
            return object_name
        expected = f"{self.prefix}/"
        if not object_name.startswith(expected):
            raise ValueError(f"object lies outside archive root: {object_name!r}")
        return object_name.removeprefix(expected)

    @staticmethod
    def _object(blob: GcsBlob) -> GcsObject:
        """Materialize stable metadata from a loaded cloud blob."""
        if blob.generation is None or blob.size is None:
            blob.reload()
        if blob.generation is None or blob.size is None:
            raise RuntimeError(f"GCS did not return metadata for {blob.name!r}")
        return GcsObject(
            name=blob.name,
            generation=int(blob.generation),
            size=blob.size,
            checksum=blob.crc32c,
        )

    @staticmethod
    def _raise_precondition(error: Exception) -> None:
        """Convert a client precondition exception without coupling tests."""
        if error.__class__.__name__ in {"PreconditionFailed", "Conflict"}:
            raise GcsPreconditionError("GCS object generation changed") from error
        raise error

    def read_bytes(self, relative_name: str, *, generation: int | None = None) -> bytes:
        """Read one object, optionally requiring its published generation."""
        blob = self.bucket.blob(self.key(relative_name), generation=generation)
        try:
            return blob.download_as_bytes(if_generation_match=generation)
        except Exception as error:
            self._raise_precondition(error)
        raise AssertionError("unreachable")

    def write_bytes(
        self,
        relative_name: str,
        payload: bytes,
        *,
        if_generation_match: int | None = None,
        content_type: str = "application/octet-stream",
    ) -> GcsObject:
        """Write an object, optionally with a generation compare-and-swap guard."""
        blob = self.bucket.blob(self.key(relative_name))
        try:
            blob.upload_from_string(
                payload,
                content_type=content_type,
                if_generation_match=if_generation_match,
            )
            blob.reload()
        except Exception as error:
            self._raise_precondition(error)
        return self._object(blob)

    def read_json(
        self, relative_name: str
    ) -> tuple[dict[str, object] | None, int | None]:
        """Read a JSON object and its generation, returning absent for a missing key."""
        blob = self.bucket.blob(self.key(relative_name))
        payload: object = None
        try:
            if not blob.exists():
                return None, None
            blob.reload()
            payload = json.loads(blob.download_as_text())
        except Exception as error:
            self._raise_precondition(error)
        if not isinstance(payload, dict):
            raise ValueError(f"GCS JSON object {relative_name!r} must be an object")
        if blob.generation is None:
            raise RuntimeError(f"GCS did not return generation for {relative_name!r}")
        return payload, int(blob.generation)

    def write_json_cas(
        self,
        relative_name: str,
        payload: dict[str, object],
        *,
        generation: int | None,
    ) -> GcsObject:
        """Atomically create or replace JSON using a GCS generation precondition."""
        return self.write_bytes(
            relative_name,
            (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode(),
            if_generation_match=0 if generation is None else generation,
            content_type="application/json",
        )

    def list(self, relative_prefix: str) -> tuple[GcsObject, ...]:
        """List loaded object metadata under an archive-relative prefix."""
        prefix = self.key(relative_prefix)
        return tuple(
            self._object(blob)
            for blob in self.client.list_blobs(self.bucket, prefix=prefix)
        )

    def delete(self, relative_name: str, *, generation: int) -> None:
        """Delete exactly the published generation of an object."""
        try:
            self.bucket.blob(self.key(relative_name)).delete(
                if_generation_match=generation
            )
        except Exception as error:
            self._raise_precondition(error)

    def lifecycle_delete_warnings(self) -> tuple[str, ...]:
        """Report lifecycle delete rules that could apply to this archive root."""
        try:
            self.bucket.reload()
            rules = self.bucket.lifecycle_rules
        except Exception as error:
            return (f"GCS lifecycle inspection unavailable: {error}",)
        warnings: list[str] = []
        for rule in rules:
            action = rule.get("action", {})
            condition = rule.get("condition", {})
            if action.get("type") != "Delete":
                continue
            prefixes = condition.get("matchesPrefix", ())
            if not isinstance(prefixes, (list, tuple)):
                prefixes = ()
            if not prefixes or any(
                self.prefix.startswith(str(prefix).strip("/"))
                or str(prefix).strip("/").startswith(self.prefix)
                for prefix in prefixes
            ):
                warnings.append(
                    "a GCS Delete lifecycle rule could match the snapshot archive"
                )
        return tuple(warnings)
