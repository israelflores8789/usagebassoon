# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""gcs.py — Google Cloud Storage implementation of the snapshot bucket contract."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Generator, Iterable, Mapping
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from time import monotonic, sleep
from typing import Protocol, cast
from urllib.parse import urlparse

from usagebassoon.buckets.base import (
    SnapshotObject,
    SnapshotPreconditionError,
    SnapshotVersion,
    validate_relative_name,
)
from usagebassoon.deadlines import (
    OperationTimeout,
    bounded,
    http_session,
    limited,
    operation,
    remaining_seconds,
)

_LOG = logging.getLogger("usagebassoon")
_MUTATION_INTERVAL = 1.1
_RETRY_SECONDS = 60.0
_RETRY_ATTEMPTS = 5


class _MutationGate:
    """Share same-object pacing across adapters in this process."""

    def __init__(self) -> None:
        """Create an idle gate without retaining provider data."""
        self.lock = Lock()
        self.next_at = 0.0
        self.users = 0


_MUTATION_GATES: dict[tuple[str, str], _MutationGate] = {}
_MUTATION_LOCK = Lock()


def _pause(seconds: float) -> None:
    """Wait only when the current operation can afford the full delay."""
    if seconds >= remaining_seconds(None):
        raise OperationTimeout("GCS pacing or backoff exceeds the operation budget")
    if seconds > 0:
        sleep(seconds)
    remaining_seconds()


@contextmanager
def _mutation(bucket_name: str, object_name: str) -> Generator[None]:
    """Pace each same-object attempt, including retries and deletes."""
    key = (bucket_name, object_name)
    with _MUTATION_LOCK:
        now = monotonic()
        for expired in [
            name
            for name, gate in _MUTATION_GATES.items()
            if not gate.users and gate.next_at <= now
        ]:
            del _MUTATION_GATES[expired]
        gate = _MUTATION_GATES.setdefault(key, _MutationGate())
        gate.users += 1
    acquired = False
    try:
        acquired = gate.lock.acquire(timeout=remaining_seconds(None))
        if not acquired:
            raise OperationTimeout("GCS mutation pacing exhausted the operation budget")
        _pause(max(0.0, gate.next_at - monotonic()))
        try:
            yield
        finally:
            gate.next_at = monotonic() + _MUTATION_INTERVAL
    finally:
        if acquired:
            gate.lock.release()
        with _MUTATION_LOCK:
            gate.users -= 1


def _retryable(error: Exception) -> bool:
    """Recognize documented transient GCS failures without retrying conflicts."""
    from google.api_core.exceptions import GoogleAPICallError
    from google.api_core.retry import if_transient_error
    from requests.exceptions import Timeout as RequestTimeout

    return (
        if_transient_error(error)
        or isinstance(error, ConnectionError | RequestTimeout)
        or (
            isinstance(error, GoogleAPICallError)
            and error.code in {408, 429, 500, 502, 503, 504}
        )
        or (
            isinstance(error, OperationTimeout)
            and isinstance(error.__cause__, RequestTimeout)
        )
    )


class GcsBlob(Protocol):
    """Internal testable seam for the subset of the Google Blob SDK we use."""

    name: str
    generation: int | str | None
    size: int | None
    crc32c: str | None

    def upload_from_filename(
        self,
        filename: str,
        *,
        if_generation_match: int,
        timeout: float,
        retry: object = None,
    ) -> None:
        """Upload a file through the SDK's resumable transfer."""
        ...

    def download_to_filename(
        self,
        filename: str,
        *,
        if_generation_match: int,
        timeout: float,
        retry: object = None,
    ) -> None:
        """Download one stable revision through the SDK."""
        ...

    def reload(self, *, timeout: float, retry: object = None) -> None:
        """Refresh object metadata."""
        ...

    def exists(self, *, timeout: float, retry: object = None) -> bool:
        """Return whether the object exists."""
        ...

    def download_as_bytes(
        self,
        *,
        if_generation_match: int | None = None,
        timeout: float,
        retry: object = None,
    ) -> bytes:
        """Download object bytes with an optional generation precondition."""
        ...

    def download_as_text(self, *, timeout: float, retry: object = None) -> str:
        """Download object text."""
        ...

    def upload_from_string(
        self,
        payload: bytes,
        *,
        content_type: str,
        if_generation_match: int | None,
        timeout: float,
        retry: object = None,
    ) -> None:
        """Upload object bytes with an optional generation precondition."""
        ...

    def delete(
        self, *, if_generation_match: int, timeout: float, retry: object = None
    ) -> None:
        """Delete with an exact generation precondition."""
        ...


class GcsBucket(Protocol):
    """Internal testable seam for the Google Bucket SDK used by this adapter."""

    lifecycle_rules: Iterable[Mapping[str, Mapping[str, object]]]

    def blob(self, name: str, generation: int | None = None) -> GcsBlob:
        """Return one blob handle."""
        ...

    def reload(self, *, timeout: float, retry: object = None) -> None:
        """Refresh bucket metadata."""
        ...


class GcsClient(Protocol):
    """Internal testable seam for the Google storage client used by this adapter."""

    def bucket(self, bucket_name: str) -> GcsBucket:
        """Return a bucket handle."""
        ...

    def list_blobs(
        self, bucket: GcsBucket, *, prefix: str, timeout: float, retry: object = None
    ) -> Iterable[GcsBlob]:
        """List blob handles below one prefix."""
        ...


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
    prefix = parsed.path.strip("/")
    return parsed.netloc, validate_relative_name(prefix, allow_empty=True)


class GcsSnapshotBucket:
    """Implement the provider-neutral ``SnapshotBucket`` contract over GCS.

    GCS generations supply immutable object versions and compare-and-swap
    publication. The GCS SDK protocols above are private adapter seams, not
    extension contracts; new providers implement ``SnapshotBucket`` directly.
    One adapter retry loop owns jitter, pacing and deadlines. SDK retries stay
    disabled to avoid nested retries. Unfenced writes are never replayed, and
    ambiguous committed mutations fail closed on a subsequent conflict.
    """

    def __init__(
        self,
        uri: str,
        *,
        project: str | None = None,
        credentials_file: Path | None = None,
        timeout_seconds: float = 60.0,
        client: GcsClient | None = None,
    ) -> None:
        """Open an archive bucket through the optional official GCS client.

        Args:
            uri: GCS archive root.
            project: Optional GCP project identifier.
            credentials_file: Optional service-account credential file.
            timeout_seconds: Fallback budget for direct adapter operations.
            client: Injectable ``google.cloud.storage.Client`` for tests.

        Raises:
            RuntimeError: If the optional GCS dependency is unavailable.
            ConfigurationError: If ambient endpoint overrides are set without
                an explicitly injected client.
        """
        self.uri = uri.rstrip("/")
        self.bucket_name, self.prefix = parse_gcs_uri(self.uri)
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.project = project
        self.credentials_file = credentials_file
        self.timeout_seconds = timeout_seconds
        with operation(timeout_seconds):
            if client is None:
                from usagebassoon.config import validate_gcs_environment

                validate_gcs_environment()
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
                        from google.oauth2.service_account import Credentials
                    except ImportError as error:
                        raise RuntimeError(
                            "GCS snapshots require the usagebassoon[gcs] extra"
                        ) from error
                    try:
                        resolved_credentials = Credentials.from_service_account_file(
                            str(credentials_file),
                            scopes=[
                                "https://www.googleapis.com/auth/devstorage.full_control"
                            ],
                        )
                    except (GoogleAuthError, OSError, ValueError) as error:
                        raise RuntimeError(
                            "GCS credentials file could not be loaded"
                        ) from error
                if resolved_credentials is None:
                    from google.auth import default as default_credentials
                    from google.auth.transport.requests import Request
                    from requests import Session

                    resolved_credentials, _ = default_credentials(
                        scopes=[
                            "https://www.googleapis.com/auth/devstorage.full_control"
                        ],
                        request=Request(session=cast(Session, http_session())),
                    )
                try:
                    client_value = cast(
                        GcsClient,
                        storage.Client(
                            project=project,
                            credentials=resolved_credentials,
                            _http=http_session(resolved_credentials),
                        ),
                    )
                except Exception as error:
                    if error.__class__.__name__ in {
                        "DefaultCredentialsError",
                        "GoogleAuthError",
                    }:
                        raise RuntimeError(
                            "GCS authentication failed; "
                            "configure ADC or credentials_file"
                        ) from error
                    raise
            else:
                client_value = client
            self.client = client_value
            self.bucket = client_value.bucket(self.bucket_name)

    def _request[T](
        self,
        name: str,
        request: Callable[[], T],
        *,
        mutation: str | None = None,
        retry: bool = True,
    ) -> T:
        """Retry one SDK operation for at most sixty seconds and five attempts.

        Full jitter grows from one to eight seconds. A shorter caller budget
        also bounds requests, pacing, backoff and lock acquisition.
        """
        from google.api_core.retry import exponential_sleep_generator

        delays = exponential_sleep_generator(initial=1.0, maximum=8.0)
        with limited(_RETRY_SECONDS):
            for attempt in range(1, _RETRY_ATTEMPTS + 1):
                remaining_seconds()
                try:
                    if mutation is None:
                        return request()
                    with _mutation(self.bucket_name, mutation):
                        return request()
                except Exception as error:
                    if not retry or not _retryable(error) or attempt == _RETRY_ATTEMPTS:
                        raise
                    _LOG.warning(
                        "GCS %s failed on attempt %s; "
                        "retrying within the operation budget",
                        name,
                        attempt,
                        exc_info=True,
                    )
                    _pause(next(delays))
        raise AssertionError("unreachable")

    def key(self, relative_name: str) -> str:
        """Return an archive-root-relative name as a bucket object name."""
        relative = validate_relative_name(relative_name, allow_empty=True)
        return f"{self.prefix}/{relative}" if self.prefix else relative

    def relative(self, object_name: str) -> str:
        """Return an object name relative to this archive root."""
        if not self.prefix:
            return validate_relative_name(object_name)
        expected = f"{self.prefix}/"
        if not object_name.startswith(expected):
            raise ValueError(f"object lies outside archive root: {object_name!r}")
        return validate_relative_name(object_name.removeprefix(expected))

    def _object(self, blob: GcsBlob) -> SnapshotObject:
        """Materialize stable metadata from a loaded cloud blob."""
        if blob.generation is None or blob.size is None:
            self._request(
                "object metadata",
                lambda: blob.reload(
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
            )
        if blob.generation is None or blob.size is None:
            raise RuntimeError(f"GCS did not return metadata for {blob.name!r}")
        return SnapshotObject(
            name=self.relative(blob.name),
            version=int(blob.generation),
            size=blob.size,
            checksum=blob.crc32c,
        )

    @staticmethod
    def _raise_precondition(
        error: Exception, *, exact_generation: bool = False
    ) -> None:
        """Convert generation-race errors without coupling to GCS exceptions."""
        if error.__class__.__name__ in {"PreconditionFailed", "Conflict"} or (
            exact_generation and error.__class__.__name__ == "NotFound"
        ):
            raise SnapshotPreconditionError("GCS object generation changed") from error
        raise error

    @bounded
    def read_bytes(self, relative_name: str, *, version: SnapshotVersion) -> bytes:
        """Read one object at its exact published GCS generation."""
        generation = self._generation(version)
        blob = self.bucket.blob(self.key(relative_name), generation=generation)
        try:
            return self._request(
                "read",
                lambda: blob.download_as_bytes(
                    if_generation_match=generation,
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
            )
        except Exception as error:
            self._raise_precondition(error, exact_generation=True)
        raise AssertionError("unreachable")

    @bounded
    def write_bytes(
        self,
        relative_name: str,
        payload: bytes,
        *,
        if_generation_match: int | None = None,
        content_type: str = "application/octet-stream",
    ) -> SnapshotObject:
        """Write an object and return immutable generation metadata."""
        blob = self.bucket.blob(self.key(relative_name))
        try:
            self._request(
                "write",
                lambda: blob.upload_from_string(
                    payload,
                    content_type=content_type,
                    if_generation_match=if_generation_match,
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
                mutation=self.key(relative_name),
                retry=if_generation_match is not None,
            )
            self._request(
                "object metadata",
                lambda: blob.reload(
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
            )
        except Exception as error:
            self._raise_precondition(error)
        return self._object(blob)

    @bounded
    def upload_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Upload a file with create-only generation fencing."""
        blob = self.bucket.blob(self.key(relative_name))
        try:
            self._request(
                "upload",
                lambda: blob.upload_from_filename(
                    str(path),
                    if_generation_match=0,
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
                mutation=self.key(relative_name),
            )
            self._request(
                "object metadata",
                lambda: blob.reload(
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
            )
        except Exception as error:
            self._raise_precondition(error)
        return self._object(blob)

    @bounded
    def download_file(self, relative_name: str, path: Path) -> SnapshotObject:
        """Resolve this location's generation and download that exact revision."""
        blob = self.bucket.blob(self.key(relative_name))
        self._request(
            "object metadata",
            lambda: blob.reload(
                timeout=remaining_seconds(min(30.0, self.timeout_seconds)), retry=None
            ),
        )
        ref = self._object(blob)
        try:
            self._request(
                "download",
                lambda: blob.download_to_filename(
                    str(path),
                    if_generation_match=self._generation(ref.version),
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
            )
        except Exception as error:
            self._raise_precondition(error, exact_generation=True)
        return ref

    @bounded
    def read_json(
        self, relative_name: str
    ) -> tuple[dict[str, object] | None, SnapshotVersion | None]:
        """Read a JSON object and its generation, returning absent for a missing key."""
        blob = self.bucket.blob(self.key(relative_name))
        payload: object = None
        try:
            if not self._request(
                "existence",
                lambda: blob.exists(
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
            ):
                return None, None
            self._request(
                "object metadata",
                lambda: blob.reload(
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
            )
            reference = self._object(blob)
            payload = json.loads(
                self.read_bytes(relative_name, version=reference.version)
            )
        except Exception as error:
            self._raise_precondition(error)
        if not isinstance(payload, dict):
            raise ValueError(f"GCS JSON object {relative_name!r} must be an object")
        if blob.generation is None:
            raise RuntimeError(f"GCS did not return generation for {relative_name!r}")
        return payload, int(blob.generation)

    @bounded
    def write_json_cas(
        self,
        relative_name: str,
        payload: dict[str, object],
        *,
        expected_version: SnapshotVersion | None,
    ) -> SnapshotObject:
        """Atomically create or replace JSON using a GCS generation precondition."""
        return self.write_bytes(
            relative_name,
            (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode(),
            if_generation_match=(
                0 if expected_version is None else self._generation(expected_version)
            ),
            content_type="application/json",
        )

    @staticmethod
    def _generation(version: SnapshotVersion) -> int:
        """Require a numeric generation from a generic snapshot version."""
        if not isinstance(version, int) or isinstance(version, bool):
            raise ValueError("GCS object versions must be integer generations")
        return version

    @bounded
    def list(self, relative_prefix: str) -> tuple[SnapshotObject, ...]:
        """List loaded object metadata under an archive-relative prefix."""
        prefix = self.key(relative_prefix)
        blobs = self._request(
            "list",
            lambda: tuple(
                self.client.list_blobs(
                    self.bucket,
                    prefix=prefix,
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                )
            ),
        )
        return tuple(self._object(blob) for blob in blobs)

    @bounded
    def delete(self, relative_name: str, *, version: SnapshotVersion) -> None:
        """Delete exactly the published generation of an object."""
        try:
            self._request(
                "delete",
                lambda: self.bucket.blob(self.key(relative_name)).delete(
                    if_generation_match=self._generation(version),
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
                mutation=self.key(relative_name),
            )
        except Exception as error:
            self._raise_precondition(error, exact_generation=True)

    @bounded
    def lifecycle_warnings(self) -> tuple[str, ...]:
        """Report lifecycle delete rules that could apply to this archive root."""
        try:
            self._request(
                "bucket metadata",
                lambda: self.bucket.reload(
                    timeout=remaining_seconds(min(30.0, self.timeout_seconds)),
                    retry=None,
                ),
            )
            rules = self.bucket.lifecycle_rules
        except Exception as error:
            _LOG.exception("could not inspect GCS lifecycle rules for %s", self.uri)
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
