# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""factory.py — Lazy snapshot provider construction and destination configuration."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

from usagebassoon.buckets.base import SnapshotBucket, bucket_scheme, bucket_uri

if TYPE_CHECKING:
    from usagebassoon.config import GcsConfig, SnapshotsConfig

type BucketFactory = Callable[[str], SnapshotBucket]


def bucket_label(uri: str) -> str:
    """Describe known and additional providers for archive inspection."""
    scheme = bucket_scheme(uri)
    return {"": "Local", "file": "Local", "gs": "GCS"}.get(scheme, scheme.upper())


def _local_bucket(uri: str) -> SnapshotBucket:
    """Construct the local provider only when requested."""
    from usagebassoon.buckets.local import LocalSnapshotBucket

    return LocalSnapshotBucket(uri)


def _gcs_bucket(uri: str, *, settings: GcsConfig | None = None) -> SnapshotBucket:
    """Construct GCS only when requested, retaining disabled-provider credentials."""
    from usagebassoon.buckets.gcs import GcsSnapshotBucket

    return GcsSnapshotBucket(
        uri,
        project=settings.project or None if settings else None,
        credentials_file=settings.credentials_file if settings else None,
        timeout_seconds=settings.timeout_seconds if settings else 60.0,
    )


@dataclass(frozen=True, slots=True)
class SnapshotDestination:
    """Provider-neutral archive location and publication obligations.

    Attributes:
        uri: Archive root available for explicit recovery.
        enabled: Whether ordinary publication targets this root.
        weekly: Whether publication also maintains weekly recovery points.
    """

    uri: str
    enabled: bool
    weekly: bool


class SnapshotBucketRegistry:
    """Resolve registered URI schemes lazily through the snapshot bucket protocol."""

    def __init__(self, *, factories: Mapping[str, BucketFactory] | None = None) -> None:
        """Register built-in providers and any additional provider factories."""
        self._factories: dict[str, BucketFactory] = {
            "": _local_bucket,
            "file": _local_bucket,
            "gs": _gcs_bucket,
        }
        for scheme, factory in (factories or {}).items():
            self.register(scheme, factory)
        self.destinations: tuple[SnapshotDestination, ...] = ()

    def register(self, scheme: str, factory: BucketFactory) -> None:
        """Register construction for a URI scheme without opening its provider."""
        if ":" in scheme or "/" in scheme or scheme != scheme.strip():
            raise ValueError("bucket provider registration requires a URI scheme")
        self._factories[scheme.lower()] = factory

    def resolve(self, uri: str) -> SnapshotBucket:
        """Construct an adapter or reject an unregistered remote URI explicitly."""
        scheme = bucket_scheme(uri)
        factory = self._factories.get(scheme)
        if factory is None:
            raise ValueError(f"unsupported snapshot bucket URI scheme: {scheme!r}")
        return factory(bucket_uri(uri))

    @classmethod
    def from_settings(cls, settings: SnapshotsConfig) -> SnapshotBucketRegistry:
        """Translate provider configuration at the adapter composition boundary."""
        result = cls(factories={"gs": partial(_gcs_bucket, settings=settings.gcs)})
        destinations = [
            SnapshotDestination(
                bucket_uri(str(settings.local.path)),
                settings.local.enable,
                not settings.local.disable_weekly,
            )
        ]
        if settings.gcs and settings.gcs.uri:
            destinations.append(
                SnapshotDestination(
                    bucket_uri(settings.gcs.uri),
                    settings.gcs.enable,
                    not settings.gcs.disable_weekly,
                )
            )
        result.destinations = tuple(destinations)
        return result
