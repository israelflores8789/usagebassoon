# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""catalog.py — Fenced archive lifecycle, portable pins, and retention policy."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import Event, RLock, Thread
from uuid import uuid4

from usagebassoon.buckets.base import (
    SnapshotBucket,
    SnapshotPreconditionError,
    SnapshotVersion,
)
from usagebassoon.snapshot.format import (
    CATALOG_NAME,
    FORMAT_VERSION,
    LEASE_SECONDS,
    snapshot_id,
    timestamp,
)

_LOG = logging.getLogger("usagebassoon")
CONTROL_NAME = "control.json"


class ArchiveBusy(RuntimeError):
    """An archive operation owns the current fenced reservation."""


class Catalog:
    """Coordinate mutations and downloads at one archive destination."""

    def __init__(self, bucket: SnapshotBucket, max_snapshots: int = 3) -> None:
        """Bind an archive and the requested shared retention ceiling."""
        self.bucket = bucket
        self.max_snapshots = max_snapshots
        self.owner = uuid4().hex
        self.fence: int | None = None
        self._failure: Exception | None = None
        self._control_lock = RLock()

    def read(self) -> tuple[dict[str, object], SnapshotVersion | None]:
        """Read the derived index, discovering complete directories when missing."""
        document, version = self.bucket.read_json(CATALOG_NAME)
        if document is None:
            return {
                "version": FORMAT_VERSION,
                "entries": self.discover(),
            }, None
        if document.get("version") != FORMAT_VERSION or not isinstance(
            document.get("entries"), list
        ):
            raise ValueError(
                "invalid archive catalog; preserve it and run snapshot repair"
            )
        return document, version

    def control(self) -> tuple[dict[str, object], SnapshotVersion | None]:
        """Read authoritative reservations and policy independently of the index."""
        document, version = self.bucket.read_json(CONTROL_NAME)
        if document is None:
            return {"version": FORMAT_VERSION, "reservation": None, "fence": 0}, None
        if document.get("version") != FORMAT_VERSION:
            raise ValueError(
                "archive control metadata is invalid; cannot safely mutate it"
            )
        return document, version

    def discover(self) -> list[dict[str, object]]:
        """Discover complete non-retired directories without trusting an index."""
        entries: list[dict[str, object]] = []
        for obj in self.bucket.list(""):
            if not obj.name.endswith("/COMPLETE") or obj.name.count("/") != 1:
                continue
            identifier = snapshot_id(obj.name.split("/")[0])
            try:
                state, _ = self.bucket.read_json(f"{identifier}/state.json")
                if state is not None and (
                    state.get("retired") is True or state.get("published") is False
                ):
                    continue
                complete, _ = self.bucket.read_json(obj.name)
                manifest, _ = self.bucket.read_json(f"{identifier}/manifest.json")
                if complete is None or manifest is None:
                    continue
                # JSON bytes are immutable; verify their actual stored representation.
                refs = self.bucket.list(f"{identifier}/manifest.json")
                ref = next(
                    (r for r in refs if r.name == f"{identifier}/manifest.json"), None
                )
                if ref is None:
                    continue
                raw = self.bucket.read_bytes(ref.name, version=ref.version)
                if complete.get("manifest_sha256") != hashlib.sha256(raw).hexdigest():
                    raise ValueError("completion digest does not match manifest")
                if (
                    complete.get("snapshot_id") != identifier
                    or manifest.get("snapshot_id") != identifier
                ):
                    raise ValueError("completion identity does not match")
                entries.append(
                    {
                        "snapshot_id": identifier,
                        "captured_at": manifest.get("captured_at"),
                    }
                )
            except (ValueError, OSError, RuntimeError) as error:
                _LOG.warning(
                    "Ignoring incomplete snapshot %s at %s: %s",
                    identifier,
                    self.bucket.uri,
                    error,
                )
        return sorted(
            entries,
            key=_entry_order,
        )

    def entries(self) -> list[dict[str, object]]:
        """Return validated discovery entries from the catalog."""
        document, _ = self.read()
        values = document["entries"]
        if not isinstance(values, list):
            raise ValueError("invalid catalog entries")
        result: list[dict[str, object]] = []
        for entry in values:
            if not isinstance(entry, dict):
                raise ValueError("invalid catalog entry")
            snapshot_id(entry.get("snapshot_id"))
            timestamp(entry.get("captured_at"))
            state, _ = self.bucket.read_json(f"{entry['snapshot_id']}/state.json")
            if state is not None and (
                state.get("retired") is True or state.get("published") is False
            ):
                continue
            result.append(entry)
        return result

    def _update_reservation(self, *, claim: bool = False) -> None:
        """Claim or renew a live reservation using compare-and-swap."""
        with self._control_lock:
            for _ in range(8):
                document, version = self.control()
                now = datetime.now(UTC)
                reservation = document.get("reservation")
                if claim:
                    if (
                        isinstance(reservation, dict)
                        and timestamp(reservation.get("expires_at")) > now
                    ):
                        raise ArchiveBusy(
                            f"snapshot archive is reserved: {self.bucket.uri}"
                        )
                    previous = document.get("fence", 0)
                    if not isinstance(previous, int):
                        raise ValueError("invalid catalog fence")
                    self.fence = previous + 1
                elif (
                    not isinstance(reservation, dict)
                    or reservation.get("owner") != self.owner
                    or reservation.get("fence") != self.fence
                    or timestamp(reservation.get("expires_at")) <= now
                ):
                    raise ArchiveBusy(
                        f"snapshot reservation was lost: {self.bucket.uri}"
                    )
                document["fence"] = self.fence
                document["reservation"] = {
                    "owner": self.owner,
                    "fence": self.fence,
                    "expires_at": (now + timedelta(seconds=LEASE_SECONDS)).isoformat(),
                }
                try:
                    self.bucket.write_json_cas(
                        CONTROL_NAME, document, expected_version=version
                    )
                except SnapshotPreconditionError:
                    continue
                return
            raise ArchiveBusy("archive control metadata changed repeatedly")

    def check(self) -> None:
        """Verify ownership before publishing completion or retiring objects."""
        if self._failure is not None:
            raise ArchiveBusy("archive reservation renewal failed") from self._failure
        self._update_reservation()

    def record_outcome(self, identifier: str, *, error: str | None = None) -> None:
        """Keep inspectable last-attempt evidence under the destination reservation."""
        with self._control_lock:
            self.check()
            document, version = self.control()
            evidence: dict[str, object] = {
                "snapshot_id": identifier,
                "at": datetime.now(UTC).isoformat(),
            }
            if error is None:
                document["last_success"] = evidence
                document["last_failure"] = None
            else:
                document["last_failure"] = {**evidence, "error": error}
            self.bucket.write_json_cas(CONTROL_NAME, document, expected_version=version)

    @contextmanager
    def hold(self, *, enforce_policy: bool = False) -> Generator[Catalog]:
        """Keep a fenced reservation alive through a complete archive operation.

        Yields:
            This catalog while its reservation remains owned.
        """
        self._update_reservation(claim=True)
        stop = Event()

        def renew() -> None:
            """Renew while foreground work holds the archive."""
            while not stop.wait(LEASE_SECONDS / 3):
                try:
                    with self._control_lock:
                        self._update_reservation()
                except Exception as error:
                    self._failure = error
                    return

        thread = Thread(target=renew, name="snapshot-reservation", daemon=True)
        thread.start()
        try:
            if enforce_policy:
                document, version = self.control()
                policy = {"max_snapshots": self.max_snapshots, "weekly_slots": 4}
                existing = document.get("policy")
                if existing is not None and existing != policy:
                    raise ValueError(
                        "archive retention policy conflicts; use snapshot policy "
                        "to explicitly reconcile it"
                    )
                if existing is None:
                    document["policy"] = policy
                    self.bucket.write_json_cas(
                        CONTROL_NAME, document, expected_version=version
                    )
            yield self
            self.check()
        finally:
            stop.set()
            thread.join()
            for _ in range(8):
                document, version = self.control()
                reservation = document.get("reservation")
                if (
                    not isinstance(reservation, dict)
                    or reservation.get("owner") != self.owner
                    or reservation.get("fence") != self.fence
                ):
                    break
                document["reservation"] = None
                try:
                    self.bucket.write_json_cas(
                        CONTROL_NAME, document, expected_version=version
                    )
                except SnapshotPreconditionError:
                    continue
                break
            self.fence = None

    def publish(self, identifier: str, captured_at: str) -> None:
        """Index an independently complete snapshot under the held reservation."""
        with self._control_lock:
            self.check()
            document, version = self.read()
            entries = self.entries()
            if any(e["snapshot_id"] == identifier for e in entries):
                raise ValueError("snapshot identity already exists in this archive")
            new_entry: dict[str, object] = {
                "snapshot_id": identifier,
                "captured_at": captured_at,
            }
            document["entries"] = sorted(
                [*entries, new_entry],
                key=_entry_order,
            )
            self.bucket.write_json_cas(CATALOG_NAME, document, expected_version=version)

    def pin(self, identifier: str) -> None:
        """Pin immutable contents by updating only portable lifecycle metadata."""
        with self._control_lock:
            self.check()
            state, version = self.bucket.read_json(f"{identifier}/state.json")
            if state is None or state.get("retired") is not False:
                raise ValueError("snapshot lifecycle state is missing or retired")
            self.bucket.write_json_cas(
                f"{identifier}/state.json",
                {**state, "pinned": True},
                expected_version=version,
            )

    def retire(self, identifier: str) -> None:
        """Persist retirement before removing exact versions, keeping its tombstone."""
        with self._control_lock:
            self.check()
            state, version = self.bucket.read_json(f"{identifier}/state.json")
            if state is None or not isinstance(state.get("pinned"), bool):
                raise ValueError(
                    "snapshot state is missing or unreadable; refusing deletion"
                )
            self.bucket.write_json_cas(
                f"{identifier}/state.json",
                {**state, "retired": True},
                expected_version=version,
            )
            document, version = self.read()
            document["entries"] = [
                e for e in self.entries() if e["snapshot_id"] != identifier
            ]
            self.bucket.write_json_cas(CATALOG_NAME, document, expected_version=version)
            for obj in self.bucket.list(identifier):
                if (
                    obj.name.startswith(identifier + "/")
                    and obj.name != f"{identifier}/state.json"
                ):
                    self.check()
                    self.bucket.delete(obj.name, version=obj.version)

    def rotate(self) -> None:
        """Retain pins, scheduled count, and four distinct successful UTC weeks."""
        with self._control_lock:
            self.check()
            entries = sorted(
                self.entries(),
                key=_entry_order,
                reverse=True,
            )
            kept_scheduled = 0
            weeks: set[str] = set()
            for entry in entries:
                identifier = str(entry["snapshot_id"])
                state, _ = self.bucket.read_json(f"{identifier}/state.json")
                if (
                    state is None
                    or state.get("retired") is not False
                    or not isinstance(state.get("pinned"), bool)
                ):
                    _LOG.warning(
                        "Preserving snapshot with uncertain lifecycle state: %s",
                        identifier,
                    )
                    continue
                if state["pinned"]:
                    continue
                roles = state.get("roles", [])
                keep = False
                if isinstance(roles, list) and (
                    "scheduled" in roles or "manual" in roles
                ):
                    kept_scheduled += 1
                    keep = kept_scheduled <= self.max_snapshots
                week = state.get("weekly_slot")
                if (
                    isinstance(roles, list)
                    and "weekly" in roles
                    and isinstance(week, str)
                    and week not in weeks
                    and len(weeks) < 4
                ):
                    weeks.add(week)
                    keep = True
                if keep:
                    continue
                self.retire(identifier)

    def repair(self) -> None:
        """Rebuild a missing index while preserving reservations and policy."""
        with self._control_lock:
            self.check()
            for obj in self.bucket.list(""):
                if obj.name.count("/") != 1 or not obj.name.endswith("/state.json"):
                    continue
                state, _ = self.bucket.read_json(obj.name)
                if state is None:
                    continue
                fence = state.get("fence")
                if (
                    state.get("kind") == "snapshot_stage"
                    and state.get("published") is False
                    and isinstance(fence, int)
                    and not isinstance(fence, bool)
                    and self.fence is not None
                    and fence < self.fence
                    and isinstance(state.get("owner"), str)
                ):
                    self.retire(snapshot_id(obj.name.split("/")[0]))
            references = self.bucket.list(CATALOG_NAME)
            current = next(
                (obj for obj in references if obj.name == CATALOG_NAME), None
            )
            version = current.version if current is not None else None
            if current is not None:
                raw = self.bucket.read_bytes(current.name, version=current.version)
                try:
                    self.read()
                except ValueError:
                    self.bucket.write_bytes(
                        f"catalog.corrupt-{uuid4().hex}.json",
                        raw,
                        content_type="application/json",
                    )
            document: dict[str, object] = {
                "version": FORMAT_VERSION,
                "entries": self.discover(),
            }
            self.bucket.write_json_cas(CATALOG_NAME, document, expected_version=version)


def _entry_order(entry: dict[str, object]) -> tuple[datetime, str]:
    """Order catalog entries by immutable capture point."""
    return timestamp(entry.get("captured_at")), str(entry["snapshot_id"])
