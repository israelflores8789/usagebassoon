# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""catalog.py — Fenced archive lifecycle, portable pins, and retention policy."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Generator
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
        if (
            type(document.get("version")) is not int
            or document.get("version") != FORMAT_VERSION
        ):
            raise ValueError(
                "archive control metadata is invalid; cannot safely mutate it"
            )
        if "records" in document and not isinstance(document["records"], dict):
            raise ValueError("invalid archive lifecycle records")
        fence = document.get("fence", 0)
        if "archive_id" in document and (
            not isinstance(document["archive_id"], str) or not document["archive_id"]
        ):
            raise ValueError("invalid archive identity")
        if not isinstance(fence, int) or isinstance(fence, bool) or fence < 0:
            raise ValueError("invalid catalog fence")
        reservation = document.get("reservation")
        if reservation is not None:
            if (
                not isinstance(reservation, dict)
                or not isinstance(reservation.get("owner"), str)
                or not reservation["owner"]
                or reservation.get("fence") != fence
                or isinstance(reservation.get("fence"), bool)
            ):
                raise ValueError("invalid archive reservation")
            timestamp(reservation.get("expires_at"))
        policy = document.get("policy")
        if policy is not None and (
            not isinstance(policy, dict)
            or not isinstance(policy.get("max_snapshots"), int)
            or isinstance(policy.get("max_snapshots"), bool)
            or policy["max_snapshots"] < 1
            or policy.get("weekly_slots") != 4
            or type(policy.get("weekly_slots")) is not int
        ):
            raise ValueError("invalid archive retention policy")
        records = document.get("records", {})
        assert isinstance(records, dict)
        for identifier, state in records.items():
            snapshot_id(identifier)
            if not isinstance(state, dict):
                raise ValueError("invalid snapshot lifecycle state")
            for field in ("pinned", "retired", "published", "indexed"):
                if field in state and not isinstance(state[field], bool):
                    raise ValueError(f"invalid lifecycle {field}")
            roles = state.get("roles", [])
            if not isinstance(roles, list) or any(
                not isinstance(role, str)
                or role not in {"manual", "scheduled", "weekly"}
                for role in roles
            ):
                raise ValueError("invalid lifecycle roles")
        return document, version

    def state(self, identifier: str) -> dict[str, object] | None:
        """Read authoritative state, falling back to a relocated portable sidecar."""
        document, _ = self.control()
        records = document.get("records", {})
        if not isinstance(records, dict):
            raise ValueError("invalid archive lifecycle records")
        state = records.get(identifier)
        if state is not None:
            if not isinstance(state, dict):
                raise ValueError("invalid snapshot lifecycle state")
            return dict(state)
        portable = self.bucket.read_json(f"{identifier}/state.json")[0]
        if (
            portable is not None
            and document.get("archive_id") is not None
            and portable.get("archive_id") == document["archive_id"]
        ):
            # An unrecorded publication from this archive cannot regain authority.
            return None
        return portable

    def mutate(self, change: Callable[[dict[str, object]], None]) -> None:
        """Atomically authorize a transition against the reservation's own version."""
        with self._control_lock:
            if self._failure is not None:
                raise ArchiveBusy(
                    "archive reservation renewal failed"
                ) from self._failure
            for _ in range(8):
                document, version = self.control()
                reservation = document.get("reservation")
                if (
                    not isinstance(reservation, dict)
                    or reservation.get("owner") != self.owner
                    or reservation.get("fence") != self.fence
                    or timestamp(reservation.get("expires_at")) <= datetime.now(UTC)
                ):
                    raise ArchiveBusy(
                        f"snapshot reservation was lost: {self.bucket.uri}"
                    )
                change(document)
                try:
                    self.bucket.write_json_cas(
                        CONTROL_NAME, document, expected_version=version
                    )
                except SnapshotPreconditionError:
                    continue
                return
            raise ArchiveBusy("archive control metadata changed repeatedly")

    def transition(self, identifier: str, state: dict[str, object]) -> None:
        """Commit portable lifecycle state under the current fence."""
        snapshot_id(identifier)
        for field in ("pinned", "retired", "published", "indexed"):
            if field in state and not isinstance(state[field], bool):
                raise ValueError(f"invalid lifecycle {field}")
        roles = state.get("roles", [])
        if not isinstance(roles, list) or any(
            not isinstance(role, str) or role not in {"manual", "scheduled", "weekly"}
            for role in roles
        ):
            raise ValueError("invalid lifecycle roles")

        def change(document: dict[str, object]) -> None:
            """Store the state in the same CAS document as mutation authority."""
            records = document.setdefault("records", {})
            if not isinstance(records, dict):
                raise ValueError("invalid archive lifecycle records")
            previous = records.get(identifier)
            if (
                isinstance(previous, dict)
                and previous.get("retired") is True
                and state.get("retired") is not True
            ):
                raise ValueError("retired snapshot identity cannot be republished")
            revision = (
                previous.get("revision", 0)
                if isinstance(previous, dict)
                else state.get("revision", 0)
            )
            if not isinstance(revision, int) or isinstance(revision, bool):
                raise ValueError("invalid lifecycle revision")
            state["revision"] = revision + 1
            state["archive_id"] = document["archive_id"]
            records[identifier] = dict(state)

        self.mutate(change)
        # Read the sidecar version before checking authority so a stale owner
        # cannot authorize old state against a newer projected version.
        try:
            try:
                existing, version = self.bucket.read_json(f"{identifier}/state.json")
            except ValueError:
                _LOG.warning(
                    "Repairing malformed portable state from authoritative lifecycle",
                    exc_info=True,
                )
                reference = next(
                    (
                        obj
                        for obj in self.bucket.list(f"{identifier}/state.json")
                        if obj.name == f"{identifier}/state.json"
                    ),
                    None,
                )
                existing = None
                version = reference.version if reference is not None else None
            self.check()
            if self.state(identifier) != state:
                raise ArchiveBusy("lifecycle changed before portable projection")
            if (
                existing is not None
                and existing.get("revision", 0) == state["revision"]
            ):
                if existing == state:
                    return
                raise ValueError("conflicting portable lifecycle revision")
            self.bucket.write_json_cas(
                f"{identifier}/state.json", state, expected_version=version
            )
        except Exception as error:
            _LOG.exception(
                "Lifecycle committed; sidecar projection failed at %s", self.bucket.uri
            )
            raise RuntimeError(
                "Lifecycle committed, but portable state was not saved; "
                "retry or repair before relocating the snapshot"
            ) from error

    def stage(self, identifier: str, state: dict[str, object]) -> None:
        """Reserve a new identity before creating any immutable objects."""
        snapshot_id(identifier)
        if self.state(identifier) is not None or any(
            obj.name.startswith(identifier + "/")
            for obj in self.bucket.list(identifier)
        ):
            raise ValueError("snapshot identity already exists in this archive")
        self.transition(identifier, {**state, "owner": self.owner, "fence": self.fence})

    def discover(
        self, *, recovery: bool = False, warning: Callable[[str], None] | None = None
    ) -> list[dict[str, object]]:
        """Discover complete non-retired directories without trusting an index."""
        entries: list[dict[str, object]] = []
        for obj in self.bucket.list(""):
            if not obj.name.endswith("/COMPLETE") or obj.name.count("/") != 1:
                continue
            identifier = obj.name.split("/")[0]
            try:
                snapshot_id(identifier)
                state = None if recovery else self.state(identifier)
                if not recovery and (
                    state is None
                    or state.get("retired") is not False
                    or state.get("published") is not True
                    or state.get("indexed") is False
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
                timestamp(manifest.get("captured_at"))
                entries.append(
                    {
                        "snapshot_id": identifier,
                        "captured_at": manifest.get("captured_at"),
                    }
                )
            except (ValueError, OSError, RuntimeError) as error:
                if warning:
                    warning(
                        f"Ignoring incomplete snapshot {identifier} "
                        f"at {self.bucket.uri}: {error}"
                    )
                _LOG.warning(
                    "Ignoring incomplete snapshot %s at %s: %s",
                    identifier,
                    self.bucket.uri,
                    error,
                    exc_info=True,
                )
        return sorted(
            entries,
            key=_entry_order,
        )

    def entries(self) -> list[dict[str, object]]:
        """Return validated discovery entries from the catalog."""
        control, _ = self.control()
        records = control.get("records")
        if isinstance(records, dict):
            published: list[dict[str, object]] = []
            for key, state in records.items():
                snapshot_id(key)
                if not isinstance(state, dict):
                    raise ValueError("invalid snapshot lifecycle state")
                if state.get("published") is True and state.get("retired") is False:
                    if state.get("indexed") is False:
                        continue
                    captured = state.get("captured_at")
                    timestamp(captured)
                    published.append({"snapshot_id": key, "captured_at": captured})
            managed = {str(entry["snapshot_id"]): entry for entry in published}
            for entry in self.discover():
                if entry["snapshot_id"] not in records:
                    managed[str(entry["snapshot_id"])] = entry
            return sorted(
                managed.values(),
                key=_entry_order,
            )
        document, _ = self.read()
        values = document["entries"]
        if not isinstance(values, list):
            raise ValueError("invalid catalog entries")
        for entry in values:
            if not isinstance(entry, dict):
                raise ValueError("invalid catalog entry")
            snapshot_id(entry.get("snapshot_id"))
            timestamp(entry.get("captured_at"))
        return self.discover()

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
                    if not isinstance(previous, int) or isinstance(previous, bool):
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
                document.setdefault("archive_id", uuid4().hex)
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
        """Commit last-attempt evidence with the current mutation authority."""

        def change(document: dict[str, object]) -> None:
            """Record the result without a separate check/write race."""
            evidence: dict[str, object] = {
                "snapshot_id": identifier,
                "at": datetime.now(UTC).isoformat(),
            }
            if error is None:
                document["last_success"] = evidence
                document["last_failure"] = None
            else:
                document["last_failure"] = {**evidence, "error": error}

        self.mutate(change)

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
                    _LOG.warning(
                        "Snapshot reservation renewal failed at %s",
                        self.bucket.uri,
                        exc_info=True,
                    )
                    return

        thread = Thread(target=renew, name="snapshot-reservation", daemon=True)
        thread.start()
        try:
            if enforce_policy:

                def policy_change(document: dict[str, object]) -> None:
                    """Preserve an existing authoritative retention policy."""
                    policy = {"max_snapshots": self.max_snapshots, "weekly_slots": 4}
                    existing = document.get("policy")
                    if existing is not None and existing != policy:
                        raise ValueError(
                            "archive retention policy conflicts; use snapshot policy "
                            "to explicitly reconcile it"
                        )
                    document["policy"] = policy

                self.mutate(policy_change)
            if enforce_policy:
                self.resume_cleanup()
            yield self
            self.check()
        finally:
            stop.set()
            thread.join()
            try:
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
                else:
                    _LOG.warning(
                        "Snapshot reservation release exhausted CAS retries at %s",
                        self.bucket.uri,
                    )
            except Exception:
                _LOG.warning(
                    "Snapshot reservation release failed at %s; claim will expire",
                    self.bucket.uri,
                    exc_info=True,
                )
            finally:
                self.fence = None

    def publish(self, identifier: str, captured_at: str) -> None:
        """Authorize a verified complete snapshot in the fenced control document."""
        self.check()
        state = self.state(identifier)
        if (
            state is None
            or state.get("owner") != self.owner
            or state.get("fence") != self.fence
        ):
            raise ArchiveBusy("cannot publish a snapshot staged by another owner")
        self.transition(
            identifier,
            {
                **state,
                "captured_at": captured_at,
                "published": True,
                "indexed": True,
                "verified_at": datetime.now(UTC).isoformat(),
            },
        )
        self.project_index()

    def project_index(self) -> None:
        """Refresh the derived index without making it an authorization boundary."""
        try:
            _, version = self.read()
            self.bucket.write_json_cas(
                CATALOG_NAME,
                {"version": FORMAT_VERSION, "entries": self.entries()},
                expected_version=version,
            )
        except Exception:
            _LOG.exception(
                "Lifecycle committed; catalog projection requires repair at %s",
                self.bucket.uri,
            )

    def pin(self, identifier: str) -> None:
        """Pin under the same fenced authority as publication and retirement."""
        state = self.state(identifier)
        if state is None or state.get("retired") is not False:
            raise ValueError("snapshot lifecycle state is missing or retired")
        self.transition(identifier, {**state, "pinned": True})

    def retire(self, identifier: str) -> None:
        """Authorize monotonic retirement and cleanup of exactly observed revisions."""
        state = self.state(identifier)
        if state is None or not isinstance(state.get("pinned"), bool):
            raise ValueError(
                "snapshot state is missing or unreadable; refusing deletion"
            )
        cleanup = state.get("cleanup")
        if cleanup is None:
            cleanup = [
                {"name": obj.name, "version": obj.version}
                for obj in self.bucket.list(identifier)
                if obj.name.startswith(identifier + "/")
                and obj.name != f"{identifier}/state.json"
            ]
        if state.get("retired") is not True or state.get("cleanup") is None:
            self.transition(identifier, {**state, "retired": True, "cleanup": cleanup})
            state = self.state(identifier)
            assert state is not None
        self.project_index()
        if not isinstance(cleanup, list):
            raise ValueError("invalid retirement cleanup evidence")
        for ref in cleanup:
            if not isinstance(ref, dict) or not isinstance(ref.get("name"), str):
                raise ValueError("invalid retirement object reference")
            name, version = ref["name"], ref.get("version")
            if not isinstance(version, (str, int)) or isinstance(version, bool):
                raise ValueError("invalid retirement object version")
            if (
                not name.startswith(identifier + "/")
                or name == f"{identifier}/state.json"
            ):
                raise ValueError("invalid retirement object scope")
            self.check()
            existing = next(
                (obj for obj in self.bucket.list(name) if obj.name == name), None
            )
            if existing is not None:
                self.bucket.delete(name, version=version)

        sidecar = f"{identifier}/state.json"
        remaining = [
            obj
            for obj in self.bucket.list(identifier)
            if obj.name.startswith(identifier + "/")
        ]
        if any(obj.name != sidecar for obj in remaining):
            raise SnapshotPreconditionError(
                "unobserved objects remain in retired snapshot"
            )
        for obj in remaining:
            self.check()
            self.bucket.delete(obj.name, version=obj.version)

        def forget(document: dict[str, object]) -> None:
            """Forget only completed cleanup under the current fenced CAS."""
            records = document.get("records", {})
            if not isinstance(records, dict) or records.get(identifier) != state:
                raise ArchiveBusy("retirement changed before cleanup completed")
            if any(
                obj.name.startswith(identifier + "/")
                for obj in self.bucket.list(identifier)
            ):
                raise SnapshotPreconditionError(
                    "retired snapshot cleanup is incomplete"
                )
            del records[identifier]

        self.mutate(forget)
        self.project_index()

    def resume_cleanup(self) -> None:
        """Retry retirements and abandoned stages without blocking independent work."""
        document, _ = self.control()
        records = document.get("records", {})
        if not isinstance(records, dict):
            raise ValueError("invalid archive lifecycle records")
        for identifier, state in records.items():
            if isinstance(state, dict):
                try:
                    fence = state.get("fence")
                    abandoned = (
                        state.get("published") is False
                        and type(fence) is int
                        and self.fence is not None
                        and fence < self.fence
                        and not any(
                            obj.name == f"{identifier}/COMPLETE"
                            for obj in self.bucket.list(f"{identifier}/COMPLETE")
                        )
                    )
                    if state.get("retired") is not True and not abandoned:
                        continue
                    self.retire(str(identifier))
                except Exception:
                    _LOG.exception("Snapshot cleanup remains pending: %s", identifier)

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
                state = self.state(identifier)
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
        """Verify immutable candidates and rebuild the index before optional cleanup."""
        from pathlib import Path
        from tempfile import TemporaryDirectory

        from usagebassoon.snapshot.reader import Candidate, SnapshotReader
        from usagebassoon.storage_model import SNAPSHOT_TABLES

        self.check()
        references = self.bucket.list(CATALOG_NAME)
        current = next((obj for obj in references if obj.name == CATALOG_NAME), None)
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
        verified: list[dict[str, object]] = []
        repair_warnings: list[str] = []
        for entry in self.discover(recovery=True, warning=repair_warnings.append):
            self.check()
            identifier = str(entry["snapshot_id"])
            try:
                state = self.state(identifier)
            except (ValueError, OSError, RuntimeError):
                state = None
                _LOG.warning(
                    "Preserving uncertain pin state during repair: %s", identifier
                )
            if state is None or (
                state.get("retired") is not False or state.get("published") is not True
            ):
                repair_warnings.append(
                    f"{identifier}: no published lifecycle authority; "
                    "available for immutable recovery"
                )
                continue
            candidate = Candidate(self.bucket, identifier, str(entry["captured_at"]))
            try:
                with TemporaryDirectory(prefix="usagebassoon-repair-") as directory:
                    SnapshotReader((self.bucket,), lambda _uri: self.bucket).download(
                        candidate,
                        Path(directory),
                        SNAPSHOT_TABLES,
                    )
            except (ValueError, OSError, RuntimeError) as error:
                _LOG.exception(
                    "Excluding damaged snapshot from repaired index: %s", candidate.uri
                )
                repair_warnings.append(f"{candidate.uri}: {error}")
                continue
            verified.append(entry)
            self.transition(
                identifier,
                {**state, "captured_at": entry["captured_at"], "indexed": True},
            )
        verified_ids = {str(entry["snapshot_id"]) for entry in verified}

        def mark_indexed(document: dict[str, object]) -> None:
            """Keep invalid recovery points out of routine management after repair."""
            records = document.get("records", {})
            if not isinstance(records, dict):
                raise ValueError("invalid archive lifecycle records")
            for identifier, state in records.items():
                if isinstance(state, dict) and state.get("published") is True:
                    state["indexed"] = identifier in verified_ids
            document["repair_warnings"] = repair_warnings

        self.mutate(mark_indexed)
        self.check()
        _, version = (
            self.bucket.read_json(CATALOG_NAME)
            if current is None
            else (None, current.version)
        )
        self.bucket.write_json_cas(
            CATALOG_NAME,
            {"version": FORMAT_VERSION, "entries": verified},
            expected_version=version,
        )

    def cleanup_abandoned(self) -> None:
        """Retire older owned stages only when no complete recovery unit survives."""
        document, _ = self.control()
        records = document.get("records", {})
        if not isinstance(records, dict):
            raise ValueError("invalid archive lifecycle records")
        for identifier, state in records.items():
            if not isinstance(state, dict):
                continue
            if state.get("retired") is True:
                self.retire(str(identifier))
                continue
            fence = state.get("fence")
            if (
                state.get("published") is False
                and isinstance(fence, int)
                and self.fence is not None
                and fence < self.fence
                and not any(
                    obj.name == f"{identifier}/COMPLETE"
                    for obj in self.bucket.list(f"{identifier}/COMPLETE")
                )
            ):
                self.retire(str(identifier))


def _entry_order(entry: dict[str, object]) -> tuple[datetime, str]:
    """Order catalog entries by immutable capture point."""
    return timestamp(entry.get("captured_at")), str(entry["snapshot_id"])
