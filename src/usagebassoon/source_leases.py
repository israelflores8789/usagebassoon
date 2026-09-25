# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""source_leases.py — Coordinate source collection leases across processes."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Generator
from contextlib import contextmanager
from threading import Event, Thread
from uuid import uuid4

from usagebassoon.backends.base import (
    SOURCE_LEASE_SECONDS,
    SourceLeaseBusy,
    SourceLeaseLost,
    SourceLeaseToken,
    StorageBackend,
    close_backend,
)
from usagebassoon.config import UsageBassoonConfig, open_backend

_RENEW_SECONDS = SOURCE_LEASE_SECONDS / 4
_RETRY_SECONDS = 5.0


class _SourceLeaseHeartbeat:
    """Keep a source lease live while tokscale and persistence run."""

    def __init__(
        self,
        config: UsageBassoonConfig,
        token: SourceLeaseToken,
        logger: logging.Logger,
    ) -> None:
        """Prepare renewal on independent backend connections."""
        self._config = config
        self._token = token
        self._logger = logger
        self._stop = Event()
        self._failure: Exception | None = None
        self._last_renewed = time.monotonic()
        self._thread = Thread(target=self._run, name="source-lease", daemon=True)

    def _run(self) -> None:
        """Renew until stopped, lost, or past the last confirmed expiry."""
        delay = _RENEW_SECONDS
        while not self._stop.wait(delay):
            backend: StorageBackend | None = None
            try:
                backend = open_backend(self._config)
                if not backend.renew_source_lease(self._token):
                    self._failure = SourceLeaseLost(
                        f"source lease was lost for {self._token.source_id}"
                    )
                    self._stop.set()
                    return
                self._last_renewed = time.monotonic()
                delay = _RENEW_SECONDS
            except Exception:
                self._logger.exception(
                    "could not renew source lease for %s", self._token.source_id
                )
                if time.monotonic() - self._last_renewed >= SOURCE_LEASE_SECONDS:
                    self._failure = SourceLeaseLost(
                        f"source lease renewal expired for {self._token.source_id}"
                    )
                    self._stop.set()
                    return
                delay = _RETRY_SECONDS
            finally:
                if backend is not None:
                    close_backend(
                        backend,
                        context=f"source lease renewal for {self._token.source_id}",
                        logger=self._logger,
                    )

    def start(self) -> None:
        """Start background renewal after the source claim succeeds."""
        self._thread.start()

    def check(self) -> None:
        """Reject a collection whose lease is known to be lost."""
        if self._failure is not None:
            raise self._failure
        if time.monotonic() - self._last_renewed >= SOURCE_LEASE_SECONDS:
            raise SourceLeaseLost(
                f"source lease renewal expired for {self._token.source_id}"
            )

    def stop(self) -> None:
        """Stop renewal before releasing an owned lease."""
        self._stop.set()
        self._thread.join()


class ActiveSourceLease:
    """Collection-visible token and heartbeat state."""

    def __init__(
        self, token: SourceLeaseToken, heartbeat: _SourceLeaseHeartbeat
    ) -> None:
        """Keep one claimed token with its renewal worker."""
        self.token = token
        self._heartbeat = heartbeat

    def check(self) -> None:
        """Raise when the current collection can no longer persist safely."""
        self._heartbeat.check()


@contextmanager
def source_lease(
    config: UsageBassoonConfig,
    run_id: str,
    logger: logging.Logger,
) -> Generator[ActiveSourceLease]:
    """Hold one fenced source lease through collection and persistence.

    Args:
        config: Configured warehouse and logical source namespace.
        run_id: Collection run that owns the claim.
        logger: Operational logger for renewal and cleanup failures.

    Yields:
        The acquired lease and its loss check.

    Raises:
        SourceLeaseBusy: Another live collection owns the source.
    """
    backend: StorageBackend | None = None
    token: SourceLeaseToken | None = None
    owner_id = str(uuid4())
    try:
        backend = open_backend(config)
        backend.apply_ddl()
        attempts = config.collection.max_retries + 1
        for attempt in range(attempts):
            try:
                backend.ensure_source_lease(config.source_id)
                token = backend.claim_source_lease(config.source_id, run_id, owner_id)
                break
            except Exception as error:
                if attempt + 1 == attempts or not backend.is_retryable_error(error):
                    raise
                delay = random.uniform(
                    0, min(30.0, config.collection.retry_initial_seconds * 2**attempt)
                )
                logger.warning(
                    "source lease claim conflicted for %s; retrying in %.1fs",
                    config.source_id,
                    delay,
                )
                time.sleep(delay)
        if token is None:
            raise SourceLeaseBusy(
                f"source {config.source_id} already has an active collection"
            )
    finally:
        if backend is not None:
            close_backend(backend, context="source lease claim", logger=logger)

    heartbeat = _SourceLeaseHeartbeat(config, token, logger)
    started = False
    try:
        heartbeat.start()
        started = True
        yield ActiveSourceLease(token, heartbeat)
    finally:
        if started:
            heartbeat.stop()
        release_backend: StorageBackend | None = None
        try:
            release_backend = open_backend(config)
            release_backend.release_source_lease(token)
        except Exception:
            logger.exception("could not release source lease for %s", config.source_id)
        finally:
            if release_backend is not None:
                close_backend(
                    release_backend,
                    context="source lease release",
                    logger=logger,
                )
