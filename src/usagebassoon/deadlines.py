# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""deadlines.py — Shared monotonic operation and bounded cleanup budgets.

Remote operations reuse an enclosing budget, including concurrent publication.
Collection retries start a new budget; their backoff is outside it. Provider
retries consume the enclosing budget. Cleanup shares at most fifteen additional
seconds. Snapshot capture and restore use separate, longer scopes.
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from math import isfinite
from threading import Lock
from time import monotonic
from typing import Protocol, cast, override

CLEANUP_SECONDS = 15.0
CONNECTION_SECONDS = 10.0
REQUEST_SECONDS = 30.0
SNAPSHOT_SECONDS = 600.0
RESTORE_SECONDS = 600.0


class OperationTimeout(TimeoutError, RuntimeError):
    """Completion is uncertain after an operation exhausts its shared budget."""


class Deadline:
    """Track elapsed time and share one cleanup allowance across workers."""

    def __init__(self, seconds: float, *, parent: Deadline | None = None) -> None:
        """Start a positive, finite monotonic budget."""
        if not isfinite(seconds) or seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        self.expires = monotonic() + seconds
        self._parent = parent
        self._cleanup: Deadline | None = None
        self._lock = Lock()

    def remaining(self, cap: float | None = None) -> float:
        """Return the remaining allowance, failing before further work starts."""
        remaining = self.expires - monotonic()
        if remaining <= 0:
            raise OperationTimeout(
                "operation deadline exceeded; completion may be uncertain"
            )
        return remaining if cap is None else min(cap, remaining)

    def cleanup(self) -> Deadline:
        """Allocate one shared cleanup grace, never one allowance per object."""
        if self._parent is not None:
            return self._parent.cleanup()
        with self._lock:
            if self._cleanup is None:
                self._cleanup = Deadline(CLEANUP_SECONDS)
                self._cleanup._cleanup = self._cleanup
            return self._cleanup


_CURRENT: ContextVar[Deadline | None] = ContextVar(
    "usagebassoon_deadline", default=None
)


def current_deadline() -> Deadline | None:
    """Return the operation scope inherited by provider calls."""
    return _CURRENT.get()


def remaining_seconds(cap: float | None = REQUEST_SECONDS) -> float:
    """Return a capped allowance, or the whole operation allowance for ``None``."""
    deadline = current_deadline()
    if deadline is None:
        return REQUEST_SECONDS if cap is None else cap
    return deadline.remaining(cap)


@contextmanager
def operation(seconds: float | None) -> Generator[Deadline | None]:
    """Reuse an enclosing deadline or establish one for this logical operation."""
    existing = current_deadline()
    deadline = existing or (Deadline(seconds) if seconds is not None else None)
    token = _CURRENT.set(deadline)
    try:
        if deadline is not None:
            deadline.remaining()
        yield deadline
        if deadline is not None:
            deadline.remaining()
    finally:
        _CURRENT.reset(token)


@contextmanager
def cleanup_budget() -> Generator[None]:
    """Bound failure cleanup independently of an exhausted foreground budget."""
    parent = current_deadline()
    token = _CURRENT.set(parent.cleanup() if parent else Deadline(CLEANUP_SECONDS))
    try:
        yield
    finally:
        _CURRENT.reset(token)


@contextmanager
def limited(seconds: float) -> Generator[Deadline]:
    """Tighten an enclosing budget while retaining its shared cleanup allowance."""
    if seconds <= 0:
        raise OperationTimeout("operation authority or retry budget expired")
    parent = current_deadline()
    if parent is not None:
        seconds = min(seconds, parent.remaining())
    deadline = Deadline(seconds, parent=parent)
    if parent is not None:
        deadline.expires = min(deadline.expires, parent.expires)
    token = _CURRENT.set(deadline)
    try:
        yield deadline
        deadline.remaining()
    finally:
        _CURRENT.reset(token)


class _TimeoutOwner(Protocol):
    """Expose the configured budget to synchronous operation wrappers."""

    timeout_seconds: float


def bounded[**P, T](method: Callable[P, T]) -> Callable[P, T]:
    """Scope a synchronous provider method to its owner's operation budget."""

    @wraps(method)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        with operation(cast(_TimeoutOwner, args[0]).timeout_seconds):
            return method(*args, **kwargs)

    return wrapped


def checked[**P, T](method: Callable[P, T]) -> Callable[P, T]:
    """Check an enclosing deadline before and after bounded local work."""

    @wraps(method)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        remaining_seconds()
        result = method(*args, **kwargs)
        remaining_seconds()
        return result

    return wrapped


def http_session(credentials: object | None = None) -> object:
    """Build a Google HTTP transport that checks every request and upload chunk.

    Imports remain lazy so local installations need no cloud dependencies.
    SDK retries also receive explicit limits at the adapter boundary. Applying
    this below the SDK covers resumable transfers, pagination and auth refresh.
    """
    from google.auth.credentials import Credentials
    from google.auth.transport.requests import AuthorizedSession, Request
    from requests import Session
    from requests.adapters import HTTPAdapter
    from requests.exceptions import Timeout as RequestTimeout
    from requests.models import PreparedRequest, Response

    class DeadlineAdapter(HTTPAdapter):
        """Cap connect/read waits anew at each underlying HTTP send."""

        @override
        def send(
            self,
            request: PreparedRequest,
            stream: bool = False,
            timeout: object = None,
            verify: bool | str = True,
            cert: bytes | str | tuple[bytes | str, bytes | str] | None = None,
            proxies: Mapping[str, str] | None = None,
        ) -> Response:
            """Prevent SDK chunks and retries from receiving fresh full budgets."""
            del timeout
            budget = remaining_seconds(40.0)
            try:
                return super().send(
                    request,
                    stream=stream,
                    timeout=(
                        min(CONNECTION_SECONDS, budget / 4),
                        min(REQUEST_SECONDS, budget * 3 / 4),
                    ),
                    verify=verify,
                    cert=cert,
                    proxies=proxies,
                )
            except RequestTimeout as error:
                raise OperationTimeout(
                    "remote request timed out; completion may be uncertain"
                ) from error

    auth = Session()
    auth.mount("https://", DeadlineAdapter())
    auth.mount("http://", DeadlineAdapter())
    if credentials is None:
        return auth
    session = AuthorizedSession(
        cast(Credentials, credentials),
        auth_request=Request(session=auth),
        refresh_timeout=CONNECTION_SECONDS,
    )
    session.mount("https://", DeadlineAdapter())
    session.mount("http://", DeadlineAdapter())
    return session
