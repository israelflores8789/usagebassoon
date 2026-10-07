# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""deadlines.py — Shared monotonic operation and bounded cleanup budgets.

Remote operations reuse an enclosing budget, including concurrent publication.
Collection retries start a new budget; their backoff is outside it. Provider
retries consume the enclosing budget. Cleanup shares at most fifteen additional
seconds. Snapshot capture and restore use separate, longer scopes.
"""

from __future__ import annotations

import io
import logging
import os
import pickle
import subprocess
import sys
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import partial, wraps
from math import isfinite
from threading import Event, Lock, Thread, Timer
from time import monotonic
from types import BuiltinMethodType, MethodType, MethodWrapperType, ModuleType
from typing import TYPE_CHECKING, Protocol, cast, override

if TYPE_CHECKING:
    from usagebassoon.config import LoggingConfig

CLEANUP_SECONDS = 15.0
CONNECTION_SECONDS = 10.0
REQUEST_SECONDS = 30.0
SNAPSHOT_SECONDS = 600.0
RESTORE_SECONDS = 600.0

_LOG = logging.getLogger("usagebassoon")


class OperationTimeout(TimeoutError, RuntimeError):
    """Completion is uncertain after an operation exhausts its shared budget."""


class ResourceUnavailable(OperationTimeout):
    """A terminated or unsafe child resource and all its scopes are invalid."""


@dataclass(frozen=True, slots=True)
class _ResourceStart[R]:
    """Construct a resource privately; only its readiness crosses the pipe."""

    construct: Callable[[], R]
    interrupt: Callable[[R], None] | None
    close: Callable[[R], None]
    expires: float
    logging: LoggingConfig | None
    logging_disabled: bool


@dataclass(frozen=True, slots=True)
class _ResourceCall[R, A, T]:
    """One explicit operation on the same child-owned resource."""

    execute: Callable[[R, A], T] | None
    argument: A
    expires: float
    cleanup: bool


@dataclass(frozen=True, slots=True)
class _ResourceReply[T]:
    """A completed request, including cancellation and resource validity."""

    value: T | None = None
    error: BaseException | None = None
    expired: bool = False
    invalid: bool = False


class SupervisedResource[R]:
    """Own a persistent child and exchange typed operations under shared deadlines.

    Constructors and operations must be trusted importable callables. Blocking
    pipe I/O runs on a daemon thread; the owner bounds that wait and kills and
    reaps the child before returning an uncertain outcome. No resource handle
    crosses the process boundary, and a killed resource is never reconnected.
    """

    def __init__(
        self,
        construct: Callable[[], R],
        *,
        interrupt: Callable[[R], None] | None,
        close: Callable[[R], None],
    ) -> None:
        """Start and await readiness inside the caller's operation budget."""
        from usagebassoon.logger import current_settings

        deadline = current_deadline()
        if deadline is None:
            raise RuntimeError("supervised resources require an operation deadline")
        self._lock = Lock()
        self._closed = False
        self._process: subprocess.Popen[bytes] = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from usagebassoon.deadlines import _resource_worker; "
                "_resource_worker()",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            bufsize=0,
        )
        assert isinstance(self._process.stdin, io.FileIO)
        assert isinstance(self._process.stdout, io.FileIO)
        self._writer = io.BufferedWriter(self._process.stdin)
        self._reader = io.BufferedReader(self._process.stdout)
        try:
            self._exchange(
                _ResourceStart(
                    construct,
                    interrupt,
                    close,
                    deadline.expires,
                    current_settings(),
                    _LOG.disabled,
                ),
                deadline,
            )
        except BaseException:
            self._terminate(deadline)
            raise

    def call[A, T](self, execute: Callable[[R, A], T], argument: A) -> T:
        """Execute an importable, unbound operation with the child resource first."""
        candidate: object = execute
        while isinstance(candidate, partial):
            candidate = candidate.func
        if isinstance(candidate, (MethodType, BuiltinMethodType, MethodWrapperType)):
            owner = candidate.__self__
            if owner is not None and not isinstance(owner, ModuleType):
                raise TypeError(
                    "child callables must be unbound; the child supplies the resource"
                )
        deadline = current_deadline()
        if deadline is None:
            raise RuntimeError("supervised calls require an operation deadline")
        deadline.remaining()
        if self._closed:
            raise ResourceUnavailable(
                "child resource is closed; completion may be uncertain"
            )
        request = _ResourceCall(
            execute, argument, deadline.expires, deadline._cleanup is deadline
        )
        return cast(T, self._exchange(request, deadline))

    def close(self) -> None:
        """Destroy and reap the child within the caller's cleanup allowance."""
        if self._closed:
            return
        deadline = current_deadline()
        if deadline is None:
            raise RuntimeError("supervised close requires a cleanup deadline")
        try:
            self._exchange(
                _ResourceCall[R, None, None](None, None, deadline.expires, True),
                deadline,
            )
            self._process.wait(timeout=deadline.remaining())
        finally:
            self._terminate(deadline)

    def _exchange(self, request: object, deadline: Deadline) -> object:
        """Bound both sending and receiving, including incomplete reply frames."""
        cleanup_end = (
            deadline.expires
            if deadline._cleanup is deadline
            else deadline.expires + CLEANUP_SECONDS
        )
        reserve = min(1.0, CLEANUP_SECONDS / 2)
        wait_end = cleanup_end - reserve
        if not self._lock.acquire(timeout=max(0.0, wait_end - monotonic())):
            raise OperationTimeout(
                "resource operation could not acquire its connection before deadline"
            )
        completed = Event()
        replies: list[_ResourceReply[object]] = []
        failures: list[BaseException] = []

        def exchange() -> None:
            """Use standard pickle frames on the private unbuffered pipes."""
            try:
                assert (
                    self._process.stdin is not None and self._process.stdout is not None
                )
                pickle.dump(request, self._writer)
                self._writer.flush()
                replies.append(cast(_ResourceReply[object], pickle.load(self._reader)))
            except BaseException as error:
                failures.append(error)
            finally:
                completed.set()

        try:
            if self._closed:
                raise ResourceUnavailable(
                    "child resource is closed; completion may be uncertain"
                )
            thread = Thread(
                target=exchange, daemon=True, name="usagebassoon-resource-io"
            )
            thread.start()
            if not completed.wait(max(0.0, wait_end - monotonic())):
                _LOG.warning("terminating stalled resource; completion is uncertain")
                self._terminate(deadline)
                thread.join(timeout=max(0.0, cleanup_end - monotonic()))
                raise ResourceUnavailable(
                    "resource did not stop within cleanup grace; "
                    "completion may be uncertain"
                )
            if failures:
                self._terminate(deadline)
                raise ResourceUnavailable(
                    "child exited or its reply was interrupted; "
                    "completion may be uncertain"
                ) from failures[0]
            reply = replies[0]
            if reply.invalid:
                self._terminate(deadline)
                raise ResourceUnavailable(
                    "child cancellation did not finish; completion may be uncertain"
                ) from reply.error
            if reply.expired:
                raise OperationTimeout(
                    "resource operation deadline exceeded; completion may be uncertain"
                ) from reply.error
            if reply.error is not None:
                raise reply.error
            return reply.value
        except (KeyboardInterrupt, SystemExit):
            self._terminate(deadline)
            raise
        finally:
            self._lock.release()

    def _terminate(self, deadline: Deadline) -> None:
        """Invalidate the resource and cap all kill, reap, and pipe cleanup waits."""
        self._closed = True
        cleanup = deadline.cleanup()
        cap = (
            deadline.expires
            if deadline._cleanup is deadline
            else deadline.expires + CLEANUP_SECONDS
        )
        cleanup.expires = min(cleanup.expires, cap)
        try:
            if self._process.poll() is None:
                self._process.kill()
            self._process.wait(
                timeout=max(0.001, min(1.0, cleanup.expires - monotonic()))
            )
        except Exception:
            _LOG.exception(
                "could not reap supervised resource process %s", self._process.pid
            )
        finally:
            if self._process.stdin is not None:
                self._process.stdin.close()
            if self._process.stdout is not None:
                self._process.stdout.close()


def _resource_worker() -> None:
    """Keep a generic native resource and its operations on one owning thread."""
    from usagebassoon.config import LoggingConfig
    from usagebassoon.logger import configure

    # Keep native stdout messages out of the private reply channel.
    output = os.fdopen(os.dup(sys.stdout.fileno()), "wb")
    with open(os.devnull, "wb") as discarded:
        os.dup2(discarded.fileno(), sys.stdout.fileno())
    spec = cast(_ResourceStart[object], pickle.load(sys.stdin.buffer))
    configure(spec.logging or LoggingConfig(disable=spec.logging_disabled))
    resource: object | None = None
    destroyed = False
    ready = False

    def respond(reply: _ResourceReply[object]) -> None:
        """Deliver a complete frame, marking an undeliverable outcome uncertain."""
        try:
            data = pickle.dumps(reply)
        except Exception:
            _LOG.exception("could not serialize resource outcome")
            data = pickle.dumps(
                _ResourceReply[object](
                    error=ResourceUnavailable(
                        "resource outcome could not be delivered"
                    ),
                    invalid=True,
                )
            )
        output.write(data)
        output.flush()

    try:
        deadline = Deadline(max(0.001, spec.expires - monotonic()))
        deadline.expires = spec.expires
        token = _CURRENT.set(deadline)
        try:
            deadline.remaining()
            resource = spec.construct()
            ready = True
            deadline.remaining()
            respond(_ResourceReply())
        except BaseException as error:
            _LOG.exception("supervised resource startup failed")
            respond(_ResourceReply(error=error))
            return
        finally:
            _CURRENT.reset(token)
        while True:
            request = cast(
                _ResourceCall[object, object, object], pickle.load(sys.stdin.buffer)
            )
            deadline = Deadline(max(0.001, request.expires - monotonic()))
            deadline.expires = request.expires
            if request.cleanup:
                deadline._cleanup = deadline
            token = _CURRENT.set(deadline)
            expired = Event()

            def interrupt(signal: Event = expired) -> None:
                """Apply the adapter's cancellation mechanism at deadline expiry."""
                signal.set()
                if spec.interrupt is not None:
                    try:
                        spec.interrupt(resource)
                    except Exception:
                        _LOG.exception("could not interrupt supervised resource")

            # Leave time to attempt native cancellation before forced cleanup.
            interrupt_at = request.expires - (
                min(5.0, CLEANUP_SECONDS / 2) if request.cleanup else 0.0
            )
            timer = Timer(max(0.0, interrupt_at - monotonic()), interrupt)
            timer.daemon = True
            timer.start()
            reply = _ResourceReply[object]()
            try:
                deadline.remaining()
                if request.execute is None:
                    spec.close(resource)
                    destroyed = True
                else:
                    reply = _ResourceReply(
                        value=request.execute(resource, request.argument)
                    )
                deadline.remaining()
            except BaseException as error:
                _LOG.exception("supervised resource operation failed")
                reply = _ResourceReply(
                    error=error,
                    expired=expired.is_set(),
                    invalid=isinstance(error, ResourceUnavailable),
                )
            finally:
                timer.cancel()
                timer.join(
                    timeout=max(
                        0.0, min(1.0, request.expires + CLEANUP_SECONDS - monotonic())
                    )
                )
                _CURRENT.reset(token)
            if timer.is_alive():
                reply = _ResourceReply(
                    error=ResourceUnavailable("resource interruption is still running"),
                    invalid=True,
                )
            respond(reply)
            if destroyed or reply.invalid:
                break
    except EOFError:
        pass
    finally:
        if ready and not destroyed:
            try:
                with cleanup_budget():
                    spec.close(resource)
            except Exception:
                _LOG.exception("could not close supervised resource after failure")
        output.close()


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
