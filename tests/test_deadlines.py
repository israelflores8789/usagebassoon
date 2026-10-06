# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_deadlines.py — Shared operation budgets and bounded cleanup guarantees."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass
from threading import Barrier

import pytest

from usagebassoon import deadlines
from usagebassoon.deadlines import (
    Deadline,
    OperationTimeout,
    bounded,
    checked,
    cleanup_budget,
    current_deadline,
    limited,
    operation,
    remaining_seconds,
)


@dataclass
class Clock:
    """Advance a deterministic monotonic clock without waiting in tests."""

    now: float = 100.0

    def __call__(self) -> float:
        """Return the current monotonic instant."""
        return self.now

    def advance(self, seconds: float) -> None:
        """Account for work or backoff in the shared clock."""
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    """Replace the operation clock while preserving real thread coordination."""
    controlled = Clock()
    monkeypatch.setattr(deadlines, "monotonic", controlled)
    return controlled


@pytest.mark.parametrize(
    "seconds", [0.0, -1.0, float("inf"), -float("inf"), float("nan")]
)
def test_deadline_rejects_invalid_budgets(seconds: float) -> None:
    """A configured deadline must represent a positive, finite allowance."""
    with pytest.raises(ValueError, match="positive and finite"):
        Deadline(seconds)


def test_remaining_allowance_decreases_without_renewing_caps(clock: Clock) -> None:
    """Request limits cap the shared allowance without creating new deadlines."""
    with operation(180) as deadline:
        assert deadline is not None
        clock.advance(6)
        assert remaining_seconds() == 30
        assert remaining_seconds(10) == 10
        assert remaining_seconds(600) == 174
        assert remaining_seconds(None) == 174
        clock.advance(169)
        assert remaining_seconds() == 5
        assert remaining_seconds(None) == 5
    assert current_deadline() is None


@pytest.mark.parametrize("elapsed", [10.0, 11.0])
def test_expired_deadline_refuses_further_work(clock: Clock, elapsed: float) -> None:
    """The exact expiry boundary and later instants both reject another request."""
    deadline = Deadline(10)
    clock.advance(elapsed)
    with pytest.raises(OperationTimeout, match="completion may be uncertain"):
        deadline.remaining()
    with pytest.raises(OperationTimeout):
        deadline.remaining(30)


@pytest.mark.parametrize("nested_seconds", [None, 1.0, 600.0])
def test_nested_operation_reuses_the_enclosing_deadline(
    clock: Clock, nested_seconds: float | None
) -> None:
    """Nested helpers cannot replace a snapshot or persistence operation budget."""
    with operation(10) as outer:
        assert outer is not None
        clock.advance(4)
        with operation(nested_seconds) as inner:
            assert inner is outer
            assert remaining_seconds(None) == 6
            clock.advance(1)
        assert current_deadline() is outer
        assert remaining_seconds(None) == 5
    assert current_deadline() is None
    with operation(10) as fresh:
        assert fresh is not outer
        assert remaining_seconds(None) == 10


def test_operation_restores_context_after_a_nested_failure(clock: Clock) -> None:
    """An inner exception restores its parent's scope without replacing the error."""
    failure = RuntimeError("primary failure")
    with operation(10) as parent:
        clock.advance(1)
        with pytest.raises(RuntimeError) as caught, operation(600):
            raise failure
        assert caught.value is failure
        assert current_deadline() is parent
        assert remaining_seconds(None) == 9
    assert current_deadline() is None


def test_operation_checks_expiry_on_successful_exit(clock: Clock) -> None:
    """Work that returns after exhausting its budget cannot report success."""
    with pytest.raises(OperationTimeout), operation(1):
        clock.advance(2)
    assert current_deadline() is None


def test_expired_parent_prevents_nested_operation_entry(clock: Clock) -> None:
    """A new helper cannot start or renew work inside an expired operation."""
    entered = False
    with pytest.raises(OperationTimeout), operation(1) as parent:
        clock.advance(2)
        with pytest.raises(OperationTimeout), operation(600):
            entered = True
        assert not entered
        assert current_deadline() is parent
    assert current_deadline() is None


def test_primary_exception_survives_operation_expiry(clock: Clock) -> None:
    """Expiry checks must not hide the exception that interrupted an operation."""
    failure = ValueError("original failure")
    with pytest.raises(ValueError) as caught, operation(1):
        clock.advance(2)
        raise failure
    assert caught.value is failure
    assert current_deadline() is None


def test_unbounded_operation_keeps_request_safeguards(clock: Clock) -> None:
    """An unbounded local scope leaves request defaults intact without a timer."""
    with operation(None) as deadline:
        assert deadline is None
        clock.advance(100)
        assert current_deadline() is None
        assert remaining_seconds() == deadlines.REQUEST_SECONDS
        assert remaining_seconds(None) == deadlines.REQUEST_SECONDS
        assert remaining_seconds(7) == 7
    assert current_deadline() is None


def test_expired_operation_has_one_independent_cleanup_allowance(clock: Clock) -> None:
    """Repeated and nested cleanup reuse one grace without hiding a primary error."""
    failure = RuntimeError("primary failure")
    with pytest.raises(RuntimeError) as caught, operation(1) as parent:
        assert parent is not None
        clock.advance(2)
        with cleanup_budget():
            grace = current_deadline()
            assert grace is not None and grace is not parent
            assert remaining_seconds(None) == deadlines.CLEANUP_SECONDS
            clock.advance(4)
            with cleanup_budget():
                assert current_deadline() is grace
                assert remaining_seconds(None) == deadlines.CLEANUP_SECONDS - 4
        assert current_deadline() is parent
        with cleanup_budget():
            assert current_deadline() is grace
            assert remaining_seconds(None) == deadlines.CLEANUP_SECONDS - 4
            clock.advance(deadlines.CLEANUP_SECONDS - 4)
            with pytest.raises(OperationTimeout):
                remaining_seconds()
        assert current_deadline() is parent
        raise failure
    assert caught.value is failure
    assert current_deadline() is None


def test_limited_scopes_tighten_restore_and_share_cleanup(clock: Clock) -> None:
    """Child bounds preserve parent time, context and one cleanup allowance."""
    with operation(100) as parent:
        with limited(50) as child:
            assert child is not parent
            assert remaining_seconds(None) == 50
            with limited(200):
                assert remaining_seconds(None) == 50
                clock.advance(5)
            assert current_deadline() is child
            assert remaining_seconds(None) == 45
            with limited(20), cleanup_budget():
                assert remaining_seconds(None) == 15
                clock.advance(10)
            assert current_deadline() is child
            assert remaining_seconds(None) == 35
            with cleanup_budget():
                assert remaining_seconds(None) == 5
            clock.advance(6)
            with cleanup_budget(), pytest.raises(OperationTimeout):
                remaining_seconds()
        assert current_deadline() is parent
        assert remaining_seconds(None) == 79
        failure = RuntimeError("limited operation failed")
        with pytest.raises(RuntimeError) as caught, limited(1):
            raise failure
        assert caught.value is failure
        assert current_deadline() is parent
    assert current_deadline() is None


@pytest.mark.parametrize("fail", [False, True])
def test_standalone_cleanup_restores_context(clock: Clock, fail: bool) -> None:
    """Cleanup outside an operation gets a bounded grace and restores context."""

    def clean() -> None:
        """Run cleanup whose scope must end even if the cleanup itself fails."""
        with cleanup_budget():
            assert current_deadline() is not None
            assert remaining_seconds(None) == deadlines.CLEANUP_SECONDS
            clock.advance(1)
            if fail:
                raise RuntimeError("cleanup failure")

    if fail:
        with pytest.raises(RuntimeError, match="cleanup failure"):
            clean()
    else:
        clean()
    assert current_deadline() is None


def test_concurrent_cleanup_shares_one_allowance(clock: Clock) -> None:
    """Concurrent workers inherit one grace instead of allocating one per worker."""
    barrier = Barrier(4)
    with operation(100) as parent:
        assert parent is not None

        def clean() -> Deadline:
            """Enter cleanup concurrently from distinct inherited contexts."""
            assert current_deadline() is parent
            barrier.wait(timeout=2)
            with cleanup_budget():
                grace = current_deadline()
                assert grace is not None
                assert remaining_seconds(None) == deadlines.CLEANUP_SECONDS
            assert current_deadline() is parent
            return grace

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(copy_context().run, clean) for _ in range(4)]
            graces = [future.result(timeout=2) for future in futures]
        assert all(grace is graces[0] for grace in graces)
        assert current_deadline() is parent
        clock.advance(10)
        with cleanup_budget():
            assert current_deadline() is graces[0]
            assert remaining_seconds(None) == deadlines.CLEANUP_SECONDS - 10
        assert current_deadline() is parent
    assert current_deadline() is None


def test_bounded_method_reuses_parent_and_gives_new_operations_fresh_budgets(
    clock: Clock,
) -> None:
    """Provider helpers preserve the caller's deadline and reject overdue returns."""
    seen: list[Deadline] = []

    class Owner:
        """Expose a configured budget without depending on any storage provider."""

        timeout_seconds = 10.0

        @bounded
        def work(self, elapsed: float) -> str:
            """Model work that does not check the deadline internally."""
            deadline = current_deadline()
            assert deadline is not None
            seen.append(deadline)
            clock.advance(elapsed)
            return "done"

    owner = Owner()
    assert owner.work(3) == "done"
    assert current_deadline() is None
    assert owner.work(3) == "done"
    assert seen[0] is not seen[1]
    with operation(4) as parent:
        clock.advance(1)
        assert owner.work(2) == "done"
        assert seen[-1] is parent
        assert remaining_seconds(None) == 1
    with pytest.raises(OperationTimeout):
        owner.work(11)
    assert current_deadline() is None


@pytest.mark.parametrize("boundary", ["before", "after"])
def test_checked_work_rejects_expired_entry_or_return(
    clock: Clock, boundary: str
) -> None:
    """Local work must not start after expiry or return success past the deadline."""
    calls: list[None] = []

    @checked
    def work() -> str:
        """Record work and optionally exhaust its enclosing operation."""
        calls.append(None)
        if boundary == "after":
            clock.advance(2)
        return "done"

    with pytest.raises(OperationTimeout), operation(1):
        if boundary == "before":
            clock.advance(2)
        work()
    assert len(calls) == (0 if boundary == "before" else 1)
    assert current_deadline() is None


def test_checked_work_preserves_results_without_creating_a_new_scope(
    clock: Clock,
) -> None:
    """Checked local work preserves return values and only uses an existing scope."""

    @checked
    def work(value: str) -> str:
        """Advance local work without choosing a provider budget."""
        clock.advance(1)
        return value

    assert work("unbounded") == "unbounded"
    assert current_deadline() is None
    with operation(10) as parent:
        assert work("bounded") == "bounded"
        assert current_deadline() is parent
        assert remaining_seconds(None) == 9
    assert current_deadline() is None
