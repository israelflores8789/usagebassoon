# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""test_collector_daily.py — Daily collection candidate and retry tests."""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import time
from contextlib import suppress
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from sys import executable
from threading import Lock
from threading import enumerate as enumerate_threads
from typing import cast

import pyarrow as pa
import pytest

from tests._bigquery_replay import BigQueryReplayBackend
from usagebassoon import collector as subprocess_collector
from usagebassoon import orchestrator as collector
from usagebassoon import persistence
from usagebassoon.backends.base import StorageBackend
from usagebassoon.backends.duckdb_local import DuckDBBackend
from usagebassoon.collection_lock import collection_lock
from usagebassoon.collector import RawCollection
from usagebassoon.config import LoggingConfig, UsageBassoonConfig
from usagebassoon.drift import SchemaDriftState
from usagebassoon.ingest import CollectionBundle, IngestStatus, IngestTarget
from usagebassoon.json_types import JsonArray, JsonObject, JsonValue
from usagebassoon.logger import LOG_DIRECTORY_ENV_VAR
from usagebassoon.normalizer import NormalizedBundle
from usagebassoon.persistence import PersistSummary

SOURCE_ID = "11111111-1111-4111-8111-111111111111"


class _FixedDatetime:
    """Provide one candidate day as the current UTC day for collection tests."""

    @classmethod
    def now(cls, tz: object | None = None) -> datetime:
        """Return the selected current day as an aware UTC timestamp."""
        del cls, tz
        return datetime(2026, 9, 10, tzinfo=UTC)


def _config(path: Path) -> UsageBassoonConfig:
    """Create the minimal local configuration consumed by collector helpers."""
    return UsageBassoonConfig(
        path=path,
        source_id=SOURCE_ID,
        backend="duckdb",
        local_database=path.parent / "collector.duckdb",
        logging=LoggingConfig(directory=path.parent / "logs"),
    )


@pytest.mark.parametrize(
    ("command", "expected_prefix"),
    [
        ("npx tokscale@latest", ["npx", "tokscale@latest"]),
        ("bunx tokscale@latest", ["bunx", "tokscale@latest"]),
        ("deno x npm:tokscale@latest", ["deno", "x", "npm:tokscale@latest"]),
    ],
)
def test_package_runner_commands_are_passed_to_tokscale(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    expected_prefix: list[str],
) -> None:
    """Pass configured package-runner prefixes and tokscale args to subprocess."""
    configuration = UsageBassoonConfig(
        path=Path("config.toml"),
        source_id=SOURCE_ID,
        backend="duckdb",
        local_database=Path(":memory:"),
        tokscale_bin=command,
    )
    monkeypatch.setenv("TOKSCALE_BIN", "ignored-environment-override")
    prefix = collector._prefix(configuration)

    assert prefix == expected_prefix


def test_daily_models_command_uses_each_candidate_day(
    monkeypatch: pytest.MonkeyPatch,
    daily_raws: dict[date, JsonObject],
) -> None:
    """Request date-filtered models facts with the canonical tokscale arguments."""
    day = min(daily_raws)
    calls: list[tuple[str, ...]] = []

    def command(
        _configuration: UsageBassoonConfig,
        _prefix: object,
        *arguments: str,
        **_kwargs: object,
    ) -> JsonValue:
        """Record one generated tokscale invocation and return its fixture."""
        calls.append(arguments)
        return daily_raws[day]

    monkeypatch.setattr(subprocess_collector, "_json_command", command)

    payloads = subprocess_collector._fetch_daily_models(
        _config(Path("config.toml")),
        ["tokscale"],
        [day],
    )

    assert payloads == {day: daily_raws[day]}
    assert calls == [
        (
            "models",
            "--json",
            "--group-by",
            "client,session,model",
            "--since",
            day.isoformat(),
            "--until",
            day.isoformat(),
        )
    ]


def test_tokscale_child_environment_excludes_unrelated_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass only documented and explicitly opted-in environment variables."""
    monkeypatch.setenv("PATH", "/test/bin")
    monkeypatch.setenv("LANG", "C.UTF-8")
    monkeypatch.setenv("TOKSCALE_EXTRA_DIRS", "/agent-storage")
    monkeypatch.setenv("CUSTOM_TOKSCALE_AUTH", "allowed")
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "secret")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/secret.json")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "secret")
    configuration = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=SOURCE_ID,
        backend="duckdb",
        local_database=Path(":memory:"),
        tokscale_env=("CUSTOM_TOKSCALE_AUTH",),
    )

    child = subprocess_collector._child_environment(configuration)

    assert child["PATH"] == "/test/bin"
    assert child["LANG"] == "C.UTF-8"
    assert child["TOKSCALE_EXTRA_DIRS"] == "/agent-storage"
    assert child["CUSTOM_TOKSCALE_AUTH"] == "allowed"
    assert "MOTHERDUCK_TOKEN" not in child
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in child
    assert "AWS_ACCESS_KEY_ID" not in child


def test_tokscale_timeout_kills_the_process(tmp_path: Path) -> None:
    """Terminate a subprocess that exceeds the configured hard timeout."""
    configuration = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=SOURCE_ID,
        backend="duckdb",
        local_database=Path(":memory:"),
        tokscale_timeout_seconds=0.05,
    )

    with pytest.raises(RuntimeError, match="exceeded 0 seconds; termination requested"):
        collector._json_command(
            configuration,
            [executable, "-c", "import time; time.sleep(30)"],
            "graph",
        )


@pytest.mark.skipif(os.name != "posix", reason="requires detached POSIX descendants")
@pytest.mark.parametrize(
    ("pipe_name", "excessive"),
    [("stdout", False), ("stderr", False), ("stdout", True)],
)
def test_tokscale_cleanup_bounds_surviving_pipe_holders(
    tmp_path: Path, pipe_name: str, excessive: bool
) -> None:
    """Bound timeout/size cleanup, close pipes, and release the collection lock."""
    ready = tmp_path / "holder.pid"
    descriptor = 1 if pipe_name == "stdout" else 2
    script = (
        "import os, time\n"
        "from pathlib import Path\n"
        "if os.fork() == 0:\n"
        "    os.setsid()\n"
        f"    os.close({3 - descriptor})\n"
        f"    Path({str(ready)!r}).write_text(str(os.getpid()))\n"
        "    time.sleep(8)\n"
        "    os._exit(0)\n"
        "time.sleep(0.1)\n"
        + ("os.write(1, b'x' * 4096)\n" if excessive else "")
        + "time.sleep(8)\n"
    )
    process = subprocess.Popen(
        [executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    configuration = _config(tmp_path / "config.toml")
    threads = set(enumerate_threads())
    started = time.monotonic()
    try:
        with (
            pytest.raises(
                RuntimeError, match="byte limit" if excessive else "exceeded 1 seconds"
            ),
            collection_lock(configuration),
        ):
            subprocess_collector._capture_process(
                process,
                timeout_seconds=1.0,
                max_stdout_bytes=128 if excessive else 4096,
                max_stderr_bytes=4096,
            )
        assert time.monotonic() - started < 4.0
        assert ready.exists(), "detached pipe holder must have started"
        assert process.poll() is not None
        assert process.stdout is not None and process.stdout.closed
        assert process.stderr is not None and process.stderr.closed
        assert set(enumerate_threads()) == threads
        with collection_lock(configuration):
            assert (
                collector._json_command(
                    configuration, [executable, "-c", "print('{}')"], "graph"
                )
                == {}
            )
    finally:
        if ready.exists():
            with suppress(ProcessLookupError):
                os.kill(int(ready.read_text()), signal.SIGKILL)
        if process.poll() is None:
            process.kill()
        process.wait(timeout=2)


@pytest.mark.parametrize("fault", ["kill", "read", "closed-pipes"])
def test_tokscale_cleanup_bounds_process_failures(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    fault: str,
) -> None:
    """Fail boundedly on unsuccessful kills, read errors, or early pipe EOF."""
    script = "import time; time.sleep(8)"
    if fault == "closed-pipes":
        script = "import os; os.close(1); os.close(2); " + script
    process = subprocess.Popen(
        [executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    def failed_kill(*_args: object) -> None:
        """Simulate both group and direct termination failure."""
        raise PermissionError("termination denied")

    def failed_read(_descriptor: int, _size: int) -> bytes:
        """Simulate an unreadable child pipe."""
        raise OSError("pipe read failed")

    started = time.monotonic()
    try:
        with monkeypatch.context() as patch:
            if fault == "kill":
                patch.setattr(
                    subprocess_collector.os, "killpg", failed_kill, raising=False
                )
                patch.setattr(process, "kill", failed_kill)
            elif fault == "read":
                patch.setattr(subprocess_collector.os, "read", failed_read)
            with pytest.raises(
                RuntimeError, match="could not read" if fault == "read" else "exceeded"
            ):
                subprocess_collector._capture_process(
                    process,
                    timeout_seconds=0.1,
                    max_stdout_bytes=4096,
                    max_stderr_bytes=4096,
                )
        assert time.monotonic() - started < 3.0
        assert process.stdout is not None and process.stdout.closed
        assert process.stderr is not None and process.stderr.closed
        if fault == "kill":
            assert process.poll() is None
            assert "could not terminate tokscale" in caplog.text
            assert "did not exit before cleanup deadline" in caplog.text
        else:
            assert process.poll() is not None
        if fault == "read":
            assert "could not read tokscale" in caplog.text
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=2)


def test_tokscale_preserves_slow_valid_output(tmp_path: Path) -> None:
    """Capture incremental valid JSON before the hard maximum, without spinners."""
    configuration = replace(
        _config(tmp_path / "config.toml"), tokscale_timeout_seconds=2.0
    )
    script = (
        "import os, sys, time\n"
        "assert sys.argv[1] == '--no-spinner'\n"
        "for chunk in [b'{', b'\"value\":', b'42', b'}']:\n"
        "    os.write(1, chunk)\n"
        "    time.sleep(0.05)\n"
    )
    assert collector._json_command(
        configuration, [executable, "-c", script], "graph"
    ) == {"value": 42}


def test_tokscale_stdout_limit_kills_the_process(tmp_path: Path) -> None:
    """Reject excessive graph output before it can exhaust collector memory."""
    configuration = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=SOURCE_ID,
        backend="duckdb",
        local_database=Path(":memory:"),
        tokscale_max_stdout_bytes=128,
    )

    with pytest.raises(RuntimeError, match="stdout exceeded its 128 byte limit"):
        collector._json_command(
            configuration,
            [executable, "-c", "import sys; sys.stdout.write('x' * 4096)"],
            "graph",
            max_stdout_bytes=128,
        )


def test_tokscale_stderr_limit_kills_the_process(tmp_path: Path) -> None:
    """Reject excessive tokscale diagnostics before retaining unbounded text."""
    configuration = UsageBassoonConfig(
        path=tmp_path / "config.toml",
        source_id=SOURCE_ID,
        backend="duckdb",
        local_database=Path(":memory:"),
        tokscale_max_stderr_bytes=128,
    )

    with pytest.raises(RuntimeError, match="stderr exceeded its 128 byte limit"):
        collector._json_command(
            configuration,
            [executable, "-c", "import sys; sys.stderr.write('x' * 4096)"],
            "graph",
        )


def test_graph_candidates_skip_completed_statuses_and_refresh_today(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    graph_raw: JsonObject,
    report_raws: dict[date, JsonArray],
    daily_raws: dict[date, JsonObject],
) -> None:
    """Use graph dates, skip completed history, and refresh the current day."""
    days = tuple(sorted(daily_raws))
    completed_day, *_, current_day = days
    captured: list[RawCollection] = []
    requested_models: list[tuple[date, ...]] = []
    requested_prices: list[dict[date, set[str]]] = []

    def command(
        _configuration: UsageBassoonConfig,
        _prefix: object,
        *arguments: str,
        **_kwargs: object,
    ) -> JsonValue:
        """Supply graph and report payloads while fetch helpers handle daily calls."""
        if arguments == ("graph",):
            return graph_raw
        if (
            len(arguments) == 7
            and arguments[:4] == ("report", "--json", "--no-summarize", "--since")
            and arguments[5] == "--until"
            and arguments[4] == arguments[6]
        ):
            return report_raws[date.fromisoformat(arguments[4])]
        raise AssertionError(f"unexpected tokscale command: {arguments}")

    def daily_models(
        _configuration: UsageBassoonConfig,
        _prefix: object,
        selected_days: tuple[date, ...],
    ) -> dict[date, JsonObject]:
        """Return all selected mandatory daily facts."""
        requested_models.append(selected_days)
        return {day: daily_raws[day] for day in selected_days}

    def pricing(
        _configuration: UsageBassoonConfig,
        _prefix: object,
        models_by_day: dict[date, set[str]],
        _logger: logging.Logger,
    ) -> tuple[dict[date, dict[str, JsonObject]], dict[date, frozenset[str]]]:
        """Record model-day pricing requests and complete the successful targets."""
        requested_prices.append(models_by_day)
        return ({day: {} for day in models_by_day}, {})

    def build(raw: RawCollection, **_kwargs: object) -> object:
        """Capture the raw bundle before normalization and persistence."""
        captured.append(raw)
        return object()

    def prefix(_configuration: UsageBassoonConfig) -> list[str]:
        """Return a deterministic tokscale executable for this unit test."""
        return ["tokscale"]

    def collection_status(
        _configuration: UsageBassoonConfig,
    ) -> tuple[
        dict[IngestTarget, IngestStatus],
        dict[date, set[str]],
        dict[date, set[str]],
        frozenset[tuple[str, str]],
        tuple[SchemaDriftState, ...],
    ]:
        """Return one completed historical day and one refreshable current day."""
        return (
            {
                (completed_day, "models"): IngestStatus(
                    completed_day, "models", "complete", 1, 1, "old", "old"
                ),
                (completed_day, "pricing"): IngestStatus(
                    completed_day, "pricing", "complete", 1, 1, "old", "old"
                ),
                (current_day, "models"): IngestStatus(
                    current_day, "models", "complete", 1, 1, "old", "old"
                ),
                (current_day, "pricing"): IngestStatus(
                    current_day, "pricing", "complete", 1, 1, "old", "old"
                ),
            },
            {},
            {},
            frozenset(),
            (),
        )

    def normalized(_bundle: CollectionBundle) -> NormalizedBundle:
        """Return an opaque normalized value because persistence is replaced."""
        return cast(NormalizedBundle, object())

    def persist(
        _configuration: UsageBassoonConfig,
        _bundle: NormalizedBundle,
        _logger: logging.Logger,
    ) -> PersistSummary:
        """Return the persistence outcome used for collection assertions."""
        return PersistSummary(0, 0, {})

    monkeypatch.setattr(collector, "datetime", _FixedDatetime)
    monkeypatch.setattr(collector, "_prefix", prefix)
    monkeypatch.setattr(collector, "_json_command", command)
    monkeypatch.setattr(subprocess_collector, "_json_command", command)
    monkeypatch.setattr(collector, "load_ingest_status", collection_status)
    monkeypatch.setattr(collector, "_fetch_daily_models", daily_models)
    monkeypatch.setattr(collector, "_fetch_pricing", pricing)
    monkeypatch.setattr(collector, "build_collection_bundle", build)
    monkeypatch.setattr(collector, "normalize", normalized)
    monkeypatch.setattr(collector, "persist_with_retries", persist)

    _, summary = collector.collect(_config(tmp_path / "config.toml"))

    expected_successes = set(days) - {completed_day}
    assert summary == PersistSummary(0, 0, {})
    assert requested_models == [tuple(day for day in days if day != completed_day)]
    assert set(requested_prices[0]) == expected_successes
    assert captured[0].graph is graph_raw
    assert set(captured[0].daily_models) == expected_successes
    assert set(captured[0].report_by_day) == set(report_raws)


def test_daily_models_failure_aborts_collection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    graph_raw: JsonObject,
) -> None:
    """Treat a required daily models failure as a failed collection run."""
    configuration = _config(tmp_path / "config.toml")
    assert configuration.local_database is not None
    backend = DuckDBBackend(configuration.local_database)
    backend.apply_ddl()
    backend.close()
    calls: list[tuple[str, ...]] = []

    def command(
        _configuration: UsageBassoonConfig,
        _prefix: object,
        *arguments: str,
        **_kwargs: object,
    ) -> JsonValue:
        """Supply the required graph, then fail mandatory model acquisition."""
        calls.append(arguments)
        if arguments == ("graph",):
            return graph_raw
        raise OSError("tokscale executable unavailable")

    monkeypatch.setattr(collector, "_json_command", command)
    monkeypatch.setattr(subprocess_collector, "_json_command", command)

    with pytest.raises(OSError, match="tokscale executable unavailable"):
        collector.collect(configuration)
    assert len(calls) == 2
    assert calls[0] == ("graph",)
    assert calls[1][0] == "models"
    backend = DuckDBBackend(configuration.local_database)
    try:
        rows = backend.query("SELECT * FROM collection_runs").to_pylist()
        assert len(rows) == 1
        assert rows[0]["status"] == "failed"
        assert rows[0]["failure_code"] == "OSError"
        assert rows[0]["source_id"] == configuration.source_id
        assert backend.query("SELECT * FROM daily_stats").num_rows == 0
    finally:
        backend.close()


def test_report_failure_is_logged_and_does_not_abort_collection(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep token collection usable when optional session metadata is unavailable."""
    logger = logging.getLogger("usagebassoon.test.report-failure")

    def command(*_args: object, **_kwargs: object) -> JsonValue:
        """Raise a transient report command failure."""
        raise RuntimeError("report process failed")

    monkeypatch.setattr(subprocess_collector, "_json_command", command)

    with caplog.at_level(logging.ERROR, logger=logger.name):
        reports, failures = subprocess_collector._fetch_reports(
            _config(Path("config.toml")),
            ["tokscale"],
            [date(2026, 9, 10)],
            logger=logger,
        )

    assert reports == {}
    assert failures == frozenset({date(2026, 9, 10)})
    assert "session report collection failed" in caplog.text


@pytest.mark.parametrize("error_type", [RuntimeError, TypeError])
def test_graph_failure_aborts_cycle_and_logs_to_operational_log(
    error_type: type[Exception],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record expected and unexpected required graph failures before propagating."""
    monkeypatch.delenv(LOG_DIRECTORY_ENV_VAR, raising=False)
    configuration = _config(tmp_path / "config.toml")
    assert configuration.local_database is not None
    backend = DuckDBBackend(configuration.local_database)
    backend.apply_ddl()
    backend.close()

    def command(*_args: object, **_kwargs: object) -> JsonValue:
        """Raise a transient graph command failure."""
        raise error_type("graph process failed")

    monkeypatch.setattr(collector, "_json_command", command)

    with pytest.raises(error_type, match="graph process failed"):
        collector.collect(configuration)

    backend = DuckDBBackend(configuration.local_database)
    try:
        rows = backend.query("SELECT * FROM collection_runs").to_pylist()
        assert len(rows) == 1
        assert rows[0]["status"] == "failed"
        assert rows[0]["failure_code"] == error_type.__name__
        assert rows[0]["source_id"] == configuration.source_id
        assert backend.query("SELECT * FROM daily_stats").num_rows == 0
    finally:
        backend.close()

    log = (tmp_path / "logs" / "usagebassoon.log").read_text()
    assert "collection cycle failed before completion" in log


@pytest.mark.parametrize("architecture", ["upsert", "append"])
def test_historical_refresh_preserves_usage_and_source_isolation(
    architecture: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    graph_raw: JsonObject,
    daily_raws: dict[date, JsonObject],
    report_raws: dict[date, JsonArray],
    pricing_raw: JsonObject,
) -> None:
    """Recover midnight and late changes without duplicates or erasing history."""
    day = max(daily_raws)
    graph = deepcopy(graph_raw)
    graph["contributions"] = [
        item
        for item in cast(JsonArray, graph["contributions"])
        if isinstance(item, dict) and item["date"] == day.isoformat()
    ]
    daily = deepcopy(daily_raws[day])
    entries = cast(list[JsonObject], daily["entries"])
    original_input = cast(int, entries[0]["input"])
    stamp = datetime.combine(day, datetime.min.time(), UTC) + timedelta(
        hours=23, minutes=45
    )
    requests: list[tuple[date, ...]] = []
    fail = False
    backend = (
        DuckDBBackend(":memory:")
        if architecture == "upsert"
        else BigQueryReplayBackend()
    )
    if isinstance(backend, DuckDBBackend):
        backend.apply_ddl()
    else:
        original_append = backend.append
        lock = Lock()

        def append(table: str, data: pa.Table) -> None:
            """Serialize only the local replay engine's independent table loads."""
            with lock:
                original_append(table, data)

        monkeypatch.setattr(backend, "append", append)

    class Clock:
        """Provide a movable UTC clock across collection boundaries."""

        @classmethod
        def now(cls, tz: object | None = None) -> datetime:
            """Return the controlled collection time."""
            del cls, tz
            return stamp

    def command(
        _config: UsageBassoonConfig,
        _prefix: object,
        *arguments: str,
        **_kwargs: object,
    ) -> JsonValue:
        """Expose an upstream graph snapshot without executing tokscale."""
        assert arguments == ("graph",)
        return deepcopy(graph)

    def models(
        _config: UsageBassoonConfig,
        _prefix: object,
        days: tuple[date, ...],
    ) -> dict[date, JsonObject]:
        """Expose changed session facts, or fail before publishing a refresh."""
        requests.append(days)
        if fail:
            raise RuntimeError("models unavailable")
        empty = deepcopy(daily)
        empty_entries: JsonArray = []
        empty["entries"] = empty_entries
        for field in (
            "totalInput",
            "totalOutput",
            "totalCacheRead",
            "totalCacheWrite",
            "totalMessages",
            "totalCost",
        ):
            empty[field] = 0
        return {
            requested: deepcopy(daily if requested == day else empty)
            for requested in days
        }

    def pricing(
        _config: UsageBassoonConfig,
        _prefix: object,
        requests: dict[date, set[str]],
        _logger: logging.Logger,
    ) -> tuple[dict[date, dict[str, JsonObject]], dict[date, frozenset[str]]]:
        """Fill only requested model-price gaps."""
        return (
            {
                date: {model: {**pricing_raw, "modelId": model} for model in models}
                for date, models in requests.items()
            },
            {},
        )

    monkeypatch.setattr(collector, "datetime", Clock)
    monkeypatch.setattr(collector, "_json_command", command)
    monkeypatch.setattr(collector, "_fetch_daily_models", models)
    monkeypatch.setattr(collector, "_fetch_pricing", pricing)

    def reports(
        _config: UsageBassoonConfig,
        _prefix: object,
        days: tuple[date, ...],
        **_kwargs: object,
    ) -> tuple[dict[date, JsonArray], frozenset[date]]:
        """Return the selected source's session metadata."""
        return {
            requested: report_raws.get(requested, []) for requested in days
        }, frozenset()

    def open_backend(_config: UsageBassoonConfig) -> StorageBackend:
        """Reuse the pipeline's in-memory backend across cycles."""
        return backend

    def close_backend(*_args: object, **_kwargs: object) -> None:
        """Keep the shared test backend open until the scenario ends."""

    monkeypatch.setattr(collector, "_fetch_reports", reports)
    monkeypatch.setattr(persistence, "open_backend", open_backend)
    monkeypatch.setattr(persistence, "close_backend", close_backend)
    config = _config(tmp_path / "config.toml")

    def facts(source: str = SOURCE_ID) -> list[dict[str, object]]:
        """Read source-specific canonical keys and counts after publication."""
        return backend.query(
            "SELECT client, session_id, model, input_tokens FROM current_daily_stats "
            f"WHERE source_id = '{source}' ORDER BY client, session_id, model"
        ).to_pylist()

    def input_count() -> int:
        """Read the fact changed after the last open-day collection."""
        return next(
            cast(int, row["input_tokens"])
            for row in facts()
            if row["session_id"] == entries[0]["sessionId"]
            and row["model"] == entries[0]["model"]
        )

    try:
        collector.collect(config)
        status = backend.query(
            "SELECT status FROM collection_status WHERE domain = 'models'"
        ).to_pylist()
        assert status == [{"status": "provisional"}]
        assert input_count() == original_input

        # Graph is unchanged: closing the day must still recover the final interval.
        entries[0]["input"] = original_input + 10
        daily["totalInput"] = cast(int, daily["totalInput"]) + 10
        stamp += timedelta(minutes=20)
        collector.collect(config)
        assert requests[-1] == (day,)
        assert input_count() == original_input + 10
        assert backend.query(
            "SELECT status FROM collection_status WHERE domain = 'models'"
        ).to_pylist() == [{"status": "complete"}]
        historical_prices = backend.query(
            "SELECT * FROM current_price_versions ORDER BY day, model"
        ).to_pylist()

        # Overlap also repairs delayed records that the graph does not expose.
        stamp += timedelta(days=2)
        entries[0]["input"] = original_input + 15
        daily["totalInput"] += 5
        collector.collect(config)
        assert requests[-1] == (day,)
        assert input_count() == original_input + 15

        # Changes beyond the overlap require an explicit historical refresh.
        stamp += timedelta(days=10)
        entries[0]["input"] = original_input + 30
        daily["totalInput"] += 15
        contribution = cast(JsonObject, cast(JsonArray, graph["contributions"])[0])
        client = cast(JsonObject, cast(JsonArray, contribution["clients"])[0])
        tokens = cast(JsonObject, client["tokens"])
        tokens["input"] = cast(int, tokens["input"]) + 20
        collector.collect(config)
        assert requests[-1] == ()
        assert input_count() == original_input + 15

        other = "22222222-2222-4222-8222-222222222222"
        collector.collect(replace(config, source_id=other))
        other_facts = facts(other)

        # Explicit refresh recovers both changed totals and session corrections.
        stamp += timedelta(seconds=1)
        entries[0]["input"] = original_input + 35
        entries[1]["input"] = cast(int, entries[1]["input"]) - 5
        collector.collect(config)
        assert requests[-1] == ()
        before_failure = facts()
        fail = True
        with pytest.raises(RuntimeError, match="models unavailable"):
            collector.collect(config, refresh=True, since=day, until=day)
        assert facts() == before_failure
        fail = False
        collector.collect(config, refresh=True)
        assert requests[-1] == tuple(
            stamp.date() - timedelta(days=offset) for offset in range(29, -1, -1)
        )
        assert input_count() == original_input + 35
        assert (
            next(
                row["input_tokens"]
                for row in facts()
                if row["session_id"] == entries[1]["sessionId"]
                and row["model"] == entries[1]["model"]
            )
            == entries[1]["input"]
        )
        collector.collect(config, refresh=True, since=day, until=day)
        assert len(facts()) == len(entries)
        assert facts(other) == other_facts
        assert (
            backend.query(
                f"SELECT * FROM current_price_versions WHERE source_id = '{SOURCE_ID}' "
                "ORDER BY day, model"
            ).to_pylist()
            == historical_prices
        )

        # A partial upstream scope never deletes keys missing from recollection.
        omitted = entries.pop()
        for total, field in (
            ("totalInput", "input"),
            ("totalOutput", "output"),
            ("totalCacheRead", "cacheRead"),
            ("totalCacheWrite", "cacheWrite"),
            ("totalMessages", "messageCount"),
        ):
            daily[total] = cast(int, daily[total]) - cast(int, omitted[field])
        daily["totalCost"] = cast(float, daily["totalCost"]) - cast(
            float, omitted["cost"]
        )
        stamp += timedelta(seconds=1)
        collector.collect(config, refresh=True, since=day, until=day)
        assert len(facts()) == len(entries) + 1
        collector.collect(config, refresh=True, since=day + timedelta(days=1))
        assert day not in requests[-1]
        assert requests[-1][0] == day + timedelta(days=1)
        assert requests[-1][-1] == stamp.date()
        stamp = datetime.combine(day + timedelta(days=30), datetime.min.time(), UTC)
        collector.collect(config, refresh=True)
        assert day not in requests[-1]
        collector.collect(config, refresh=True, until=day)
        assert requests[-1][-1] == day
    finally:
        backend.close()
