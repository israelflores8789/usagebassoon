# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""diagnostics.py — Read-only health queries and check reporting."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa

from usagebassoon import ingest, reconcile
from usagebassoon.backends.base import ActiveTransaction, StorageBackend
from usagebassoon.drift import SchemaDriftRecord, format_drift

if TYPE_CHECKING:
    from usagebassoon.config import UsageBassoonConfig

CheckStatus = Literal["ok", "warning", "error"]
type ReadResult[T] = tuple[T | None, Exception | None]
DRIFT_ISSUE_URL = "https://github.com/israelflores8789/usagebassoon/issues/new"
_LOG = logging.getLogger("usagebassoon")

REQUIRED_RELATIONS: tuple[str, ...] = (
    "collection_runs",
    "schema_drift_events",
    "sessions",
    "session_model_stats",
    "daily_stats",
    "price_versions",
    "collection_status",
    "reconciliation_issues",
    "tags",
    "notes",
    "session_model_stats_current",
    "report_summary",
    "report_models",
)


@dataclass(frozen=True, slots=True)
class DoctorCheck:
    """One named diagnostic result.

    Attributes:
        name: Stable diagnostic name suitable for automation.
        status: The result severity.
        message: Short human-readable result.
        details: Optional supporting messages.
    """

    name: str
    status: CheckStatus
    message: str
    details: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DoctorReport:
    """Complete read-only health report for a UsageBassoon installation.

    Attributes:
        checks: Checks in display order.
    """

    checks: tuple[DoctorCheck, ...]

    @property
    def errors(self) -> tuple[DoctorCheck, ...]:
        """Return checks that represent hard failures."""
        return tuple(check for check in self.checks if check.status == "error")

    @property
    def warnings(self) -> tuple[DoctorCheck, ...]:
        """Return checks that represent non-fatal attention items."""
        return tuple(check for check in self.checks if check.status == "warning")

    @property
    def status(self) -> CheckStatus:
        """Return the aggregate report status."""
        if self.errors:
            return "error"
        if self.warnings:
            return "warning"
        return "ok"

    def exit_code(self, *, strict: bool = False) -> int:
        """Return a shell-friendly exit code.

        Args:
            strict: Treat warnings as failures, useful in CI or cron checks.

        Returns:
            Zero when the report meets the requested policy, otherwise one.
        """
        return int(bool(self.errors or (strict and self.warnings)))


def maintenance_health(backend: StorageBackend | None) -> DoctorCheck:
    """Inspect native maintenance without enabling or disabling any schedule."""
    if backend is None:
        return DoctorCheck("maintenance", "warning", "backend unavailable")
    try:
        result = backend.maintenance_status()
    except Exception as error:
        return DoctorCheck("maintenance", "warning", f"inspection unavailable: {error}")
    if result is None:
        return DoctorCheck("maintenance", "ok", "scheduled compaction is not required")
    ready, message = result
    return DoctorCheck("maintenance", "ok" if ready else "warning", message)


def snapshot_health(configuration: UsageBassoonConfig | None) -> DoctorCheck:
    """Report recovery coverage and last creation verification for each archive."""
    if configuration is None:
        return DoctorCheck("backup_freshness", "warning", "configuration unavailable")
    settings = configuration.snapshots
    if not settings.enabled:
        return DoctorCheck(
            "backup_freshness", "ok", "automatic snapshots are not configured"
        )
    from usagebassoon.archiver import SnapshotArchiver
    from usagebassoon.config import parse_interval
    from usagebassoon.snapshot.catalog import Catalog
    from usagebassoon.snapshot.format import timestamp

    details: list[str] = []
    warnings: list[str] = []
    try:
        archiver = SnapshotArchiver.from_config(configuration)
        rows = archiver.reader.listing()
        now = datetime.now(UTC)
        for bucket in archiver.reader.buckets:
            control, _ = Catalog(bucket).control()
            last_failure = control.get("last_failure")
            repair_warnings = control.get("repair_warnings")
            if isinstance(repair_warnings, list):
                warnings.extend(
                    f"archive repair warning: {value}" for value in repair_warnings
                )
            if isinstance(last_failure, dict):
                warnings.append(
                    f"last snapshot attempt failed at {bucket.uri} "
                    f"({last_failure.get('at')}): {last_failure.get('error')}"
                )
            available = [
                r
                for r in rows
                if str(r.get("uri", "")).startswith(bucket.uri + "/")
                and "error" not in r
            ]
            if not available:
                warnings.append(f"no complete recovery point at {bucket.uri}")
                continue
            points = [timestamp(r["captured_at"]) for r in available]
            verified = [
                str(r["verified_at"]) for r in available if r.get("verified_at")
            ]
            details.append(
                f"{bucket.uri}: {len(available)} copies, captures "
                f"{min(points).isoformat()} through {max(points).isoformat()}, "
                "last creation verification "
                f"{max(verified) if verified else 'unavailable'}"
            )
            weekly = not settings.schedule.disable_weekly
            interval = parse_interval(settings.schedule.interval)
            scheduled = [
                timestamp(r["captured_at"])
                for r in available
                if isinstance(r.get("roles"), list) and "scheduled" in r["roles"]
            ]
            if interval is not None:
                details.append(
                    f"{bucket.uri}: scheduled last capture "
                    f"{max(scheduled).isoformat() if scheduled else 'unavailable'}, "
                    f"interval {interval}"
                )
                if not scheduled or now > max(scheduled) + interval:
                    warnings.append(
                        f"scheduled recovery capture is overdue at {bucket.uri}; "
                        "check snapshot scheduler"
                    )
            if weekly:
                slots = {
                    str(r["weekly_slot"])
                    for r in available
                    if isinstance(r.get("roles"), list)
                    and "weekly" in r["roles"]
                    and r.get("weekly_slot")
                }
                current_slot = now.strftime("%G-W%V")
                weekly_points = [
                    timestamp(r["captured_at"])
                    for r in available
                    if isinstance(r.get("roles"), list) and "weekly" in r["roles"]
                ]
                details.append(
                    f"{bucket.uri}: weekly slots "
                    f"{', '.join(sorted(slots)) or 'none'}; "
                    f"{min(4, len(slots))}/4 recovery slots accumulated"
                )
                week_start = now.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) - timedelta(days=now.weekday())
                if current_slot not in slots and now >= week_start + timedelta(hours=1):
                    warnings.append(
                        f"current weekly recovery slot is overdue at {bucket.uri}"
                    )
                if (
                    len(slots) < 4
                    and weekly_points
                    and min(weekly_points) <= now - timedelta(weeks=4)
                ):
                    warnings.append(
                        f"weekly recovery coverage is incomplete at {bucket.uri}: "
                        f"{len(slots)}/4 slots"
                    )
        warnings.extend(str(r["error"]) for r in rows if "error" in r)
    except Exception as error:
        warnings.append(f"archive inspection unavailable: {error}")
    return DoctorCheck(
        "backup_freshness",
        "warning" if warnings else "ok",
        "; ".join(warnings) if warnings else "recovery points are available",
        tuple(details),
    )


def _row_value(row: dict[str, object], key: str) -> object | None:
    """Read a nullable value from an Arrow materialized row."""
    return row.get(key)


def _as_optional_string(value: object | None) -> str | None:
    """Convert a nullable database value to text."""
    return None if value is None else str(value)


def _as_required_string(value: object | None) -> str:
    """Convert a required database value to text."""
    if value is None:
        raise TypeError("required diagnostic value was null")
    return str(value)


def _as_optional_datetime(value: object | None) -> datetime | None:
    """Convert a nullable database value to a datetime."""
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"expected datetime, got {type(value).__name__}")
    return value


def _as_optional_int(value: object | None) -> int | None:
    """Convert a nullable database value to an integer."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (float, str)):
        return int(value)
    raise TypeError(f"expected integer-like value, got {type(value).__name__}")


def _as_required_datetime(value: object | None) -> datetime:
    """Convert a required database value to a datetime."""
    if not isinstance(value, datetime):
        raise TypeError("required diagnostic timestamp was null or invalid")
    return value


def _as_required_int(value: object | None) -> int:
    """Convert a required database value to an integer."""
    result = _as_optional_int(value)
    if result is None:
        raise TypeError("required diagnostic count was null")
    return result


def _materialized_rows(backend: StorageBackend, sql: str) -> list[dict[str, object]]:
    """Execute a query and normalize its rows for diagnostic parsing."""
    return [cast(dict[str, object], row) for row in backend.query(sql).to_pylist()]


def _capture_read[T](operation: Callable[[], T]) -> tuple[T | None, Exception | None]:
    """Return a concurrent diagnostic result without propagating its failure."""
    try:
        return operation(), None
    except Exception as error:
        return None, error


def _read_value[T](result: ReadResult[T]) -> T:
    """Return a captured diagnostic value or re-raise its recorded failure."""
    value, error = result
    if error is not None:
        raise error
    return cast(T, value)


def unresolved_schema_drift(
    backend: StorageBackend,
    *,
    limit: int = 20,
) -> tuple[SchemaDriftRecord, ...]:
    """Load unresolved current schema-drift events from a backend.

    Args:
        backend: Backend containing current schema-drift events.
        limit: Maximum number of newest events to load.

    Returns:
        Most recently updated unresolved events first.

    Raises:
        ValueError: If ``limit`` is not positive.
        Exception: If the backend cannot execute the diagnostic query.
    """
    if limit < 1:
        raise ValueError("limit must be positive")
    rows = _materialized_rows(
        backend,
        "SELECT domain, tokscale_ver, drift_key, drift_kind, path, detail, "
        "contract_tokscale_ver, created_at, collected_at, "
        "observation_count FROM open_schema_drift_events "
        f"ORDER BY collected_at DESC LIMIT {limit}",
    )
    return tuple(
        SchemaDriftRecord(
            domain=_as_required_string(_row_value(row, "domain")),
            tokscale_ver=_as_required_string(_row_value(row, "tokscale_ver")),
            drift_key=_as_required_string(_row_value(row, "drift_key")),
            drift_kind=_as_required_string(_row_value(row, "drift_kind")),
            path=_as_required_string(_row_value(row, "path")),
            detail=_as_required_string(_row_value(row, "detail")),
            contract_tokscale_ver=_as_required_string(
                _row_value(row, "contract_tokscale_ver")
            ),
            created_at=_as_required_datetime(_row_value(row, "created_at")),
            collected_at=_as_required_datetime(_row_value(row, "collected_at")),
            observation_count=_as_required_int(_row_value(row, "observation_count")),
        )
        for row in rows
    )


def reconciliation_issues(
    backend: StorageBackend, *, limit: int = 20
) -> tuple[reconcile.ReconciliationIssueRecord, ...]:
    """Load the latest distinct reconciliation assertion failures.

    Args:
        backend: Backend containing the reconciliation issue table.
        limit: Maximum issues to return.

    Returns:
        Latest issues by last detection time.
    """
    if limit < 1:
        raise ValueError("limit must be positive")
    rows = _materialized_rows(
        backend,
        "SELECT check_name, issue_key, message, created_at, collected_at, "
        "observation_count "
        "FROM open_reconciliation_issues "
        f"ORDER BY collected_at DESC NULLS LAST LIMIT {limit}",
    )
    return tuple(
        reconcile.ReconciliationIssueRecord(
            check_name=_as_required_string(row.get("check_name")),
            issue_key=_as_required_string(row.get("issue_key")),
            message=_as_optional_string(row.get("message")),
            created_at=_as_optional_datetime(row.get("created_at")),
            collected_at=_as_optional_datetime(row.get("collected_at")),
            observation_count=_as_required_int(_row_value(row, "observation_count")),
        )
        for row in rows
    )


def ingest_issues(
    backend: StorageBackend,
    *,
    limit: int = 20,
) -> tuple[ingest.IngestIssue, ...]:
    """Load recent collection runs with non-success statuses.

    Args:
        backend: Backend containing the ingest audit log.
        limit: Maximum number of runs to load.

    Returns:
        Recent failed, partial, or schema-drift runs first.

    Raises:
        ValueError: If ``limit`` is not positive.
    """
    if limit < 1:
        raise ValueError("limit must be positive")
    rows = _materialized_rows(
        backend,
        "SELECT run_id, finished_at, status "
        "FROM collection_runs "
        "WHERE status IN ('failed', 'partial', 'schema_drift') "
        "ORDER BY finished_at DESC NULLS LAST "
        f"LIMIT {limit}",
    )
    return tuple(
        ingest.IngestIssue(
            run_id=_as_required_string(_row_value(row, "run_id")),
            finished_at=_as_optional_datetime(_row_value(row, "finished_at")),
            status=_as_optional_string(_row_value(row, "status")),
        )
        for row in rows
    )


def run_doctor(
    backend: StorageBackend | None,
    *,
    backend_name: str,
    database: str | None,
    config_path: str | None = None,
    config_error: str | None = None,
    connection_error: str | None = None,
    snapshot_enabled: bool | None = None,
    snapshot_warnings: tuple[str, ...] = (),
    limit: int = 20,
    logger: logging.Logger | None = None,
) -> DoctorReport:
    """Run read-only diagnostics against a configured backend.

    Args:
        backend: Open backend, or ``None`` when connection setup failed.
        backend_name: Configured backend name.
        database: Configured database, dataset, or file path.
        config_path: User configuration path, when one was considered.
        config_error: Configuration parsing or validation error, if any.
        connection_error: Backend construction error, if any.
        snapshot_enabled: Whether optional snapshots are configured.
        snapshot_warnings: Advisories from configured snapshot storage.
        limit: Maximum number of drift and ingest records to display.
        logger: Optional logger used for recoverable diagnostic failures.

    Returns:
        Ordered diagnostic results. Backend exceptions are converted to checks
        so the CLI can report all available information in one invocation.
    """
    if limit < 1:
        raise ValueError("limit must be positive")

    active_logger = logger or _LOG

    checks: list[DoctorCheck] = []
    config_details = (f"path: {config_path}",) if config_path else ()
    if config_error:
        checks.append(
            DoctorCheck("configuration", "error", config_error, config_details)
        )
    elif not backend_name:
        checks.append(
            DoctorCheck("configuration", "error", "backend is not configured")
        )
    elif not database:
        checks.append(
            DoctorCheck(
                "configuration",
                "error",
                "database is not configured",
                config_details,
            )
        )
    else:
        checks.append(
            DoctorCheck(
                "configuration",
                "ok",
                f"backend={backend_name}, database={database}",
                config_details,
            )
        )

    if backend is None:
        checks.append(
            DoctorCheck(
                "backend",
                "error",
                connection_error or "backend could not be opened",
            )
        )
        return DoctorReport(tuple(checks))

    with ExitStack() as reads:
        try:
            backend = reads.enter_context(backend.consistent_read())
        except Exception as error:
            active_logger.exception("doctor snapshot initialization failed")
            checks.append(
                DoctorCheck("backend", "error", f"read snapshot failed: {error}")
            )
            return DoctorReport(tuple(checks))

        try:
            backend.query("SELECT 1 AS doctor_ok")
        except Exception as error:
            active_logger.exception("doctor connectivity check failed")
            checks.append(
                DoctorCheck("backend", "error", f"connectivity failed: {error}")
            )
            return DoctorReport(tuple(checks))
        checks.append(DoctorCheck("backend", "ok", "connection and query succeeded"))
        workers = backend.max_concurrent_queries
        if workers < 1:
            raise ValueError("backend query concurrency must be positive")

        def check_relation(table: str) -> str | None:
            try:
                backend.query(f"SELECT * FROM {table} LIMIT 0")
            except Exception:
                active_logger.exception("doctor schema check failed for %s", table)
                return table
            return None

        probe = " UNION ALL ".join(
            f"(SELECT '{table}' AS relation FROM {table} LIMIT 0)"
            for table in REQUIRED_RELATIONS
        )
        missing: list[str]
        try:
            backend.query(probe)
        except Exception:
            if workers > 1:
                with ThreadPoolExecutor(
                    max_workers=min(workers, len(REQUIRED_RELATIONS))
                ) as executor:
                    missing = [
                        table
                        for table in executor.map(check_relation, REQUIRED_RELATIONS)
                        if table is not None
                    ]
            else:
                missing = [
                    table
                    for table in (check_relation(name) for name in REQUIRED_RELATIONS)
                    if table is not None
                ]
        else:
            missing = []
        if missing:
            checks.append(
                DoctorCheck(
                    "schema",
                    "error",
                    "required tables or views are unavailable",
                    tuple(missing),
                )
            )
            return DoctorReport(tuple(checks))
        checks.append(
            DoctorCheck(
                "schema",
                "ok",
                f"all {len(REQUIRED_RELATIONS)} required tables and views exist",
            )
        )

        transactions_result: ReadResult[tuple[ActiveTransaction, ...]] | None = None
        backlog_result: ReadResult[pa.Table | None] | None = None
        drift_result: ReadResult[tuple[SchemaDriftRecord, ...]] | None = None
        ingest_result: ReadResult[tuple[ingest.IngestIssue, ...]] | None = None
        reconciliation_result: (
            ReadResult[tuple[reconcile.ReconciliationIssueRecord, ...]] | None
        ) = None
        if workers > 1:
            with ThreadPoolExecutor(max_workers=min(workers, 5)) as executor:
                transactions_future = executor.submit(
                    _capture_read, lambda: backend.active_transactions(limit)
                )
                backlog_future = executor.submit(
                    _capture_read, backend.compaction_backlog
                )
                drift_future = executor.submit(
                    _capture_read, lambda: unresolved_schema_drift(backend, limit=limit)
                )
                ingest_future = executor.submit(
                    _capture_read, lambda: ingest_issues(backend, limit=limit)
                )
                reconciliation_future = executor.submit(
                    _capture_read, lambda: reconciliation_issues(backend, limit=limit)
                )
                transactions_result = transactions_future.result()
                backlog_result = backlog_future.result()
                drift_result = drift_future.result()
                ingest_result = ingest_future.result()
                reconciliation_result = reconciliation_future.result()

        else:
            transactions_result = _capture_read(
                lambda: backend.active_transactions(limit)
            )
            backlog_result = _capture_read(backend.compaction_backlog)
            drift_result = _capture_read(
                lambda: unresolved_schema_drift(backend, limit=limit)
            )
            ingest_result = _capture_read(lambda: ingest_issues(backend, limit=limit))
            reconciliation_result = _capture_read(
                lambda: reconciliation_issues(backend, limit=limit)
            )

        try:
            transactions = _read_value(transactions_result)
        except Exception as error:
            active_logger.exception("doctor transaction inspection failed")
            checks.append(
                DoctorCheck(
                    "transactions",
                    "warning",
                    f"could not inspect active backend transactions: {error}",
                )
            )
        else:
            checks.append(
                DoctorCheck(
                    "transactions",
                    "warning" if transactions else "ok",
                    (
                        f"{len(transactions)} active warehouse transaction(s)"
                        if transactions
                        else "no active backend transactions"
                    ),
                    tuple(
                        f"job {transaction.job_id}; "
                        f"transaction {transaction.transaction_id}"
                        for transaction in transactions
                    ),
                )
            )

        try:
            backlog_table = _read_value(backlog_result)
        except Exception as error:
            checks.append(
                DoctorCheck(
                    "compaction",
                    "error",
                    f"could not read compaction progress: {error}",
                )
            )
        else:
            if backlog_table is not None:
                backlog = backlog_table.to_pylist()
                at_risk = any(row["age_days"] >= 80 for row in backlog)
                checks.append(
                    DoctorCheck(
                        "compaction",
                        "error" if at_risk else "warning" if backlog else "ok",
                        "uncompacted observations approach the 90-day retention limit"
                        if at_risk
                        else "nightly compaction is overdue"
                        if backlog
                        else "no overdue compaction buckets",
                        tuple(
                            f"{row['domain']}: {row['pending_rows']} "
                            "pending observation(s); "
                            f"arrival {row['arrival_day']} ({row['age_days']} days old)"
                            for row in backlog[:limit]
                        ),
                    )
                )

        try:
            drift = _read_value(drift_result)
        except Exception as error:
            active_logger.exception("doctor schema-drift inspection failed")
            checks.append(
                DoctorCheck(
                    "schema_drift",
                    "error",
                    f"could not read drift log: {error}",
                )
            )
        else:
            details = tuple(format_drift(event) for event in drift)
            if drift:
                details += (f"report unexpected drift: {DRIFT_ISSUE_URL}",)
            checks.append(
                DoctorCheck(
                    "schema_drift",
                    "warning" if drift else "ok",
                    (
                        f"{len(drift)} unresolved event(s)"
                        if drift
                        else "no unresolved events"
                    ),
                    details,
                )
            )

        try:
            issues = _read_value(ingest_result)
        except Exception as error:
            active_logger.exception("doctor ingest-run inspection failed")
            checks.append(
                DoctorCheck(
                    "collection_runs",
                    "error",
                    f"could not read run log: {error}",
                )
            )
        else:
            details = tuple(
                f"{issue.status or 'unknown'} run {issue.run_id}" for issue in issues
            )
            checks.append(
                DoctorCheck(
                    "collection_runs",
                    "warning" if issues else "ok",
                    (
                        f"{len(issues)} recent non-success run(s)"
                        if issues
                        else "recent runs are healthy"
                    ),
                    details,
                )
            )

        try:
            recorded = _read_value(reconciliation_result)
        except Exception as error:
            active_logger.exception("doctor reconciliation inspection failed")
            checks.append(
                DoctorCheck(
                    "reconciliation",
                    "error",
                    f"could not read reconciliation log: {error}",
                )
            )
        else:
            checks.append(
                DoctorCheck(
                    "reconciliation",
                    "warning" if recorded else "ok",
                    f"{len(recorded)} recorded issue(s)"
                    if recorded
                    else "no recorded issues",
                    tuple(
                        f"{item.check_name}/{item.issue_key}: {item.message or ''} "
                        f"({item.observation_count} observation(s); "
                        f"first {item.created_at or 'unknown'} "
                        f"latest {item.collected_at or 'unknown'} "
                        ")"
                        for item in recorded
                    ),
                )
            )

        if snapshot_enabled is None:
            checks.append(
                DoctorCheck(
                    "snapshots",
                    "warning",
                    "snapshot configuration was not inspected",
                )
            )
        elif snapshot_enabled:
            checks.append(
                DoctorCheck(
                    "snapshots",
                    "warning" if snapshot_warnings else "ok",
                    "snapshot configuration is enabled",
                    snapshot_warnings,
                )
            )
        else:
            checks.append(
                DoctorCheck("snapshots", "ok", "snapshots are disabled (optional)")
            )

        return DoctorReport(tuple(checks))


def format_checks(checks: Sequence[DoctorCheck]) -> tuple[str, ...]:
    """Format checks without coupling the core diagnostics to Rich.

    Args:
        checks: Checks to format.

    Returns:
        One plain-text line per check and detail.
    """
    lines: list[str] = []
    for check in checks:
        lines.append(f"{check.status.upper()} {check.name}: {check.message}")
        lines.extend(f"  {detail}" for detail in check.details)
    return tuple(lines)
