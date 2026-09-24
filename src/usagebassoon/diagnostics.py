# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""diagnostics.py — Read-only health queries and check reporting."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast

from usagebassoon.backends.base import StorageBackend
from usagebassoon.drift import SchemaDriftRecord, format_drift

CheckStatus = Literal["ok", "warning", "error"]
DRIFT_ISSUE_URL = "https://github.com/israelflores8789/usagebassoon/issues/new"
_LOG = logging.getLogger("usagebassoon")

REQUIRED_RELATIONS: tuple[str, ...] = (
    "ingest_runs",
    "schema_drift_events",
    "sessions",
    "session_model_stats",
    "daily_stats",
    "daily_activity",
    "price_versions",
    "ingest_status",
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
class IngestIssue:
    """A collection run whose persisted status needs attention.

    Attributes:
        run_id: Collection run identifier.
        finished_at: Collection completion time, when available.
        status: Persisted collection status.
        drift_events: Number of drift events recorded for the run.
    """

    run_id: str
    finished_at: datetime | None
    status: str | None
    drift_events: int | None


@dataclass(frozen=True, slots=True)
class ReconciliationIssueRecord:
    """One persisted assertion failure and its detection history."""

    check_name: str
    issue_key: str
    message: str | None
    created_at: datetime | None
    updated_at: datetime | None
    detected_run_id: str | None
    updated_run_id: str | None
    observation_count: int


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
        "contract_tokscale_ver, created_at, updated_at, detected_run_id, "
        "updated_run_id, observation_count FROM schema_drift_events "
        "WHERE resolved = FALSE "
        f"ORDER BY updated_at DESC LIMIT {limit}",
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
            updated_at=_as_required_datetime(_row_value(row, "updated_at")),
            detected_run_id=_as_required_string(_row_value(row, "detected_run_id")),
            updated_run_id=_as_required_string(_row_value(row, "updated_run_id")),
            observation_count=_as_required_int(_row_value(row, "observation_count")),
        )
        for row in rows
    )


def reconciliation_issues(
    backend: StorageBackend, *, limit: int = 20
) -> tuple[ReconciliationIssueRecord, ...]:
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
        "SELECT check_name, issue_key, message, created_at, updated_at, "
        "detected_run_id, updated_run_id, observation_count "
        "FROM reconciliation_issues WHERE resolved = FALSE "
        f"ORDER BY updated_at DESC NULLS LAST LIMIT {limit}",
    )
    return tuple(
        ReconciliationIssueRecord(
            check_name=_as_required_string(row.get("check_name")),
            issue_key=_as_required_string(row.get("issue_key")),
            message=_as_optional_string(row.get("message")),
            created_at=_as_optional_datetime(row.get("created_at")),
            updated_at=_as_optional_datetime(row.get("updated_at")),
            detected_run_id=_as_optional_string(row.get("detected_run_id")),
            updated_run_id=_as_optional_string(row.get("updated_run_id")),
            observation_count=_as_required_int(_row_value(row, "observation_count")),
        )
        for row in rows
    )


def ingest_issues(
    backend: StorageBackend,
    *,
    limit: int = 20,
) -> tuple[IngestIssue, ...]:
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
        "SELECT run_id, finished_at, status, drift_events "
        "FROM ingest_runs "
        "WHERE status IN ('failed', 'partial', 'schema_drift') "
        "ORDER BY finished_at DESC NULLS LAST "
        f"LIMIT {limit}",
    )
    return tuple(
        IngestIssue(
            run_id=_as_required_string(_row_value(row, "run_id")),
            finished_at=_as_optional_datetime(_row_value(row, "finished_at")),
            status=_as_optional_string(_row_value(row, "status")),
            drift_events=_as_optional_int(_row_value(row, "drift_events")),
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

    try:
        backend.query("SELECT 1 AS doctor_ok")
    except Exception as error:
        active_logger.exception("doctor connectivity check failed")
        checks.append(DoctorCheck("backend", "error", f"connectivity failed: {error}"))
        return DoctorReport(tuple(checks))
    checks.append(DoctorCheck("backend", "ok", "connection and query succeeded"))

    missing: list[str] = []
    for table in REQUIRED_RELATIONS:
        try:
            backend.query(f"SELECT * FROM {table} LIMIT 0")
        except Exception:
            active_logger.exception("doctor schema check failed for %s", table)
            missing.append(table)
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

    if backend_name == "bigquery":
        try:
            transactions = backend.active_transactions(limit)
        except Exception as error:
            active_logger.exception("doctor transaction inspection failed")
            checks.append(
                DoctorCheck(
                    "transactions",
                    "warning",
                    f"could not inspect active BigQuery transactions: {error}",
                )
            )
        else:
            checks.append(
                DoctorCheck(
                    "transactions",
                    "warning" if transactions else "ok",
                    (
                        f"{len(transactions)} active transaction(s) may delay "
                        "collection"
                        if transactions
                        else "no active BigQuery transactions for this dataset"
                    ),
                    tuple(
                        f"job {transaction.job_id}; "
                        f"transaction {transaction.transaction_id}"
                        for transaction in transactions
                    ),
                )
            )

    try:
        drift = unresolved_schema_drift(backend, limit=limit)
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
        issues = ingest_issues(backend, limit=limit)
    except Exception as error:
        active_logger.exception("doctor ingest-run inspection failed")
        checks.append(
            DoctorCheck(
                "ingest_runs",
                "error",
                f"could not read run log: {error}",
            )
        )
    else:
        details = tuple(
            f"{issue.status or 'unknown'} run {issue.run_id}"
            + (f" ({issue.drift_events} drift event(s))" if issue.drift_events else "")
            for issue in issues
        )
        checks.append(
            DoctorCheck(
                "ingest_runs",
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
        recorded = reconciliation_issues(backend, limit=limit)
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
                    f"in run {item.detected_run_id or 'unknown'}; "
                    f"latest {item.updated_at or 'unknown'} "
                    f"in run {item.updated_run_id or 'unknown'})"
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
