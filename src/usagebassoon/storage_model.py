# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""storage_model.py — Shared identities and observation ordering contracts."""

import base64
import json
import re
from collections.abc import Mapping
from datetime import datetime
from typing import cast
from uuid import NAMESPACE_URL, UUID, uuid5

import pyarrow as pa

DATA_SCHEMA_VERSION = 1

# Only observations winning the shared freshness ordering may advance tokens.
TOKEN_COMPONENTS = (
    "input_tokens",
    "output_tokens",
    "cache_read",
    "cache_write",
    "reasoning",
)
TOKEN_FIELDS = (*TOKEN_COMPONENTS, "total_tokens")


def token_total_sql(components: Mapping[str, str], minimum: str) -> str:
    """Build a total that preserves recorded usage and component maxima.

    Args:
        components: Trusted SQL expressions for every token component.
        minimum: Trusted SQL expression for the greatest recorded total.

    Returns:
        The greater of the recorded total and the sum of retained components.
    """
    total = " + ".join(
        f"COALESCE({components[field]}, 0)" for field in TOKEN_COMPONENTS
    )
    return f"GREATEST({minimum}, {total})"


def preserve_daily_tokens(incoming: pa.Table, current: pa.Table) -> pa.Table:
    """Prepare freshness-winning token observations before immutable publication.

    Args:
        incoming: Newly normalized daily observations with stable event IDs.
        current: Source-scoped canonical facts captured during preflight.

    Returns:
        Winning observations retaining token maxima, with original metadata.
    """
    keys = STATE_KEYS["daily_stats"]
    prior_rows: list[dict[str, object]] = current.to_pylist()
    prior = {tuple(row[field] for field in keys): row for row in prior_rows}
    rows: list[dict[str, object]] = incoming.to_pylist()
    accepted: list[dict[str, object]] = []
    for row in rows:
        previous = prior.get(tuple(row[field] for field in keys))
        if previous is not None:
            order = tuple(
                row[field] for field in ("collected_at", "total_tokens", "event_id")
            )
            previous_order = tuple(
                previous[field]
                for field in ("collected_at", "total_tokens", "event_id")
            )
            if cast(tuple[datetime, int, str], order) <= cast(
                tuple[datetime, int, str], previous_order
            ):
                continue
            for field in TOKEN_COMPONENTS:
                row[field] = max(cast(int, row[field]), cast(int, previous[field] or 0))
            row["total_tokens"] = max(
                cast(int, previous["total_tokens"]),
                cast(int, row["total_tokens"]),
                sum(cast(int, row[field]) for field in TOKEN_COMPONENTS),
            )
        accepted.append(row)
    return pa.Table.from_pylist(accepted, schema=incoming.schema)


STATE_KEYS: dict[str, tuple[str, ...]] = {
    "sessions": ("source_id", "client", "session_id"),
    "daily_stats": ("source_id", "day", "client", "session_id", "model"),
    "price_versions": ("source_id", "day", "model"),
    "tags": ("scope", "client", "workspace", "session_id", "tag"),
    "notes": ("source_id", "client", "session_id"),
}
EVENT_KEYS: dict[str, tuple[str, ...]] = {
    "collection_ledger": ("source_id", "run_id", "day", "domain"),
    "schema_drift_events": ("source_id", "domain", "tokscale_ver", "drift_key"),
    "reconciliation_issues": ("source_id", "check_name", "issue_key"),
}
DEBUG_TABLES = frozenset({"schema_drift_events", "reconciliation_issues"})
SNAPSHOT_TABLES = tuple(STATE_KEYS) + tuple(EVENT_KEYS)


def note_id_for_session(source_id: str, client: str, session_id: str) -> str:
    """Derive a lossless 24-character UUIDv5 handle from the exact note key.

    JSON array encoding distinguishes keys containing delimiters or Unicode.
    The fixed namespace and name prefix separate notes from other UUID users.
    This handle never replaces the natural key for observation deduplication.
    """
    name = json.dumps(
        ["usagebassoon", "notes", source_id, client, session_id],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return "n_" + base64.urlsafe_b64encode(
        uuid5(NAMESPACE_URL, name).bytes
    ).decode().rstrip("=")


def normalize_note_id(value: str) -> str:
    """Validate a compact or standard UUIDv5 note ID and return its compact form."""
    if value.startswith("n_"):
        value = value[2:]
    if re.fullmatch(r"[A-Za-z0-9_-]{22}", value):
        identifier = UUID(bytes=base64.urlsafe_b64decode(value + "=="))
        compact = base64.urlsafe_b64encode(identifier.bytes).decode().rstrip("=")
        if compact != value:
            raise ValueError("note ID has a noncanonical compact encoding")
    else:
        try:
            identifier = UUID(value)
        except ValueError as error:
            raise ValueError("note ID must be a compact or standard UUIDv5") from error
        compact = base64.urlsafe_b64encode(identifier.bytes).decode().rstrip("=")
    if identifier.version != 5:
        raise ValueError("note ID must be a UUIDv5")
    return "n_" + compact


def observation_order(table: str, prefix: str = "") -> str:
    """Return the shared freshness ordering for accepting observations."""
    fields = [f"{prefix}collected_at DESC"]
    if table in {"tags", "notes"}:
        fields.append(f"({prefix}op = 'upsert') DESC")
    elif table in DEBUG_TABLES:
        fields.append(f"{prefix}resolved DESC")
    elif table == "daily_stats":
        fields.append(f"{prefix}total_tokens DESC")
    elif table == "price_versions":
        fields.append(f"{prefix}price_output_per_token DESC NULLS LAST")
    elif table == "collection_ledger":
        fields.append(f"{prefix}finished_at DESC NULLS LAST")
    fields.append(f"{prefix}event_id DESC")
    return ", ".join(fields)


def newer_observation(table: str) -> str:
    """Return a null-safe predicate for a winning DuckDB observation."""
    fields = ["collected_at"]
    if table in {"tags", "notes"}:
        fields.append("op = 'upsert'")
    elif table == "daily_stats":
        fields.append("total_tokens")
    elif table == "price_versions":
        fields.extend(
            [
                "price_output_per_token IS NOT NULL",
                "COALESCE(price_output_per_token, 0.0)",
            ]
        )
    fields.append("event_id")

    def qualified(alias: str) -> str:
        """Qualify the fields in one lexicographic ordering tuple."""
        values = []
        for field in fields:
            if field.startswith("NOT "):
                values.append(f"NOT {alias}.{field[4:]}")
            elif field.startswith("COALESCE("):
                values.append(field.replace("COALESCE(", f"COALESCE({alias}."))
            else:
                values.append(f"{alias}.{field}")
        return "(" + ", ".join(values) + ")"

    return f"{qualified('source')} > {qualified('target')}"


_TIMESTAMP = pa.timestamp("us", tz="UTC")

CANONICAL_TABLE_SCHEMAS: dict[str, pa.Schema] = {
    "sessions": pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("client", pa.string(), nullable=False),
            pa.field("session_id", pa.string(), nullable=False),
            pa.field("workspace", pa.string()),
            pa.field("workspace_label", pa.string()),
            pa.field("created_at", _TIMESTAMP),
            pa.field("last_active", _TIMESTAMP),
            pa.field("duration_minutes", pa.int64()),
            pa.field("message_count", pa.int64()),
            pa.field("tokscale_cost_usd", pa.float64()),
            pa.field("models_used", pa.list_(pa.string())),
            pa.field("session_label", pa.string()),
            pa.field("first_seen_at", _TIMESTAMP, nullable=False),
            pa.field("last_seen_at", _TIMESTAMP, nullable=False),
            pa.field("collected_at", _TIMESTAMP, nullable=False),
        ]
    ),
    "daily_stats": pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("day", pa.date32(), nullable=False),
            pa.field("client", pa.string(), nullable=False),
            pa.field("session_id", pa.string(), nullable=False),
            pa.field("model", pa.string(), nullable=False),
            pa.field("provider", pa.string()),
            pa.field("input_tokens", pa.int64()),
            pa.field("output_tokens", pa.int64()),
            pa.field("cache_read", pa.int64()),
            pa.field("cache_write", pa.int64()),
            pa.field("reasoning", pa.int64()),
            pa.field("total_tokens", pa.int64(), nullable=False),
            pa.field("message_count", pa.int64()),
            pa.field("tokscale_cost_usd", pa.float64()),
            pa.field("perf_duration_ms", pa.int64()),
            pa.field("perf_timed_tokens", pa.int64()),
            pa.field("perf_sample_count", pa.int64()),
            pa.field("perf_token_coverage", pa.float64()),
            pa.field("tokscale_ms_per_1k_tokens", pa.float64()),
            pa.field("collected_at", _TIMESTAMP, nullable=False),
        ]
    ),
    "price_versions": pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("day", pa.date32(), nullable=False),
            pa.field("model", pa.string(), nullable=False),
            pa.field("source", pa.string(), nullable=False),
            pa.field("matched_key", pa.string()),
            pa.field("match_kind", pa.string()),
            pa.field("price_input_per_token", pa.float64()),
            pa.field("price_output_per_token", pa.float64()),
            pa.field("price_cache_read_per_token", pa.float64()),
            pa.field("price_cache_write_per_token", pa.float64()),
            pa.field("collected_at", _TIMESTAMP, nullable=False),
        ]
    ),
    "collection_ledger": pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("day", pa.date32(), nullable=False),
            pa.field("domain", pa.string(), nullable=False),
            pa.field("expected_count", pa.int64()),
            pa.field("succeeded_count", pa.int64()),
            pa.field("failure_code", pa.string()),
            pa.field("collected_at", _TIMESTAMP, nullable=False),
            pa.field("started_at", _TIMESTAMP, nullable=False),
            pa.field("finished_at", _TIMESTAMP),
            pa.field("host", pa.string()),
            pa.field("os_name", pa.string()),
            pa.field("os_version", pa.string()),
            pa.field("architecture", pa.string()),
            pa.field("cpu_model", pa.string()),
            pa.field("cpu_count", pa.int64()),
            pa.field("memory_bytes", pa.int64()),
            pa.field("invoke_method", pa.string()),
            pa.field("tokscale_ver", pa.string()),
            pa.field("status", pa.string()),
        ]
    ),
    "reconciliation_issues": pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("check_name", pa.string(), nullable=False),
            pa.field("issue_key", pa.string(), nullable=False),
            pa.field("message", pa.string()),
            pa.field("created_at", _TIMESTAMP),
            pa.field("collected_at", _TIMESTAMP, nullable=False),
            pa.field("resolved", pa.bool_(), nullable=False),
            pa.field("observation_count", pa.int64(), nullable=False),
        ]
    ),
    "schema_drift_events": pa.schema(
        [
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("domain", pa.string(), nullable=False),
            pa.field("tokscale_ver", pa.string(), nullable=False),
            pa.field("drift_key", pa.string(), nullable=False),
            pa.field("drift_kind", pa.string(), nullable=False),
            pa.field("path", pa.string(), nullable=False),
            pa.field("detail", pa.string(), nullable=False),
            pa.field("contract_tokscale_ver", pa.string(), nullable=False),
            pa.field("created_at", _TIMESTAMP, nullable=False),
            pa.field("collected_at", _TIMESTAMP, nullable=False),
            pa.field("resolved", pa.bool_(), nullable=False),
            pa.field("observation_count", pa.int64(), nullable=False),
        ]
    ),
}
CANONICAL_TABLE_SCHEMAS.update(
    {
        "tags": pa.schema(
            [
                pa.field("event_id", pa.string(), nullable=False),
                pa.field("scope", pa.string(), nullable=False),
                pa.field("source_id", pa.string(), nullable=False),
                pa.field("client", pa.string(), nullable=False),
                pa.field("workspace", pa.string(), nullable=False),
                pa.field("session_id", pa.string(), nullable=False),
                pa.field("tag", pa.string(), nullable=False),
                pa.field("created_at", _TIMESTAMP, nullable=False),
                pa.field("updated_at", _TIMESTAMP, nullable=False),
                pa.field("collected_at", _TIMESTAMP, nullable=False),
                pa.field("op", pa.string(), nullable=False),
                pa.field("op_id", pa.string()),
            ]
        ),
        "notes": pa.schema(
            [
                pa.field("event_id", pa.string(), nullable=False),
                pa.field("note_id", pa.string(), nullable=False),
                pa.field("source_id", pa.string(), nullable=False),
                pa.field("client", pa.string(), nullable=False),
                pa.field("session_id", pa.string(), nullable=False),
                pa.field("note", pa.string(), nullable=False),
                pa.field("created_at", _TIMESTAMP, nullable=False),
                pa.field("updated_at", _TIMESTAMP, nullable=False),
                pa.field("collected_at", _TIMESTAMP, nullable=False),
                pa.field("op", pa.string(), nullable=False),
                pa.field("op_id", pa.string()),
            ]
        ),
    }
)
