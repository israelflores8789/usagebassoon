# SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
# SPDX-License-Identifier: AGPL-3.0-only

"""storage_model.py — Shared identities and observation ordering contracts."""

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


def observation_order(table: str, prefix: str = "") -> str:
    """Return the shared latest-observation ordering for one relation."""
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
