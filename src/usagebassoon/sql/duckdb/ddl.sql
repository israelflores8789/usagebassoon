-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- usagebassoon warehouse DDL for DuckDB and MotherDuck.
-- Arrow owns cross-backend normalization. Usage facts are current-state
-- upserts: new natural keys are inserted, changed keys update in place,
-- and payload absent observations are never deleted.

-- Source leases serialize collection planning and persistence across processes.
-- The bootstrap row serializes first-time source provisioning on BigQuery.
CREATE TABLE IF NOT EXISTS source_leases (
    source_id TEXT PRIMARY KEY,
    owner_id TEXT,
    run_id TEXT,
    fence BIGINT NOT NULL,
    lease_expires_at TIMESTAMPTZ,
    last_renewed_at TIMESTAMPTZ
);

-- Metadata about each ingest information about the collection environment.
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    host TEXT,
    os_name TEXT,
    os_version TEXT,
    architecture TEXT,
    cpu_model TEXT,
    cpu_count INTEGER,
    memory_bytes BIGINT,
    shell TEXT,
    tokscale_ver TEXT,
    status TEXT,
    rows_in INTEGER,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    drift_events INTEGER
);

-- Artifacts of tokscale payload drift events from the expected tokscale schema.
-- Reported in `bassoon doctor`.
CREATE TABLE IF NOT EXISTS schema_drift_events (
    source_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    tokscale_ver TEXT NOT NULL,
    drift_key TEXT NOT NULL,
    drift_kind TEXT NOT NULL,
    path TEXT NOT NULL,
    detail TEXT NOT NULL,
    contract_tokscale_ver TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    detected_run_id TEXT NOT NULL,
    updated_run_id TEXT NOT NULL,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    observation_count BIGINT NOT NULL,
    PRIMARY KEY (source_id, domain, tokscale_ver, drift_key)
);

-- Holds metadata about each usage session typically from `tokscale report`.
CREATE TABLE IF NOT EXISTS sessions (
    source_id TEXT NOT NULL,
    client TEXT NOT NULL,
    session_id TEXT NOT NULL,
    workspace TEXT,
    workspace_label TEXT,
    created_at TIMESTAMPTZ,
    last_active TIMESTAMPTZ,
    duration_minutes INTEGER,
    message_count BIGINT,
    tokscale_cost_usd DOUBLE,
    models_used TEXT[],
    session_label TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, client, session_id)
);

-- Date-filtered usage facts from `tokscale models` at the session×model grain.
CREATE TABLE IF NOT EXISTS daily_stats (
    source_id TEXT NOT NULL,
    day DATE NOT NULL,
    client TEXT NOT NULL,
    session_id TEXT NOT NULL,
    model TEXT NOT NULL,
    provider TEXT,
    input_tokens BIGINT,
    output_tokens BIGINT,
    cache_read BIGINT,
    cache_write BIGINT,
    reasoning BIGINT,
    total_tokens BIGINT NOT NULL,
    message_count BIGINT,
    tokscale_cost_usd DOUBLE,
    perf_duration_ms BIGINT,
    perf_timed_tokens BIGINT,
    perf_sample_count BIGINT,
    perf_token_coverage DOUBLE,
    tokscale_ms_per_1k_tokens DOUBLE,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, day, client, session_id, model)
);

-- Activity reported by `tokscale graph`.
-- Used to identify candidate days for usage history collection.
CREATE TABLE IF NOT EXISTS daily_activity (
    source_id TEXT NOT NULL,
    day DATE NOT NULL,
    intensity INTEGER,
    active_time_ms BIGINT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, day)
);

-- Snapshots of rates for models in use at the daily grain, typically
-- from `tokscale pricing`. Allows for historical cost accuracy.
CREATE TABLE IF NOT EXISTS price_versions (
    source_id TEXT NOT NULL,
    day DATE NOT NULL,
    model TEXT NOT NULL,
    source TEXT NOT NULL,
    matched_key TEXT,
    match_kind TEXT,
    price_input_per_token DOUBLE,
    price_output_per_token DOUBLE,
    price_cache_read_per_token DOUBLE,
    price_cache_write_per_token DOUBLE,
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, day, model)
);

-- A ledger of days where token usage history has already been collected and
-- a status of the collection's success state. This table allows for tolerant
-- collection that prioritizes token usage data over metadata. It also makes
-- collection calls more efficient by providing the intelligence to not have
-- to ask tokscale for all usage history every call.
CREATE TABLE IF NOT EXISTS ingest_status (
    source_id TEXT NOT NULL,
    day DATE NOT NULL,
    domain TEXT NOT NULL,
    status TEXT NOT NULL,
    expected_count BIGINT,
    succeeded_count BIGINT,
    last_attempted_run TEXT NOT NULL,
    last_succeeded_run TEXT,
    failure_code TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, day, domain)
);

-- Artifacts of unexpected math errors when performing reconciliation checks on
-- the usage data. Reported in `bassoon doctor`.
-- `check_name` is the name of the reconciliation check that failed.
-- `issue_key` is the specific issue that was detected within that check.
CREATE TABLE IF NOT EXISTS reconciliation_issues (
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    issue_key TEXT NOT NULL,
    message TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    detected_run_id TEXT,
    updated_run_id TEXT,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    observation_count BIGINT NOT NULL
);

-- Table of user-curated tags across usage data.
CREATE TABLE IF NOT EXISTS tags (
    scope TEXT NOT NULL CHECK (scope IN ('client', 'workspace', 'session')),
    source_id TEXT NOT NULL,
    client TEXT NOT NULL DEFAULT '',
    workspace TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL DEFAULT '',
    tag TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, scope, client, workspace, session_id, tag),
    CHECK (
        (scope = 'client' AND client <> '' AND workspace = '' AND session_id = '')
        OR (scope = 'workspace' AND client = '' AND workspace <> '' AND session_id = '')
        OR (scope = 'session' AND client <> '' AND workspace = '' AND session_id <> '')
    )
);

-- Table of user-curated notes across usage data.
CREATE TABLE IF NOT EXISTS notes (
    source_id TEXT NOT NULL,
    client TEXT NOT NULL,
    session_id TEXT NOT NULL,
    note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, client, session_id)
);
