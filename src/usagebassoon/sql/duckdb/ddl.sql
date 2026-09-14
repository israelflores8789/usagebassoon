-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- usagebassoon warehouse DDL for DuckDB and MotherDuck.
--
-- Arrow owns cross-backend normalization and validation. For example, the
-- SQL backends store run_id as TEXT instead of UUID so DuckDB/MotherDuck
-- and BigQuery paths receive the same canonical value.
--
-- Fact tables use current-state upserts: an existing natural key is updated
-- in place, a new natural key is inserted, and absent input rows are never
-- deleted. last_updated_at records the last material change, not collection
-- freshness or a version key.
-- Normalized table snapshots provide restore capability; raw tokscale
-- JSON is intentionally not stored on every ingest.

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    host            TEXT,
    tokscale_ver    TEXT,
    status          TEXT,             -- ok | partial | schema_drift | failed
    rows_in         INTEGER,
    rows_inserted   INTEGER,          -- new natural keys inserted this run
    rows_updated    INTEGER,          -- existing natural keys overwritten
    drift_events    INTEGER
);

-- Non-fatal schema contract deviations observed during collection.
-- Arrow validates UUID-shaped run_id values before they reach this table.
CREATE TABLE IF NOT EXISTS schema_drift (
    drift_id        TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL,
    payload_kind    TEXT,
    drift_kind      TEXT,             -- unknown_field | missing_field | type_change
    path            TEXT,
    detail          TEXT,
    tokscale_ver    TEXT,
    resolved        BOOLEAN DEFAULT FALSE
);

-- Session dimension from `tokscale report --json --no-summarize`.
-- One current row exists per (client, session_id). Stable fields are kept;
-- tokscale-generated summary fields are intentionally excluded. session_label
-- is derived deterministically in Arrow for backend portability.
CREATE TABLE IF NOT EXISTS sessions (
    client          TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    workspace       TEXT,
    workspace_label TEXT,
    created_at      TIMESTAMPTZ,
    last_active     TIMESTAMPTZ,
    duration_minutes INTEGER,
    message_count   BIGINT,
    cost_usd        DOUBLE,
    models_used     TEXT[],
    session_label   TEXT,
    first_seen_at   TIMESTAMPTZ NOT NULL,
    last_seen_at    TIMESTAMPTZ NOT NULL,
    last_updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (client, session_id)
);

-- Fact table from `tokscale models --json --group-by client,session,model`.
-- Values are cumulative for each natural key and are overwritten by a newer
-- observation. Point-in-time pricing is embedded so each current row remains
-- self-contained. total_tokens is computed in Arrow, not by DuckDB, so both
-- SQL dialects receive the same normalized value.
CREATE TABLE IF NOT EXISTS session_model_stats (
    client          TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    model           TEXT NOT NULL,
    provider        TEXT,
    input_tokens    BIGINT,
    output_tokens   BIGINT,
    cache_read      BIGINT,
    cache_write     BIGINT,
    reasoning       BIGINT,
    total_tokens    BIGINT NOT NULL,
    message_count   BIGINT,
    cost_usd        DOUBLE,
    ms_per_1k_tokens    DOUBLE,
    perf_duration_ms    BIGINT,
    perf_token_coverage DOUBLE,
    price_input_per_token       DOUBLE,
    price_output_per_token      DOUBLE,
    price_cache_read_per_token  DOUBLE,
    price_cache_write_per_token DOUBLE,
    price_matched_key   TEXT,
    price_match_kind    TEXT,
    price_alias_applied BOOLEAN,
    price_source        TEXT,
    price_captured_at   TIMESTAMPTZ,
    first_seen_at       TIMESTAMPTZ NOT NULL,
    last_seen_at        TIMESTAMPTZ NOT NULL,
    last_updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (client, session_id, model)
);

-- Daily fact from `tokscale graph` contributions[]. One current row exists
-- per (day, client, model); newer observations overwrite that natural key.
CREATE TABLE IF NOT EXISTS daily_stats (
    day               DATE NOT NULL,
    client            TEXT NOT NULL,
    model             TEXT NOT NULL,
    provider          TEXT,
    input_tokens      BIGINT,
    output_tokens     BIGINT,
    cache_read        BIGINT,
    cache_write       BIGINT,
    reasoning         BIGINT,
    message_count     BIGINT,
    cost_usd          DOUBLE,
    last_updated_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (day, client, model)
);

-- Day-level activity from contributions[].
CREATE TABLE IF NOT EXISTS daily_activity (
    day               DATE PRIMARY KEY,
    intensity         INTEGER,
    active_time_ms    BIGINT,
    last_updated_at   TIMESTAMPTZ NOT NULL
);

-- Historical point-in-time rates resolved by tokscale. Unlike current-state
-- usage facts, pricing history is intentionally append-mostly.
CREATE TABLE IF NOT EXISTS pricing_snapshots (
    captured_at     TIMESTAMPTZ NOT NULL,
    model           TEXT NOT NULL,
    source          TEXT NOT NULL,
    matched_key     TEXT,
    match_kind      TEXT,
    price_input_per_token       DOUBLE,
    price_output_per_token      DOUBLE,
    price_cache_read_per_token  DOUBLE,
    price_cache_write_per_token DOUBLE,
    PRIMARY KEY (captured_at, model)
);

-- Run-level aggregate telemetry from graph summary and timeMetrics.
-- One row is retained for each collection run.
CREATE TABLE IF NOT EXISTS run_metrics (
    run_id                 TEXT PRIMARY KEY,
    captured_at            TIMESTAMPTZ NOT NULL,
    total_tokens           BIGINT,
    total_cost             DOUBLE,
    active_days            INTEGER,
    total_active_time_ms   BIGINT,
    longest_continuous_ms  BIGINT,
    max_concurrent_sessions INTEGER,
    graph_session_count    INTEGER
);

-- Non-fatal cross-payload reconciliation observations per run.
CREATE TABLE IF NOT EXISTS reconciliation_issues (
    run_id          TEXT NOT NULL,
    check_name      TEXT,
    issue_key       TEXT,
    message         TEXT
);

-- User curation: a client-scoped tag uses an empty session_id; a session-
-- scoped tag names one session. Tags are plaintext by definition.
CREATE TABLE IF NOT EXISTS tags (
    scope           TEXT NOT NULL CHECK (scope IN ('client', 'session')),
    client          TEXT NOT NULL,
    session_id      TEXT NOT NULL DEFAULT '',
    tag             TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (scope, client, session_id, tag)
);

-- User curation: one editable free-text note per session.
CREATE TABLE IF NOT EXISTS notes (
    client          TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    note            TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (client, session_id)
);
