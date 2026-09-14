-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- usagebassoon warehouse DDL - BigQuery Standard SQL dialect.
--
-- Mirrored from duckdb/ddl.sql under the mapping:
--   TEXT -> STRING,  INTEGER/BIGINT -> INT64,  DOUBLE -> FLOAT64,
--   TIMESTAMPTZ -> TIMESTAMP,  BOOLEAN -> BOOL,  TEXT[] -> ARRAY<STRING>.
-- BigQuery does not enforce primary keys or CHECK constraints here;
-- uniqueness comes from the current-state merge logic. There are no generated
-- columns: Arrow computes derived columns so both backends receive identical
-- normalized values.
--
-- Fact tables use current-state upserts: existing natural keys are updated in
-- place, new natural keys are inserted, and absent input rows are never
-- deleted. Normalized table snapshots provide restore capability; raw tokscale
-- JSON is intentionally not stored on every ingest.

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id          STRING NOT NULL,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    host            STRING,
    tokscale_ver    STRING,
    status          STRING,             -- ok | partial | schema_drift | failed
    rows_in         INT64,
    rows_inserted   INT64,              -- new natural keys inserted this run
    rows_updated    INT64,              -- existing natural keys overwritten
    drift_events    INT64
);

CREATE TABLE IF NOT EXISTS schema_drift (
    drift_id        STRING NOT NULL,
    run_id          STRING NOT NULL,
    detected_at     TIMESTAMP NOT NULL,
    payload_kind    STRING,
    drift_kind      STRING,             -- unknown_field | missing_field | type_change
    path            STRING,
    detail          STRING,
    tokscale_ver    STRING,
    resolved        BOOL DEFAULT FALSE
);

-- Session dimension from `tokscale report --json --no-summarize`.
-- One current row exists per (client, session_id). Stable fields are kept;
-- tokscale-generated summary fields are intentionally excluded. session_label
-- is derived deterministically in Arrow for backend portability.
CREATE TABLE IF NOT EXISTS sessions (
    client          STRING NOT NULL,
    session_id      STRING NOT NULL,
    workspace       STRING,
    workspace_label STRING,
    created_at      TIMESTAMP,
    last_active     TIMESTAMP,
    duration_minutes INT64,
    message_count   INT64,
    cost_usd        FLOAT64,
    models_used     ARRAY<STRING>,
    session_label   STRING,
    first_seen_at   TIMESTAMP NOT NULL,
    last_seen_at    TIMESTAMP NOT NULL,
    last_updated_at TIMESTAMP NOT NULL
);

-- Fact table from `tokscale models --json --group-by client,session,model`.
-- Values are cumulative for each natural key and are overwritten by a newer
-- observation. Point-in-time pricing is embedded so each current row remains
-- self-contained. total_tokens is computed in Arrow for backend portability.
CREATE TABLE IF NOT EXISTS session_model_stats (
    client          STRING NOT NULL,
    session_id      STRING NOT NULL,
    model           STRING NOT NULL,
    provider        STRING,
    input_tokens    INT64,
    output_tokens   INT64,
    cache_read      INT64,
    cache_write     INT64,
    reasoning       INT64,
    total_tokens    INT64 NOT NULL,
    message_count   INT64,
    cost_usd        FLOAT64,
    ms_per_1k_tokens    FLOAT64,
    perf_duration_ms    INT64,
    perf_token_coverage FLOAT64,
    price_input_per_token       FLOAT64,
    price_output_per_token      FLOAT64,
    price_cache_read_per_token  FLOAT64,
    price_cache_write_per_token FLOAT64,
    price_matched_key   STRING,
    price_match_kind    STRING,
    price_alias_applied BOOL,
    price_source        STRING,
    price_captured_at   TIMESTAMP,
    first_seen_at       TIMESTAMP NOT NULL,
    last_seen_at        TIMESTAMP NOT NULL,
    last_updated_at     TIMESTAMP NOT NULL
);

-- Daily fact from `tokscale graph` contributions[]. One current row exists
-- per (day, client, model); newer observations overwrite that natural key.
CREATE TABLE IF NOT EXISTS daily_stats (
    day             DATE NOT NULL,
    client          STRING NOT NULL,
    model           STRING NOT NULL,
    provider        STRING,
    input_tokens    INT64,
    output_tokens   INT64,
    cache_read      INT64,
    cache_write     INT64,
    reasoning       INT64,
    message_count   INT64,
    cost_usd          FLOAT64,
    last_updated_at   TIMESTAMP NOT NULL
);

-- Day-level activity from contributions[].
CREATE TABLE IF NOT EXISTS daily_activity (
    day               DATE NOT NULL,
    intensity         INT64,
    active_time_ms    INT64,
    last_updated_at   TIMESTAMP NOT NULL
);

-- Historical point-in-time rates resolved by tokscale. Unlike current-state
-- usage facts, pricing history is intentionally append-mostly.
CREATE TABLE IF NOT EXISTS pricing_snapshots (
    captured_at     TIMESTAMP NOT NULL,
    model           STRING NOT NULL,
    source          STRING NOT NULL,
    matched_key     STRING,
    match_kind      STRING,
    price_input_per_token       FLOAT64,
    price_output_per_token      FLOAT64,
    price_cache_read_per_token  FLOAT64,
    price_cache_write_per_token FLOAT64
);

-- Run-level aggregate telemetry from graph summary and timeMetrics.
-- One row is retained for each collection run.
CREATE TABLE IF NOT EXISTS run_metrics (
    run_id                  STRING NOT NULL,
    captured_at             TIMESTAMP NOT NULL,
    total_tokens            INT64,
    total_cost              FLOAT64,
    active_days             INT64,
    total_active_time_ms    INT64,
    longest_continuous_ms   INT64,
    max_concurrent_sessions INT64,
    graph_session_count     INT64
);

-- Non-fatal cross-payload reconciliation observations per run.
CREATE TABLE IF NOT EXISTS reconciliation_issues (
    run_id          STRING NOT NULL,
    check_name      STRING,
    issue_key       STRING,
    message         STRING
);

-- User curation: a client-scoped tag uses an empty session_id; a session-
-- scoped tag names one session. Tags are plaintext by definition.
CREATE TABLE IF NOT EXISTS tags (
    scope           STRING NOT NULL,
    client          STRING NOT NULL,
    session_id      STRING NOT NULL DEFAULT '',
    tag             STRING NOT NULL,
    created_at      TIMESTAMP NOT NULL
);

-- User curation: one editable free-text note per session.
CREATE TABLE IF NOT EXISTS notes (
    client          STRING NOT NULL,
    session_id      STRING NOT NULL,
    note            STRING NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);
