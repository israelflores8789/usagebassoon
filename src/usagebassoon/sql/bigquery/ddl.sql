-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- usagebassoon warehouse DDL for BigQuery Standard SQL.
-- Arrow owns cross-backend normalization. Current-state facts use MERGE;
-- payload absent observations are never deleted.
-- See sql/duckdb/ for comments on the purpose of each table.

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id STRING NOT NULL,
    source_id STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    host STRING,
    os_name STRING,
    os_version STRING,
    architecture STRING,
    cpu_model STRING,
    cpu_count INT64,
    memory_bytes INT64,
    shell STRING,
    tokscale_ver STRING,
    status STRING,
    rows_in INT64,
    rows_inserted INT64,
    rows_updated INT64,
    drift_events INT64
);

CREATE TABLE IF NOT EXISTS schema_drift (
    drift_id STRING NOT NULL,
    run_id STRING NOT NULL,
    source_id STRING NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    payload_kind STRING,
    drift_kind STRING,
    path STRING,
    detail STRING,
    tokscale_ver STRING,
    resolved BOOL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS sessions (
    source_id STRING NOT NULL,
    client STRING NOT NULL,
    session_id STRING NOT NULL,
    workspace STRING,
    workspace_label STRING,
    created_at TIMESTAMP,
    last_active TIMESTAMP,
    duration_minutes INT64,
    message_count INT64,
    tokscale_cost_usd FLOAT64,
    models_used ARRAY<STRING>,
    session_label STRING,
    first_seen_at TIMESTAMP NOT NULL,
    last_seen_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_stats (
    source_id STRING NOT NULL,
    day DATE NOT NULL,
    client STRING NOT NULL,
    session_id STRING NOT NULL,
    model STRING NOT NULL,
    provider STRING,
    input_tokens INT64,
    output_tokens INT64,
    cache_read INT64,
    cache_write INT64,
    reasoning INT64,
    total_tokens INT64 NOT NULL,
    message_count INT64,
    tokscale_cost_usd FLOAT64,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_activity (
    source_id STRING NOT NULL,
    day DATE NOT NULL,
    intensity INT64,
    active_time_ms INT64,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS price_versions (
    source_id STRING NOT NULL,
    day DATE NOT NULL,
    model STRING NOT NULL,
    source STRING NOT NULL,
    matched_key STRING,
    match_kind STRING,
    price_input_per_token FLOAT64,
    price_output_per_token FLOAT64,
    price_cache_read_per_token FLOAT64,
    price_cache_write_per_token FLOAT64,
    observed_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_status (
    source_id STRING NOT NULL,
    day DATE NOT NULL,
    domain STRING NOT NULL,
    status STRING NOT NULL,
    expected_count INT64,
    succeeded_count INT64,
    last_attempted_run STRING NOT NULL,
    last_succeeded_run STRING,
    failure_code STRING,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS reconciliation_issues (
    run_id STRING NOT NULL,
    source_id STRING NOT NULL,
    check_name STRING NOT NULL,
    issue_key STRING NOT NULL,
    message STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    detected_run_id STRING,
    updated_run_id STRING,
    resolved_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tags (
    scope STRING NOT NULL,
    source_id STRING NOT NULL,
    client STRING DEFAULT '' NOT NULL,
    workspace STRING DEFAULT '' NOT NULL,
    session_id STRING DEFAULT '' NOT NULL,
    tag STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS notes (
    source_id STRING NOT NULL,
    client STRING NOT NULL,
    session_id STRING NOT NULL,
    note STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
