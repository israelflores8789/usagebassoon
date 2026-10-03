-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

CREATE TABLE IF NOT EXISTS restore_receipts (
    source_id STRING NOT NULL,
    operation_id STRING NOT NULL,
    snapshot_id STRING NOT NULL,
    committed_at TIMESTAMP NOT NULL
);

-- Permanent gold state shares the DuckDB logical schema.
CREATE TABLE sessions (
    event_id STRING NOT NULL,
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
    collected_at TIMESTAMP NOT NULL
)
CLUSTER BY source_id;

CREATE TABLE daily_stats (
    event_id STRING NOT NULL,
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
    perf_duration_ms INT64,
    perf_timed_tokens INT64,
    perf_sample_count INT64,
    perf_token_coverage FLOAT64,
    tokscale_ms_per_1k_tokens FLOAT64,
    collected_at TIMESTAMP NOT NULL
)
PARTITION BY day
CLUSTER BY source_id, model;

CREATE TABLE price_versions (
    event_id STRING NOT NULL,
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
    collected_at TIMESTAMP NOT NULL
)
PARTITION BY day
CLUSTER BY source_id, model;

CREATE TABLE tags (
    event_id STRING NOT NULL,
    scope STRING NOT NULL,
    source_id STRING NOT NULL,
    client STRING NOT NULL,
    workspace STRING NOT NULL,
    session_id STRING NOT NULL,
    tag STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    op STRING NOT NULL,
    op_id STRING
)
CLUSTER BY source_id;

CREATE TABLE notes (
    event_id STRING NOT NULL,
    source_id STRING NOT NULL,
    client STRING NOT NULL,
    session_id STRING NOT NULL,
    note STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    op STRING NOT NULL,
    op_id STRING
)
CLUSTER BY source_id;

-- Arrival partitions retain historical backfills for a full 90 days.
CREATE TABLE raw_sessions (
    event_id STRING NOT NULL,
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
    collected_at TIMESTAMP NOT NULL
)
PARTITION BY _PARTITIONDATE
CLUSTER BY source_id
OPTIONS (partition_expiration_days = 90);

-- Arrival partitions retain historical backfills for a full 90 days.
CREATE TABLE raw_daily_stats (
    event_id STRING NOT NULL,
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
    perf_duration_ms INT64,
    perf_timed_tokens INT64,
    perf_sample_count INT64,
    perf_token_coverage FLOAT64,
    tokscale_ms_per_1k_tokens FLOAT64,
    collected_at TIMESTAMP NOT NULL
)
PARTITION BY _PARTITIONDATE
CLUSTER BY day, source_id
OPTIONS (partition_expiration_days = 90);

-- Arrival partitions retain historical backfills for a full 90 days.
CREATE TABLE raw_price_versions (
    event_id STRING NOT NULL,
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
    collected_at TIMESTAMP NOT NULL
)
PARTITION BY _PARTITIONDATE
CLUSTER BY day, source_id
OPTIONS (partition_expiration_days = 90);

-- Arrival partitions retain historical backfills for a full 90 days.
CREATE TABLE raw_tags (
    event_id STRING NOT NULL,
    scope STRING NOT NULL,
    source_id STRING NOT NULL,
    client STRING NOT NULL,
    workspace STRING NOT NULL,
    session_id STRING NOT NULL,
    tag STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    op STRING NOT NULL,
    op_id STRING
)
PARTITION BY _PARTITIONDATE
CLUSTER BY source_id
OPTIONS (partition_expiration_days = 90);

-- Arrival partitions retain historical backfills for a full 90 days.
CREATE TABLE raw_notes (
    event_id STRING NOT NULL,
    source_id STRING NOT NULL,
    client STRING NOT NULL,
    session_id STRING NOT NULL,
    note STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    op STRING NOT NULL,
    op_id STRING
)
PARTITION BY _PARTITIONDATE
CLUSTER BY source_id
OPTIONS (partition_expiration_days = 90);

-- Arrival partitions retain historical backfills for a full 90 days.
CREATE TABLE raw_schema_drift_events (
    run_id STRING NOT NULL,
    event_id STRING NOT NULL,
    source_id STRING NOT NULL,
    domain STRING NOT NULL,
    tokscale_ver STRING NOT NULL,
    drift_key STRING NOT NULL,
    drift_kind STRING NOT NULL,
    path STRING NOT NULL,
    detail STRING NOT NULL,
    contract_tokscale_ver STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    resolved BOOL NOT NULL,
    observation_count INT64 NOT NULL
)
PARTITION BY _PARTITIONDATE
CLUSTER BY source_id, domain, tokscale_ver
OPTIONS (partition_expiration_days = 90);

-- Arrival partitions retain historical backfills for a full 90 days.
CREATE TABLE raw_reconciliation_issues (
    event_id STRING NOT NULL,
    run_id STRING NOT NULL,
    source_id STRING NOT NULL,
    check_name STRING NOT NULL,
    issue_key STRING NOT NULL,
    message STRING,
    created_at TIMESTAMP,
    collected_at TIMESTAMP NOT NULL,
    resolved BOOL NOT NULL,
    observation_count INT64 NOT NULL
)
PARTITION BY _PARTITIONDATE
CLUSTER BY source_id
OPTIONS (partition_expiration_days = 90);

-- The client-side collection ledger is permanent and append-only.
CREATE TABLE collection_ledger (
    event_id STRING NOT NULL,
    run_id STRING NOT NULL,
    day DATE NOT NULL,
    domain STRING NOT NULL,
    expected_count INT64,
    succeeded_count INT64,
    failure_code STRING,
    collected_at TIMESTAMP NOT NULL,
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
    status STRING
)
PARTITION BY day
CLUSTER BY domain, source_id;

-- The control row serializes compaction/restore; progress rows are append-only.
CREATE TABLE compaction_ledger (
    source_id STRING NOT NULL,
    event_id STRING NOT NULL,
    run_id STRING NOT NULL,
    domain STRING NOT NULL,
    day DATE NOT NULL,
    arrival_day DATE NOT NULL,
    compacted_at TIMESTAMP NOT NULL,
    compacted_up_to TIMESTAMP NOT NULL,
    raw_rows_processed INT64 NOT NULL
)
PARTITION BY DATE(compacted_at)
CLUSTER BY source_id, domain, day;

INSERT INTO compaction_ledger VALUES (
    '00000000-0000-0000-0000-000000000000', GENERATE_UUID(), GENERATE_UUID(),
    '__lock__', DATE '0001-01-01', DATE '0001-01-01',
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0
);

CREATE TABLE schema_migrations (
    source_id STRING NOT NULL,
    version INT64 NOT NULL,
    schema_hash STRING NOT NULL,
    applied_at TIMESTAMP NOT NULL
);

-- The marker lives in table labels so normal opens need metadata reads only.
CREATE TABLE schema_marker (
    source_id STRING NOT NULL,
    version INT64 NOT NULL,
    schema_hash STRING NOT NULL
);
