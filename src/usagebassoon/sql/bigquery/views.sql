-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- Current state includes every retained observation; progress never hides raw data.
CREATE OR REPLACE VIEW current_sessions AS
SELECT event_id, source_id, client, session_id, workspace, workspace_label, created_at, last_active, duration_minutes, message_count, tokscale_cost_usd, models_used, session_label, earliest_seen AS first_seen_at, latest_seen AS last_seen_at, collected_at
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, client, session_id ORDER BY collected_at DESC, event_id DESC) AS observation_rank, MIN(first_seen_at) OVER (PARTITION BY source_id, client, session_id) AS earliest_seen, MAX(last_seen_at) OVER (PARTITION BY source_id, client, session_id) AS latest_seen
FROM (SELECT * FROM sessions UNION ALL SELECT * FROM raw_sessions) AS observations
) AS ranked
WHERE observation_rank = 1;

-- Current state includes every retained observation; progress never hides raw data.
CREATE OR REPLACE VIEW current_daily_stats AS
SELECT event_id, source_id, day, client, session_id, model, provider, input_tokens, output_tokens, cache_read, cache_write, reasoning, total_tokens, message_count, tokscale_cost_usd, perf_duration_ms, perf_timed_tokens, perf_sample_count, perf_token_coverage, tokscale_ms_per_1k_tokens, collected_at
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, day, client, session_id, model ORDER BY collected_at DESC, total_tokens DESC, event_id DESC) AS observation_rank
FROM (SELECT * FROM daily_stats UNION ALL SELECT * FROM raw_daily_stats) AS observations
) AS ranked
WHERE observation_rank = 1;

-- Current state includes every retained observation; progress never hides raw data.
CREATE OR REPLACE VIEW current_price_versions AS
SELECT event_id, source_id, day, model, source, matched_key, match_kind, price_input_per_token, price_output_per_token, price_cache_read_per_token, price_cache_write_per_token, collected_at
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, day, model ORDER BY collected_at DESC, price_output_per_token DESC NULLS LAST, event_id DESC) AS observation_rank
FROM (SELECT * FROM price_versions UNION ALL SELECT * FROM raw_price_versions) AS observations
) AS ranked
WHERE observation_rank = 1;

-- Current state includes every retained observation; progress never hides raw data.
CREATE OR REPLACE VIEW current_tags AS
SELECT event_id, scope, source_id, client, workspace, session_id, tag, created_at, updated_at, collected_at, op, op_id
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY scope, client, workspace, session_id, tag ORDER BY collected_at DESC, (op = 'upsert') DESC, event_id DESC) AS observation_rank
FROM (SELECT * FROM tags UNION ALL SELECT * FROM raw_tags) AS observations
) AS ranked
WHERE observation_rank = 1 AND op = 'upsert';

-- Current state includes every retained observation; progress never hides raw data.
CREATE OR REPLACE VIEW current_notes AS
SELECT event_id, note_id, source_id, client, session_id, note, created_at, updated_at, collected_at, op, op_id
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, client, session_id ORDER BY collected_at DESC, (op = 'upsert') DESC, event_id DESC) AS observation_rank
FROM (SELECT * FROM notes UNION ALL SELECT * FROM raw_notes) AS observations
) AS ranked
WHERE observation_rank = 1 AND op = 'upsert';

CREATE OR REPLACE VIEW current_collection_ledger AS
SELECT event_id, run_id, day, domain, expected_count, succeeded_count, failure_code, collected_at, source_id, started_at, finished_at, host, os_name, os_version, architecture, cpu_model, cpu_count, memory_bytes, shell, tokscale_ver, status
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, run_id, day, domain ORDER BY collected_at DESC, finished_at DESC NULLS LAST, event_id DESC) AS observation_rank
FROM (SELECT * FROM collection_ledger) AS observations
) AS ranked
WHERE observation_rank = 1;

CREATE OR REPLACE VIEW schema_drift_events AS
SELECT run_id, event_id, source_id, domain, tokscale_ver, drift_key, drift_kind, path, detail, contract_tokscale_ver, created_at, collected_at, resolved, observation_count FROM (
SELECT events.*, ROW_NUMBER() OVER (PARTITION BY source_id, event_id ORDER BY collected_at DESC, resolved DESC, event_id DESC) AS replay_rank
FROM raw_schema_drift_events AS events
) AS replays WHERE replay_rank = 1;

CREATE OR REPLACE VIEW replay_schema_drift_events AS
SELECT run_id, event_id, source_id, domain, tokscale_ver, drift_key, drift_kind, path, detail, contract_tokscale_ver, created_at, collected_at, resolved, observation_count FROM (
SELECT events.*, ROW_NUMBER() OVER (PARTITION BY source_id, event_id ORDER BY collected_at DESC, resolved DESC, event_id DESC) AS replay_rank
FROM raw_schema_drift_events AS events
) AS replays WHERE replay_rank = 1;

CREATE OR REPLACE VIEW current_schema_drift_events AS
SELECT run_id, event_id, source_id, domain, tokscale_ver, drift_key, drift_kind, path, detail, contract_tokscale_ver, first_detection AS created_at, collected_at, resolved, CAST(sightings AS INT64) AS observation_count
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, domain, tokscale_ver, drift_key ORDER BY collected_at DESC, resolved DESC, event_id DESC) AS observation_rank, MIN(created_at) OVER (PARTITION BY source_id, domain, tokscale_ver, drift_key) AS first_detection, SUM(observation_count) OVER (PARTITION BY source_id, domain, tokscale_ver, drift_key) AS sightings
FROM (SELECT * FROM replay_schema_drift_events) AS observations
) AS ranked
WHERE observation_rank = 1;

CREATE OR REPLACE VIEW reconciliation_issues AS
SELECT event_id, run_id, source_id, check_name, issue_key, message, created_at, collected_at, resolved, observation_count FROM (
SELECT events.*, ROW_NUMBER() OVER (PARTITION BY source_id, event_id ORDER BY collected_at DESC, resolved DESC, event_id DESC) AS replay_rank
FROM raw_reconciliation_issues AS events
) AS replays WHERE replay_rank = 1;

CREATE OR REPLACE VIEW replay_reconciliation_issues AS
SELECT event_id, run_id, source_id, check_name, issue_key, message, created_at, collected_at, resolved, observation_count FROM (
SELECT events.*, ROW_NUMBER() OVER (PARTITION BY source_id, event_id ORDER BY collected_at DESC, resolved DESC, event_id DESC) AS replay_rank
FROM raw_reconciliation_issues AS events
) AS replays WHERE replay_rank = 1;

CREATE OR REPLACE VIEW current_reconciliation_issues AS
SELECT event_id, run_id, source_id, check_name, issue_key, latest_message AS message, first_detection AS created_at, collected_at, resolved, CAST(sightings AS INT64) AS observation_count
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, check_name, issue_key ORDER BY collected_at DESC, resolved DESC, event_id DESC) AS observation_rank, MIN(created_at) OVER (PARTITION BY source_id, check_name, issue_key) AS first_detection, SUM(observation_count) OVER (PARTITION BY source_id, check_name, issue_key) AS sightings, FIRST_VALUE(message IGNORE NULLS) OVER (PARTITION BY source_id, check_name, issue_key ORDER BY collected_at DESC, resolved DESC, event_id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_message
FROM (SELECT * FROM replay_reconciliation_issues) AS observations
) AS ranked
WHERE observation_rank = 1;

-- Doctor freshness is enforced at read time, independent of physical pruning.
CREATE OR REPLACE VIEW open_schema_drift_events AS
SELECT * FROM current_schema_drift_events
WHERE resolved = FALSE AND collected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY);

CREATE OR REPLACE VIEW open_reconciliation_issues AS
SELECT * FROM current_reconciliation_issues
WHERE resolved = FALSE AND collected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY);

CREATE OR REPLACE VIEW collection_runs AS
SELECT * FROM current_collection_ledger WHERE domain = 'collection';

-- One source summary combines all canonical domains and one latest metadata row.
CREATE OR REPLACE VIEW audit_sources AS
WITH sources AS (
    SELECT source_id FROM current_sessions
    UNION DISTINCT SELECT source_id FROM current_daily_stats
    UNION DISTINCT SELECT source_id FROM current_price_versions
    UNION DISTINCT SELECT source_id FROM current_tags
    UNION DISTINCT SELECT source_id FROM current_notes
    UNION DISTINCT SELECT source_id FROM current_collection_ledger
    UNION DISTINCT SELECT source_id FROM current_schema_drift_events
    UNION DISTINCT SELECT source_id FROM current_reconciliation_issues
), totals AS (
    SELECT source_id,
        MIN(COALESCE(finished_at, started_at)) AS first_activity,
        MAX(COALESCE(finished_at, started_at)) AS last_activity,
        COUNT(DISTINCT run_id) AS run_count
    FROM current_collection_ledger
    GROUP BY source_id
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY source_id
        ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST,
            collected_at DESC NULLS LAST,
            CASE WHEN domain = 'collection' THEN 1 ELSE 0 END DESC,
            event_id DESC
    ) AS evidence_rank
    FROM current_collection_ledger
), latest AS (
    SELECT * FROM ranked WHERE evidence_rank = 1
)
SELECT sources.source_id, totals.first_activity, totals.last_activity,
    COALESCE(totals.run_count, 0) AS run_count,
    latest.status AS latest_outcome,
    latest.host, latest.os_name, latest.os_version, latest.architecture,
    latest.cpu_model, latest.cpu_count, latest.memory_bytes, latest.shell
FROM sources
LEFT JOIN totals ON sources.source_id = totals.source_id
LEFT JOIN latest ON sources.source_id = latest.source_id;

CREATE OR REPLACE VIEW collection_status AS
SELECT event_id, run_id, day, domain, expected_count, succeeded_count, failure_code, collected_at, source_id, started_at, finished_at, host, os_name, os_version, architecture, cpu_model, cpu_count, memory_bytes, shell, tokscale_ver, status FROM (
SELECT current_collection_ledger.*, ROW_NUMBER() OVER (
    PARTITION BY source_id, day, domain
    ORDER BY collected_at DESC, finished_at DESC NULLS LAST, event_id DESC
) AS target_rank
FROM current_collection_ledger AS current_collection_ledger
WHERE domain <> 'collection'
) AS targets WHERE target_rank = 1;

-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- Reasoning tokens use the output rate, as tokscale's pricing semantics do.
CREATE OR REPLACE VIEW daily_cost AS
SELECT
    daily_stats.*,
    CASE
        WHEN daily_stats.perf_timed_tokens > 0
            AND daily_stats.perf_duration_ms IS NOT NULL
        THEN 1000.0 * daily_stats.perf_duration_ms / daily_stats.perf_timed_tokens
    END AS ms_per_1k_tokens,
    CASE
        WHEN (daily_stats.input_tokens <> 0 AND price_versions.price_input_per_token IS NULL)
            OR ((daily_stats.output_tokens <> 0 OR daily_stats.reasoning <> 0)
                AND price_versions.price_output_per_token IS NULL)
            OR (daily_stats.cache_read <> 0 AND price_versions.price_cache_read_per_token IS NULL)
            OR (daily_stats.cache_write <> 0 AND price_versions.price_cache_write_per_token IS NULL)
        THEN NULL
        ELSE COALESCE(daily_stats.input_tokens, 0) * COALESCE(price_versions.price_input_per_token, 0)
            + (COALESCE(daily_stats.output_tokens, 0) + COALESCE(daily_stats.reasoning, 0))
                * COALESCE(price_versions.price_output_per_token, 0)
            + COALESCE(daily_stats.cache_read, 0) * COALESCE(price_versions.price_cache_read_per_token, 0)
            + COALESCE(daily_stats.cache_write, 0) * COALESCE(price_versions.price_cache_write_per_token, 0)
    END AS cost_usd
FROM current_daily_stats AS daily_stats
LEFT JOIN current_price_versions AS price_versions
    ON price_versions.source_id = daily_stats.source_id
    AND price_versions.day = daily_stats.day
    AND price_versions.model = daily_stats.model;

-- Familiar all-time session and model totals are calculated from daily facts.
CREATE OR REPLACE VIEW session_model_stats AS
SELECT
    totals.*,
    CASE
        WHEN totals.perf_timed_tokens > 0
        THEN 1000.0 * totals.perf_duration_ms / totals.perf_timed_tokens
    END AS ms_per_1k_tokens
FROM (
SELECT
    source_id,
    client,
    session_id,
    model,
    MAX(provider) AS provider,
    SUM(input_tokens) AS input_tokens,
    SUM(output_tokens) AS output_tokens,
    SUM(cache_read) AS cache_read,
    SUM(cache_write) AS cache_write,
    SUM(reasoning) AS reasoning,
    SUM(total_tokens) AS total_tokens,
    SUM(message_count) AS message_count,
    SUM(tokscale_cost_usd) AS tokscale_cost_usd,
    SUM(CASE
        WHEN perf_duration_ms IS NOT NULL AND perf_timed_tokens > 0
        THEN perf_duration_ms
    END) AS perf_duration_ms,
    SUM(CASE
        WHEN perf_duration_ms IS NOT NULL AND perf_timed_tokens > 0
        THEN perf_timed_tokens
    END) AS perf_timed_tokens,
    SUM(perf_sample_count) AS perf_sample_count,
    CASE WHEN COUNT(cost_usd) = COUNT(*) THEN SUM(cost_usd) END AS cost_usd,
    MAX(collected_at) AS collected_at
FROM daily_cost AS daily_cost
GROUP BY source_id, client, session_id, model
) AS totals;

CREATE OR REPLACE VIEW session_model_stats_current AS
SELECT * FROM session_model_stats;

-- Report source views preserve filter dimensions; terminal commands aggregate
-- them after applying source, client, model, workspace, and effective-tag filters.
CREATE OR REPLACE VIEW report_daily_usage AS
SELECT
    daily_cost.source_id,
    daily_cost.day,
    daily_cost.client,
    daily_cost.session_id,
    daily_cost.model,
    sessions.workspace,
    daily_cost.input_tokens,
    daily_cost.output_tokens,
    daily_cost.cache_read,
    daily_cost.cache_write,
    daily_cost.reasoning,
    daily_cost.total_tokens,
    daily_cost.perf_duration_ms,
    daily_cost.perf_timed_tokens,
    daily_cost.perf_sample_count,
    daily_cost.perf_token_coverage,
    daily_cost.tokscale_ms_per_1k_tokens,
    daily_cost.ms_per_1k_tokens,
    daily_cost.cost_usd,
    daily_cost.tokscale_cost_usd
FROM daily_cost AS daily_cost
LEFT JOIN current_sessions AS sessions
    ON sessions.source_id = daily_cost.source_id
    AND sessions.client = daily_cost.client
    AND sessions.session_id = daily_cost.session_id;

CREATE OR REPLACE VIEW report_session_models AS
SELECT
    session_model_stats.source_id,
    session_model_stats.client,
    session_model_stats.session_id,
    session_model_stats.model,
    sessions.workspace,
    sessions.created_at,
    sessions.last_active,
    session_model_stats.input_tokens,
    session_model_stats.output_tokens,
    session_model_stats.cache_read,
    session_model_stats.cache_write,
    session_model_stats.reasoning,
    session_model_stats.total_tokens,
    (SELECT SUM(duration_facts.perf_duration_ms)
     FROM current_daily_stats AS duration_facts
     WHERE duration_facts.source_id = session_model_stats.source_id
       AND duration_facts.client = session_model_stats.client
       AND duration_facts.session_id = session_model_stats.session_id
       AND duration_facts.model = session_model_stats.model) AS perf_duration_ms,
    session_model_stats.perf_duration_ms AS perf_timed_duration_ms,
    session_model_stats.perf_timed_tokens,
    session_model_stats.perf_sample_count,
    session_model_stats.ms_per_1k_tokens,
    session_model_stats.cost_usd,
    session_model_stats.tokscale_cost_usd
FROM session_model_stats AS session_model_stats
LEFT JOIN current_sessions AS sessions
    ON sessions.source_id = session_model_stats.source_id
    AND sessions.client = session_model_stats.client
    AND sessions.session_id = session_model_stats.session_id;

-- Retained public relations for the query API and doctor checks. The terminal
-- summary command now uses report_session_models directly.
CREATE OR REPLACE VIEW report_summary AS
SELECT
    COUNT(*) AS sessions,
    COALESCE(SUM(cost_usd), 0) AS cost_usd
FROM (
    SELECT
        source_id,
        client,
        session_id,
        CASE WHEN COUNT(cost_usd) = COUNT(*) THEN SUM(cost_usd) END AS cost_usd
    FROM report_session_models
    GROUP BY source_id, client, session_id
) AS session_costs;

CREATE OR REPLACE VIEW report_summary_models AS
SELECT
    model,
    COALESCE(SUM(total_tokens), 0) AS total_tokens,
    COALESCE(SUM(cost_usd), 0) AS cost_usd
FROM report_session_models
GROUP BY model
ORDER BY cost_usd DESC, model;

-- Model reports filter source facts before aggregating by client and model.
CREATE OR REPLACE VIEW report_models AS
SELECT
    source_id,
    day,
    client,
    session_id,
    model,
    workspace,
    input_tokens,
    output_tokens,
    cache_read,
    cache_write,
    reasoning,
    total_tokens,
    perf_duration_ms,
    perf_timed_tokens,
    perf_sample_count,
    perf_token_coverage,
    tokscale_ms_per_1k_tokens,
    ms_per_1k_tokens,
    cost_usd,
    tokscale_cost_usd
FROM report_daily_usage;

CREATE OR REPLACE VIEW session_tags AS
SELECT DISTINCT
    sessions.source_id,
    sessions.client,
    sessions.session_id,
    sessions.workspace,
    tags.tag,
    tags.scope AS tag_scope
FROM current_sessions AS sessions
JOIN current_tags AS tags
    ON (
        (tags.scope = 'client' AND tags.client = sessions.client)
        OR (tags.scope = 'workspace' AND tags.workspace = sessions.workspace)
        OR (
            tags.scope = 'session'
            AND tags.client = sessions.client
            AND tags.session_id = sessions.session_id
        )
    );

CREATE OR REPLACE VIEW tagged_sessions AS
SELECT sessions.*, session_tags.tag, session_tags.tag_scope
FROM current_sessions AS sessions
JOIN session_tags AS session_tags
    ON session_tags.source_id = sessions.source_id
    AND session_tags.client = sessions.client
    AND session_tags.session_id = sessions.session_id;

CREATE OR REPLACE VIEW noted_sessions AS
SELECT sessions.*, notes.note_id, notes.note, notes.created_at AS note_created_at,
       notes.updated_at AS note_updated_at,
       notes.collected_at AS note_collected_at
FROM current_sessions AS sessions
JOIN current_notes AS notes
    ON notes.source_id = sessions.source_id
    AND notes.client = sessions.client
    AND notes.session_id = sessions.session_id;

-- Curation commands read notes through this stable, dialect-paired view.
CREATE OR REPLACE VIEW session_notes AS
SELECT note_id, source_id, client, session_id, note, created_at, updated_at, collected_at
FROM current_notes AS notes;

-- Planning and resolution share one snapshot and one query job.
CREATE OR REPLACE VIEW collection_preflight AS
SELECT DISTINCT 'status' AS record_kind, source_id, day, domain, status, expected_count, succeeded_count, run_id, failure_code, CAST(NULL AS STRING) AS model, CAST(NULL AS STRING) AS check_name, CAST(NULL AS STRING) AS issue_key, CAST(NULL AS STRING) AS tokscale_ver, CAST(NULL AS STRING) AS drift_key, CAST(NULL AS STRING) AS drift_kind, CAST(NULL AS STRING) AS path, CAST(NULL AS STRING) AS detail, CAST(NULL AS STRING) AS contract_tokscale_ver, CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS INT64) AS observation_count
FROM collection_status
UNION ALL
SELECT DISTINCT 'models' AS record_kind, source_id, day, CAST(NULL AS STRING) AS domain, CAST(NULL AS STRING) AS status, CAST(NULL AS INT64) AS expected_count, CAST(NULL AS INT64) AS succeeded_count, CAST(NULL AS STRING) AS run_id, CAST(NULL AS STRING) AS failure_code, model, CAST(NULL AS STRING) AS check_name, CAST(NULL AS STRING) AS issue_key, CAST(NULL AS STRING) AS tokscale_ver, CAST(NULL AS STRING) AS drift_key, CAST(NULL AS STRING) AS drift_kind, CAST(NULL AS STRING) AS path, CAST(NULL AS STRING) AS detail, CAST(NULL AS STRING) AS contract_tokscale_ver, CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS INT64) AS observation_count
FROM current_daily_stats
UNION ALL
SELECT DISTINCT 'prices' AS record_kind, source_id, day, CAST(NULL AS STRING) AS domain, CAST(NULL AS STRING) AS status, CAST(NULL AS INT64) AS expected_count, CAST(NULL AS INT64) AS succeeded_count, CAST(NULL AS STRING) AS run_id, CAST(NULL AS STRING) AS failure_code, model, CAST(NULL AS STRING) AS check_name, CAST(NULL AS STRING) AS issue_key, CAST(NULL AS STRING) AS tokscale_ver, CAST(NULL AS STRING) AS drift_key, CAST(NULL AS STRING) AS drift_kind, CAST(NULL AS STRING) AS path, CAST(NULL AS STRING) AS detail, CAST(NULL AS STRING) AS contract_tokscale_ver, CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS INT64) AS observation_count
FROM current_price_versions
UNION ALL
SELECT DISTINCT 'issues' AS record_kind, source_id, CAST(NULL AS DATE) AS day, CAST(NULL AS STRING) AS domain, CAST(NULL AS STRING) AS status, CAST(NULL AS INT64) AS expected_count, CAST(NULL AS INT64) AS succeeded_count, CAST(NULL AS STRING) AS run_id, CAST(NULL AS STRING) AS failure_code, CAST(NULL AS STRING) AS model, check_name, issue_key, CAST(NULL AS STRING) AS tokscale_ver, CAST(NULL AS STRING) AS drift_key, CAST(NULL AS STRING) AS drift_kind, CAST(NULL AS STRING) AS path, CAST(NULL AS STRING) AS detail, CAST(NULL AS STRING) AS contract_tokscale_ver, CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS INT64) AS observation_count
FROM current_reconciliation_issues WHERE resolved = FALSE
UNION ALL
SELECT DISTINCT 'drift' AS record_kind, source_id, CAST(NULL AS DATE) AS day, domain, CAST(NULL AS STRING) AS status, CAST(NULL AS INT64) AS expected_count, CAST(NULL AS INT64) AS succeeded_count, CAST(NULL AS STRING) AS run_id, CAST(NULL AS STRING) AS failure_code, CAST(NULL AS STRING) AS model, CAST(NULL AS STRING) AS check_name, CAST(NULL AS STRING) AS issue_key, tokscale_ver, drift_key, drift_kind, path, detail, contract_tokscale_ver, created_at, observation_count
FROM current_schema_drift_events WHERE resolved = FALSE;

-- Report pending arrival buckets before raw retention can remove observations.
CREATE OR REPLACE VIEW compaction_backlog AS
SELECT counts.*, counts.raw_rows - COALESCE(progress.processed, 0) AS pending_rows,
       DATE_DIFF(CURRENT_DATE(), counts.arrival_day, DAY) AS age_days
FROM (
SELECT source_id, 'sessions' AS domain, DATE '0001-01-01' AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows
FROM raw_sessions
GROUP BY source_id, domain, day, arrival_day
UNION ALL
SELECT source_id, 'daily_stats' AS domain, day AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows
FROM raw_daily_stats
GROUP BY source_id, domain, day, arrival_day
UNION ALL
SELECT source_id, 'price_versions' AS domain, day AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows
FROM raw_price_versions
GROUP BY source_id, domain, day, arrival_day
UNION ALL
SELECT source_id, 'tags' AS domain, DATE '0001-01-01' AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows
FROM raw_tags
GROUP BY source_id, domain, day, arrival_day
UNION ALL
SELECT source_id, 'notes' AS domain, DATE '0001-01-01' AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows
FROM raw_notes
GROUP BY source_id, domain, day, arrival_day
) AS counts
LEFT JOIN (
    SELECT source_id, domain, day, arrival_day, MAX(raw_rows_processed) AS processed
    FROM compaction_ledger WHERE domain <> '__lock__'
    GROUP BY source_id, domain, day, arrival_day
) AS progress USING (source_id, domain, day, arrival_day)
WHERE counts.raw_rows > COALESCE(progress.processed, 0);
