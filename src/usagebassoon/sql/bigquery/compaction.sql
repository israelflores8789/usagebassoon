-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- Run nightly. Append loads committed after cutoff remain visible in current views
-- and are detected by the next arrival-bucket count. Never delete raw observations.
DECLARE cutoff TIMESTAMP;
DECLARE compaction_run STRING DEFAULT GENERATE_UUID();
BEGIN TRANSACTION;
SET cutoff = CURRENT_TIMESTAMP();

CREATE TEMP TABLE previous_progress AS
SELECT * FROM compaction_ledger FOR SYSTEM_TIME AS OF cutoff;

-- Mutating the singleton makes overlapping compactors conflict atomically.
UPDATE compaction_ledger SET compacted_at = cutoff, run_id = compaction_run
WHERE domain = '__lock__';
ASSERT (SELECT COUNT(*) FROM compaction_ledger WHERE domain = '__lock__') = 1
AS 'compaction control row is missing or duplicated';

CREATE TEMP TABLE counts_sessions AS
SELECT source_id, DATE '0001-01-01' AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows_processed
FROM raw_sessions FOR SYSTEM_TIME AS OF cutoff
GROUP BY source_id, day, arrival_day;

CREATE TEMP TABLE candidates_sessions AS
SELECT counts.* FROM counts_sessions AS counts
LEFT JOIN (
    SELECT source_id, day, arrival_day, MAX(raw_rows_processed) AS processed
    FROM previous_progress
    WHERE domain = 'sessions'
    GROUP BY source_id, day, arrival_day
) AS progress USING (source_id, day, arrival_day)
WHERE counts.raw_rows_processed > COALESCE(progress.processed, 0);

CREATE TEMP TABLE winners_sessions AS
SELECT event_id, source_id, client, session_id, workspace, workspace_label, created_at, last_active, duration_minutes, message_count, tokscale_cost_usd, models_used, session_label, earliest_seen AS first_seen_at, latest_seen AS last_seen_at, collected_at
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, client, session_id ORDER BY collected_at DESC, event_id DESC) AS observation_rank, MIN(first_seen_at) OVER (PARTITION BY source_id, client, session_id) AS earliest_seen, MAX(last_seen_at) OVER (PARTITION BY source_id, client, session_id) AS latest_seen
FROM (SELECT * FROM sessions FOR SYSTEM_TIME AS OF cutoff WHERE source_id IN (SELECT DISTINCT source_id FROM candidates_sessions) UNION ALL SELECT * FROM raw_sessions FOR SYSTEM_TIME AS OF cutoff WHERE source_id IN (SELECT DISTINCT source_id FROM candidates_sessions)) AS observations
) AS ranked
WHERE observation_rank = 1;

-- Existing gold rows participate, including keys no longer present in raw.
DELETE FROM sessions WHERE source_id IN (SELECT DISTINCT source_id FROM candidates_sessions);
INSERT INTO sessions (event_id, source_id, client, session_id, workspace, workspace_label, created_at, last_active, duration_minutes, message_count, tokscale_cost_usd, models_used, session_label, first_seen_at, last_seen_at, collected_at) SELECT * FROM winners_sessions;
INSERT INTO compaction_ledger
SELECT source_id, GENERATE_UUID(), compaction_run, 'sessions', day, arrival_day,
       cutoff, cutoff, raw_rows_processed
FROM candidates_sessions;

CREATE TEMP TABLE counts_daily_stats AS
SELECT source_id, day AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows_processed
FROM raw_daily_stats FOR SYSTEM_TIME AS OF cutoff
GROUP BY source_id, day, arrival_day;

CREATE TEMP TABLE candidates_daily_stats AS
SELECT counts.* FROM counts_daily_stats AS counts
LEFT JOIN (
    SELECT source_id, day, arrival_day, MAX(raw_rows_processed) AS processed
    FROM previous_progress
    WHERE domain = 'daily_stats'
    GROUP BY source_id, day, arrival_day
) AS progress USING (source_id, day, arrival_day)
WHERE counts.raw_rows_processed > COALESCE(progress.processed, 0);

CREATE TEMP TABLE winners_daily_stats AS
SELECT event_id, source_id, day, client, session_id, model, provider, input_tokens, output_tokens, cache_read, cache_write, reasoning, total_tokens, message_count, tokscale_cost_usd, perf_duration_ms, perf_timed_tokens, perf_sample_count, perf_token_coverage, tokscale_ms_per_1k_tokens, collected_at
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, day, client, session_id, model ORDER BY collected_at DESC, total_tokens DESC, event_id DESC) AS observation_rank
FROM (SELECT * FROM daily_stats FOR SYSTEM_TIME AS OF cutoff WHERE day IN (SELECT DISTINCT day FROM candidates_daily_stats) UNION ALL SELECT * FROM raw_daily_stats FOR SYSTEM_TIME AS OF cutoff WHERE day IN (SELECT DISTINCT day FROM candidates_daily_stats)) AS observations
) AS ranked
WHERE observation_rank = 1;

-- Existing gold rows participate, including keys no longer present in raw.
DELETE FROM daily_stats WHERE day IN (SELECT DISTINCT day FROM candidates_daily_stats);
INSERT INTO daily_stats (event_id, source_id, day, client, session_id, model, provider, input_tokens, output_tokens, cache_read, cache_write, reasoning, total_tokens, message_count, tokscale_cost_usd, perf_duration_ms, perf_timed_tokens, perf_sample_count, perf_token_coverage, tokscale_ms_per_1k_tokens, collected_at) SELECT * FROM winners_daily_stats;
INSERT INTO compaction_ledger
SELECT source_id, GENERATE_UUID(), compaction_run, 'daily_stats', day, arrival_day,
       cutoff, cutoff, raw_rows_processed
FROM candidates_daily_stats;

CREATE TEMP TABLE counts_price_versions AS
SELECT source_id, day AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows_processed
FROM raw_price_versions FOR SYSTEM_TIME AS OF cutoff
GROUP BY source_id, day, arrival_day;

CREATE TEMP TABLE candidates_price_versions AS
SELECT counts.* FROM counts_price_versions AS counts
LEFT JOIN (
    SELECT source_id, day, arrival_day, MAX(raw_rows_processed) AS processed
    FROM previous_progress
    WHERE domain = 'price_versions'
    GROUP BY source_id, day, arrival_day
) AS progress USING (source_id, day, arrival_day)
WHERE counts.raw_rows_processed > COALESCE(progress.processed, 0);

CREATE TEMP TABLE winners_price_versions AS
SELECT event_id, source_id, day, model, source, matched_key, match_kind, price_input_per_token, price_output_per_token, price_cache_read_per_token, price_cache_write_per_token, collected_at
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, day, model ORDER BY collected_at DESC, price_output_per_token DESC NULLS LAST, event_id DESC) AS observation_rank
FROM (SELECT * FROM price_versions FOR SYSTEM_TIME AS OF cutoff WHERE day IN (SELECT DISTINCT day FROM candidates_price_versions) UNION ALL SELECT * FROM raw_price_versions FOR SYSTEM_TIME AS OF cutoff WHERE day IN (SELECT DISTINCT day FROM candidates_price_versions)) AS observations
) AS ranked
WHERE observation_rank = 1;

-- Existing gold rows participate, including keys no longer present in raw.
DELETE FROM price_versions WHERE day IN (SELECT DISTINCT day FROM candidates_price_versions);
INSERT INTO price_versions (event_id, source_id, day, model, source, matched_key, match_kind, price_input_per_token, price_output_per_token, price_cache_read_per_token, price_cache_write_per_token, collected_at) SELECT * FROM winners_price_versions;
INSERT INTO compaction_ledger
SELECT source_id, GENERATE_UUID(), compaction_run, 'price_versions', day, arrival_day,
       cutoff, cutoff, raw_rows_processed
FROM candidates_price_versions;

CREATE TEMP TABLE counts_tags AS
SELECT source_id, DATE '0001-01-01' AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows_processed
FROM raw_tags FOR SYSTEM_TIME AS OF cutoff
GROUP BY source_id, day, arrival_day;

CREATE TEMP TABLE candidates_tags AS
SELECT counts.* FROM counts_tags AS counts
LEFT JOIN (
    SELECT source_id, day, arrival_day, MAX(raw_rows_processed) AS processed
    FROM previous_progress
    WHERE domain = 'tags'
    GROUP BY source_id, day, arrival_day
) AS progress USING (source_id, day, arrival_day)
WHERE counts.raw_rows_processed > COALESCE(progress.processed, 0);

-- Curation is global: reconcile affected keys across every provenance source.
CREATE TEMP TABLE keys_tags AS
SELECT DISTINCT scope, client, workspace, session_id, tag
FROM raw_tags FOR SYSTEM_TIME AS OF cutoff
WHERE source_id IN (SELECT DISTINCT source_id FROM candidates_tags);

CREATE TEMP TABLE winners_tags AS
SELECT event_id, scope, source_id, client, workspace, session_id, tag, created_at, updated_at, collected_at, op, op_id
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY scope, client, workspace, session_id, tag ORDER BY collected_at DESC, (op = 'upsert') DESC, event_id DESC) AS observation_rank
FROM (SELECT * FROM tags FOR SYSTEM_TIME AS OF cutoff UNION ALL SELECT * FROM raw_tags FOR SYSTEM_TIME AS OF cutoff) AS observations
WHERE EXISTS (SELECT 1 FROM keys_tags AS keys WHERE keys.scope = observations.scope AND keys.client = observations.client AND keys.workspace = observations.workspace AND keys.session_id = observations.session_id AND keys.tag = observations.tag)
) AS ranked
WHERE observation_rank = 1;

-- Existing gold rows participate, including keys no longer present in raw.
DELETE FROM tags AS target WHERE EXISTS (SELECT 1 FROM keys_tags AS keys WHERE keys.scope = target.scope AND keys.client = target.client AND keys.workspace = target.workspace AND keys.session_id = target.session_id AND keys.tag = target.tag);
INSERT INTO tags (event_id, scope, source_id, client, workspace, session_id, tag, created_at, updated_at, collected_at, op, op_id) SELECT * FROM winners_tags;
INSERT INTO compaction_ledger
SELECT source_id, GENERATE_UUID(), compaction_run, 'tags', day, arrival_day,
       cutoff, cutoff, raw_rows_processed
FROM candidates_tags;

CREATE TEMP TABLE counts_notes AS
SELECT source_id, DATE '0001-01-01' AS day, _PARTITIONDATE AS arrival_day, COUNT(*) AS raw_rows_processed
FROM raw_notes FOR SYSTEM_TIME AS OF cutoff
GROUP BY source_id, day, arrival_day;

CREATE TEMP TABLE candidates_notes AS
SELECT counts.* FROM counts_notes AS counts
LEFT JOIN (
    SELECT source_id, day, arrival_day, MAX(raw_rows_processed) AS processed
    FROM previous_progress
    WHERE domain = 'notes'
    GROUP BY source_id, day, arrival_day
) AS progress USING (source_id, day, arrival_day)
WHERE counts.raw_rows_processed > COALESCE(progress.processed, 0);

-- Notes use the same source/client/session identity as their target sessions.
CREATE TEMP TABLE keys_notes AS
SELECT DISTINCT source_id, client, session_id
FROM raw_notes FOR SYSTEM_TIME AS OF cutoff
WHERE source_id IN (SELECT DISTINCT source_id FROM candidates_notes);

CREATE TEMP TABLE winners_notes AS
SELECT event_id, source_id, client, session_id, note, created_at, updated_at, collected_at, op, op_id
FROM (
SELECT observations.*, ROW_NUMBER() OVER (PARTITION BY source_id, client, session_id ORDER BY collected_at DESC, (op = 'upsert') DESC, event_id DESC) AS observation_rank
FROM (SELECT * FROM notes FOR SYSTEM_TIME AS OF cutoff UNION ALL SELECT * FROM raw_notes FOR SYSTEM_TIME AS OF cutoff) AS observations
WHERE EXISTS (SELECT 1 FROM keys_notes AS keys WHERE keys.source_id = observations.source_id AND keys.client = observations.client AND keys.session_id = observations.session_id)
) AS ranked
WHERE observation_rank = 1;

-- Existing gold rows participate, including keys no longer present in raw.
DELETE FROM notes AS target WHERE EXISTS (SELECT 1 FROM keys_notes AS keys WHERE keys.source_id = target.source_id AND keys.client = target.client AND keys.session_id = target.session_id);
INSERT INTO notes (event_id, source_id, client, session_id, note, created_at, updated_at, collected_at, op, op_id) SELECT * FROM winners_notes;
INSERT INTO compaction_ledger
SELECT source_id, GENERATE_UUID(), compaction_run, 'notes', day, arrival_day,
       cutoff, cutoff, raw_rows_processed
FROM candidates_notes;

COMMIT TRANSACTION;
