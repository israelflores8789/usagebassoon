-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- Current-state compatibility views.
--
-- The base fact tables contain one row per natural key because collection
-- upserts existing facts and inserts only new facts.

CREATE OR REPLACE VIEW sessions_current AS
SELECT * FROM sessions;

CREATE OR REPLACE VIEW session_model_stats_current AS
SELECT * FROM session_model_stats;

CREATE OR REPLACE VIEW daily_stats_current AS
SELECT * FROM daily_stats;

CREATE OR REPLACE VIEW daily_activity_current AS
SELECT * FROM daily_activity;

-- One derived row per effective tag. Client and workspace assignments are
-- inherited by matching sessions; session assignments remain direct.
CREATE OR REPLACE VIEW session_tags AS
SELECT DISTINCT
    sessions.source_id,
    sessions.client,
    sessions.session_id,
    sessions.workspace,
    tags.tag,
    tags.scope AS tag_scope
FROM sessions
JOIN tags
    ON tags.source_id = sessions.source_id
    AND (
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
FROM sessions
JOIN session_tags
    ON session_tags.source_id = sessions.source_id
    AND session_tags.client = sessions.client
    AND session_tags.session_id = sessions.session_id;

CREATE OR REPLACE VIEW noted_sessions AS
SELECT sessions.*, notes.note, notes.created_at AS note_created_at,
       notes.updated_at AS note_updated_at
FROM sessions
JOIN notes
    ON notes.source_id = sessions.source_id
    AND notes.client = sessions.client
    AND notes.session_id = sessions.session_id;
