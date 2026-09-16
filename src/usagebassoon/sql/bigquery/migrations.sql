-- SPDX-FileCopyrightText: 2026 Israel Flores-Arbolay
-- SPDX-License-Identifier: AGPL-3.0-only

-- This file is reserved for ordered, idempotent changes to datasets created
-- by earlier UsageBassoon releases. The project has not released a schema yet,
-- so no migration is required. Keep a harmless statement because backend
-- initialization executes every packaged migration file.
SELECT 1 WHERE FALSE;
