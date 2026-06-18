-- One-time cleanup: deactivate channels auto-created by sync_dialogs().
-- Run in Render dashboard: PostgreSQL -> Shell.
--
-- Adjust the timestamp if needed. Default: all channels created on 2026-06-18
-- (when v3 started auto-creating broadcast channels) will be deactivated.

-- 1) Preview what will be deactivated
SELECT id, telegram_id, numeric_id, title, username, created_at
FROM channels
WHERE created_at >= '2026-06-18 00:00:00+00'
  AND is_active = true
ORDER BY created_at DESC;

-- 2) Deactivate them
-- UPDATE channels
-- SET is_active = false
-- WHERE created_at >= '2026-06-18 00:00:00+00'
--   AND is_active = true;
