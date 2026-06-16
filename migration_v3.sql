-- migration_v3.sql
-- Миграция для парсера v3: access_hash cache + parse_state таблица
-- Применять через: psql $DATABASE_URL -f migration_v3.sql

BEGIN;

-- ============================================================================
-- 1. Добавляем access_hash и entity_resolved_at в таблицу channels
-- ============================================================================

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS access_hash BIGINT,
    ADD COLUMN IF NOT EXISTS entity_resolved_at TIMESTAMPTZ;

-- ============================================================================
-- 2. Создаём таблицу parse_state (singleton — одна строка)
-- ============================================================================

CREATE TABLE IF NOT EXISTS parse_state (
    id          INTEGER PRIMARY KEY DEFAULT 1,
    last_run_at TIMESTAMPTZ,
    last_run_channels       INTEGER DEFAULT 0,
    last_run_new_posts      INTEGER DEFAULT 0,
    last_run_duration_sec   INTEGER DEFAULT 0,
    last_error              TEXT,
    global_cooldown_until   TIMESTAMPTZ,
    total_api_calls         BIGINT  DEFAULT 0,
    total_flood_waits       INTEGER DEFAULT 0,

    -- CONSTRAINT: гарантируем, что всегда только 1 строка (singleton pattern)
    CONSTRAINT  parse_state_single_row CHECK (id = 1)
);

-- Создаём начальную строку если таблица пустая
INSERT INTO parse_state (id) VALUES (1)
    ON CONFLICT (id) DO NOTHING;

-- ============================================================================
-- 3. Индексы для channels
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_channels_access_hash
    ON channels(access_hash);

CREATE INDEX IF NOT EXISTS ix_channels_entity_resolved
    ON channels(entity_resolved_at);

-- ============================================================================
-- 4. Индексы для parse_state
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_parse_state_id
    ON parse_state(id);

-- ============================================================================
-- 5. Краткое пояснение изменений
-- ============================================================================

COMMENT ON COLUMN channels.access_hash IS
    'Telegram access_hash для построения InputPeerChannel без get_entity()';

COMMENT ON COLUMN channels.entity_resolved_at IS
    'Когда access_hash был получен через get_dialogs()';

COMMENT ON TABLE parse_state IS
    'Глобальное состояние парсера (singleton — всегда одна строка с id=1)';

COMMIT;
