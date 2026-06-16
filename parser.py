#!/usr/bin/env python3
"""
MultiChannelParser — асинхронный многоканальный парсер Telegram.

Ключевые возможности:
    * Параллельный парсинг каналов через asyncio.Semaphore
    * Retry logic с exponential backoff
    * Per-channel health tracking (авто-отключение при 3+ ошибках)
    * Глобальная дедупликация постов по text_hash
    * Graceful shutdown (SIGTERM / SIGINT)
    * Rate limiting между API-запросами

Использование::

    from parser import MultiChannelParser

    parser = MultiChannelParser()
    await parser.run_once()          # Один прогон
    await parser.run_scheduled()     # Периодический запуск
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    SessionPasswordNeededError,
    SessionRevokedError,
)
from telethon.sessions import StringSession
from telethon.tl.types import Message

from config import settings
from models import Base, Channel, ChannelError, ParseLog, Post, User

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    """Текущее UTC время (вызывается каждый раз — не кэшируется)."""
    return datetime.now(timezone.utc)


def _extract_hashtags(text: str) -> list[str]:
    r"""Извлекает хэштеги (#\S+) из текста."""
    return re.findall(r"#\S+", text) if text else []


def _extract_mentions(text: str) -> list[str]:
    """Извлекает @упоминания из текста."""
    return re.findall(r"@\w+", text) if text else []


def _extract_urls(text: str) -> list[str]:
    """Извлекает http(s)-ссылки из текста."""
    return re.findall(r"https?://[^\s]+", text) if text else []


def _make_text_hash(text: str) -> str:
    """SHA-256 хэш текста для дедупликации."""
    return hashlib.sha256(text.encode()).hexdigest() if text else ""


def _get_media_type(message: Message) -> Optional[str]:
    """Определяет тип медиа в сообщении."""
    if not message.media:
        return None
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    if message.document:
        return "document"
    return "other"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ParseResult:
    """Результат парсинга одного канала."""

    posts_parsed: int = 0          # Всего обработано постов
    posts_new: int = 0             # Новых вставлено
    error_message: Optional[str] = None
    duration_ms: int = 0


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class MultiChannelParser:
    """Многоканальный асинхронный парсер Telegram-каналов.

    Args:
        api_id: Telegram API ID.
        api_hash: Telegram API hash.
        session_str: Строковая сессия Telethon.
        db_url: URL подключения к PostgreSQL (asyncpg).
    """

    def __init__(
        self,
        api_id: int = settings.TG_API_ID,
        api_hash: str = settings.TG_API_HASH,
        session_str: str = settings.TG_STRING_SESSION,
        db_url: str = settings.database_url_async,
    ) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_str = session_str
        self.db_url = db_url

        # Callback for live progress reporting (injected by web layer)
        self._progress_callback: Optional[Callable[..., None]] = None

        # Telethon client (инициализируется позже)
        self._client: Optional[TelegramClient] = None

        # DB engine & session factory (инициализируются позже)
        self._engine = None
        self._session_factory = None

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------

    async def init_db(self) -> None:
        """Создаёт engine, session factory и таблицы (если не существуют).

        Idempotent: safe to call multiple times. Disposes old engine
        to prevent connection leaks.
        """
        if self._engine is not None:
            await self._engine.dispose()

        self._engine = create_async_engine(
            self.db_url,
            echo=False,
            pool_size=10,
            max_overflow=20,
        )
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        # create_all вне транзакции — избегаем deadlock при параллельных инстансах
        async with self._engine.connect() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.commit()

        # One-time migration: make parse_logs.channel_id nullable
        # Check first to avoid AccessExclusiveLock contention
        async with self._engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT is_nullable FROM information_schema.columns
                WHERE table_name = 'parse_logs' AND column_name = 'channel_id'
            """))
            row = result.fetchone()
            if row and row[0] == 'NO':
                await conn.execute(text("SET LOCAL lock_timeout = '3s'"))
                try:
                    await conn.execute(text(
                        "ALTER TABLE parse_logs ALTER COLUMN channel_id DROP NOT NULL"
                    ))
                    logger.info("[migrate] parse_logs.channel_id → nullable")
                except Exception:
                    pass  # Already done by another instance

            # Migration: add sender_name to posts if missing
            try:
                result = await conn.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'posts' AND column_name = 'sender_name'
                """))
                if not result.fetchone():
                    await conn.execute(text("SET LOCAL lock_timeout = '3s'"))
                    await conn.execute(text(
                        "ALTER TABLE posts ADD COLUMN sender_name VARCHAR(255)"
                    ))
                    logger.info("[migrate] posts.sender_name column added")
            except Exception:
                pass

        logger.info("База данных инициализирована")
        # Create default admin user if not exists
        await self._ensure_admin_user()

    async def _ensure_admin_user(self) -> None:
        """Create default admin 'vlad' if no users exist."""
        async with self._db_session() as session:
            result = await session.execute(select(User))
            if result.scalars().first() is None:
                # No users yet — create default admin
                admin = User(
                    username="vlad",
                    is_active=True,
                )
                admin.set_password("!1234567890")
                session.add(admin)
                await session.commit()
                logger.info("Default admin user 'vlad' created")
            else:
                logger.debug("Users already exist, skipping default admin creation")

    @asynccontextmanager
    async def _db_session(self):
        """Async context manager для сессии БД."""
        if self._session_factory is None:
            raise RuntimeError("init_db() не был вызван")
        async with self._session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    # ------------------------------------------------------------------
    # Telegram client
    # ------------------------------------------------------------------

    async def _ensure_client(self) -> TelegramClient:
        """Возвращает подключённого TelegramClient. Lazy signal setup."""
        if self._client is not None and self._client.is_connected():
            return self._client

        # Lazy signal handler registration (guaranteed running loop here)
        self._setup_signal_handlers()

        if self._client is not None:
            await self._client.disconnect()

        session = (
            StringSession(self.session_str)
            if self.session_str
            else settings.TG_SESSION
        )
        self._client = TelegramClient(session, self.api_id, self.api_hash)
        await self._client.start()
        me = await self._client.get_me()
        logger.info(
            "TelegramClient подключён как %s (@%s)",
            me.first_name,
            me.username,
        )
        return self._client

    async def _disconnect_client(self) -> None:
        """Отключает TelegramClient."""
        if self._client is not None and self._client.is_connected():
            await self._client.disconnect()
            logger.info("TelegramClient отключён")

    # ------------------------------------------------------------------
    # Channel sync
    # ------------------------------------------------------------------

    async def sync_channel(self, db_session: AsyncSession, identifier: str) -> Channel:
        """Получает (или создаёт) запись Channel в БД, синхронизируя метаданные.

        Args:
            db_session: Активная сессия SQLAlchemy.
            identifier: Username или numeric ID канала (без @).

        Returns:
            Экземпляр Channel (существующий или новый).
        """
        client = await self._ensure_client()

        # Конвертируем numeric ID в полный telegram ID
        entity_id = identifier
        if identifier.isdigit():
            # Numeric ID → -100{ID} (приватный канал)
            entity_id = int(f"-100{identifier}")
            logger.debug("[sync_channel] numeric ID '%s' → telegram ID %s", identifier, entity_id)
        elif identifier.startswith('-') and identifier[1:].isdigit():
            # Bare negative ID (группа/чат)
            entity_id = int(identifier)
            logger.debug("[sync_channel] bare negative ID → %s", entity_id)

        try:
            # Use PeerChannel for negative IDs to ensure correct entity type
            if isinstance(entity_id, int) and entity_id < 0:
                from telethon.tl.types import PeerChannel
                entity = await client.get_entity(PeerChannel(abs(entity_id)))
                logger.debug("[sync_channel] Used PeerChannel(%s) for %s", abs(entity_id), identifier)
            else:
                entity = await client.get_entity(entity_id)
        except FloodWaitError as e:
            logger.warning("[sync_channel] FloodWait %d сек для %s — пропускаем", e.seconds, identifier)
            raise ValueError(f"FloodWait: {e.seconds} сек — канал временно недоступен")

        # Определяем тип канала и numeric_id
        has_username = bool(getattr(entity, "username", None))
        channel_type = "public" if has_username else "private"

        # Вычисляем numeric_id (без -100 префикса) для приватных каналов
        telegram_id = entity.id
        numeric_id = None
        if telegram_id < 0:
            numeric_id = abs(telegram_id) % 1_000_000_000_000
        else:
            numeric_id = telegram_id

        # Ищем канал по telegram_id или username
        if has_username:
            result = await db_session.execute(
                select(Channel).where(Channel.username == entity.username)
            )
        else:
            result = await db_session.execute(
                select(Channel).where(Channel.telegram_id == telegram_id)
            )
        channel: Optional[Channel] = result.scalar_one_or_none()

        if channel is None:
            channel = Channel(
                telegram_id=telegram_id,
                numeric_id=numeric_id,
                channel_type=channel_type,
                username=entity.username if has_username else None,
                title=entity.title,
                description=getattr(entity, "about", None),
                subscriber_count=getattr(entity, "participants_count", 0),
                is_active=True,
                parse_error_count=0,
                last_error_message=None,
                last_error_at=None,
                total_posts_parsed=0,
                last_parsed_at=None,
            )
            db_session.add(channel)
            await db_session.flush()
            logger.info(
                "Канал создан: %s (type=%s, tid=%s, numeric=%s)",
                entity.username or str(numeric_id),
                channel_type,
                telegram_id,
                numeric_id,
            )
        else:
            channel.title = entity.title
            channel.subscriber_count = getattr(entity, "participants_count", 0)
            channel.numeric_id = numeric_id
            channel.channel_type = channel_type
            # При успешном sync — сбрасываем error_count (канал жив)
            if channel.parse_error_count > 0:
                channel.parse_error_count = 0
                channel.last_error_message = None
            logger.debug("Канал обновлён: %s", entity.username or str(numeric_id))

        return channel

    # ------------------------------------------------------------------
    # Retry helper
    # ------------------------------------------------------------------

    async def _with_retry(
        self,
        coro_factory,
        channel_name: str,
        operation: str,
    ):
        """Выполняет корутину с retry и exponential backoff.

        Args:
            coro_factory: Callable, возвращающий awaitable (фабрика, не сам объект!).
            channel_name: Имя канала для логирования.
            operation: Описание операции для логирования.

        Returns:
            Результат выполнения корутины.

        Raises:
            Последнее исключение, если все попытки исчерпаны.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, settings.RETRY_ATTEMPTS + 1):
            try:
                return await coro_factory()
            except FloodWaitError as e:
                wait = e.seconds
                logger.warning(
                    "[%s] FloodWaitError (%s) — ждём %d сек (попытка %d/%d)",
                    channel_name,
                    operation,
                    wait,
                    attempt,
                    settings.RETRY_ATTEMPTS,
                )
                await asyncio.sleep(wait)
                last_exc = e
            except (SessionRevokedError, SessionPasswordNeededError) as e:
                logger.error(
                    "[%s] Ошибка сессии (%s): %s",
                    channel_name,
                    type(e).__name__,
                    e,
                )
                raise
            except ValueError as e:
                # Приватные / удалённые / несуществующие каналы
                logger.error(
                    "[%s] Недоступный канал (%s): %s",
                    channel_name,
                    operation,
                    e,
                )
                raise
            except Exception as e:
                last_exc = e
                delay = settings.RETRY_DELAY_BASE * (2 ** (attempt - 1))
                logger.warning(
                    "[%s] Ошибка %s (попытка %d/%d): %s — retry через %ds",
                    channel_name,
                    operation,
                    attempt,
                    settings.RETRY_ATTEMPTS,
                    e,
                    delay,
                )
                if attempt < settings.RETRY_ATTEMPTS:
                    await asyncio.sleep(delay)

        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Error tracking
    # ------------------------------------------------------------------

    async def _record_channel_error(
        self,
        db_session: AsyncSession,
        channel: Channel,
        error_type: str,
        error_message: str,
    ) -> None:
        """Записывает ошибку в channel_errors и обновляет счётчик в канале."""
        error = ChannelError(
            channel_id=channel.id,
            error_type=error_type,
            error_message=error_message,
        )
        db_session.add(error)

        channel.parse_error_count += 1
        channel.last_error_message = error_message
        channel.last_error_at = _now()
        await db_session.commit()

    # ------------------------------------------------------------------
    # Global text dedup cache
    # ------------------------------------------------------------------

    async def _is_text_hash_exists(
        self,
        db_session: AsyncSession,
        text_hash: str,
    ) -> bool:
        """Проверяет глобально, существует ли пост с таким text_hash.

        Returns:
            True если такой хэш уже есть от ЛЮБОГО канала.
        """
        if not text_hash:
            return False
        result = await db_session.execute(
            select(Post.id).where(Post.text_hash == text_hash).limit(1)
        )
        return result.scalar_one_or_none() is not None

    # ------------------------------------------------------------------
    # Per-channel parsing
    # ------------------------------------------------------------------

    async def parse_single_channel(
        self,
        db_session: AsyncSession,
        channel: Channel,
        limit: Optional[int] = None,
        history: bool = False,
    ) -> ParseResult:
        """Парсит один канал с retry logic и rate limiting.

        Args:
            db_session: Активная сессия SQLAlchemy.
            channel: Модель Channel (должна быть синхронизирована).
            limit: Лимит постов (None — без ограничений).
            history: Если True — парсить всю историю, иначе инкрементально.

        Returns:
            ParseResult с результатами парсинга.
        """
        result = ParseResult()
        parsed_count = 0
        new_count = 0
        username = channel.username or str(channel.telegram_id)

        try:
            # --- Определяем min_id для инкрементального парсинга ---
            min_id = 0
            if not history:
                res = await db_session.execute(
                    select(Post.telegram_message_id)
                    .where(Post.channel_id == channel.id)
                    .order_by(Post.telegram_message_id.desc())
                    .limit(1)
                )
                last_msg_id = res.scalar_one_or_none()
                if last_msg_id:
                    min_id = last_msg_id
                    logger.info(
                        "[%s] Инкрементальный парсинг: min_id=%d",
                        username,
                        min_id,
                    )

            # --- Используем telegram_id напрямую (entity уже получен в sync_channel) ---
            client = await self._ensure_client()

            # --- Rate limit перед началом iter_messages ---
            await asyncio.sleep(0.5)

            # --- Итерируем сообщения (по ID канала, без повторного get_entity) ---
            # Use PeerChannel for negative IDs to ensure correct entity type
            if channel.telegram_id < 0 and not channel.username:
                from telethon.tl.types import PeerChannel
                entity_id = PeerChannel(abs(channel.telegram_id))
                logger.debug("[%s] Using PeerChannel(%s)", username, abs(channel.telegram_id))
            else:
                entity_id = channel.telegram_id
            msg_counter = 0
            async for message in client.iter_messages(
                entity_id,
                limit=limit,
                min_id=min_id if not history else 0,
            ):
                # Rate limit: sleep каждые 50 сообщений

                # Пропускаем посты без текста и без медиа
                if not message.text and not message.media:
                    continue

                # Rate limit: sleep каждые 50 сообщений (не на каждое!)
                msg_counter += 1
                if msg_counter % 50 == 0:
                    await asyncio.sleep(0.5)

                parsed_count += 1

                text = message.text or ""
                text_hash = _make_text_hash(text)

                # --- Глобальная дедупликация по text_hash ---
                if text_hash:
                    exists = await self._is_text_hash_exists(
                        db_session, text_hash
                    )
                    if exists:
                        logger.debug(
                            "[%s] Дубликат по text_hash, пропуск: msg_id=%d",
                            username,
                            message.id,
                        )
                        continue

                # --- Проверка уникальности (channel_id, telegram_message_id) ---
                dup_check = await db_session.execute(
                    select(Post.id).where(
                        (Post.channel_id == channel.id)
                        & (Post.telegram_message_id == message.id)
                    )
                )
                if dup_check.scalar_one_or_none() is not None:
                    continue

                # --- Sender info ---
                sender_name: Optional[str] = None
                if message.sender:
                    sender = message.sender
                    if hasattr(sender, 'first_name'):
                        sender_name = (sender.first_name or '') + (' ' + sender.last_name if sender.last_name else '')
                        if not sender_name.strip() and hasattr(sender, 'username') and sender.username:
                            sender_name = '@' + sender.username
                    elif hasattr(sender, 'title'):
                        sender_name = sender.title

                # --- Forward info ---
                forward_from: Optional[str] = None
                if message.forward and message.forward.chat:
                    forward_from = (
                        message.forward.chat.username
                        or message.forward.chat.title
                    )

                # --- Создаём пост ---
                post = Post(
                    channel_id=channel.id,
                    telegram_message_id=message.id,
                    text=text,
                    text_hash=text_hash,
                    views_count=message.views or 0,
                    forwards_count=message.forwards or 0,
                    replies_count=(
                        message.replies.replies
                        if message.replies
                        else 0
                    ),
                    hashtags=_extract_hashtags(text),
                    mentions=_extract_mentions(text),
                    urls=_extract_urls(text),
                    forward_from=forward_from,
                    sender_name=sender_name,
                    has_media=message.media is not None,
                    media_type=_get_media_type(message),
                    published_at=message.date,
                    edited_at=message.edit_date,
                )
                db_session.add(post)
                new_count += 1

                # --- Коммит каждые 100 постов ---
                if new_count % 100 == 0:
                    await db_session.commit()
                    logger.info(
                        "[%s] Сохранено %d новых постов...",
                        username,
                        new_count,
                    )

            # --- Финальный коммит ---
            await db_session.commit()

            # --- Обновляем канал ---
            channel.total_posts_parsed += new_count
            channel.last_parsed_at = _now()
            channel.parse_error_count = 0
            channel.last_error_message = None
            await db_session.commit()

            result.posts_parsed = parsed_count
            result.posts_new = new_count
            logger.info(
                "[%s] Готово! Обработано: %d, Новых: %d",
                username,
                parsed_count,
                new_count,
            )

        except ValueError as e:
            # Приватный / удалённый / несуществующий канал
            error_msg = f"Недоступный канал: {e}"
            result.error_message = error_msg
            logger.error("[%s] %s", username, error_msg)
            await self._record_channel_error(
                db_session, channel, "unavailable", error_msg
            )

        except FloodWaitError as e:
            error_msg = f"FloodWait: {e.seconds} сек"
            result.error_message = error_msg
            logger.error("[%s] %s", username, error_msg)
            await self._record_channel_error(
                db_session, channel, "flood_wait", error_msg
            )

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            result.error_message = error_msg
            logger.exception("[%s] Критическая ошибка: %s", username, error_msg)
            await self._record_channel_error(
                db_session, channel, type(e).__name__.lower(), error_msg
            )

        return result

    # ------------------------------------------------------------------
    # Channel lookup (NO get_entity — safe from FloodWait)
    # ------------------------------------------------------------------

    async def _lookup_channel(self, db_session: AsyncSession, identifier: str) -> Optional[Channel]:
        """Ищет канал в БД по identifier БЕЗ вызова get_entity().

        Returns:
            Channel если найден, иначе None.
        """
        logger.debug("[_lookup_channel] Searching for: '%s'", identifier)

        # По username
        result = await db_session.execute(
            select(Channel).where(Channel.username == identifier)
        )
        ch = result.scalar_one_or_none()
        if ch:
            logger.info("[_lookup_channel] '%s' found by username", identifier)
            return ch

        # По numeric_id (bare positive number like "3147415698")
        if identifier.isdigit():
            num_id = int(identifier)
            result = await db_session.execute(
                select(Channel).where(Channel.numeric_id == num_id)
            )
            ch = result.scalar_one_or_none()
            if ch:
                logger.info("[_lookup_channel] '%s' found by numeric_id=%s", identifier, num_id)
                return ch
            # По telegram_id (-1003147415698)
            tid = int(f"-100{identifier}")
            result = await db_session.execute(
                select(Channel).where(Channel.telegram_id == tid)
            )
            ch = result.scalar_one_or_none()
            if ch:
                logger.info("[_lookup_channel] '%s' found by telegram_id=%s", identifier, tid)
                return ch
            logger.warning("[_lookup_channel] '%s' NOT found (numeric_id=%s, tid=%s)", identifier, num_id, tid)

        # По bare negative ID (like "-740684703")
        if identifier.startswith('-') and identifier[1:].isdigit():
            int_id = int(identifier)
            result = await db_session.execute(
                select(Channel).where(Channel.telegram_id == int_id)
            )
            ch = result.scalar_one_or_none()
            if ch:
                logger.info("[_lookup_channel] '%s' found by bare negative tid=%s", identifier, int_id)
                return ch
            logger.warning("[_lookup_channel] '%s' NOT found (bare tid=%s)", identifier, int_id)

        # CRITICAL: Log ALL channels to debug why lookup fails
        result = await db_session.execute(
            select(Channel.id, Channel.telegram_id, Channel.numeric_id, Channel.username, Channel.title, Channel.is_active)
        )
        all_rows = result.all()
        logger.error("[_lookup_channel] '%s' NOT FOUND. ALL %d channels in DB:", identifier, len(all_rows))
        for row in all_rows:
            logger.error("  id=%s tid=%s numeric=%s user=%s title=%s active=%s",
                row.id, row.telegram_id, row.numeric_id, row.username, row.title, row.is_active)

        return None

    # ------------------------------------------------------------------
    # Core: parse all channels
    # ------------------------------------------------------------------

    async def _parse_one_channel(
        self,
        username: str,
        limit: Optional[int] = None,
        history: bool = False,
    ) -> tuple[str, ParseResult]:
        """Парсит один канал. При ошибке — rollback сессии, лог, идем дальше."""
        start_ts = time.monotonic()

        # Report: starting this channel
        if self._progress_callback:
            self._progress_callback(
                current_channel=username,
                current_operation=f"Parsing {username}...",
            )

        # Fresh DB session per channel — isolated, safe to rollback
        async with self._db_session() as db_session:
            channel: Optional[Channel] = None
            try:
                # 1. Ищем канал в БД
                channel = await self._lookup_channel(db_session, username)
                if not channel:
                    return username, ParseResult(error_message="Канал не найден в БД")
                if not channel.is_active:
                    return username, ParseResult(error_message="Канал деактивирован")

                # 2. Парсим
                result = await self.parse_single_channel(
                    db_session, channel, limit=limit, history=history
                )

                # 3. Лог
                db_session.add(ParseLog(
                    started_at=_now(), channel_id=channel.id,
                    posts_parsed=result.posts_parsed, posts_new=result.posts_new,
                    error_message=result.error_message, finished_at=_now(),
                    duration_ms=int((time.monotonic() - start_ts) * 1000),
                ))
                await db_session.commit()

                if self._progress_callback:
                    self._progress_callback(
                        increment_channels_done=1,
                        increment_posts_new=result.posts_new,
                        increment_posts_parsed=result.posts_parsed,
                        current_operation=f"Done {username}: +{result.posts_new} posts",
                    )
                return username, result

            except Exception as e:
                try:
                    await db_session.rollback()
                except Exception:
                    pass

                result = ParseResult(error_message=f"{type(e).__name__}: {e}")
                logger.exception("[%s] Ошибка: %s", username, e)

                # Log error
                try:
                    db_session.add(ParseLog(
                        started_at=_now(), channel_id=channel.id if channel else None,
                        posts_parsed=0, posts_new=0,
                        error_message=result.error_message, finished_at=_now(),
                        duration_ms=int((time.monotonic() - start_ts) * 1000),
                    ))
                    await db_session.commit()
                except Exception:
                    pass

                return username, result

    async def _get_active_channels_from_db(self) -> list[str]:
        """Получает список активных каналов из БД.

        Returns:
            Список идентификаторов (username или numeric_id как строка).
        """
        async with self._db_session() as db_session:
            result = await db_session.execute(
                select(Channel).where(Channel.is_active == True)
            )
            channels = result.scalars().all()
            identifiers = []
            for ch in channels:
                # Используем username если есть, иначе numeric_id или telegram_id
                if ch.username:
                    identifiers.append(ch.username)
                elif ch.numeric_id:
                    identifiers.append(str(ch.numeric_id))
                else:
                    identifiers.append(str(ch.telegram_id))
            return identifiers

    async def parse_all(
        self,
        channels: Optional[list[str]] = None,
        limit: Optional[int] = None,
        history: bool = False,
    ) -> dict[str, ParseResult]:
        """Парсит все каналы параллельно.

        Args:
            channels: Список username каналов. Если None — берётся из БД (active).
            limit: Лимит постов на канал.
            history: Если True — парсить всю историю.

        Returns:
            Словарь {username: ParseResult}.
        """
        # Если channels переданы явно — используем их (для ручного запуска)
        # Иначе берём активные каналы из БД
        if channels:
            channel_list = channels
        else:
            channel_list = await self._get_active_channels_from_db()

        if not channel_list:
            logger.warning("Список каналов пуст — нечего парсить")
            return {}

        total_start = time.monotonic()
        logger.info(
            "=== Запуск парсинга: %d каналов ===",
            len(channel_list),
        )
        if self._progress_callback:
            self._progress_callback(
                channels_total=len(channel_list),
                channels_done=0,
                current_operation=f"Starting {len(channel_list)} channels...",
            )

        results: dict[str, ParseResult] = {}
        total_parsed = 0
        total_new = 0
        total_errors = 0

        for ch in channel_list:
            try:
                username, result = await self._parse_one_channel(ch, limit, history)
                results[username] = result
                total_parsed += result.posts_parsed
                total_new += result.posts_new
                if result.error_message:
                    total_errors += 1
            except Exception as e:
                total_errors += 1
                logger.error("Канал %s упал: %s", ch, e)
                results[ch] = ParseResult(error_message=str(e))

            # Пауза между каналами (FloodWait защита)
            await asyncio.sleep(5)

        total_duration = int((time.monotonic() - total_start) * 1000)
        logger.info(
            "=== Все каналы завершены: parsed=%d, new=%d, errors=%d, "
            "time=%dms ===",
            total_parsed,
            total_new,
            total_errors,
            total_duration,
        )
        return results

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    async def run_once(self) -> dict[str, ParseResult]:
        """Одиночный прогон парсера.

        Returns:
            Словарь {username: ParseResult}.
        """
        settings.validate_parser()
        await self.init_db()
        await self._ensure_client()

        try:
            results = await self.parse_all(
                limit=settings.LIMIT,
                history=settings.HISTORY,
            )
        finally:
            await self._disconnect_client()

        return results

    async def run_scheduled(self) -> None:
        """Периодический запуск парсера по расписанию."""
        settings.validate_parser()
        await self.init_db()
        await self._ensure_client()

        logger.info(
            "Периодический парсинг: интервал=%d сек", settings.INTERVAL_SEC
        )

        try:
            while True:
                logger.info("[%s] Запуск парсинга...", _now().isoformat())

                try:
                    await self.parse_all(
                        limit=None,          # В режиме schedule — без лимита
                        history=False,       # Только новые посты
                    )
                except Exception as e:
                    logger.exception("Ошибка в цикле парсинга: %s", e)

                logger.info("Сон %d сек до следующего запуска", settings.INTERVAL_SEC)
                await asyncio.sleep(settings.INTERVAL_SEC)

        finally:
            logger.info("Завершение run_scheduled")
            await self._disconnect_client()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    """Точка входа CLI."""
    parser = MultiChannelParser()

    if settings.SCHEDULE_MODE:
        await parser.run_scheduled()
    else:
        results = await parser.run_once()

        # Краткая сводка
        print("\n" + "=" * 60)
        print("РЕЗУЛЬТАТЫ ПАРСИНГА")
        print("=" * 60)
        total_parsed = 0
        total_new = 0
        for username, res in results.items():
            status = "OK" if not res.error_message else "ERR"
            print(
                f"  [{status}] {username:25s} "
                f"parsed={res.posts_parsed:4d}  "
                f"new={res.posts_new:4d}  "
                f"time={res.duration_ms}ms"
            )
            if res.error_message:
                print(f"       → {res.error_message}")
            total_parsed += res.posts_parsed
            total_new += res.posts_new
        print("-" * 60)
        print(f"  ИТОГО: parsed={total_parsed}, new={total_new}")
        print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
