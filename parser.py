#!/usr/bin/env python3
"""
CITG Parser v3 — многоканальный парсер Telegram-каналов.

Ключевые особенности:
  * Кэширование access_hash в БД (обход get_entity() для private channels)
  * AdaptiveRateLimiter — адаптивный rate limiting
  * CircuitBreaker — per-channel + global защита от FloodWait
  * Последовательный парсинг с jitter (безопасно для 20+ каналов)
  * Zero API calls на resolve sender — используем message.post_author

Архитектура:
  ChannelResolver   → get_dialogs() + кэш access_hash в БД
  ChannelParser     → iter_messages(InputPeerChannel) без get_entity()
  MultiChannelParser→ оркестратор: sequential + circuit breaker + rate limit
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from sqlalchemy import select, text, BigInteger, Integer, DateTime, Text, String, Boolean, ForeignKey, Index, PrimaryKeyConstraint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    FloodWaitError,
    SessionPasswordNeededError,
    SessionRevokedError,
)
from telethon.sessions import StringSession
from telethon.tl.types import (
    Message,
    Channel as TlChannel,
    InputPeerChannel,
    PeerChannel,
)

# ─── Configuration ──────────────────────────────────────────
# Lazy import to avoid circular deps
_config = None

def _get_config():
    global _config
    if _config is None:
        try:
            from config import settings as _config
        except ImportError:
            # Fallback for standalone testing
            class _FallbackConfig:
                TG_API_ID = int(__import__('os').getenv('TG_API_ID', 0))
                TG_API_HASH = __import__('os').getenv('TG_API_HASH', '')
                TG_STRING_SESSION = __import__('os').getenv('TG_STRING_SESSION', '')
                DATABASE_URL = __import__('os').getenv('DATABASE_URL', 'postgresql+asyncpg://localhost/citg')
                SCHEDULE_MODE = __import__('os').getenv('SCHEDULE_MODE', 'false').lower() == 'true'
                INTERVAL_SEC = int(__import__('os').getenv('INTERVAL_SEC', 1200))
                MAX_CONCURRENT_CHANNELS = int(__import__('os').getenv('MAX_CONCURRENT_CHANNELS', 1))
                RETRY_ATTEMPTS = int(__import__('os').getenv('RETRY_ATTEMPTS', 2))
                CHANNEL_DELAY_SEC = int(__import__('os').getenv('CHANNEL_DELAY_SEC', 10))
                API_CALL_DELAY_MS = int(__import__('os').getenv('API_CALL_DELAY_MS', 1000))
                FLOOD_COOLDOWN_MIN = int(__import__('os').getenv('FLOOD_COOLDOWN_MIN', 30))
                BATCH_COMMIT_SIZE = int(__import__('os').getenv('BATCH_COMMIT_SIZE', 50))
                JITTER_SEC = float(__import__('os').getenv('JITTER_SEC', 10.0))
                LOG_LEVEL = __import__('os').getenv('LOG_LEVEL', 'INFO')
                LIMIT = None
                HISTORY = False
                HISTORY_BATCH_SIZE = int(__import__('os').getenv('HISTORY_BATCH_SIZE', 5000))
                HISTORY_MAX_BATCHES_PER_RUN = int(__import__('os').getenv('HISTORY_MAX_BATCHES_PER_RUN', 5))
                HISTORY_MAX_SECONDS_PER_CHANNEL = int(__import__('os').getenv('HISTORY_MAX_SECONDS_PER_CHANNEL', 900))
                database_url_async = property(lambda self: self.DATABASE_URL)
                @property
                def channels_list(self): return []
            _config = _FallbackConfig()
    return _config


logger = logging.getLogger(__name__)

# ─── Helpers ────────────────────────────────────────────────

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def extract_hashtags(text: str) -> list[str]:
    return re.findall(r"#\S+", text) if text else []


def extract_mentions(text: str) -> list[str]:
    return re.findall(r"@\w+", text) if text else []


def extract_urls(text: str) -> list[str]:
    return re.findall(r"https?://[^\s]+", text) if text else []


def hash_text(text: str) -> Optional[str]:
    return hashlib.sha256(text.encode()).hexdigest() if text else None


def get_media_type(message: Message) -> Optional[str]:
    if not message.media:
        return None
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    if message.audio:
        return "audio"
    if message.voice:
        return "voice"
    if message.document:
        return "document"
    if message.poll:
        return "poll"
    if message.geo:
        return "geo"
    if message.web_preview:
        return "web_preview"
    return "other"


def normalize_channel_id(telegram_id: int) -> int:
    """Преобразует telegram_id (например -1003147415698) в channel_id (3147415698)."""
    if telegram_id < 0:
        return abs(telegram_id) % 1_000_000_000_000
    return telegram_id


# ─── Dataclasses ────────────────────────────────────────────

@dataclass
class ParseResult:
    parsed: int = 0
    new: int = 0
    error: Optional[str] = None
    duration_ms: int = 0
    flood_wait_sec: int = 0
    batches: int = 0
    oldest_id: Optional[int] = None
    history_complete: bool = False


# ─── AdaptiveRateLimiter ────────────────────────────────────

class GlobalCooldownError(Exception):
    """Глобальный cooldown активен — нельзя делать API calls."""
    pass


class AdaptiveRateLimiter:
    """Адаптивный rate limiter: замедляется при ошибках, ускоряется при успехе.

    Args:
        min_delay: Минимальная задержка между API calls (сек).
        max_delay: Максимальная задержка (сек).
        initial_delay: Начальная задержка (сек).
    """

    def __init__(
        self,
        min_delay: float = 0.5,
        max_delay: float = 30.0,
        initial_delay: float = 1.0,
    ) -> None:
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.current_delay = initial_delay
        self.last_call_time = 0.0
        self.flood_wait_until = 0.0
        self.consecutive_errors = 0
        self.total_calls = 0
        self.total_errors = 0
        self.total_flood_waits = 0

    async def before_call(self) -> None:
        """Проверить cooldown и выждать адаптивную задержку.

        Raises:
            GlobalCooldownError: Если активен глобальный cooldown от FloodWait.
        """
        now = time.monotonic()

        # Глобальный cooldown
        if now < self.flood_wait_until:
            wait = self.flood_wait_until - now
            raise GlobalCooldownError(f"Глобальный cooldown: {wait:.0f} сек")

        # Адаптивная задержка между calls
        elapsed = now - self.last_call_time
        if elapsed < self.current_delay:
            await asyncio.sleep(self.current_delay - elapsed)

        self.last_call_time = time.monotonic()
        self.total_calls += 1

    def on_success(self) -> None:
        """Уменьшить задержку при успешном API call."""
        self.current_delay = max(
            self.min_delay,
            self.current_delay * 0.95,
        )
        if self.consecutive_errors > 0:
            self.consecutive_errors = max(0, self.consecutive_errors - 1)

    def on_error(self, error: Exception) -> None:
        """Увеличить задержку при ошибке."""
        self.consecutive_errors += 1
        self.total_errors += 1
        multiplier = min(2 ** self.consecutive_errors, 8)
        self.current_delay = min(self.max_delay, self.current_delay * multiplier)
        logger.debug(
            "RateLimiter: ошибка #%d, задержка %.2f сек",
            self.consecutive_errors,
            self.current_delay,
        )

    def on_flood_wait(self, seconds: int) -> None:
        """Установить глобальный cooldown при FloodWait от Telegram."""
        self.total_flood_waits += 1
        self.flood_wait_until = time.monotonic() + seconds + 5  # +5 сек buffer
        self.consecutive_errors += 1
        new_delay = max(seconds, self.current_delay * 2)
        self.current_delay = min(self.max_delay, new_delay)
        logger.warning(
            "RateLimiter: FloodWait %d сек, cooldown до %s, задержка %.2f сек",
            seconds,
            datetime.fromtimestamp(self.flood_wait_until).strftime("%H:%M:%S"),
            self.current_delay,
        )

    @property
    def is_cooldown_active(self) -> bool:
        return time.monotonic() < self.flood_wait_until

    def get_stats(self) -> dict[str, Any]:
        return {
            "current_delay_sec": round(self.current_delay, 2),
            "consecutive_errors": self.consecutive_errors,
            "total_calls": self.total_calls,
            "total_errors": self.total_errors,
            "total_flood_waits": self.total_flood_waits,
            "cooldown_active": self.is_cooldown_active,
            "cooldown_until": datetime.fromtimestamp(
                self.flood_wait_until, tz=timezone.utc
            ).isoformat() if self.flood_wait_until > 0 else None,
        }


# ─── CircuitBreaker ─────────────────────────────────────────

@dataclass
class ChannelCircuit:
    state: str = "closed"           # "closed" | "open" | "half-open"
    failure_count: int = 0
    last_failure_at: float = 0.0
    opened_at: float = 0.0
    total_failures: int = 0


class CircuitBreaker:
    """Circuit breaker: per-channel + global защита.

    Per-channel: 3 ошибки → circuit OPEN на 30 мин.
    Global: FloodWait > 60 сек → остановка всех каналов на 30 мин.
    """

    # Per-channel thresholds
    CHANNEL_THRESHOLD = 3           # Ошибок до открытия
    CHANNEL_OPEN_MINUTES = 30       # Минут circuit open
    CHANNEL_MAX_FAILURES = 10       # Деактивация канала

    # Global thresholds
    GLOBAL_FLOOD_THRESHOLD = 60     # FloodWait сек → глобальная пауза
    GLOBAL_COOLDOWN_MINUTES = 30    # Минут глобальной паузы

    def __init__(self) -> None:
        self._channels: dict[int, ChannelCircuit] = {}
        self._global_opened_at: float = 0.0
        self._global_flood_seconds: int = 0

    def _get(self, channel_id: int) -> ChannelCircuit:
        if channel_id not in self._channels:
            self._channels[channel_id] = ChannelCircuit()
        return self._channels[channel_id]

    def is_channel_open(self, channel_id: int) -> bool:
        """Проверить, открыт ли circuit для канала (про skip)."""
        circ = self._get(channel_id)
        if circ.state == "open":
            minutes_open = (time.monotonic() - circ.opened_at) / 60
            if minutes_open >= self.CHANNEL_OPEN_MINUTES:
                circ.state = "half-open"
                logger.info("[circuit] Канал %d: half-open (прошло %d мин)",
                           channel_id, int(minutes_open))
                return False
            return True
        return False

    def is_global_open(self) -> bool:
        """Проверить, активна ли глобальная пауза."""
        if self._global_opened_at == 0:
            return False
        minutes_open = (time.monotonic() - self._global_opened_at) / 60
        if minutes_open >= self.GLOBAL_COOLDOWN_MINUTES:
            self._global_opened_at = 0.0
            logger.info("[circuit] Глобальная пауза завершена")
            return False
        return True

    def on_channel_success(self, channel_id: int) -> None:
        circ = self._get(channel_id)
        if circ.state == "half-open":
            circ.state = "closed"
            circ.failure_count = 0
            logger.info("[circuit] Канал %d: circuit closed (восстановлен)", channel_id)
        elif circ.state == "closed":
            if circ.failure_count > 0:
                circ.failure_count = max(0, circ.failure_count - 1)

    def on_channel_failure(self, channel_id: int, error_type: str) -> None:
        circ = self._get(channel_id)
        circ.failure_count += 1
        circ.total_failures += 1
        circ.last_failure_at = time.monotonic()

        if circ.state == "half-open":
            circ.state = "open"
            circ.opened_at = time.monotonic()
            logger.warning(
                "[circuit] Канал %d: circuit OPEN (half-open → ошибка %s)",
                channel_id, error_type,
            )
        elif circ.failure_count >= self.CHANNEL_THRESHOLD and circ.state == "closed":
            circ.state = "open"
            circ.opened_at = time.monotonic()
            logger.warning(
                "[circuit] Канал %d: circuit OPEN (%d ошибок)",
                channel_id, circ.failure_count,
            )

    def on_global_flood(self, seconds: int) -> None:
        """Активировать глобальную паузу."""
        if seconds >= self.GLOBAL_FLOOD_THRESHOLD:
            self._global_opened_at = time.monotonic()
            self._global_flood_seconds = seconds
            logger.error(
                "[circuit] ГЛОБАЛЬНАЯ ПАУЗА на %d мин (FloodWait %d сек)",
                self.GLOBAL_COOLDOWN_MINUTES, seconds,
            )

    def should_deactivate(self, channel_id: int) -> bool:
        """Проверить, нужно ли деактивировать канал (>10 ошибок)."""
        return self._get(channel_id).total_failures >= self.CHANNEL_MAX_FAILURES

    def get_stats(self) -> dict[str, Any]:
        return {
            "global_cooldown_active": self.is_global_open(),
            "global_cooldown_remaining_min": max(0, int(
                self.GLOBAL_COOLDOWN_MINUTES -
                (time.monotonic() - self._global_opened_at) / 60
            )) if self._global_opened_at else 0,
            "channel_circuits": {
                str(cid): {
                    "state": c.state,
                    "failures": c.failure_count,
                    "total_failures": c.total_failures,
                }
                for cid, c in self._channels.items()
            },
        }


# ─── SenderResolver ─────────────────────────────────────────

class SenderResolver:
    """Resolve sender names via PostgreSQL cache + get_entity() fallback.

    Priority:
      1. msg.post_author (channels — already have author name)
      2. Memory cache (already resolved in this run)
      3. DB cache (resolved in previous runs)
      4. get_entity() API call → save to DB + memory cache

    All API calls go through AdaptiveRateLimiter to avoid FloodWait.
    """

    def __init__(
        self,
        client: TelegramClient,
        db_session_factory: async_sessionmaker,
        rate_limiter: AdaptiveRateLimiter,
    ) -> None:
        self.client = client
        self.db_factory = db_session_factory
        self.rate_limiter = rate_limiter
        self._memory_cache: dict[int, str] = {}  # Local cache per run

    async def get_sender_name(self, msg: Message) -> Optional[str]:
        """Get sender name for a message using the 4-tier cache."""
        # Tier 1: Channels have post_author directly
        if msg.post_author:
            return msg.post_author

        sender_id = msg.sender_id
        if not sender_id:
            return None

        # Tier 2: Memory cache (fastest, per-run)
        if sender_id in self._memory_cache:
            return self._memory_cache[sender_id]

        # Lazy import models to avoid circular deps
        try:
            from models import Sender as DbSender
        except ImportError:
            return str(sender_id)  # Fallback: just the ID

        try:
            async with self.db_factory() as session:
                # Tier 3: DB cache (survives container restarts)
                result = await session.execute(
                    select(DbSender).where(DbSender.telegram_user_id == sender_id)
                )
                db_sender = result.scalar_one_or_none()

                if db_sender:
                    name = db_sender.display_name
                    self._memory_cache[sender_id] = name
                    return name

                # Tier 4: API call — resolve via get_entity with rate limit
                try:
                    await self.rate_limiter.before_call()
                    entity = await self.client.get_entity(sender_id)
                    self.rate_limiter.on_success()

                    db_sender = DbSender(
                        telegram_user_id=sender_id,
                        first_name=getattr(entity, "first_name", None),
                        last_name=getattr(entity, "last_name", None),
                        username=getattr(entity, "username", None),
                    )
                    session.add(db_sender)
                    await session.commit()

                    name = db_sender.display_name
                    self._memory_cache[sender_id] = name
                    logger.info(
                        "[sender] Resolved %d → '%s' (@%s)",
                        sender_id,
                        name,
                        db_sender.username or "n/a",
                    )
                    return name

                except FloodWaitError as e:
                    self.rate_limiter.on_flood_wait(e.seconds)
                    logger.warning(
                        "[sender] FloodWait %d сек при resolve %d",
                        e.seconds,
                        sender_id,
                    )
                    return str(sender_id)

                except Exception as e:
                    logger.warning(
                        "[sender] Failed to resolve %d: %s",
                        sender_id,
                        e,
                    )
                    return str(sender_id)

        except Exception as e:
            logger.warning("[sender] DB error for %d: %s", sender_id, e)
            return str(sender_id)

    def get_stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        return {
            "memory_cache_size": len(self._memory_cache),
        }


# ─── ChannelResolver ────────────────────────────────────────

class ChannelResolver:
    """Резолвинг каналов через get_dialogs() + кэш access_hash в БД.

    Ключевой инсайт: StringSession не кэширует entities. При каждом
    cold start entity cache пуст. Для 20 private channels get_entity()
    вызовет FloodWait.

    Решение: get_dialogs() — один bulk call → все каналы с access_hash.
    Сохраняем в БД. При парсинге: InputPeerChannel(id, access_hash) из БД.
    """

    def __init__(
        self,
        client: TelegramClient,
        db_session_factory: async_sessionmaker,
    ) -> None:
        self.client = client
        self.db_factory = db_session_factory

    async def sync_dialogs(self) -> list[dict[str, Any]]:
        """Синхронизировать каналы через get_dialogs().

        Returns:
            Список словарей {id, title, access_hash} для каждого канала.
        """
        logger.info("[resolver] Получаем диалоги через get_dialogs()...")
        dialogs = await self.client.get_dialogs(limit=None)
        logger.info("[resolver] Получено %d диалогов", len(dialogs))

        channels: list[dict[str, Any]] = []
        async with self.db_factory() as session:
            for dialog in dialogs:
                entity = dialog.entity
                if not isinstance(entity, TlChannel):
                    continue

                channel_id = normalize_channel_id(entity.id)
                # Telegram хранит ID каналов/супергрупп как -100<numeric_id>.
                telegram_id = int(f"-100{entity.id}")
                access_hash = entity.access_hash

                channels.append({
                    "telegram_id": telegram_id,
                    "channel_id": channel_id,
                    "title": entity.title or "",
                    "username": entity.username,
                    "access_hash": access_hash,
                    "broadcast": entity.broadcast,
                })

                # Обновляем access_hash для всех существующих каналов/групп.
                # Новые каналы НЕ создаём — список для парсинга строго
                # контролируется через web UI (/channels).
                await self._upsert_channel(
                    session, telegram_id, channel_id,
                    entity.title, entity.username, access_hash,
                    allow_create=False,
                )

            await session.commit()

        logger.info(
            "[resolver] Синхронизировано %d каналов/групп (%d broadcast)",
            len(channels),
            sum(1 for c in channels if c.get("broadcast")),
        )
        return channels

    async def _upsert_channel(
        self,
        session: AsyncSession,
        telegram_id: int,
        channel_id: int,
        title: str,
        username: Optional[str],
        access_hash: int,
        allow_create: bool = True,
    ) -> None:
        """Обновить или создать канал в БД."""
        # Lazy import models to avoid circular deps
        try:
            from models import Channel as DbChannel
        except ImportError:
            return  # Will be handled by caller

        result = await session.execute(
            select(DbChannel).where(DbChannel.telegram_id == telegram_id)
        )
        db_ch = result.scalar_one_or_none()

        if db_ch is None:
            # Пробуем найти по username
            if username:
                result = await session.execute(
                    select(DbChannel).where(DbChannel.username == username)
                )
                db_ch = result.scalar_one_or_none()

        if db_ch is None:
            if not allow_create:
                logger.debug(
                    "[resolver] Пропуск: %s (tid=%d) не в БД (create disabled)",
                    title, telegram_id,
                )
                return
            # Создаём новый канал только если разрешено
            db_ch = DbChannel(
                telegram_id=telegram_id,
                numeric_id=channel_id,
                title=title,
                username=username,
                channel_type="public" if username else "private",
                access_hash=access_hash,
                entity_resolved_at=utc_now(),
            )
            session.add(db_ch)
            logger.info("[resolver] Создан новый канал: %s (tid=%d)", title, telegram_id)
        else:
            db_ch.title = title
            db_ch.username = username
            db_ch.access_hash = access_hash
            db_ch.entity_resolved_at = utc_now()
            if db_ch.channel_type == "public" and not username:
                db_ch.channel_type = "private"
            elif db_ch.channel_type == "private" and username:
                db_ch.channel_type = "public"
            logger.debug("[resolver] Обновлён: %s", title)

    async def get_active_channels(self) -> list[Any]:
        """Получить список активных каналов из БД."""
        try:
            from models import Channel as DbChannel
        except ImportError:
            return []

        async with self.db_factory() as session:
            result = await session.execute(
                select(DbChannel)
                .where(DbChannel.is_active == True)
                .order_by(DbChannel.last_parsed_at.asc().nullsfirst())
            )
            return list(result.scalars().all())

    async def build_input_peer(self, channel: Any):
        """Построить input peer. Chat первым — порядок ВАЖЕН.
        
        Chat (basic group): telegram_id > 0 → InputPeerChat (no access_hash)
        Channel: telegram_id < 0 → InputPeerChannel (needs access_hash)
        """
        from telethon.tl.types import InputPeerChat
        
        tid = channel.telegram_id
        
        # === 1. Chat (basic group) — telegram_id is positive ===
        if tid is not None and tid > 0:
            logger.info("[resolver] InputPeerChat(chat_id=%d) для '%s' [Chat]", tid, channel.title)
            return InputPeerChat(tid)
        
        # === 2. Channel — needs access_hash ===
        if not channel.access_hash:
            logger.warning("[resolver] '%s' tid=%d: нет access_hash, fallback...", channel.title, tid)
            try:
                from telethon.tl.types import PeerChannel
                entity = await self.client.get_entity(PeerChannel(abs(tid) % 1_000_000_000_000))
                if isinstance(entity, TlChannel):
                    channel.access_hash = entity.access_hash
                    channel.entity_resolved_at = utc_now()
                    async with self.db_factory() as session:
                        result = await session.execute(
                            select(type(channel)).where(type(channel).id == channel.id)
                        )
                        db_ch = result.scalar_one()
                        db_ch.access_hash = entity.access_hash
                        db_ch.entity_resolved_at = utc_now()
                        await session.commit()
                    logger.info("[resolver] Got access_hash для '%s'", channel.title)
                else:
                    raise ValueError(f"Entity is {type(entity).__name__}, not Channel")
            except ChannelPrivateError:
                raise ValueError(
                    f"'{channel.title}' (tid={tid}): недоступен. "
                    f"Убедитесь, что аккаунт состоит в этом private-канале/группе."
                )
            except ChannelInvalidError:
                raise ValueError(
                    f"'{channel.title}' (tid={tid}): не существует или удалён."
                )
            except Exception as e:
                raise ValueError(f"'{channel.title}' (tid={tid}): нет access_hash, ошибка: {e}")
        
        channel_id = normalize_channel_id(tid)
        logger.debug("[resolver] InputPeerChannel(id=%d) для '%s'", channel_id, channel.title)
        return InputPeerChannel(channel_id, channel.access_hash)


# ─── ChannelParser ──────────────────────────────────────────

class ChannelParser:
    """Парсинг одного канала: iter_messages без лишних API calls."""

    def __init__(
        self,
        client: TelegramClient,
        db_session_factory: async_sessionmaker,
        rate_limiter: AdaptiveRateLimiter,
        batch_size: int = 50,
        sender_resolver: Optional["SenderResolver"] = None,
        progress_callback: Optional[Callable[..., None]] = None,
    ) -> None:
        self.client = client
        self.db_factory = db_session_factory
        self.rate_limiter = rate_limiter
        self.batch_size = batch_size
        self.sender_resolver = sender_resolver
        self.progress_callback = progress_callback

    async def parse(
        self,
        channel: Any,
        input_peer: Any,  # InputPeerChannel | InputPeerChat
        *,
        history_batch_size: int = 5000,
        max_history_batches: Optional[int] = None,
        max_channel_seconds: Optional[int] = None,
    ) -> ParseResult:
        """Парсит один канал: инкрементально + постраничный backfill истории.

        Args:
            channel: Модель Channel из БД.
            input_peer: InputPeerChannel из кэша (без get_entity!).
            history_batch_size: Размер одной исторической партии.
            max_history_batches: Макс. число исторических партий за прогон.
            max_channel_seconds: Макс. время на канал за прогон.

        Returns:
            ParseResult с количеством обработанных и новых постов.
        """
        start_ts = time.monotonic()
        result = ParseResult()

        try:
            from models import Post as DbPost, ParseLog, Channel as DbChannel
        except ImportError as e:
            result.error = f"Models import error: {e}"
            return result

        # ─── Шаг 1: Загружаем состояние канала ───
        last_msg_id: Optional[int] = None
        existing_hashes: set[str] = set()
        existing_msg_ids: set[int] = set()

        try:
            async with self.db_factory() as session:
                last_msg_id = await self._get_last_message_id(session, channel.id)
                existing_hashes, existing_msg_ids = await self._load_existing_hashes(
                    session, channel.id
                )

                logger.info(
                    "[parse] Канал '%s' [%d]: last_msg_id=%s, "
                    "cached_hashes=%d, cached_msg_ids=%d, "
                    "history_complete=%s, cursor=%s",
                    channel.title, channel.id,
                    last_msg_id if last_msg_id else "(новый)",
                    len(existing_hashes), len(existing_msg_ids),
                    channel.history_complete,
                    channel.history_cursor,
                )
        except Exception as e:
            logger.warning("[parse] Ошибка загрузки кэша: %s", e)
            existing_hashes = set()
            existing_msg_ids = set()

        total_parsed = 0
        total_new = 0
        history_batches = 0
        channel_deadline: Optional[float] = None
        if max_channel_seconds:
            channel_deadline = time.monotonic() + max_channel_seconds

        # ─── Шаг 2: Инкрементальная подгрузка новых сообщений ───
        if last_msg_id is not None:
            try:
                inc_posts, inc_parsed = await self._fetch_batch(
                    input_peer,
                    min_id=last_msg_id,
                    existing_hashes=existing_hashes,
                    existing_msg_ids=existing_msg_ids,
                    limit=None,
                )
                if inc_posts:
                    inc_new = await self._save_posts(channel.id, inc_posts)
                    total_parsed += inc_parsed
                    total_new += inc_new
                    for p in inc_posts:
                        if p["text_hash"]:
                            existing_hashes.add(p["text_hash"])
                        existing_msg_ids.add(p["telegram_message_id"])

                    logger.info(
                        "[parse] Канал '%s': инкрементально "
                        "обработано=%d, новых=%d",
                        channel.title, inc_parsed, inc_new,
                    )

                    if self.progress_callback:
                        self.progress_callback(
                            current_channel=channel.title or str(channel.id),
                            current_operation="Инкрементальная подгрузка",
                            increment_posts_parsed=inc_parsed,
                            increment_posts_new=inc_new,
                        )
            except (FloodWaitError, ChannelInvalidError, ChannelPrivateError) as e:
                return self._handle_telegram_error(e, channel, result)
            except Exception as e:
                result.error = f"{type(e).__name__}: {e}"
                logger.exception(
                    "[parse] Канал '%s' [%d]: Ошибка инкрементальной загрузки",
                    channel.title, channel.id,
                )
                return result

        # ─── Шаг 3: Backfill истории ───
        if not channel.history_complete:
            try:
                history_batches = await self._backfill_history(
                    channel=channel,
                    input_peer=input_peer,
                    existing_hashes=existing_hashes,
                    existing_msg_ids=existing_msg_ids,
                    history_batch_size=history_batch_size,
                    max_history_batches=max_history_batches,
                    channel_deadline=channel_deadline,
                )
            except (FloodWaitError, ChannelInvalidError, ChannelPrivateError) as e:
                return self._handle_telegram_error(e, channel, result)
            except Exception as e:
                result.error = f"{type(e).__name__}: {e}"
                logger.exception(
                    "[parse] Канал '%s' [%d]: Ошибка загрузки истории",
                    channel.title, channel.id,
                )
                return result

        # ─── Шаг 4: Финальное обновление канала и лог ───
        try:
            async with self.db_factory() as session:
                db_channel = await session.get(DbChannel, channel.id)
                if db_channel is None:
                    result.error = f"Channel {channel.id} not found in DB"
                    return result

                db_channel.total_posts_parsed = (db_channel.total_posts_parsed or 0) + total_new
                db_channel.last_parsed_at = utc_now()
                db_channel.parse_error_count = 0
                db_channel.last_error_message = None
                db_channel.last_error_at = None
                db_channel.metadata_json = channel.metadata_json
                await session.commit()

                duration_ms = int((time.monotonic() - start_ts) * 1000)
                session.add(ParseLog(
                    channel_id=channel.id,
                    posts_parsed=total_parsed,
                    posts_new=total_new,
                    duration_ms=duration_ms,
                    started_at=datetime.fromtimestamp(start_ts, tz=timezone.utc),
                    finished_at=utc_now(),
                ))
                await session.commit()

                result.parsed = total_parsed
                result.new = total_new
                result.duration_ms = duration_ms
                result.batches = history_batches
                result.oldest_id = channel.history_cursor
                result.history_complete = channel.history_complete

                logger.info(
                    "[parse] Канал '%s' [%d]: обработано=%d, новых=%d, "
                    "партий истории=%d, история_завершена=%s, за %d мс",
                    channel.title, channel.id, total_parsed, total_new,
                    history_batches, channel.history_complete, duration_ms,
                )

        except Exception as e:
            result.error = f"Insert/state error: {type(e).__name__}: {e}"
            logger.exception(
                "[parse] Канал '%s' [%d]: Ошибка записи в БД",
                channel.title, channel.id,
            )

        return result

    async def _load_existing_hashes(
        self,
        session: AsyncSession,
        channel_id: int,
    ) -> tuple[set[str], set[int]]:
        """Предзагружает text_hash и telegram_message_id канала в память."""
        try:
            from models import Post as DbPost
        except ImportError:
            return set(), set()

        hash_result = await session.execute(
            select(DbPost.text_hash)
            .where(
                (DbPost.channel_id == channel_id)
                & (DbPost.text_hash.isnot(None))
            )
        )
        existing_hashes = {row[0] for row in hash_result.all() if row[0]}

        id_result = await session.execute(
            select(DbPost.telegram_message_id)
            .where(DbPost.channel_id == channel_id)
        )
        existing_msg_ids = {row[0] for row in id_result.all()}
        return existing_hashes, existing_msg_ids

    async def _fetch_batch(
        self,
        input_peer: Any,
        *,
        existing_hashes: set[str],
        existing_msg_ids: set[int],
        min_id: Optional[int] = None,
        offset_id: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Читает одну партию сообщений из Telegram.

        Args:
            input_peer: InputPeer канала/чата.
            existing_hashes: Уже известные хэши текста (для дедупликации).
            existing_msg_ids: Уже известные ID сообщений.
            min_id: Если задан — читаем сообщения с id > min_id (инкремент).
            offset_id: Если задан — читаем сообщения старше offset_id (backfill).
            limit: Максимальное количество сообщений.

        Returns:
            (список raw_posts, количество обработанных сообщений).
        """
        kwargs: dict[str, Any] = {"limit": limit}
        if min_id is not None:
            kwargs["min_id"] = min_id
        if offset_id is not None:
            kwargs["offset_id"] = offset_id

        await self.rate_limiter.before_call()

        raw_posts: list[dict[str, Any]] = []
        parsed = 0

        async for msg in self.client.iter_messages(input_peer, **kwargs):
            parsed += 1

            text = msg.text or ""
            text_hash = hash_text(text)

            # Дедупликация в памяти (O(1), без БД)
            if text_hash and text_hash in existing_hashes:
                continue
            if msg.id in existing_msg_ids:
                continue

            # Resolve sender name (channels: post_author, chats: cache/API)
            sender_name = msg.post_author
            sender_telegram_id = None
            if not sender_name and self.sender_resolver:
                try:
                    sender_name = await self.sender_resolver.get_sender_name(msg)
                    sender_telegram_id = msg.sender_id
                except Exception as e:
                    logger.debug("[parse] Sender resolve error: %s", e)
                    sender_name = None

            raw_posts.append({
                "telegram_message_id": msg.id,
                "text": text,
                "text_hash": text_hash,
                "views_count": msg.views or 0,
                "forwards_count": msg.forwards or 0,
                "replies_count": (
                    msg.replies.replies
                    if msg.replies and hasattr(msg.replies, "replies")
                    else 0
                ),
                "hashtags": extract_hashtags(text),
                "mentions": extract_mentions(text),
                "urls": extract_urls(text),
                "forward_from": (
                    msg.forward.chat.username or msg.forward.chat.title
                    if msg.forward and msg.forward.chat else None
                ),
                "sender_name": sender_name,
                "sender_telegram_id": sender_telegram_id,
                "has_media": msg.media is not None,
                "media_type": get_media_type(msg),
                "published_at": msg.date,
                "edited_at": msg.edit_date,
            })

            # Rate limit каждые 100 сообщений (фикс C4)
            if parsed % 100 == 0:
                await self.rate_limiter.before_call()
                self.rate_limiter.on_success()

        # AI sentiment analysis for the collected posts
        if raw_posts:
            try:
                import sentiment_ai
                texts = [p["text"] for p in raw_posts]
                sentiments = await sentiment_ai.analyze_batch(texts)
                for p, s in zip(raw_posts, sentiments):
                    p["sentiment_label"] = s["label"]
                    p["sentiment_score"] = s["score"]
                    p["sentiment_source"] = s["source"]
            except Exception as e:
                logger.warning("[parse] Sentiment analysis failed: %s", e)
                for p in raw_posts:
                    p["sentiment_label"] = None
                    p["sentiment_score"] = None
                    p["sentiment_source"] = None

        return raw_posts, parsed

    async def _save_posts(
        self,
        channel_id: int,
        raw_posts: list[dict[str, Any]],
    ) -> int:
        """Сохраняет список постов в БД пачками, игнорируя дубли."""
        if not raw_posts:
            return 0

        try:
            from models import Post as DbPost
        except ImportError:
            return 0

        new_posts = 0
        async with self.db_factory() as session:
            for i in range(0, len(raw_posts), self.batch_size):
                batch = raw_posts[i:i + self.batch_size]
                db_posts = [
                    DbPost(channel_id=channel_id, **item)
                    for item in batch
                ]
                session.add_all(db_posts)
                try:
                    await session.commit()
                    new_posts += len(batch)
                except IntegrityError:
                    await session.rollback()
                    # Fallback: коммитим по одному, игнорируя дубли
                    for db_post in db_posts:
                        session.add(db_post)
                        try:
                            await session.commit()
                            new_posts += 1
                        except IntegrityError:
                            await session.rollback()

                logger.debug(
                    "[parse] Канал %d: коммит %d/%d (новых %d)",
                    channel_id, i + len(batch), len(raw_posts), new_posts,
                )

        return new_posts

    async def _backfill_history(
        self,
        channel: Any,
        input_peer: Any,
        existing_hashes: set[str],
        existing_msg_ids: set[int],
        history_batch_size: int,
        max_history_batches: Optional[int],
        channel_deadline: Optional[float],
    ) -> int:
        """Постранично догружает историю канала от новых к старым."""
        from models import Channel as DbChannel

        batches = 0

        while True:
            if max_history_batches is not None and batches >= max_history_batches:
                logger.info(
                    "[backfill] Канал '%s': достигнут лимит партий за прогон (%d)",
                    channel.title, max_history_batches,
                )
                break

            if channel_deadline and time.monotonic() >= channel_deadline:
                logger.info(
                    "[backfill] Канал '%s': достигнут лимит времени на канал",
                    channel.title,
                )
                break

            offset_id = channel.history_cursor
            posts, parsed = await self._fetch_batch(
                input_peer,
                offset_id=offset_id,
                existing_hashes=existing_hashes,
                existing_msg_ids=existing_msg_ids,
                limit=history_batch_size,
            )

            batches += 1

            if not posts:
                channel.history_complete = True
                channel.history_cursor = None
                logger.info(
                    "[backfill] Канал '%s': история полностью догружена",
                    channel.title,
                )
                break

            new = await self._save_posts(channel.id, posts)

            # Обновляем in-memory кэши
            for p in posts:
                if p["text_hash"]:
                    existing_hashes.add(p["text_hash"])
                existing_msg_ids.add(p["telegram_message_id"])

            # Следующая партия старше min_id текущей
            min_id = min(p["telegram_message_id"] for p in posts)
            channel.history_cursor = min_id

            # Сохраняем cursor после каждой партии, чтобы можно было продолжить
            async with self.db_factory() as session:
                db_channel = await session.get(DbChannel, channel.id)
                if db_channel:
                    db_channel.metadata_json = channel.metadata_json
                    await session.commit()

            logger.info(
                "[backfill] Канал '%s': партия %d, "
                "обработано=%d, новых=%d, cursor=%d",
                channel.title, batches, parsed, new, min_id,
            )

            if self.progress_callback:
                self.progress_callback(
                    current_channel=channel.title or str(channel.id),
                    current_operation=f"История: партия {batches}, cursor={min_id}",
                    increment_posts_parsed=parsed,
                    increment_posts_new=new,
                )

            if len(posts) < history_batch_size:
                channel.history_complete = True
                channel.history_cursor = None
                logger.info(
                    "[backfill] Канал '%s': история полностью догружена "
                    "(последняя партия неполная)",
                    channel.title,
                )
                break

            # Короткая пауза между историческими партиями
            await asyncio.sleep(self.rate_limiter.current_delay)

        return batches

    def _handle_telegram_error(
        self,
        error: Exception,
        channel: Any,
        result: ParseResult,
    ) -> ParseResult:
        """Обрабатывает ошибки Telegram и возвращает ParseResult."""
        if isinstance(error, FloodWaitError):
            result.error = f"FloodWait: {error.seconds} сек"
            result.flood_wait_sec = error.seconds
            self.rate_limiter.on_flood_wait(error.seconds)
            logger.warning(
                "[parse] Канал '%s' [%d]: FloodWait %d сек",
                channel.title, channel.id, error.seconds,
            )
        elif isinstance(error, ChannelPrivateError):
            result.error = (
                f"Канал/чат недоступен: убедитесь, что аккаунт состоит в "
                f"'{channel.title}' (tid={channel.telegram_id})"
            )
            logger.error(
                "[parse] Канал '%s' [%d]: недоступен — аккаунт не состоит в канале/чате",
                channel.title, channel.id,
            )
        elif isinstance(error, ChannelInvalidError):
            result.error = (
                f"Канал/чат не существует или удалён: '{channel.title}' "
                f"(tid={channel.telegram_id})"
            )
            logger.error(
                "[parse] Канал '%s' [%d]: не существует или удалён",
                channel.title, channel.id,
            )
        else:
            result.error = f"{type(error).__name__}: {error}"
            logger.exception(
                "[parse] Канал '%s' [%d]: Ошибка чтения из Telegram",
                channel.title, channel.id,
            )
        return result

    async def _get_last_message_id(self, session: AsyncSession, channel_id: int) -> Optional[int]:
        try:
            from models import Post as DbPost
        except ImportError:
            return None
        result = await session.execute(
            select(DbPost.telegram_message_id)
            .where(DbPost.channel_id == channel_id)
            .order_by(DbPost.telegram_message_id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


# ─── MultiChannelParser ─────────────────────────────────────

class MultiChannelParser:
    """Оркестратор: последовательный парсинг 20+ каналов с защитой."""

    def __init__(
        self,
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None,
        session_str: Optional[str] = None,
        db_url: Optional[str] = None,
    ) -> None:
        cfg = _get_config()
        self.api_id = api_id or cfg.TG_API_ID
        self.api_hash = api_hash or cfg.TG_API_HASH
        self.session_str = session_str or cfg.TG_STRING_SESSION
        self.db_url = db_url or getattr(cfg, "database_url_async", cfg.DATABASE_URL)

        self.channel_delay_sec = getattr(cfg, "CHANNEL_DELAY_SEC", 10)
        self.api_call_delay_ms = getattr(cfg, "API_CALL_DELAY_MS", 1000)
        self.flood_cooldown_min = getattr(cfg, "FLOOD_COOLDOWN_MIN", 30)
        self.batch_size = getattr(cfg, "BATCH_COMMIT_SIZE", 50)
        self.jitter_sec = getattr(cfg, "JITTER_SEC", 10.0)
        self.schedule_interval_sec = getattr(cfg, "INTERVAL_SEC", 1200)

        # Backfill (history) settings
        self.history_batch_size = getattr(cfg, "HISTORY_BATCH_SIZE", 5000)
        self.history_max_batches_per_run = getattr(cfg, "HISTORY_MAX_BATCHES_PER_RUN", 5)
        self.history_max_seconds_per_channel = getattr(cfg, "HISTORY_MAX_SECONDS_PER_CHANNEL", 900)

        # Components (initialized in init_db)
        self._engine: Any = None
        self._db_factory: Any = None
        self._client: Optional[TelegramClient] = None
        self._rate_limiter: Optional[AdaptiveRateLimiter] = None
        self._circuit: Optional[CircuitBreaker] = None
        self._resolver: Optional[ChannelResolver] = None
        self._parser: Optional[ChannelParser] = None
        self._progress_callback: Optional[Callable[..., None]] = None

    # ─── Lifecycle ──────────────────────────────────────────

    async def init_db(self) -> None:
        """Инициализировать engine, таблицы, компоненты."""
        if self._engine is not None:
            await self._engine.dispose()

        self._engine = create_async_engine(
            self.db_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        self._db_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        # Create tables
        try:
            from models import Base
            async with self._engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        except ImportError:
            logger.warning("Models not available — skipping table creation")

        # Init components
        initial_delay = self.api_call_delay_ms / 1000.0
        self._rate_limiter = AdaptiveRateLimiter(
            min_delay=0.5,
            max_delay=30.0,
            initial_delay=initial_delay,
        )
        self._circuit = CircuitBreaker()

        logger.info("[init] База данных инициализирована (URL: %s...)", self.db_url[:30])

    async def ensure_client(self) -> TelegramClient:
        """Подключить TelegramClient."""
        if self._client is not None and self._client.is_connected():
            return self._client

        if self._client is not None:
            await self._client.disconnect()

        session = StringSession(self.session_str) if self.session_str else None
        self._client = TelegramClient(session, self.api_id, self.api_hash)
        await self._client.start()

        me = await self._client.get_me()
        logger.info(
            "[client] Подключён как %s (@%s, id=%d)",
            me.first_name, me.username, me.id,
        )
        return self._client

    async def disconnect_client(self) -> None:
        """Отключить TelegramClient."""
        if self._client and self._client.is_connected():
            await self._client.disconnect()
            logger.info("[client] Отключён")

    # ─── Core: parse all channels ───────────────────────────

    async def run_once(self) -> dict[int, ParseResult]:
        """Один прогон парсера по всем активным каналам.

        Алгоритм:
          1. Подключиться к Telegram
          2. get_dialogs() → синхронизировать access_hash
          3. Получить активные каналы из БД
          4. Последовательно парсить каждый (с jitter + rate limit)
          5. Отключиться

        Returns:
            Словарь {channel_id: ParseResult}.
        """
        start_ts = time.monotonic()

        if not self._engine:
            await self.init_db()

        client = await self.ensure_client()

        # Компоненты
        self._resolver = ChannelResolver(client, self._db_factory)
        sender_resolver = SenderResolver(client, self._db_factory, self._rate_limiter)
        self._parser = ChannelParser(
            client, self._db_factory,
            self._rate_limiter, self.batch_size,
            sender_resolver=sender_resolver,
            progress_callback=self._progress_callback,
        )

        results: dict[int, ParseResult] = {}

        try:
            # Шаг 1: Синхронизируем access_hash
            logger.info("=" * 60)
            logger.info("CITG Parser v3 — старт прогона")
            logger.info("=" * 60)

            await self._resolver.sync_dialogs()

            # Шаг 2: Получаем активные каналы
            channels = await self._resolver.get_active_channels()
            if not channels:
                logger.warning("Нет активных каналов для парсинга")
                return results

            # Сначала догружаем историю для недогруженных каналов
            channels.sort(
                key=lambda ch: (
                    ch.history_complete,
                    ch.last_parsed_at or datetime.min.replace(tzinfo=timezone.utc),
                )
            )

            logger.info("Каналов к парсингу: %d", len(channels))

            # Шаг 3: Парсим последовательно
            for i, channel in enumerate(channels):
                # Jitter перед каналом
                jitter = random.uniform(0, self.jitter_sec)
                logger.debug("Jitter: %.1f сек перед каналом '%s'", jitter, channel.title)
                await asyncio.sleep(jitter)

                # Проверяем circuit breaker
                if self._circuit.is_channel_open(channel.id):
                    logger.info(
                        "[%d/%d] '%s' [%d]: circuit OPEN — пропускаем",
                        i + 1, len(channels), channel.title, channel.id,
                    )
                    results[channel.id] = ParseResult(
                        error="Circuit breaker: channel open",
                    )
                    continue

                # Проверяем глобальный cooldown
                if self._circuit.is_global_open():
                    logger.warning(
                        "[%d/%d] Глобальная пауза активна — останавливаемся",
                        i + 1, len(channels),
                    )
                    break

                # Парсим канал
                logger.info(
                    "[%d/%d] Парсим '%s' [%d]...",
                    i + 1, len(channels), channel.title, channel.id,
                )

                try:
                    input_peer = await self._resolver.build_input_peer(channel)
                    result = await self._parser.parse(
                        channel, input_peer,
                        history_batch_size=self.history_batch_size,
                        max_history_batches=self.history_max_batches_per_run,
                        max_channel_seconds=self.history_max_seconds_per_channel,
                    )
                    results[channel.id] = result

                    if result.error:
                        self._circuit.on_channel_failure(
                            channel.id, result.error[:50],
                        )
                        if result.flood_wait_sec > self._circuit.GLOBAL_FLOOD_THRESHOLD:
                            self._circuit.on_global_flood(result.flood_wait_sec)
                    else:
                        self._circuit.on_channel_success(channel.id)
                        self._rate_limiter.on_success()
                    
                    # Callback: прогресс после каждого канала
                    if self._progress_callback:
                        self._progress_callback(
                            increment_channels_done=1,
                            increment_posts_new=result.new,
                            increment_posts_parsed=result.parsed,
                            current_channel=channel.title or str(channel.id),
                        )

                except GlobalCooldownError as e:
                    # Глобальный cooldown — останавливаем весь прогон
                    logger.warning(
                        "[%d/%d] GlobalCooldownError — останавливаем прогон: %s",
                        i + 1, len(channels), e,
                    )
                    break

                except FloodWaitError as e:
                    # FloodWait на уровне оркестратора
                    logger.warning(
                        "[%d/%d] FloodWait %d сек — активируем глобальную паузу",
                        i + 1, len(channels), e.seconds,
                    )
                    self._rate_limiter.on_flood_wait(e.seconds)
                    self._circuit.on_global_flood(e.seconds)
                    results[channel.id] = ParseResult(
                        error=f"FloodWait: {e.seconds}с", flood_wait_sec=e.seconds,
                    )
                    break

                except Exception as e:
                    logger.exception("Ошибка парсинга канала %d: %s", channel.id, e)
                    results[channel.id] = ParseResult(error=str(e))
                    self._circuit.on_channel_failure(channel.id, type(e).__name__)

                # Пауза между каналами (adaptive)
                if i < len(channels) - 1:
                    pause = max(
                        self.channel_delay_sec,
                        self._rate_limiter.current_delay,
                    )
                    logger.debug("Пауза %.1f сек между каналами", pause)
                    await asyncio.sleep(pause)

        finally:
            await self.disconnect_client()

        # Summary
        total_parsed = sum(r.parsed for r in results.values())
        total_new = sum(r.new for r in results.values())
        total_errors = sum(1 for r in results.values() if r.error)
        duration_sec = int(time.monotonic() - start_ts)

        logger.info("=" * 60)
        logger.info("РЕЗУЛЬТАТЫ: каналов=%d, обработано=%d, новых=%d, ошибок=%d, время=%d сек",
                    len(results), total_parsed, total_new, total_errors, duration_sec)
        logger.info("RateLimiter: %s", self._rate_limiter.get_stats())
        logger.info("CircuitBreaker: %s", self._circuit.get_stats())
        logger.info("=" * 60)

        # Сохраняем state
        await self._save_run_state(len(results), total_new, duration_sec, total_errors)

        return results

    async def parse_all(self, history: bool = False) -> dict[int, ParseResult]:
        """API для web.py: запускает run_once() с прогресс-колбэком.

        Args:
            history: Если True — игнорируется (v3 всегда инкрементальный).

        Returns:
            dict[int, ParseResult]: результаты по каналам.
        """
        logger.info("[parse_all] Запуск из web.py (history=%s)", history)
        return await self.run_once()

    async def run_scheduled(self) -> None:
        """Периодический запуск парсера."""
        logger.info(
            "[scheduled] Периодический режим: интервал=%d сек (%d мин)",
            self.schedule_interval_sec, self.schedule_interval_sec // 60,
        )

        while True:
            try:
                await self.run_once()
            except Exception as e:
                logger.exception("[scheduled] Ошибка в цикле: %s", e)

            logger.info(
                "[scheduled] Сон %d сек до следующего прогона...",
                self.schedule_interval_sec,
            )
            await asyncio.sleep(self.schedule_interval_sec)

    async def _save_run_state(
        self,
        channels: int,
        new_posts: int,
        duration_sec: int,
        errors: int,
    ) -> None:
        """Сохранить состояние прогона в ParseState."""
        try:
            from models import ParseState
            async with self._db_factory() as session:
                result = await session.execute(select(ParseState))
                state = result.scalar_one_or_none()
                if state is None:
                    state = ParseState()
                    session.add(state)

                state.last_run_at = utc_now()
                state.last_run_channels = channels
                state.last_run_new_posts = new_posts
                state.last_run_duration_sec = duration_sec
                state.last_error = None if errors == 0 else f"{errors} errors"

                if self._rate_limiter:
                    state.total_api_calls = self._rate_limiter.total_calls
                    state.total_flood_waits = self._rate_limiter.total_flood_waits

                if self._circuit and self._circuit.is_global_open():
                    state.global_cooldown_until = datetime.fromtimestamp(
                        self._circuit._global_opened_at +
                        self._circuit.GLOBAL_COOLDOWN_MINUTES * 60,
                        tz=timezone.utc,
                    )
                else:
                    state.global_cooldown_until = None

                await session.commit()
        except Exception as e:
            logger.warning("Не удалось сохранить ParseState: %s", e)


# ─── CLI Entry Point ────────────────────────────────────────

async def main() -> None:
    """Точка входа CLI."""
    cfg = _get_config()

    # Logging
    logging.basicConfig(
        level=getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = MultiChannelParser()

    if getattr(cfg, "SCHEDULE_MODE", False):
        await parser.run_scheduled()
    else:
        results = await parser.run_once()

        # Таблица результатов
        print("\n" + "=" * 70)
        print(f"{'Канал':<30} {'Обработано':>10} {'Новых':>8} {'Статус':>15}")
        print("-" * 70)

        for ch_id, res in sorted(results.items()):
            status = "OK" if not res.error else f"ERR: {res.error[:30]}"
            # Получаем название канала
            name = f"ID:{ch_id}"
            print(f"{name:<30} {res.parsed:>10} {res.new:>8} {status:>15}")

        total_parsed = sum(r.parsed for r in results.values())
        total_new = sum(r.new for r in results.values())
        total_errs = sum(1 for r in results.values() if r.error)

        print("-" * 70)
        print(f"{'ИТОГО':<30} {total_parsed:>10} {total_new:>8} {total_errs:>15} ошибок")
        print("=" * 70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
