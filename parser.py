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
        """Обновить access_hash для СУЩЕСТВУЮЩИХ каналов через get_dialogs().

        ВАЖНО: НЕ создаёт новые каналы — только обновляет access_hash
        для тех что уже есть в БД (is_active). Это предотвращает
        парсинг всех 2446+ каналов аккаунта.

        Returns:
            Список каналов у которых обновлён access_hash.
        """
        logger.info("[resolver] Получаем диалоги через get_dialogs()...")
        dialogs = await self.client.get_dialogs(limit=None)
        logger.info("[resolver] Получено %d диалогов", len(dialogs))

        # Строим маппинг: telegram_id → (access_hash, title, username)
        tg_channels: dict[int, dict[str, Any]] = {}
        for dialog in dialogs:
            entity = dialog.entity
            if not isinstance(entity, TlChannel):
                continue
            if not entity.broadcast:
                continue
            tg_channels[entity.id] = {
                "access_hash": entity.access_hash,
                "title": entity.title or "",
                "username": entity.username,
            }

        # Обновляем access_hash ТОЛЬКО для существующих каналов в БД
        updated = 0
        skipped = 0
        try:
            from models import Channel as DbChannel
        except ImportError:
            return []

        async with self.db_factory() as session:
            # Получаем ВСЕ активные каналы из БД
            result = await session.execute(
                select(DbChannel).where(DbChannel.is_active == True)
            )
            db_channels = result.scalars().all()

            for db_ch in db_channels:
                tg_id = db_ch.telegram_id
                if tg_id not in tg_channels:
                    skipped += 1
                    continue  # Канал из списка не найден в Telegram — skip

                info = tg_channels[tg_id]
                db_ch.access_hash = info["access_hash"]
                db_ch.entity_resolved_at = utc_now()
                db_ch.title = info["title"]
                if info["username"]:
                    db_ch.username = info["username"]
                    db_ch.channel_type = "public"
                updated += 1

            await session.commit()

        logger.info(
            "[resolver] Обновлено access_hash: %d каналов (пропущено: %d)",
            updated, skipped,
        )
        return []  # возвращаем пустой список — результат не нужен

    async def get_active_channels(self) -> list[Any]:
        """Получить список активных каналов из БД.

        Если settings.channels_list задан — фильтруем по нему.
        Иначе — все is_active каналы (обратная совместимость).
        """
        try:
            from models import Channel as DbChannel
            from config import settings as cfg
        except ImportError:
            return []

        async with self.db_factory() as session:
            query = (
                select(DbChannel)
                .where(DbChannel.is_active == True)
                .order_by(DbChannel.last_parsed_at.asc().nullsfirst())
            )

            # Если задан список каналов — фильтруем по username
            channel_list = getattr(cfg, "channels_list", [])
            if channel_list:
                query = query.where(DbChannel.username.in_(channel_list))
                logger.info(
                    "[resolver] Фильтр по списку: %d каналов",
                    len(channel_list),
                )

            result = await session.execute(query)
            channels = list(result.scalars().all())
            logger.info(
                "[resolver] Активных каналов к парсингу: %d",
                len(channels),
            )
            return channels

    async def build_input_peer(self, channel: Any) -> InputPeerChannel:
        """Построить InputPeerChannel из кэша БД — БЕЗ API call.

        Args:
            channel: Экземпляр модели Channel из БД.

        Returns:
            InputPeerChannel готовый для iter_messages().

        Raises:
            ValueError: Если access_hash отсутствует.
        """
        if not channel.access_hash:
            raise ValueError(
                f"Канал {channel.id} ({channel.title}) не имеет access_hash. "
                f"Запустите sync_dialogs() сначала."
            )

        channel_id = normalize_channel_id(channel.telegram_id)
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
    ) -> None:
        self.client = client
        self.db_factory = db_session_factory
        self.rate_limiter = rate_limiter
        self.batch_size = batch_size

    async def parse(
        self,
        channel: Any,
        input_peer: InputPeerChannel,
    ) -> ParseResult:
        """Парсит один канал инкрементально.

        Архитектура (исправление C1, C2, C4):
          1. Короткая сессия БД: получаем last_msg_id + загружаем text_hash'и в память
          2. Читаем сообщения из Telegram в список (сессия БД ЗАКРЫТА)
          3. Короткая сессия БД: dedup + insert batch

        Args:
            channel: Модель Channel из БД.
            input_peer: InputPeerChannel из кэша (без get_entity!).

        Returns:
            ParseResult с количеством обработанных и новых постов.
        """
        start_ts = time.monotonic()
        result = ParseResult()

        try:
            from models import Post as DbPost, ParseLog
        except ImportError as e:
            result.error = f"Models import error: {e}"
            return result

        # ─── Шаг 1: Короткая сессия — last_msg_id + text_hash'и в память ───
        last_msg_id: Optional[int] = None
        existing_hashes: set[str] = set()
        existing_msg_ids: set[int] = set()

        try:
            async with self.db_factory() as session:
                last_msg_id = await self._get_last_message_id(session, channel.id)

                # Предзагружаем ВСЕ text_hash'и канала в память (фикс C2: N+1)
                hash_result = await session.execute(
                    select(DbPost.text_hash)
                    .where(
                        (DbPost.channel_id == channel.id)
                        & (DbPost.text_hash.isnot(None))
                    )
                )
                existing_hashes = {row[0] for row in hash_result.all() if row[0]}

                # Предзагружаем ВСЕ telegram_message_id канала
                id_result = await session.execute(
                    select(DbPost.telegram_message_id)
                    .where(DbPost.channel_id == channel.id)
                )
                existing_msg_ids = {row[0] for row in id_result.all()}

                logger.info(
                    "[parse] Канал '%s' [%d]: min_id=%s, "
                    "cached_hashes=%d, cached_msg_ids=%d",
                    channel.title, channel.id,
                    last_msg_id if last_msg_id else "(вся история)",
                    len(existing_hashes), len(existing_msg_ids),
                )
        except Exception as e:
            logger.warning("[parse] Ошибка загрузки кэша: %s", e)
            existing_hashes = set()
            existing_msg_ids = set()

        # ─── Шаг 2: Читаем из Telegram (сессия БД ЗАКРЫТА) ───
        raw_posts: list[dict[str, Any]] = []
        parsed = 0

        try:
            kwargs = {"limit": None}
            if last_msg_id:
                kwargs["min_id"] = last_msg_id

            await self.rate_limiter.before_call()

            async for msg in self.client.iter_messages(input_peer, **kwargs):
                parsed += 1

                text = msg.text or ""
                text_hash = hash_text(text)

                # Дедупликация в памяти (O(1), без БД)
                if text_hash and text_hash in existing_hashes:
                    continue
                if msg.id in existing_msg_ids:
                    continue

                # Собираем данные (не создаём SQLAlchemy объект — просто dict)
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
                    "sender_name": msg.post_author,
                    "has_media": msg.media is not None,
                    "media_type": get_media_type(msg),
                    "published_at": msg.date,
                    "edited_at": msg.edit_date,
                })

                # Rate limit каждые 100 сообщений (фикс C4)
                if parsed % 100 == 0:
                    await self.rate_limiter.before_call()
                    self.rate_limiter.on_success()

                # Safety limit: не более 5000 сообщений за раз
                if len(raw_posts) >= 5000:
                    logger.warning(
                        "[parse] Канал '%s': достигнут лимит 5000 сообщений",
                        channel.title,
                    )
                    break

        except FloodWaitError as e:
            result.error = f"FloodWait: {e.seconds} сек"
            result.flood_wait_sec = e.seconds
            self.rate_limiter.on_flood_wait(e.seconds)
            logger.warning(
                "[parse] Канал '%s' [%d]: FloodWait %d сек",
                channel.title, channel.id, e.seconds,
            )
            return result

        except (ChannelInvalidError, ChannelPrivateError) as e:
            result.error = f"Channel inaccessible: {type(e).__name__}"
            logger.error(
                "[parse] Канал '%s' [%d]: Недоступен — %s",
                channel.title, channel.id, e,
            )
            return result

        except Exception as e:
            result.error = f"{type(e).__name__}: {e}"
            logger.exception(
                "[parse] Канал '%s' [%d]: Ошибка чтения из Telegram",
                channel.title, channel.id,
            )
            return result

        # ─── Шаг 3: Короткая сессия БД — insert batch ───
        try:
            new_posts = 0
            async with self.db_factory() as session:
                for i in range(0, len(raw_posts), self.batch_size):
                    batch = raw_posts[i:i + self.batch_size]
                    db_posts = [
                        DbPost(channel_id=channel.id, **item)
                        for item in batch
                    ]
                    session.add_all(db_posts)
                    await session.commit()
                    new_posts += len(batch)
                    logger.debug(
                        "[parse] Канал '%s': коммит %d/%d",
                        channel.title, new_posts, len(raw_posts),
                    )

                # Обновляем канал
                channel.total_posts_parsed = (channel.total_posts_parsed or 0) + new_posts
                channel.last_parsed_at = utc_now()
                channel.parse_error_count = 0
                channel.last_error_message = None
                channel.last_error_at = None
                await session.commit()

                # Логируем прогон
                duration_ms = int((time.monotonic() - start_ts) * 1000)
                session.add(ParseLog(
                    channel_id=channel.id,
                    posts_parsed=parsed,
                    posts_new=new_posts,
                    duration_ms=duration_ms,
                    started_at=datetime.fromtimestamp(start_ts, tz=timezone.utc),
                    finished_at=utc_now(),
                ))
                await session.commit()

                result.parsed = parsed
                result.new = new_posts
                result.duration_ms = duration_ms

                logger.info(
                    "[parse] Канал '%s' [%d]: обработано=%d, новых=%d, за %d мс",
                    channel.title, channel.id, parsed, new_posts, duration_ms,
                )

        except Exception as e:
            result.error = f"Insert error: {type(e).__name__}: {e}"
            logger.exception(
                "[parse] Канал '%s' [%d]: Ошибка записи в БД",
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

        # Components (initialized in init_db)
        self._engine: Any = None
        self._db_factory: Any = None
        self._client: Optional[TelegramClient] = None
        self._rate_limiter: Optional[AdaptiveRateLimiter] = None
        self._circuit: Optional[CircuitBreaker] = None
        self._resolver: Optional[ChannelResolver] = None
        self._parser: Optional[ChannelParser] = None

        # Progress callback for web UI live tracking
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

        # Create tables + auto-migrate (add missing columns)
        try:
            from models import Base
            async with self._engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

                # ─── Auto-migration v3: add access_hash + entity_resolved_at ───
                # These columns are needed for InputPeerChannel caching
                for col_name, col_type in [
                    ("access_hash", "BIGINT"),
                    ("entity_resolved_at", "TIMESTAMPTZ"),
                ]:
                    result = await conn.execute(text(f"""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name = 'channels' AND column_name = '{col_name}'
                    """))
                    if not result.fetchone():
                        await conn.execute(text(
                            f"ALTER TABLE channels ADD COLUMN {col_name} {col_type}"
                        ))
                        logger.info("[migrate] channels.%s → added (%s)", col_name, col_type)
                    else:
                        logger.debug("[migrate] channels.%s already exists", col_name)

                # Create parse_state table if missing (for parser state tracking)
                result = await conn.execute(text("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_name = 'parse_state'
                """))
                if not result.fetchone():
                    await conn.execute(text("""
                        CREATE TABLE parse_state (
                            id INTEGER PRIMARY KEY DEFAULT 1,
                            last_run_at TIMESTAMPTZ,
                            last_run_channels INTEGER DEFAULT 0,
                            last_run_new_posts INTEGER DEFAULT 0,
                            last_run_duration_sec INTEGER DEFAULT 0,
                            last_error TEXT,
                            global_cooldown_until TIMESTAMPTZ,
                            total_api_calls BIGINT DEFAULT 0,
                            total_flood_waits INTEGER DEFAULT 0
                        )
                    """))
                    logger.info("[migrate] parse_state table created")

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
        self._parser = ChannelParser(
            client, self._db_factory,
            self._rate_limiter, self.batch_size,
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

            logger.info("Каналов к парсингу: %d", len(channels))

            # Callback: сообщаем вебу общее количество каналов
            if self._progress_callback:
                self._progress_callback(
                    channels_total=len(channels),
                    current_operation=f"Parsing {len(channels)} channels...",
                )

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
                    result = await self._parser.parse(channel, input_peer)
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

                # Callback: прогресс после каждого канала
                if self._progress_callback:
                    self._progress_callback(
                        increment_channels_done=1,
                        increment_posts_new=result.new,
                        increment_posts_parsed=result.parsed,
                        current_channel=channel.title or str(channel.id),
                    )

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
