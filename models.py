"""
SQLAlchemy 2.0 async ORM models for Telegram Parser (citg_v3).

Содержит все модели БД: каналы, посты, логи парсинга, ошибки,
группы каналов, пользователи — а также **singleton-таблицу parse_state**
для отслеживания глобального состояния парсера (v3).

Новое в v3:
    - Channel.access_hash — кэш Telegram access_hash для InputPeerChannel
    - Channel.entity_resolved_at — когда access_hash был получен
    - ParseState — глобальное состояние парсера (1 строка)

Использование::

    from models import Base, Channel, Post, ParseLog, ChannelError, ChannelGroup, ParseState

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional
import hashlib
import secrets

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """Async-compatible declarative base for all models."""

    type_annotation_map: dict[type, Any] = {
        dict: JSON,
        list: JSON,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def utc_now() -> datetime:
    """Return the current UTC-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Channel(Base):
    """Telegram channel metadata and parsing state.

    Attributes:
        id: Internal surrogate primary key.
        telegram_id: Telegram's own channel ID (bigint, unique).
        numeric_id: Channel ID without the -100 prefix (used for private channel links).
        channel_type: 'public' (has @username) or 'private' (no username, numeric ID).
        username: Telegram @username (unique, NULL for private channels).
        title: Human-readable channel title.
        description: Channel bio / description.
        subscriber_count: Number of subscribers at last check.
        is_active: Whether the channel should be parsed.
        parse_error_count: Consecutive parse errors count.
        last_error_message: Most recent error message.
        last_error_at: Timestamp of the most recent error.
        total_posts_parsed: Cumulative number of posts parsed across all runs.
        metadata_json: Flexible JSON field for extra channel data.
        last_parsed_at: When the channel was last successfully parsed.
        created_at: Record creation timestamp (UTC).

    **v3 — новые поля:**
        access_hash: Telegram access_hash для построения InputPeerChannel
                     без дополнительного API call (get_entity).
        entity_resolved_at: Когда access_hash был получен через get_dialogs().
    """

    __tablename__ = "channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(
        BigInteger,
        unique=True,
        nullable=False,
        comment="Telegram internal channel ID (e.g. -1003147415698)",
    )
    numeric_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        unique=True,
        nullable=True,
        comment="Channel ID without -100 prefix (e.g. 3147415698) for private links",
    )
    channel_type: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        default="public",
        comment="'public' (has @username) or 'private' (numeric ID only)",
    )
    username: Mapped[Optional[str]] = mapped_column(
        String(255),
        unique=True,
        nullable=True,
        comment="Telegram @username; NULL for private channels",
    )
    title: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        comment="Human-readable channel title",
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Channel bio / description",
    )
    subscriber_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Subscriber count at last check",
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="Whether parsing is enabled for this channel",
    )
    parse_error_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Consecutive parse error counter",
    )
    last_error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Text of the most recent parse error",
    )
    last_error_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of the most recent parse error (UTC)",
    )
    total_posts_parsed: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        comment="Cumulative posts parsed across all runs",
    )
    metadata_json: Mapped[dict] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
        comment="Flexible JSON metadata for the channel",
    )
    last_parsed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last successful parsing timestamp (UTC)",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        comment="Record creation timestamp (UTC)",
    )

    # --- v3: access_hash cache для InputPeerChannel ---
    access_hash: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Telegram access_hash для построения InputPeerChannel без get_entity()",
    )
    entity_resolved_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Когда access_hash был получен через get_dialogs()",
    )

    # Relationships
    posts: Mapped[List["Post"]] = relationship(
        "Post",
        back_populates="channel",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    parse_logs: Mapped[List["ParseLog"]] = relationship(
        "ParseLog",
        back_populates="channel",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    channel_errors: Mapped[List["ChannelError"]] = relationship(
        "ChannelError",
        back_populates="channel",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    groups: Mapped[List["ChannelGroup"]] = relationship(
        "ChannelGroup",
        secondary="channel_group_members",
        back_populates="channels",
        lazy="selectin",
    )

    __table_args__: tuple[Index, ...] = (
        # Fast lookup by Telegram ID
        Index("ix_channels_telegram_id", "telegram_id"),
        # Fast lookup active channels
        Index("ix_channels_is_active", "is_active"),
        # Fast ordering by parse time
        Index("ix_channels_last_parsed_at", "last_parsed_at"),
        # v3: lookup by access_hash
        Index("ix_channels_access_hash", "access_hash"),
        # v3: channels resolved via get_dialogs
        Index("ix_channels_entity_resolved", "entity_resolved_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<Channel(id={self.id}, telegram_id={self.telegram_id}, "
            f"username={self.username!r}, is_active={self.is_active})>"
        )

    # --- Backfill helpers: постраничная загрузка истории ---
    @property
    def history_cursor(self) -> Optional[int]:
        """ID самого старого загруженного сообщения; следующая партия
        будет загружена сообщения старше этого ID."""
        return self.metadata_json.get("history_cursor")

    @history_cursor.setter
    def history_cursor(self, value: Optional[int]) -> None:
        self.metadata_json["history_cursor"] = value

    @property
    def history_complete(self) -> bool:
        """True, если вся история канала уже догружена."""
        return self.metadata_json.get("history_complete", False)

    @history_complete.setter
    def history_complete(self, value: bool) -> None:
        self.metadata_json["history_complete"] = value


class Sender(Base):
    """Cache of Telegram user names for resolving message senders.

    For channels (Channel), sender_name comes from msg.post_author directly.
    For chats (Chat), msg.post_author is None — we resolve the sender
    via get_entity(sender_id) and cache the result here.

    Attributes:
        telegram_user_id: Telegram's numeric user ID (primary key).
        first_name: User's first name.
        last_name: User's last name.
        username: Telegram @username.
        resolved_at: When this record was created/updated (UTC).
    """

    __tablename__ = "senders"

    telegram_user_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        comment="Telegram numeric user ID",
    )
    first_name: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="User first name",
    )
    last_name: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="User last name",
    )
    username: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="Telegram @username",
    )
    resolved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        comment="When this sender was resolved (UTC)",
    )

    @property
    def display_name(self) -> str:
        """Return a human-readable name for this sender.

        Priority: full name > first name > @username > ID fallback.
        """
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        if self.first_name:
            return self.first_name
        if self.username:
            return f"@{self.username}"
        return f"ID:{self.telegram_user_id}"

    def __repr__(self) -> str:
        return (
            f"<Sender(telegram_user_id={self.telegram_user_id}, "
            f"display_name={self.display_name!r})>"
        )


class Post(Base):
    """Telegram post / message extracted from a channel.

    Attributes:
        id: Internal surrogate primary key.
        channel_id: FK -> channels.id.
        telegram_message_id: Telegram's message ID within the channel.
        text: Post text content.
        text_hash: SHA-256 hash of the text for deduplication.
        views_count: Number of views.
        forwards_count: Number of forwards.
        replies_count: Number of replies.
        hashtags: List of hashtags extracted from the text.
        mentions: List of @mentions extracted from the text.
        urls: List of URLs extracted from the text.
        forward_from: Original source if this post is a forward.
        sender_name: Name of the sender (post_author from Telegram).
        sender_telegram_id: Numeric ID of the sender (for cache lookups).
        has_media: Whether the post contains media.
        media_type: Type of media (photo, video, document, etc.).
        published_at: When the post was published (UTC).
        edited_at: When the post was last edited (UTC).
        created_at: Record creation timestamp (UTC).
    """

    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"),
        nullable=False,
        comment="FK to channels.id",
    )
    telegram_message_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="Telegram message ID within the channel",
    )
    text: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Post text content",
    )
    text_hash: Mapped[Optional[str]] = mapped_column(
        String(64),
        nullable=True,
        comment="SHA-256 hash of the text for deduplication",
    )
    views_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of views",
    )
    forwards_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of forwards",
    )
    replies_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of replies",
    )
    hashtags: Mapped[Optional[list]] = mapped_column(
        JSON,
        nullable=True,
        comment="List of hashtags extracted from the post",
    )
    mentions: Mapped[Optional[list]] = mapped_column(
        JSON,
        nullable=True,
        comment="List of @mentions extracted from the post",
    )
    urls: Mapped[Optional[list]] = mapped_column(
        JSON,
        nullable=True,
        comment="List of URLs extracted from the post",
    )
    forward_from: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Original source if this is a forwarded post",
    )
    sender_name: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Name of the sender (user or channel that posted the message)",
    )
    sender_telegram_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Telegram numeric user ID of the sender (for sender cache)",
    )
    has_media: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="Whether the post contains media attachments",
    )
    media_type: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        comment="Type of media: photo, video, document, audio, etc.",
    )
    published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Original publication timestamp (UTC)",
    )
    edited_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last edit timestamp (UTC)",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        comment="Record creation timestamp (UTC)",
    )

    # Relationships
    channel: Mapped["Channel"] = relationship("Channel", back_populates="posts")

    __table_args__: tuple[UniqueConstraint | Index, ...] = (
        # Prevent duplicate posts within the same channel
        UniqueConstraint(
            "channel_id",
            "telegram_message_id",
            name="uq_posts_channel_message",
        ),
        # Fast filtering by channel
        Index("ix_posts_channel_id", "channel_id"),
        # Fast deduplication lookup
        Index("ix_posts_text_hash", "text_hash"),
        # Fast ordering by publish date
        Index("ix_posts_published_at", "published_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<Post(id={self.id}, channel_id={self.channel_id}, "
            f"telegram_message_id={self.telegram_message_id}, "
            f"published_at={self.published_at})>"
        )


class ParseLog(Base):
    """Log entry for a single channel parsing run.

    Attributes:
        id: Internal surrogate primary key.
        channel_id: FK -> channels.id.
        posts_parsed: Total posts processed during this run.
        posts_new: New posts actually inserted.
        duration_ms: Parsing duration in milliseconds.
        error_message: Error text if the run failed.
        started_at: Run start timestamp (UTC).
        finished_at: Run end timestamp (UTC).
    """

    __tablename__ = "parse_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"),
        nullable=True,
        comment="FK to channels.id (NULL if channel sync failed)",
    )
    posts_parsed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total posts processed in this run",
    )
    posts_new: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="New posts inserted in this run",
    )
    duration_ms: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Parsing duration in milliseconds",
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Error text if the parsing run failed",
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        comment="Run start timestamp (UTC)",
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Run end timestamp (UTC)",
    )

    # Relationships
    channel: Mapped["Channel"] = relationship("Channel", back_populates="parse_logs")

    __table_args__: tuple[Index, ...] = (
        # Fast lookup logs by channel
        Index("ix_parse_logs_channel_id", "channel_id"),
        # Fast ordering by start time
        Index("ix_parse_logs_started_at", "started_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<ParseLog(id={self.id}, channel_id={self.channel_id}, "
            f"posts_parsed={self.posts_parsed}, posts_new={self.posts_new}, "
            f"duration_ms={self.duration_ms}, started_at={self.started_at})>"
        )


# ============================================================================
# v3: ParseState — глобальное состояние парсера (singleton, 1 строка)
# ============================================================================


class ParseState(Base):
    """Глобальное состояние парсера (singleton-таблица, всегда 1 строка).

    Используется для отслеживания:
        - Когда был последний запуск парсера
        - Сколько каналов и постов обработано
        - Глобальный cooldown при FloodWait
        - Общая статистика API calls и flood wait-ов

    Attributes:
        id: Всегда 1 (singleton pattern).
        last_run_at: Время последнего запуска парсера.
        last_run_channels: Количество обработанных каналов.
        last_run_new_posts: Количество новых постов.
        last_run_duration_sec: Длительность прогона (сек).
        last_error: Текст последней ошибки.
        global_cooldown_until: До какого времени действует глобальная пауза.
        total_api_calls: Общий счётчик API вызовов.
        total_flood_waits: Общий счётчик FloodWait ошибок.
    """

    __tablename__ = "parse_state"
    __table_args__: tuple[Index, ...] = (
        Index("ix_parse_state_id", "id"),
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        default=1,
        comment="Всегда 1 — singleton pattern",
    )
    last_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Время последнего запуска парсера (UTC)",
    )
    last_run_channels: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Количество каналов в последнем прогоне",
    )
    last_run_new_posts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Количество новых постов в последнем прогоне",
    )
    last_run_duration_sec: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Длительность последнего прогона (секунды)",
    )
    last_error: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Текст последней ошибки",
    )
    global_cooldown_until: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Глобальная пауза активна до (UTC)",
    )
    total_api_calls: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        comment="Общий счётчик API вызовов",
    )
    total_flood_waits: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Общий счётчик FloodWait ошибок",
    )

    def __repr__(self) -> str:
        return (
            f"<ParseState(id={self.id}, last_run_at={self.last_run_at}, "
            f"channels={self.last_run_channels}, new_posts={self.last_run_new_posts}, "
            f"cooldown_until={self.global_cooldown_until})>"
        )


class ChannelError(Base):
    """Individual error record for a channel parsing attempt.

    Attributes:
        id: Internal surrogate primary key.
        channel_id: FK -> channels.id.
        error_message: Full error text.
        error_type: Short error category (e.g., "timeout", "flood_wait").
        created_at: Error timestamp (UTC).
    """

    __tablename__ = "channel_errors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"),
        nullable=False,
        comment="FK to channels.id",
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Full error message text",
    )
    error_type: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        comment="Error category: timeout, flood_wait, deleted, etc.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        comment="Error timestamp (UTC)",
    )

    # Relationships
    channel: Mapped["Channel"] = relationship(
        "Channel",
        back_populates="channel_errors",
    )

    __table_args__: tuple[Index, ...] = (
        # Fast lookup errors by channel
        Index("ix_channel_errors_channel_id", "channel_id"),
        # Fast ordering by error time
        Index("ix_channel_errors_created_at", "created_at"),
        # Composite index for channel error history queries
        Index(
            "ix_channel_errors_channel_created",
            "channel_id",
            "created_at",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ChannelError(id={self.id}, channel_id={self.channel_id}, "
            f"error_type={self.error_type!r}, created_at={self.created_at})>"
        )


class ChannelGroup(Base):
    """Named group for organising channels.

    Attributes:
        id: Internal surrogate primary key.
        name: Group name (human-readable).
        description: Optional group description.
        created_at: Record creation timestamp (UTC).
    """

    __tablename__ = "channel_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Human-readable group name",
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Optional group description",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        comment="Record creation timestamp (UTC)",
    )

    # Relationships
    channels: Mapped[List["Channel"]] = relationship(
        "Channel",
        secondary="channel_group_members",
        back_populates="groups",
        lazy="selectin",
    )

    __table_args__: tuple[Index, ...] = (
        # Fast lookup by name
        Index("ix_channel_groups_name", "name"),
    )

    def __repr__(self) -> str:
        return f"<ChannelGroup(id={self.id}, name={self.name!r})>"


class ChannelGroupMember(Base):
    """Association table linking channels to groups (many-to-many).

    Attributes:
        channel_id: FK -> channels.id (part of composite PK).
        group_id: FK -> channel_groups.id (part of composite PK).
        added_at: When the channel was added to the group (UTC).
    """

    __tablename__ = "channel_group_members"

    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"),
        nullable=False,
        comment="FK to channels.id",
    )
    group_id: Mapped[int] = mapped_column(
        ForeignKey("channel_groups.id", ondelete="CASCADE"),
        nullable=False,
        comment="FK to channel_groups.id",
    )
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        comment="When the channel was added to the group (UTC)",
    )

    __table_args__: tuple[PrimaryKeyConstraint, ...] = (
        PrimaryKeyConstraint("channel_id", "group_id", name="pk_channel_group_members"),
    )

    def __repr__(self) -> str:
        return (
            f"<ChannelGroupMember(channel_id={self.channel_id}, "
            f"group_id={self.group_id}, added_at={self.added_at})>"
        )


# ---------------------------------------------------------------------------
# User
# ---------------------------------------------------------------------------


class User(Base):
    """User account for dashboard authentication.

    No registration endpoint — users are created via code/console only.
    Passwords are hashed with PBKDF2-HMAC-SHA256 + random salt.
    """

    __tablename__ = "users"
    __table_args__ = (
        Index("ix_users_username", "username", unique=True),
        {"comment": "Dashboard users (created via code only, no registration)"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Internal surrogate PK",
    )
    username: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        unique=True,
        comment="Login username",
    )
    password_hash: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="PBKDF2-HMAC-SHA256 hash",
    )
    password_salt: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Random hex salt",
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="Account enabled flag",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        comment="When the user was created (UTC)",
    )

    def set_password(self, plain_password: str) -> None:
        """Hash and store a new password."""
        salt = secrets.token_hex(32)
        pwd_hash = hashlib.pbkdf2_hmac(
            "sha256", plain_password.encode("utf-8"), salt.encode("ascii"), 100_000
        ).hex()
        self.password_salt = salt
        self.password_hash = pwd_hash

    def check_password(self, plain_password: str) -> bool:
        """Verify a plain password against the stored hash."""
        pwd_hash = hashlib.pbkdf2_hmac(
            "sha256", plain_password.encode("utf-8"), self.password_salt.encode("ascii"), 100_000
        ).hex()
        return secrets.compare_digest(pwd_hash, self.password_hash)

    def __repr__(self) -> str:
        return f"<User(id={self.id}, username='{self.username}', is_active={self.is_active})>"
