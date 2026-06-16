"""
Централизованная конфигурация Telegram-Parser (citg_v3).

Модуль предоставляет единую точку доступа ко всем настройкам приложения
через pydantic-settings с валидацией переменных окружения.

Новое в v3:
    - CHANNEL_DELAY_SEC — пауза между каналами (default: 10)
    - API_CALL_DELAY_MS — задержка между API calls в мс (default: 1000)
    - FLOOD_COOLDOWN_MIN — глобальный cooldown при FloodWait в мин (default: 30)
    - BATCH_COMMIT_SIZE — размер пачки для коммита в БД (default: 50)
    - JITTER_SEC — максимальный jitter перед каналом в сек (default: 10.0)

Использование:
    from config import settings

    # Доступ к настройкам
    api_id = settings.TG_API_ID
    channels = settings.channels_list
    db_url = settings.database_url_async

    # Валидация перед запуском парсера
    settings.validate_parser()
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Конфигурация приложения Telegram-Parser.

    Все параметры читаются из переменных окружения и/или .env-файла.
    Обязательные поля должны быть заданы до запуска приложения.

    Attributes:
        TG_API_ID: Telegram API ID (положительное целое число).
        TG_API_HASH: Telegram API hash (строка).
        DATABASE_URL: PostgreSQL connection string.
        TG_STRING_SESSION: Строковая сессия Telethon.
        TG_SESSION: Путь к файловой сессии Telethon.
        CHANNELS: Список каналов через запятую.
        HISTORY: Флаг парсинга всей истории.
        LIMIT: Лимит постов (None — без ограничений).
        SCHEDULE_MODE: Периодический запуск.
        INTERVAL_SEC: Интервал периодического запуска (сек).
        MAX_CONCURRENT_CHANNELS: Максимум одновременно парсимых каналов.
        RETRY_ATTEMPTS: Количество попыток retry при ошибке.
        RETRY_DELAY_BASE: Базовая задержка retry (сек).
        LOG_LEVEL: Уровень логирования.
        PORT: Порт веб-сервера.
        APP_MODE: Режим работы ("parser" или "web").

        --- Parser v3 settings ---
        CHANNEL_DELAY_SEC: Пауза между каналами (сек).
        API_CALL_DELAY_MS: Задержка между API calls (мс).
        FLOOD_COOLDOWN_MIN: Глобальный cooldown при FloodWait (мин).
        BATCH_COMMIT_SIZE: Размер пачки для коммита в БД.
        JITTER_SEC: Максимальный jitter перед каналом (сек).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
    )

    # --- Обязательные поля ---

    TG_API_ID: int = Field(
        0,
        description="Telegram API ID (положительное целое число)",
        ge=0,
    )
    TG_API_HASH: str = Field(
        "",
        description="Telegram API hash",
    )
    DATABASE_URL: str = Field(
        ...,
        description="PostgreSQL connection string",
        min_length=1,
    )

    # --- Опциональные поля (legacy) ---

    TG_STRING_SESSION: str = Field(
        default="",
        description="Строковая сессия Telethon",
        repr=False,
    )
    TG_SESSION: str = Field(
        default="/app/sessions/parser_session",
        description="Путь к файловой сессии Telethon",
    )
    CHANNELS: str = Field(
        default="",
        description="Список каналов через запятую",
    )
    HISTORY: bool = Field(
        default=False,
        description="Парсить всю историю сообщений",
    )
    LIMIT: Optional[int] = Field(
        default=None,
        description="Лимит постов (None — без ограничений)",
        ge=1,
    )
    SCHEDULE_MODE: bool = Field(
        default=False,
        description="Периодический запуск по расписанию",
    )
    INTERVAL_SEC: int = Field(
        default=300,
        description="Интервал периодического запуска (секунды)",
        ge=1,
    )
    MAX_CONCURRENT_CHANNELS: int = Field(
        default=10,
        description="Максимум одновременно парсимых каналов",
        ge=1,
    )
    RETRY_ATTEMPTS: int = Field(
        default=3,
        description="Количество попыток retry при ошибке",
        ge=0,
    )
    RETRY_DELAY_BASE: int = Field(
        default=2,
        description="Базовая задержка retry (секунды)",
        ge=1,
    )
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Уровень логирования (DEBUG/INFO/WARNING/ERROR)",
    )
    PORT: int = Field(
        default=10000,
        description="Порт для веб-сервера",
        ge=1,
        le=65535,
    )
    APP_MODE: str = Field(
        default="parser",
        description="Режим работы: 'parser' или 'web'",
    )

    # --- Parser v3 settings ---

    CHANNEL_DELAY_SEC: int = Field(
        default=10,
        description="Пауза между каналами (секунды)",
        ge=0,
    )
    API_CALL_DELAY_MS: int = Field(
        default=1000,
        description="Задержка между API calls (миллисекунды)",
        ge=0,
    )
    FLOOD_COOLDOWN_MIN: int = Field(
        default=30,
        description="Глобальный cooldown при FloodWait (минуты)",
        ge=1,
    )
    BATCH_COMMIT_SIZE: int = Field(
        default=50,
        description="Размер пачки для коммита в БД (количество постов)",
        ge=1,
    )
    JITTER_SEC: float = Field(
        default=10.0,
        description="Максимальный jitter перед началом парсинга канала (секунды)",
        ge=0,
    )

    # --- Валидаторы ---

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def _normalize_database_url(cls, value: str) -> str:
        """Нормализует DATABASE_URL к формату postgresql+asyncpg://.

        Поддерживаемые преобразования:
            - postgres://       -> postgresql+asyncpg://
            - postgresql://     -> postgresql+asyncpg://
            - postgresql+asyncpg:// -> без изменений

        Args:
            value: Исходная строка подключения к БД.

        Returns:
            Нормализованная строка подключения.
        """
        if not isinstance(value, str):
            raise ValueError("DATABASE_URL должен быть строкой")

        # postgres:// -> postgresql+asyncpg://
        if value.startswith("postgres://"):
            value = value.replace("postgres://", "postgresql+asyncpg://", 1)
            logger.debug("DATABASE_URL: преобразовано postgres:// -> postgresql+asyncpg://")

        # postgresql:// -> postgresql+asyncpg://
        elif value.startswith("postgresql://") and not value.startswith("postgresql+"):
            value = value.replace("postgresql://", "postgresql+asyncpg://", 1)
            logger.debug("DATABASE_URL: преобразовано postgresql:// -> postgresql+asyncpg://")

        return value

    @field_validator("LOG_LEVEL")
    @classmethod
    def _validate_log_level(cls, value: str) -> str:
        """Валидирует уровень логирования.

        Допустимые значения: DEBUG, INFO, WARNING, ERROR.

        Args:
            value: Строка с уровнем логирования.

        Returns:
            Верхний регистр валидного уровня.

        Raises:
            ValueError: Если указан недопустимый уровень.
        """
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        value_upper = value.upper()
        if value_upper not in allowed:
            raise ValueError(
                f"LOG_LEVEL='{value}' недопустим. "
                f"Разрешённые значения: {', '.join(sorted(allowed))}"
            )
        return value_upper

    @field_validator("APP_MODE")
    @classmethod
    def _validate_app_mode(cls, value: str) -> str:
        """Валидирует режим работы приложения.

        Args:
            value: Строка с режимом работы.

        Returns:
            Валидный режим в нижнем регистре.

        Raises:
            ValueError: Если указан недопустимый режим.
        """
        allowed = {"parser", "web"}
        value_lower = value.lower()
        if value_lower not in allowed:
            raise ValueError(
                f"APP_MODE='{value}' недопустим. "
                f"Разрешённые значения: {', '.join(sorted(allowed))}"
            )
        return value_lower

    # --- Properties ---

    @property
    def channels_list(self) -> list[str]:
        """Возвращает список каналов, разбитый по запятым.

        Пустые строки и лишние пробелы фильтруются.

        Returns:
            Список имён каналов (без @-префикса).
        """
        if not self.CHANNELS:
            return []
        return [
            ch.strip().lstrip("@").strip()
            for ch in self.CHANNELS.split(",")
            if ch.strip()
        ]

    @property
    def database_url_async(self) -> str:
        """Возвращает DATABASE_URL гарантированно с драйвером asyncpg.

        Returns:
            Строка подключения вида postgresql+asyncpg://...
        """
        url = self.DATABASE_URL
        if url.startswith("postgresql+asyncpg://"):
            return url
        # Fallback: принудительная замена на asyncpg
        if url.startswith("postgresql+"):
            # Заменяем любой существующий драйвер на asyncpg
            url = re.sub(r"^postgresql\+[^/]+", "postgresql+asyncpg", url)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url

    @property
    def database_url_sync(self) -> str:
        """Возвращает DATABASE_URL без драйвера asyncpg (для синхронных инструментов).

        Используется для Alembic, sync SQLAlchemy и других
        синхронных инструментов.

        Returns:
            Строка подключения вида postgresql://...
        """
        url = self.DATABASE_URL
        if url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql+asyncpg://", "postgresql://", 1)
        if url.startswith("postgresql+"):
            # Убираем любой драйвер
            url = re.sub(r"^postgresql\+[^/]+://", "postgresql://", url)
            return url
        return url

    # --- v3: convenience properties ---

    @property
    def api_call_delay_sec(self) -> float:
        """API_CALL_DELAY_MS в секундах (float).

        Returns:
            Задержка в секундах (например, 1000 -> 1.0).
        """
        return self.API_CALL_DELAY_MS / 1000.0

    @property
    def flood_cooldown_sec(self) -> int:
        """FLOOD_COOLDOWN_MIN в секундах.

        Returns:
            Cooldown в секундах (например, 30 -> 1800).
        """
        return self.FLOOD_COOLDOWN_MIN * 60

    # --- Методы ---

    def validate_parser(self) -> None:
        """Проверяет, что все обязательные поля для парсера заполнены корректно.

        Проверяет:
            - TG_API_ID > 0
            - TG_API_HASH не пустой
            - DATABASE_URL не пустой и корректный
            - channels_list не пустой
            - RETRY_ATTEMPTS >= 0
            - v3: BATCH_COMMIT_SIZE >= 1
            - v3: FLOOD_COOLDOWN_MIN >= 1

        Raises:
            ValueError: Если какое-либо обязательное поле не заполнено
                       или имеет некорректное значение.
        """
        errors: list[str] = []

        if self.TG_API_ID <= 0:
            errors.append("TG_API_ID должен быть положительным числом")

        if not self.TG_API_HASH or not self.TG_API_HASH.strip():
            errors.append("TG_API_HASH не задан или пустой")

        if not self.DATABASE_URL or not self.DATABASE_URL.strip():
            errors.append("DATABASE_URL не задан или пустой")
        elif not self.DATABASE_URL.startswith("postgresql"):
            errors.append(
                f"DATABASE_URL должен начинаться с 'postgresql://' "
                f"(текущее значение: {self.DATABASE_URL[:30]}...)"
            )

        if not self.channels_list:
            errors.append(
                "CHANNELS не задан или пустой. "
                "Укажите хотя бы один канал через запятую."
            )

        if self.RETRY_ATTEMPTS < 0:
            errors.append("RETRY_ATTEMPTS не может быть отрицательным")

        # v3 validation
        if self.BATCH_COMMIT_SIZE < 1:
            errors.append("BATCH_COMMIT_SIZE должен быть >= 1")

        if self.FLOOD_COOLDOWN_MIN < 1:
            errors.append("FLOOD_COOLDOWN_MIN должен быть >= 1 минута")

        if errors:
            raise ValueError(
                "Конфигурация парсера не валидна:\n  - "
                + "\n  - ".join(errors)
            )

        logger.info("Конфигурация парсера валидна: %d каналов", len(self.channels_list))


# Singleton instance
settings = Settings()
