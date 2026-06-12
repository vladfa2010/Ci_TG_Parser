"""
Централизованная конфигурация Telegram-Parser.

Модуль предоставляет единую точку доступа ко всем настройкам приложения
через pydantic-settings с валидацией переменных окружения.

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
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Обязательные поля ---

    TG_API_ID: int = Field(
        ...,
        description="Telegram API ID (положительное целое число)",
        gt=0,
    )
    TG_API_HASH: str = Field(
        ...,
        description="Telegram API hash",
        min_length=1,
    )
    DATABASE_URL: str = Field(
        ...,
        description="PostgreSQL connection string",
        min_length=1,
    )

    # --- Опциональные поля ---

    TG_STRING_SESSION: str = Field(
        default="",
        description="Строковая сессия Telethon",
    )
    TG_SESSION: str = Field(
        default="/app/sessions/parser_session",
        description="Путь к файловой сессии Telethon",
    )
    CHANNELS: str = Field(
        default="markettwits",
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
        default=5,
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
        ge=0,
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
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR"}
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

    # --- Методы ---

    def validate_parser(self) -> None:
        """Проверяет, что все обязательные поля для парсера заполнены корректно.

        Проверяет:
            - TG_API_ID > 0
            - TG_API_HASH не пустой
            - DATABASE_URL не пустой и корректный
            - channels_list не пустой
            - RETRY_ATTEMPTS >= 0

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

        if errors:
            raise ValueError(
                "Конфигурация парсера содержит ошибки:\n  - "
                + "\n  - ".join(errors)
            )

    def logging_config(self) -> dict[str, Any]:
        """Возвращает словарь конфигурации логгера в формате dictConfig.

        Конфигурация включает:
            - Форматтер с timestamp, уровнем, именем модуля и сообщением
            - StreamHandler для вывода в stdout
            - Уровень логирования из LOG_LEVEL

        Returns:
            Словарь конфигурации logging.dictConfig.
        """
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": (
                        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
                    ),
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
                "detailed": {
                    "format": (
                        "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d "
                        "| %(message)s"
                    ),
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": self.LOG_LEVEL,
                    "formatter": "standard",
                    "stream": "ext://sys.stdout",
                },
            },
            "loggers": {
                "": {
                    "handlers": ["console"],
                    "level": self.LOG_LEVEL,
                    "propagate": False,
                },
            },
        }


# --- Глобальный singleton ---
settings = Settings()


__all__ = [
    "Settings",
    "settings",
]
