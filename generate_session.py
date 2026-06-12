"""
Генератор строковой сессии Telethon.

Запуск:
    python generate_session.py

Выводит строку сессии, которую нужно сохранить в .env как TG_STRING_SESSION.
"""

from __future__ import annotations

import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession

from config import settings


async def main() -> None:
    """Создаёт клиент, авторизуется и выводит строку сессии."""
    client = TelegramClient(
        StringSession(),
        settings.TG_API_ID,
        settings.TG_API_HASH,
    )
    await client.start()
    session_string = client.session.save()
    print(f"\n=== STRING SESSION ===\n{session_string}\n======================")
    print("Сохраните её в .env как TG_STRING_SESSION=<строка>")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
