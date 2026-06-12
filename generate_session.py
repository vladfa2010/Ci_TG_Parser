#!/usr/bin/env python3
"""
Generate a string session for Telethon.
Run this ONCE locally, then save the output to TG_STRING_SESSION env var.
"""
import asyncio
import os

from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.getenv("TG_API_ID", input("Enter API_ID: ")))
API_HASH = os.getenv("TG_API_HASH", input("Enter API_HASH: "))


async def main():
    async with TelegramClient(StringSession(), API_ID, API_HASH) as client:
        # Will prompt for phone + code
        session_string = client.session.save()
        print("\n" + "=" * 60)
        print("YOUR STRING SESSION (save it securely):")
        print("=" * 60)
        print(session_string)
        print("=" * 60)
        print("\nSet it as TG_STRING_SESSION environment variable on Render.")


if __name__ == "__main__":
    asyncio.run(main())
