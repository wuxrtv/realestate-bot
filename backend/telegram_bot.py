"""
Telegram Bot API helpers: send messages, set webhook, download files.
"""

import logging
import os

import httpx

logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
API = f"https://api.telegram.org/bot{TOKEN}"


async def send_message(chat_id: str, text: str) -> bool:
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return False
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{API}/sendMessage", json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
        })
        if not resp.json().get("ok"):
            logger.warning(f"sendMessage failed: {resp.text}")
        return resp.json().get("ok", False)


async def send_typing(chat_id: str):
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(f"{API}/sendChatAction", json={
            "chat_id": chat_id,
            "action": "typing",
        })


async def answer_callback_query(callback_query_id: str) -> bool:
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.post(f"{API}/answerCallbackQuery", json={
            "callback_query_id": callback_query_id,
        })
        return resp.json().get("ok", False)


async def set_webhook(webhook_url: str) -> dict:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{API}/setWebhook", json={
            "url": webhook_url,
            "allowed_updates": ["message", "callback_query", "channel_post"],
            "drop_pending_updates": True,
        })
        result = resp.json()
        logger.info(f"setWebhook result: {result}")
        return result


async def get_file_bytes(file_id: str) -> bytes | None:
    """Download a file from Telegram by file_id. Returns raw bytes or None on failure."""
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(f"{API}/getFile", json={"file_id": file_id})
        data = resp.json()
        if not data.get("ok"):
            logger.warning(f"getFile failed: {resp.text[:200]}")
            return None
        file_path = data["result"]["file_path"]
        dl = await client.get(f"https://api.telegram.org/file/bot{TOKEN}/{file_path}")
        if dl.status_code != 200:
            logger.warning(f"File download failed: status {dl.status_code}")
            return None
        return dl.content
