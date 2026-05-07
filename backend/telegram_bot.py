"""
Telegram Bot API helpers: send messages, set webhook, download files.
All functions accept an optional `token` parameter for multi-agency support.
"""

import logging
import os

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")


def _api(token: str) -> str:
    return f"https://api.telegram.org/bot{token}"


async def send_message(chat_id: str, text: str, token: str = "") -> bool:
    tok = token or _DEFAULT_TOKEN
    if not tok:
        logger.error("No bot token for send_message")
        return False
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{_api(tok)}/sendMessage", json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
        })
        if not resp.json().get("ok"):
            logger.warning(f"sendMessage failed: {resp.text}")
        return resp.json().get("ok", False)


async def send_message_with_keyboard(chat_id: str, text: str, keyboard: dict, token: str = "") -> int | None:
    """Send message with inline keyboard. Returns message_id or None."""
    tok = token or _DEFAULT_TOKEN
    if not tok:
        return None
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{_api(tok)}/sendMessage", json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "reply_markup": keyboard,
        })
        data = resp.json()
        if not data.get("ok"):
            logger.warning(f"sendMessage+keyboard failed: {resp.text}")
            return None
        return data.get("result", {}).get("message_id")


async def edit_message_text(chat_id: str, message_id: int, text: str, token: str = "") -> bool:
    """Edit an existing message (used to update confirmation buttons)."""
    tok = token or _DEFAULT_TOKEN
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{_api(tok)}/editMessageText", json={
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "Markdown",
        })
        return resp.json().get("ok", False)


async def send_typing(chat_id: str, token: str = "") -> None:
    tok = token or _DEFAULT_TOKEN
    if not tok:
        return
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(f"{_api(tok)}/sendChatAction", json={
            "chat_id": chat_id,
            "action": "typing",
        })


async def answer_callback_query(callback_query_id: str, token: str = "") -> bool:
    tok = token or _DEFAULT_TOKEN
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.post(f"{_api(tok)}/answerCallbackQuery", json={
            "callback_query_id": callback_query_id,
        })
        return resp.json().get("ok", False)


async def set_webhook(webhook_url: str, token: str = "") -> dict:
    tok = token or _DEFAULT_TOKEN
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{_api(tok)}/setWebhook", json={
            "url": webhook_url,
            "allowed_updates": ["message", "callback_query", "channel_post"],
            "drop_pending_updates": True,
        })
        result = resp.json()
        logger.info(f"setWebhook result: {result}")
        return result


async def get_file_bytes(file_id: str, token: str = "") -> bytes | None:
    """Download a file from Telegram by file_id. Returns raw bytes or None on failure."""
    tok = token or _DEFAULT_TOKEN
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(f"{_api(tok)}/getFile", json={"file_id": file_id})
        data = resp.json()
        if not data.get("ok"):
            logger.warning(f"getFile failed: {resp.text[:200]}")
            return None
        file_path = data["result"]["file_path"]
        dl = await client.get(f"https://api.telegram.org/file/bot{tok}/{file_path}")
        if dl.status_code != 200:
            logger.warning(f"File download failed: status {dl.status_code}")
            return None
        return dl.content
