"""
Direct Telegram Bot integration — no n8n needed.
Sends messages, sets webhook, handles reminder/follow-up notifications.
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


async def send_photo(chat_id: str, photo_url: str, caption: str = "") -> bool:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{API}/sendPhoto", json={
            "chat_id": chat_id,
            "photo": photo_url,
            "caption": caption,
            "parse_mode": "Markdown",
        })
        if not resp.json().get("ok"):
            # Telegram rejected the photo URL — send as text fallback
            await send_message(chat_id, caption)
        return resp.json().get("ok", False)


async def send_location(chat_id: str, latitude: float, longitude: float) -> bool:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{API}/sendLocation", json={
            "chat_id": chat_id,
            "latitude": latitude,
            "longitude": longitude,
        })
        return resp.json().get("ok", False)


async def send_typing(chat_id: str):
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(f"{API}/sendChatAction", json={
            "chat_id": chat_id,
            "action": "typing",
        })


async def set_webhook(webhook_url: str) -> dict:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{API}/setWebhook", json={
            "url": webhook_url,
            "allowed_updates": ["message", "callback_query"],
            "drop_pending_updates": True,
        })
        result = resp.json()
        logger.info(f"setWebhook result: {result}")
        return result


async def notify_agent(summary: str, client_user_id: str, client_name: str):
    """Send a notification to the agent's Telegram."""
    agent_id = os.getenv("AGENT_TELEGRAM_ID")
    if not agent_id:
        return
    text = (
        f"🔔 *Новый лид / нужен звонок*\n\n"
        f"👤 Клиент: {client_name}\n"
        f"🆔 Telegram ID: `{client_user_id}`\n\n"
        f"{summary}\n\n"
        f"📞 Свяжитесь с клиентом как можно скорее!"
    )
    await send_message(agent_id, text)


async def dispatch_response(user_id: str, result: dict, client_name: str = ""):
    """
    Send all messages from the backend response to the user.
    result format: {messages: [{type, content/photo_url/caption/latitude/longitude}], notify_agent, agent_summary}
    """
    for msg in result.get("messages", []):
        if msg["type"] == "text":
            await send_message(user_id, msg["content"])
        elif msg["type"] == "photo":
            await send_photo(user_id, msg["photo_url"], msg.get("caption", ""))
        elif msg["type"] == "location":
            await send_location(user_id, msg["latitude"], msg["longitude"])

    if result.get("notify_agent"):
        await notify_agent(
            summary=result.get("agent_summary", ""),
            client_user_id=user_id,
            client_name=client_name,
        )
