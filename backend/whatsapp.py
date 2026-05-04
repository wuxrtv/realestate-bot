"""
Green API — WhatsApp integration.
Sends messages, receives incoming notifications via webhook or polling.
"""

import logging
import os

import httpx

logger = logging.getLogger(__name__)

INSTANCE_ID = os.getenv("GREEN_API_INSTANCE", "7107607792")
API_TOKEN = os.getenv("GREEN_API_TOKEN", "")
API_URL = os.getenv("GREEN_API_URL", "https://7107.api.greenapi.com")

_base = f"{API_URL}/waInstance{INSTANCE_ID}"


async def send_message(chat_id: str, text: str) -> bool:
    """Send a text message to a WhatsApp chat or group."""
    if not API_TOKEN:
        logger.error("GREEN_API_TOKEN not set")
        return False
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{_base}/sendMessage/{API_TOKEN}",
            json={"chatId": chat_id, "message": text},
        )
        ok = bool(resp.json().get("idMessage"))
        if not ok:
            logger.warning(f"WA sendMessage failed: {resp.text}")
        return ok


async def receive_notification() -> dict | None:
    """Poll one notification from the queue (used as fallback if webhook not set)."""
    if not API_TOKEN:
        return None
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{_base}/receiveNotification/{API_TOKEN}")
        data = resp.json()
        return data if data else None


async def delete_notification(receipt_id: int) -> bool:
    """Delete a processed notification so it doesn't get delivered again."""
    if not API_TOKEN:
        return False
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.delete(
            f"{_base}/deleteNotification/{API_TOKEN}/{receipt_id}"
        )
        return resp.json().get("result", False)


async def reboot_instance() -> dict:
    """Reboot the Green API instance so it reconnects to WhatsApp and receives group messages."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{_base}/reboot/{API_TOKEN}")
        return resp.json()


async def get_state() -> dict:
    """Return current state of the Green API instance (authorized / notAuthorized / etc.)."""
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{_base}/getStateInstance/{API_TOKEN}")
        return resp.json()


async def set_webhook(webhook_url: str) -> bool:
    """Tell Green API to POST incoming messages to our webhook URL."""
    if not API_TOKEN:
        logger.warning("GREEN_API_TOKEN not set — skipping WA webhook setup")
        return False
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{_base}/setSettings/{API_TOKEN}",
            json={
                "webhookUrl": webhook_url,
                "webhookUrlToken": "",
                "delaySendMessagesMilliseconds": 1000,
                "markIncomingMessagesReaded": "yes",
                "incomingWebhook": "yes",
                "outgoingMessageWebhook": "no",
                "outgoingAPIMessageWebhook": "no",
                "pollMessageWebhook": "yes",
                "incomingCallWebhook": "no",
            },
        )
        logger.info(f"WA setWebhook result: {resp.text}")
        return resp.status_code == 200
