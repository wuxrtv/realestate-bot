"""
WhatsApp bot via Green API.
Mirrors Telegram bot logic: admin messages → AdminAgent, group mentions → Tony.
"""

import asyncio
import json
import logging
import os
import re

import anthropic
import httpx
from sqlalchemy.orm import Session

from database import SessionLocal
from excel_parser import format_unit_card
from models import Agency, ToniProject, WhatsAppGroup
from toni_bot import _SYSTEM_BASE, _load_group_history, _save_group_history

logger = logging.getLogger(__name__)

_BOT_NAMES = re.compile(r"\bтони\b|\btoni\b|\btony\b", re.IGNORECASE)
_WA_BASE = "https://api.green-api.com"


# ─── Low-level Green API helpers ─────────────────────────────────────────────

def _wa_url(instance_id: str, token: str, method: str) -> str:
    return f"{_WA_BASE}/waInstance{instance_id}/{method}/{token}"


async def _send_wa(instance_id: str, token: str, chat_id: str, text: str) -> bool:
    if not instance_id or not token:
        logger.warning("WhatsApp credentials not configured")
        return False
    url = _wa_url(instance_id, token, "sendMessage")
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.post(url, json={"chatId": chat_id, "message": text})
            ok = resp.status_code == 200
            if not ok:
                logger.warning(f"WA sendMessage failed: {resp.text[:200]}")
            return ok
        except Exception:
            logger.exception("WA send error")
            return False


async def set_wa_webhook(instance_id: str, token: str, webhook_url: str):
    """Configure Green API webhook URL for this instance."""
    if not instance_id or not token:
        return
    url = _wa_url(instance_id, token, "setSettings")
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.post(url, json={
                "webhookUrl": webhook_url,
                "delaySendMessagesMilliseconds": 500,
                "markIncomingMessagesReaded": "yes",
            })
            if resp.status_code == 200:
                logger.info(f"WA webhook set: {webhook_url}")
            else:
                logger.warning(f"WA setSettings failed: {resp.text[:200]}")
        except Exception:
            logger.exception("WA setSettings error")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _normalize_phone(wa_id: str) -> str:
    """'79001234567@c.us' → '79001234567'"""
    return wa_id.split("@")[0]


def _is_tony_mentioned(text: str) -> bool:
    return bool(_BOT_NAMES.search(text))


def _is_admin(sender_phone: str, agency: Agency) -> bool:
    for num in (agency.wa_admin_numbers or []):
        clean = num.lstrip("+").strip()
        if clean in sender_phone or sender_phone in clean:
            return True
    return False


# ─── Main update handler ──────────────────────────────────────────────────────

async def handle_update(data: dict, agency: Agency):
    if data.get("typeWebhook") != "incomingMessageReceived":
        return

    message_data = data.get("messageData", {})
    if message_data.get("typeMessage") != "textMessage":
        return

    sender_data = data.get("senderData", {})
    chat_id: str = sender_data.get("chatId", "")
    sender_wid: str = sender_data.get("sender", "")
    sender_name: str = sender_data.get("senderName", "Agent")
    text: str = message_data.get("textMessageData", {}).get("textMessage", "").strip()

    if not text or not chat_id:
        return

    # Ignore own messages
    if sender_wid == data.get("instanceData", {}).get("wid", ""):
        return

    is_group = chat_id.endswith("@g.us")
    sender_phone = _normalize_phone(sender_wid)

    db = SessionLocal()
    try:
        if not is_group and _is_admin(sender_phone, agency):
            await _handle_admin_message(chat_id, sender_phone, text, db, agency)
        elif is_group and _is_tony_mentioned(text):
            group_title = sender_data.get("chatName", chat_id)
            await _handle_group_message(chat_id, group_title, sender_name, text, db, agency)
    except Exception:
        logger.exception("WA handle_update error")
    finally:
        db.close()


# ─── Admin private message ────────────────────────────────────────────────────

async def _handle_admin_message(chat_id: str, sender_phone: str, text: str,
                                db: Session, agency: Agency):
    from admin_agent import AdminAgent
    agent = AdminAgent()
    try:
        reply = await agent.process(agency, f"wa_{sender_phone}", text, db)
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, reply)
    except Exception:
        logger.exception("WA admin agent error")
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                       "Something went wrong, please try again.")


# ─── Group message ────────────────────────────────────────────────────────────

async def _handle_group_message(chat_id: str, group_title: str, sender_name: str,
                                text: str, db: Session, agency: Agency):
    # Auto-register group
    existing = db.query(WhatsAppGroup).filter(
        WhatsAppGroup.chat_id == chat_id,
        WhatsAppGroup.agency_id == agency.id,
    ).first()
    if not existing:
        db.add(WhatsAppGroup(chat_id=chat_id, title=group_title, active=True, agency_id=agency.id))
        db.commit()
    elif not existing.active:
        return

    projects = db.query(ToniProject).filter(
        ToniProject.is_active == True, ToniProject.agency_id == agency.id
    ).all()
    if projects:
        proj_lines = "\n".join(f"  • {p.project_name} — {p.unit_count} units" for p in projects)
        system = _SYSTEM_BASE + f"\n\nAvailable projects:\n{proj_lines}"
    else:
        system = _SYSTEM_BASE

    # WA groups use "wa_" prefix to separate from Telegram history
    conv, history = _load_group_history(db, agency.id, f"wa_{chat_id}")
    history.append({"role": "user", "content": f"[{sender_name}]: {text}"})

    try:
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system=system,
            messages=history,
        )
        raw = resp.content[0].text.strip()
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not match:
            _save_group_history(db, conv, history)
            return
        parsed = json.loads(match.group())
    except Exception:
        logger.exception(f"WA Claude error for: {text[:80]}")
        return

    intent = parsed.get("intent", "off_topic")
    unit_numbers: list = parsed.get("unit_numbers") or []
    project_name: str = parsed.get("project_name") or ""
    keywords: list = parsed.get("keywords") or []

    if intent == "unit_query" and unit_numbers:
        await _respond_unit(chat_id, unit_numbers, projects, agency)
    elif intent == "property_search":
        if project_name and project_name not in keywords:
            keywords = [project_name] + keywords
        await _respond_search(chat_id, keywords, projects, agency)
    elif intent == "brochure_request":
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                       f"Please contact {agency.umar_contact} for brochures and presentations.")
    elif intent == "direct_question":
        reply = (parsed.get("reply") or "").strip()
        if reply:
            await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, reply)

    history.append({"role": "assistant", "content": raw})
    _save_group_history(db, conv, history)


# ─── Unit lookup ──────────────────────────────────────────────────────────────

async def _respond_unit(chat_id: str, unit_numbers: list, projects: list, agency: Agency):
    not_found = []
    sent = 0
    for unit in unit_numbers:
        found = False
        for proj in projects:
            idx: dict = proj.unit_index or {}
            if unit in idx:
                card = format_unit_card(unit, idx[unit], proj.project_name)
                await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, card)
                sent += 1
                found = True
                break
        if not found:
            not_found.append(unit)
        if sent >= 3:
            break
    if not_found:
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                       f"Unit {', '.join(not_found)} not found. Contact {agency.umar_contact}.")


# ─── Property search ──────────────────────────────────────────────────────────

async def _respond_search(chat_id: str, keywords: list, projects: list, agency: Agency):
    matched = []
    for proj in projects:
        idx: dict = proj.unit_index or {}
        proj_hit = any(kw.lower() in proj.project_name.lower() for kw in keywords)
        other_kws = [kw for kw in keywords if kw.lower() not in proj.project_name.lower()]
        for unit_num, data in idx.items():
            if proj_hit:
                if other_kws:
                    searchable = " ".join(str(v) for v in data.values()).lower()
                    if not any(kw.lower() in searchable for kw in other_kws):
                        continue
                matched.append((unit_num, data, proj.project_name))
            else:
                searchable = " ".join(str(v) for v in data.values()).lower()
                if any(kw.lower() in searchable for kw in keywords):
                    matched.append((unit_num, data, proj.project_name))
            if len(matched) >= 3:
                break
        if len(matched) >= 3:
            break

    if matched:
        for unit_num, data, proj_name in matched[:3]:
            await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                           format_unit_card(unit_num, data, proj_name))
    else:
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                       f"No matches found. Specify project, floor or room count — or contact {agency.umar_contact}.")


# ─── Broadcast to all WA groups ──────────────────────────────────────────────

async def announce_to_wa_groups(db: Session, message: str, agency: Agency) -> int:
    groups = db.query(WhatsAppGroup).filter(
        WhatsAppGroup.active == True,
        WhatsAppGroup.agency_id == agency.id,
    ).all()
    for i, g in enumerate(groups):
        if i > 0:
            await asyncio.sleep(30)
        await _send_wa(agency.wa_instance_id, agency.wa_token, g.chat_id, message)
    return len(groups)
