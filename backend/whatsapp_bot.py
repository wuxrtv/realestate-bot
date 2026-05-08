"""
WhatsApp bot via Green API.
Mirrors Telegram bot logic: admin messages → AdminAgent, group mentions → Tony.
"""

import asyncio
import json
import logging
import os
import random
import re

import anthropic
import httpx
from sqlalchemy.orm import Session

from database import SessionLocal
from excel_parser import format_unit_card
from models import Agency, ToniProject, WhatsAppGroup
from toni_bot import (
    _FOLLOWUP_MSGS,
    _MIDDAY_MSGS,
    _MORNING_GREETINGS,
    _SYSTEM_BASE,
    _day_state,
    _load_group_history,
    _save_group_history,
    mark_admin_active,
)

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
    webhook_type = data.get("typeWebhook")
    logger.info(f"WA webhook received: type={webhook_type}")

    if webhook_type != "incomingMessageReceived":
        return

    message_data = data.get("messageData", {})
    msg_type = message_data.get("typeMessage")
    logger.info(f"WA message type: {msg_type}")

    if msg_type not in ("textMessage", "documentMessage"):
        return

    sender_data = data.get("senderData", {})
    chat_id: str = sender_data.get("chatId", "")
    sender_wid: str = sender_data.get("sender", "")
    sender_name: str = sender_data.get("senderName", "Agent")

    if not chat_id:
        return

    if sender_wid == data.get("instanceData", {}).get("wid", ""):
        return

    is_group = chat_id.endswith("@g.us")
    sender_phone = _normalize_phone(sender_wid)
    admin_check = _is_admin(sender_phone, agency)
    logger.info(f"WA is_group={is_group} sender_phone={sender_phone} is_admin={admin_check}")

    db = SessionLocal()
    try:
        if msg_type == "documentMessage" and not is_group and admin_check:
            file_data = message_data.get("fileMessageData", {})
            download_url = file_data.get("downloadUrl", "")
            file_name = file_data.get("fileName", "file")
            caption = file_data.get("caption", "").strip()
            if download_url:
                mark_admin_active(agency.id)
                await _handle_admin_document(chat_id, sender_phone, download_url, file_name, caption, db, agency)
        elif msg_type == "textMessage":
            text: str = message_data.get("textMessageData", {}).get("textMessage", "").strip()
            logger.info(f"WA from={sender_wid} chat={chat_id} text={text[:50]!r}")
            if not text:
                return
            if not is_group and admin_check:
                mark_admin_active(agency.id)
                await _handle_admin_message(chat_id, sender_phone, text, db, agency)
            elif is_group and _is_tony_mentioned(text):
                group_title = sender_data.get("chatName", chat_id)
                await _handle_group_message(chat_id, group_title, sender_name, text, db, agency)
            elif not is_group and not admin_check:
                await _handle_stranger_message(chat_id, agency)
            else:
                logger.info(f"WA message not handled: is_group={is_group} is_admin={admin_check} tony_mentioned={_is_tony_mentioned(text)}")
    except Exception:
        logger.exception("WA handle_update error")
    finally:
        db.close()


# ─── Stranger private message ────────────────────────────────────────────────

_STRANGER_MSGS = [
    "Hey! 👋 I work in the groups mostly 😄\nInterested in a project? Ask in the group or contact: {contact}",
    "Hi there! 👋 I'm mainly active in group chats.\nFor personal assistance, reach out to: {contact}",
    "Hey! 😊 I handle group requests — for direct help, message: {contact}",
    "Привет! 👋 Я работаю в группах, а для личного общения лучше написать: {contact}",
]


async def _handle_stranger_message(chat_id: str, agency: Agency):
    msg = random.choice(_STRANGER_MSGS).format(contact=agency.umar_contact or "@support")
    await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, msg)


# ─── WhatsApp scheduled jobs ──────────────────────────────────────────────────

async def send_wa_morning_greeting():
    """08:00 — morning greeting to WA admins."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not agency.wa_instance_id or not agency.wa_token:
                continue
            greeting = random.choice(_MORNING_GREETINGS)
            for phone in (agency.wa_admin_numbers or []):
                await _send_wa(agency.wa_instance_id, agency.wa_token, f"{phone}@c.us", greeting)
    finally:
        db.close()


async def send_wa_morning_followup():
    """08:45 — follow up once if WA admin hasn't replied."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not agency.wa_instance_id or not agency.wa_token:
                continue
            state = _day_state(agency.id)
            if state["morning_replied"] or state["follow_up_sent"]:
                continue
            state["follow_up_sent"] = True
            msg = random.choice(_FOLLOWUP_MSGS)
            for phone in (agency.wa_admin_numbers or []):
                await _send_wa(agency.wa_instance_id, agency.wa_token, f"{phone}@c.us", msg)
    finally:
        db.close()


async def send_wa_midday_checkin():
    """14:00 — midday check-in if admin hasn't been active today."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not agency.wa_instance_id or not agency.wa_token:
                continue
            if _day_state(agency.id)["morning_replied"]:
                continue
            msg = random.choice(_MIDDAY_MSGS)
            for phone in (agency.wa_admin_numbers or []):
                await _send_wa(agency.wa_instance_id, agency.wa_token, f"{phone}@c.us", msg)
    finally:
        db.close()


# ─── Admin document upload ───────────────────────────────────────────────────

async def _handle_admin_document(chat_id: str, sender_phone: str, download_url: str,
                                 file_name: str, caption: str, db: Session, agency: Agency):
    import re as _re
    from datetime import datetime as _dt
    from excel_parser import (build_unit_index, diff_unit_indexes, format_diff_report,
                              normalize_project_name, parse_csv, parse_excel, parse_pdf)

    fname_lower = file_name.lower()
    if not fname_lower.endswith((".xlsx", ".xls", ".csv", ".pdf")):
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                       "Поддерживаются файлы: .xlsx, .xls, .csv, .pdf")
        return

    await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, f"📊 Читаю файл {file_name}...")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(download_url)
            file_bytes = resp.content
    except Exception:
        logger.exception("WA file download error")
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, "❌ Не удалось скачать файл.")
        return

    try:
        if fname_lower.endswith(".csv"):
            sheets_data = parse_csv(file_bytes)
        elif fname_lower.endswith(".pdf"):
            sheets_data = parse_pdf(file_bytes)
        else:
            sheets_data = parse_excel(file_bytes)
    except Exception as e:
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, f"❌ Ошибка чтения файла: {e}")
        return

    if not sheets_data:
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, "❌ Файл пустой или без данных.")
        return

    # All sheets → ONE project (same logic as Telegram bot)
    _GENERIC = _re.compile(r"^(sheet\s*\d*|лист\s*\d*|data|данные|table)$", _re.IGNORECASE)
    non_generic = [s for s in sheets_data.keys() if not _GENERIC.match(s.strip())]
    if caption.strip():
        name = caption.strip()
    elif len(non_generic) == 1:
        name = non_generic[0].strip()
    else:
        name = normalize_project_name(file_name)

    results = []
    for name, sheets in [(name, sheets_data)]:
        unit_index = build_unit_index(sheets)
        if not unit_index:
            results.append({"status": "skipped", "name": name})
            continue
        existing = (db.query(ToniProject)
                    .filter(ToniProject.project_name == name,
                            ToniProject.is_active == True,
                            ToniProject.agency_id == agency.id)
                    .first())
        if existing:
            diff = diff_unit_indexes(existing.unit_index or {}, unit_index)
            report = format_diff_report(diff, name)
            new_ver = existing.version + 1
            existing.is_active = False
            db.flush()
            db.add(ToniProject(project_name=name, version=new_ver, sheet_count=len(sheets),
                               unit_count=len(unit_index), sheets_data=sheets, unit_index=unit_index,
                               is_active=True, uploaded_at=_dt.now(),
                               uploaded_by=f"wa_{sender_phone}", agency_id=agency.id))
            db.commit()
            results.append({"status": "updated", "name": name, "units": len(unit_index),
                            "version": new_ver, "diff_report": report})
        else:
            db.add(ToniProject(project_name=name, version=1, sheet_count=len(sheets),
                               unit_count=len(unit_index), sheets_data=sheets, unit_index=unit_index,
                               is_active=True, uploaded_at=_dt.now(),
                               uploaded_by=f"wa_{sender_phone}", agency_id=agency.id))
            db.commit()
            results.append({"status": "created", "name": name, "units": len(unit_index), "version": 1})

    saved = [r for r in results if r["status"] != "skipped"]
    if not saved:
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, "❌ Юниты не найдены.")
        return

    if len(saved) == 1:
        r = saved[0]
        if r["status"] == "updated":
            msg = f"✅ Проект *{r['name']}* обновлён → v{r['version']}\nЮнитов: {r['units']}\n\n{r['diff_report']}"
        else:
            msg = f"✅ Проект *{r['name']}* сохранён!\nЮнитов: {r['units']}"
    else:
        lines = [f"✅ Сохранено {len(saved)} проектов:"]
        for r in saved:
            lines.append(f"{'🔄' if r['status'] == 'updated' else '📁'} {r['name']} — {r['units']} юн.")
        msg = "\n".join(lines)

    await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, msg)


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
