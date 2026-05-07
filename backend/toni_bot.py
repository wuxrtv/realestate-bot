"""
Toni — AI assistant for real estate sales agents.
All public functions accept an `agency` model instance for multi-agency support.
"""

import asyncio
import json
import logging
import os
import random
import re
from datetime import datetime

import anthropic
import httpx
from sqlalchemy.orm import Session, flag_modified

from database import SessionLocal
from excel_parser import format_unit_card
from models import Agency, GroupConversation, ToniFile, ToniGroup, ToniProject

logger = logging.getLogger(__name__)

GREETING = (
    "Привет! Я Тони AI-помощник. "
    "Я помогу вам найти информацию по юнитам, брошюры проектов и актуальные новости. "
    "Просто напишите что вас интересует!"
)

_UNIT_RE = re.compile(r"\b(\d{3,5})\b")
_BOT_NAMES = re.compile(r"\bтони\b|\btoni\b|\btony\b", re.IGNORECASE)

_SYSTEM_BASE = """You are TONY — an AI Sales Assistant for a real estate agency in Dubai.
You are in a group chat with real estate agents. Your name is Tony.

Respond ONLY with valid JSON (no markdown, no code blocks):
{
  "intent": "unit_query" | "brochure_request" | "property_search" | "direct_question" | "off_topic",
  "unit_numbers": ["1507", "1435"],
  "project_name": "project name or null",
  "keywords": ["2 rooms", "villa", "floor 20"],
  "reply": "your reply (only for direct_question, empty string otherwise)"
}

━━━ WHEN TO RESPOND ━━━
✅ Respond when:
• Someone mentions "Tony" or "Тони"
• Someone asks about units, prices, availability, projects in the database
• Question is clearly real-estate related

❌ Do NOT respond (use "off_topic" intent) when:
• People are having personal conversations
• Topics unrelated to real estate
• You don't have accurate data to answer
• When in doubt — don't respond

━━━ INTENT RULES ━━━
• unit_query: asking for specific unit number ("unit 1507", "show 1435", "2301 bormi")
• brochure_request: asking for brochure, price list, or project presentation
• property_search: searching by parameters or project ("Bugatti", "3-bedroom villa", "20th floor", "2M budget")
• direct_question: any other work question — answer in "reply" using the project context below
• off_topic: personal talk or unrelated topic — leave reply as empty string

━━━ STYLE ━━━
• Professional but friendly — like a reliable colleague
• Never guess facts. Never improvise prices or availability.
• Be accurate and concise. If unsure — say so.

LANGUAGE: Detect the language of the message. Respond ONLY in that same language.
Russian → Russian. English → English. Uzbek → Uzbek."""


# ─── Bot mention detection ───────────────────────────────────────────────────

def _is_bot_mentioned(message: dict, bot_username: str) -> bool:
    text = (message.get("text") or "").lower()

    if _BOT_NAMES.search(text):
        return True

    if bot_username:
        uname = bot_username.lower().lstrip("@")
        for ent in message.get("entities") or []:
            if ent.get("type") == "mention":
                mention = text[ent["offset"]: ent["offset"] + ent["length"]].lstrip("@")
                if mention == uname:
                    return True

    if message.get("reply_to_message", {}).get("from", {}).get("is_bot"):
        return True

    return False


# ─── Low-level Telegram API ───────────────────────────────────────────────────

async def _tg(method: str, token: str, **kwargs) -> dict:
    if not token:
        logger.warning("No bot token for _tg call")
        return {}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(f"https://api.telegram.org/bot{token}/{method}", json=kwargs)
        data = resp.json()
        if not data.get("ok"):
            logger.warning(f"Toni {method} failed: {resp.text[:200]}")
        return data


async def _send(chat_id: str, text: str, token: str) -> bool:
    data = await _tg("sendMessage", token, chat_id=chat_id, text=text, parse_mode="Markdown")
    return data.get("ok", False)


async def _copy(chat_id: str, from_chat_id: str, message_id: int, token: str) -> bool:
    data = await _tg("copyMessage", token, chat_id=chat_id,
                     from_chat_id=from_chat_id, message_id=message_id)
    return bool(data.get("result"))


async def _get_bot_id(token: str) -> int:
    data = await _tg("getMe", token)
    return data.get("result", {}).get("id", 0)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _extract_units(text: str) -> list[str]:
    return list(set(_UNIT_RE.findall(text or "")))


def _all_group_ids(db: Session, agency_id: int) -> list[str]:
    return [
        g.chat_id for g in
        db.query(ToniGroup).filter(ToniGroup.active == True, ToniGroup.agency_id == agency_id).all()
    ]


# ─── Main update handler ──────────────────────────────────────────────────────

async def handle_update(update: dict, agency: Agency):
    db = SessionLocal()
    try:
        if channel_post := update.get("channel_post"):
            cid = str(channel_post.get("chat", {}).get("id", ""))
            if agency.db_channel_id and cid == agency.db_channel_id:
                await _handle_channel_post(channel_post, db, agency)
            return

        message = update.get("message")
        if not message:
            return

        chat = message.get("chat", {})
        chat_id = str(chat.get("id", ""))
        chat_type = chat.get("type", "")

        for m in message.get("new_chat_members", []):
            if m.get("id") == await _get_bot_id(agency.bot_token):
                await _on_added_to_group(chat_id, chat.get("title", ""), db, agency)
                return

        if chat_type in ("group", "supergroup"):
            if agency.db_channel_id and chat_id == agency.db_channel_id:
                await _handle_channel_post(message, db, agency)
            else:
                await _handle_group_message(message, chat_id, chat.get("title", ""), db, agency)
    except Exception:
        logger.exception("Toni handle_update error")
    finally:
        db.close()


# ─── Bot added to a group ─────────────────────────────────────────────────────

async def _on_added_to_group(chat_id: str, title: str, db: Session, agency: Agency):
    existing = db.query(ToniGroup).filter(
        ToniGroup.chat_id == chat_id, ToniGroup.agency_id == agency.id
    ).first()
    if existing:
        existing.active = True
    else:
        db.add(ToniGroup(chat_id=chat_id, title=title, active=True, agency_id=agency.id))
    db.commit()
    await _send(chat_id, GREETING, agency.bot_token)
    logger.info(f"Toni added to group {chat_id} ({title}) for agency {agency.id}")


# ─── New file in database channel ─────────────────────────────────────────────

async def _handle_channel_post(message: dict, db: Session, agency: Agency):
    chat_id = str(message.get("chat", {}).get("id", ""))
    message_id = message.get("message_id")
    caption = (message.get("caption") or "").strip()

    file_id = file_unique_id = file_name = file_type = None

    if doc := message.get("document"):
        file_id, file_unique_id = doc.get("file_id"), doc.get("file_unique_id")
        file_name, file_type = doc.get("file_name", "Документ"), "document"
    elif photos := message.get("photo"):
        photo = photos[-1]
        file_id, file_unique_id = photo.get("file_id"), photo.get("file_unique_id")
        file_name, file_type = caption or "Фото", "photo"
    elif video := message.get("video"):
        file_id, file_unique_id = video.get("file_id"), video.get("file_unique_id")
        file_name, file_type = video.get("file_name", "Видео"), "video"
    else:
        return

    if not file_unique_id:
        return

    if db.query(ToniFile).filter(ToniFile.file_unique_id == file_unique_id).first():
        return

    units = list(set(_extract_units(file_name) + _extract_units(caption)))
    db.add(ToniFile(
        agency_id=agency.id,
        file_id=file_id,
        file_unique_id=file_unique_id,
        file_name=file_name,
        caption=caption,
        file_type=file_type,
        unit_numbers=units,
        message_id=message_id,
        channel_chat_id=chat_id,
    ))
    db.commit()
    logger.info(f"Toni indexed file: {file_name}, units={units}")

    date_str = datetime.now().strftime("%d.%m.%Y")
    label = caption if caption else file_name
    announcement = f"🆕 Новое обновление {date_str}: {label}. Доступно уже сейчас!"

    group_ids = _all_group_ids(db, agency.id)
    for i, gid in enumerate(group_ids):
        if i > 0:
            await asyncio.sleep(30)
        await _send(gid, announcement, agency.bot_token)
        await _copy(gid, chat_id, message_id, agency.bot_token)


# ─── Group conversation history helpers ──────────────────────────────────────

def _load_group_history(db: Session, agency_id: int, chat_id: str) -> tuple[GroupConversation, list]:
    conv = db.query(GroupConversation).filter(
        GroupConversation.agency_id == agency_id,
        GroupConversation.chat_id == chat_id,
    ).first()
    if not conv:
        conv = GroupConversation(agency_id=agency_id, chat_id=chat_id, history=[])
        db.add(conv)
        db.flush()
    return conv, list(conv.history or [])


def _save_group_history(db: Session, conv: GroupConversation, history: list):
    conv.history = history[-20:]  # keep last 10 exchanges
    conv.updated_at = __import__("datetime").datetime.now()
    flag_modified(conv, "history")
    db.commit()


# ─── Group message handler ────────────────────────────────────────────────────

async def _handle_group_message(message: dict, chat_id: str, chat_title: str,
                                db: Session, agency: Agency):
    if message.get("from", {}).get("is_bot"):
        return

    text = (message.get("text") or "").strip()
    if not text:
        return

    if not _is_bot_mentioned(message, agency.bot_username or ""):
        return

    existing_group = db.query(ToniGroup).filter(
        ToniGroup.chat_id == chat_id, ToniGroup.agency_id == agency.id
    ).first()
    if not existing_group:
        db.add(ToniGroup(chat_id=chat_id, title=chat_title, active=True, agency_id=agency.id))
        db.commit()

    projects_snapshot = db.query(ToniProject).filter(
        ToniProject.is_active == True, ToniProject.agency_id == agency.id
    ).all()
    if projects_snapshot:
        proj_lines = "\n".join(f"  • {p.project_name} — {p.unit_count} юн." for p in projects_snapshot)
        system = _SYSTEM_BASE + f"\n\nДоступные проекты в базе:\n{proj_lines}"
    else:
        system = _SYSTEM_BASE

    # Load group history and append current message (with sender name for context)
    sender = (message.get("from", {}).get("first_name") or "Agent").strip()
    conv, history = _load_group_history(db, agency.id, chat_id)
    history.append({"role": "user", "content": f"[{sender}]: {text}"})

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
            logger.warning(f"Toni: no JSON in response: {raw[:100]}")
            _save_group_history(db, conv, history)
            return
        parsed = json.loads(match.group())
    except Exception:
        logger.exception(f"Toni: Claude API error for message: {text[:80]}")
        return

    intent = parsed.get("intent", "off_topic")
    unit_numbers: list[str] = parsed.get("unit_numbers") or []
    project_name: str = parsed.get("project_name") or ""

    if intent == "unit_query" and unit_numbers:
        await _respond_unit(chat_id, unit_numbers, db, agency)
    elif intent == "brochure_request":
        await _respond_brochure(chat_id, project_name, db, agency)
    elif intent == "property_search":
        keywords: list[str] = parsed.get("keywords") or []
        if project_name and project_name not in keywords:
            keywords = [project_name] + keywords
        await _respond_property_search(chat_id, keywords, db, agency)
    elif intent == "direct_question":
        reply = (parsed.get("reply") or "").strip()
        if reply:
            await _send(chat_id, reply, agency.bot_token)

    # Save assistant turn so next message has context
    history.append({"role": "assistant", "content": raw})
    _save_group_history(db, conv, history)


# ─── Unit query response ──────────────────────────────────────────────────────

async def _respond_unit(chat_id: str, unit_numbers: list[str], db: Session, agency: Agency):
    projects = db.query(ToniProject).filter(
        ToniProject.is_active == True, ToniProject.agency_id == agency.id
    ).all()
    not_found: list[str] = []
    sent = 0

    for unit in unit_numbers:
        found = False

        for proj in projects:
            idx: dict = proj.unit_index or {}
            if unit in idx:
                card = format_unit_card(unit, idx[unit], proj.project_name)
                await _send(chat_id, card, agency.bot_token)
                sent += 1
                found = True
                break

        if not found:
            all_files = db.query(ToniFile).filter(ToniFile.agency_id == agency.id).all()
            matches = [f for f in all_files if unit in (f.unit_numbers or [])]
            if matches:
                for f in matches[:1]:
                    await _copy(chat_id, f.channel_chat_id, f.message_id, agency.bot_token)
                    sent += 1
                found = True

        if not found:
            not_found.append(unit)

        if sent >= 3:
            break

    if not_found:
        units_str = ", ".join(not_found)
        await _send(
            chat_id,
            f"К сожалению, юнит {units_str} недоступен. "
            f"Напишите напрямую {agency.umar_contact} для уточнения.",
            agency.bot_token,
        )


# ─── Brochure request response ────────────────────────────────────────────────

async def _respond_brochure(chat_id: str, project_name: str, db: Session, agency: Agency):
    all_files = db.query(ToniFile).filter(ToniFile.agency_id == agency.id).all()

    if project_name:
        pn = project_name.lower()
        matched = [
            f for f in all_files
            if pn in (f.file_name or "").lower() or pn in (f.caption or "").lower()
        ]
    else:
        matched = sorted(all_files, key=lambda x: x.id, reverse=True)[:3]

    if matched:
        for f in matched[:3]:
            await _copy(chat_id, f.channel_chat_id, f.message_id, agency.bot_token)
    else:
        await _send(
            chat_id,
            f"Брошюра не найдена в базе. Обратитесь напрямую к {agency.umar_contact}.",
            agency.bot_token,
        )


# ─── Property search response ─────────────────────────────────────────────────

async def _respond_property_search(chat_id: str, keywords: list[str],
                                   db: Session, agency: Agency):
    matched_units: list[tuple[str, dict, str]] = []
    projects = db.query(ToniProject).filter(
        ToniProject.is_active == True, ToniProject.agency_id == agency.id
    ).all()

    if keywords:
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
                    matched_units.append((unit_num, data, proj.project_name))
                else:
                    searchable = " ".join(str(v) for v in data.values()).lower()
                    if any(kw.lower() in searchable for kw in keywords):
                        matched_units.append((unit_num, data, proj.project_name))

                if len(matched_units) >= 3:
                    break
            if len(matched_units) >= 3:
                break

    if matched_units:
        for unit_num, data, proj_name in matched_units[:3]:
            await _send(chat_id, format_unit_card(unit_num, data, proj_name), agency.bot_token)
        return

    if keywords:
        all_files = db.query(ToniFile).filter(ToniFile.agency_id == agency.id).all()
        matched_files = [
            f for f in all_files
            if any(kw.lower() in f"{f.file_name or ''} {f.caption or ''}".lower() for kw in keywords)
        ]
        if matched_files:
            for f in matched_files[:3]:
                await _copy(chat_id, f.channel_chat_id, f.message_id, agency.bot_token)
            return

    if projects:
        proj = projects[0]
        idx = proj.unit_index or {}
        sample = list(idx.items())[:3]
        if sample:
            await _send(chat_id, f"Точного совпадения не нашёл, вот доступные варианты из *{proj.project_name}*:", agency.bot_token)
            for unit_num, data in sample:
                await _send(chat_id, format_unit_card(unit_num, data, proj.project_name), agency.bot_token)
            return

    await _send(
        chat_id,
        f"Вариантов по запросу не нашёл. Уточните название проекта, этаж или количество комнат — помогу точнее. "
        f"Или напишите {agency.umar_contact}.",
        agency.bot_token,
    )


# ─── Morning greeting to admin (08:00) ───────────────────────────────────────

_MORNING_GREETINGS = [
    "Morning! What are we pushing to the groups today? 💼",
    "Good morning! Ready when you are — what's the plan for today?",
    "Morning boss! What's on the agenda? Any new projects to broadcast?",
    "Rise and shine! What files are we sending out today?",
    "Good morning! Got everything ready — what are we working with today?",
    "Hey, morning! What do you need from me today?",
    "Morning! The groups are waiting — what do we broadcast today?",
]

async def send_morning_greeting_to_admin():
    """Send a personal morning message to all admins at 08:00."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            greeting = random.choice(_MORNING_GREETINGS)
            for admin_id in (agency.admin_ids or []):
                await _send(admin_id, greeting, agency.bot_token)
    finally:
        db.close()


# ─── Morning report to groups (08:00) ────────────────────────────────────────

async def send_morning_report():
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            projects = db.query(ToniProject).filter(
                ToniProject.is_active == True,
                ToniProject.agency_id == agency.id,
            ).all()

            if not projects:
                text = "📊 Good morning! No projects loaded in the database yet."
            else:
                total_units = sum(p.unit_count for p in projects)
                lines = [f"📊 Good morning! Database has {len(projects)} project(s), {total_units} units:\n"]
                for p in projects:
                    lines.append(f"• *{p.project_name}* — {p.unit_count} units")
                lines.append("\nAsk me a unit number or search parameters!")
                text = "\n".join(lines)

            for gid in _all_group_ids(db, agency.id):
                await _send(gid, text, agency.bot_token)
    finally:
        db.close()


# ─── End of day report to admin (20:00) ──────────────────────────────────────

async def send_end_of_day_report():
    """Send daily wrap-up report to all admins at 20:00."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            projects = db.query(ToniProject).filter(
                ToniProject.is_active == True,
                ToniProject.agency_id == agency.id,
            ).all()
            groups = db.query(ToniGroup).filter(
                ToniGroup.active == True,
                ToniGroup.agency_id == agency.id,
            ).all()

            date_str = datetime.now().strftime("%d.%m.%Y")
            proj_lines = "\n".join(f"  • {p.project_name} ({p.unit_count} units)" for p in projects) or "  — none"
            group_list = "\n".join(f"  • {g.title or g.chat_id}" for g in groups) or "  — none"

            report = (
                f"📊 Daily wrap — {date_str}\n\n"
                f"👥 Active groups: {len(groups)}\n{group_list}\n\n"
                f"📁 Projects in database: {len(projects)}\n{proj_lines}\n\n"
                f"📌 System running smoothly. Check group chats for any pending questions."
            )

            for admin_id in (agency.admin_ids or []):
                await _send(admin_id, report, agency.bot_token)
    finally:
        db.close()
