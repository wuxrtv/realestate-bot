"""
Toni — AI assistant for real estate sales agents.
All public functions accept an `agency` model instance for multi-agency support.
"""

import json
import logging
import os
import re
from datetime import datetime

import anthropic
import httpx
from sqlalchemy.orm import Session

from database import SessionLocal
from excel_parser import format_unit_card
from models import Agency, ToniFile, ToniGroup, ToniProject

logger = logging.getLogger(__name__)

GREETING = (
    "Привет! Я Тони AI-помощник. "
    "Я помогу вам найти информацию по юнитам, брошюры проектов и актуальные новости. "
    "Просто напишите что вас интересует!"
)

_UNIT_RE = re.compile(r"\b(\d{3,5})\b")
_BOT_NAMES = re.compile(r"\bтони\b|\btoni\b|\btony\b", re.IGNORECASE)

_SYSTEM_BASE = """Ты — Тони, AI-помощник агентов по недвижимости. К тебе обратились напрямую.

Ответь ТОЛЬКО валидным JSON без markdown:
{
  "intent": "unit_query" | "brochure_request" | "property_search" | "direct_question",
  "unit_numbers": ["1507", "1435"],
  "project_name": "название проекта или null",
  "keywords": ["2 комнаты", "villa", "20 этаж"],
  "reply": "твой ответ (только для direct_question)"
}

ПРАВИЛА ВЫБОРА intent:
- unit_query: спрашивают конкретный номер юнита ("юнит 1507", "покажи 1435", "2301 bormi")
- brochure_request: просят брошюру, прайс-лист, презентацию проекта
- property_search: ищут вариант по параметрам или называют проект ("Bugatti", "Breez 3-комнатная", "вилла", "бюджет 2M", "20 этаж")
- direct_question: любой другой вопрос — что есть в базе, сколько юнитов, общие вопросы. В "reply" ответь сам, используя список проектов из контекста.

Отвечай на языке агента (русский, узбекский, английский)."""


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

    for gid in _all_group_ids(db, agency.id):
        await _send(gid, announcement, agency.bot_token)
        await _copy(gid, chat_id, message_id, agency.bot_token)


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

    try:
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system=system,
            messages=[{"role": "user", "content": text}],
        )
        raw = resp.content[0].text.strip()
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not match:
            logger.warning(f"Toni: no JSON in response: {raw[:100]}")
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


# ─── Morning report (called by scheduler at 9:00) ────────────────────────────

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
                text = "📊 Доброе утро! Проекты в базе пока не загружены."
            else:
                total_units = sum(p.unit_count for p in projects)
                lines = [f"📊 Доброе утро! В базе {len(projects)} проект(а), {total_units} юнитов:\n"]
                for p in projects:
                    lines.append(f"• *{p.project_name}* — {p.unit_count} юн.")
                lines.append("\nЗадайте номер юнита или параметры поиска!")
                text = "\n".join(lines)

            for gid in _all_group_ids(db, agency.id):
                await _send(gid, text, agency.bot_token)
    finally:
        db.close()
