"""
Toni — AI assistant for real estate sales agents.
Monitors a private Telegram channel as file database and serves multiple agent groups.
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
from models import Property, ToniFile, ToniGroup

logger = logging.getLogger(__name__)

TONI_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TONI_API = f"https://api.telegram.org/bot{TONI_TOKEN}"
DB_CHANNEL_ID = os.getenv("TONI_DB_CHANNEL", "")   # private channel numeric ID, e.g. "-100123456789"
UMAR_CONTACT = os.getenv("TONI_UMAR_CONTACT", "@Umar")

GREETING = (
    "Привет! Я Тони AI-помощник. "
    "Я помогу вам найти информацию по юнитам, брошюры проектов и актуальные новости. "
    "Просто напишите что вас интересует!"
)

_UNIT_RE = re.compile(r"\b(\d{3,5})\b")

_SYSTEM = """Ты — Тони, AI-помощник агентов по недвижимости в Telegram группе.

Главное правило: НЕ вмешивайся в разговор если тебя не спрашивают.

Ответь ТОЛЬКО валидным JSON без markdown:
{
  "intent": "unit_query" | "brochure_request" | "property_search" | "direct_question" | "silent",
  "unit_numbers": ["1507", "1435"],
  "project_name": "название проекта или null",
  "keywords": ["2 qavvat", "uy", "villa"],
  "reply": "твой ответ (для direct_question)"
}

КОГДА ОТВЕЧАТЬ:
- unit_query: спрашивают конкретный юнит ("есть юнит 1507?", "покажи 1435", "unit 2301 bor mi")
- brochure_request: просят брошюру, прайс-лист, презентацию проекта
- property_search: описывают что ищут — тип, этажность, комнаты, район, цена ("2 qavvatan uy kerakan", "3 комнатная нужна", "villa bor mi", "нужна квартира в Юнусабаде")
- direct_question: агент обращается напрямую к тебе — пишет "@botname", "тони", "бот", задаёт тебе вопрос лично

КОГДА МОЛЧАТЬ (silent):
- люди общаются друг с другом, даже если тема — недвижимость
- обсуждают цены, проекты, новости между собой
- пишут "ok", "ha", "понял", "спасибо", "salom" — не тебе

Отвечай на языке агента (русский, узбекский, английский)"""


# ─── Low-level Telegram API ───────────────────────────────────────────────────

async def _tg(method: str, **kwargs) -> dict:
    if not TONI_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not set")
        return {}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(f"{_TONI_API}/{method}", json=kwargs)
        data = resp.json()
        if not data.get("ok"):
            logger.warning(f"Toni {method} failed: {resp.text[:200]}")
        return data


async def _send(chat_id: str, text: str) -> bool:
    data = await _tg("sendMessage", chat_id=chat_id, text=text, parse_mode="Markdown")
    return data.get("ok", False)


async def _copy(chat_id: str, from_chat_id: str, message_id: int) -> bool:
    """Copy a file from the database channel to an agent group (no 'Forwarded from' label)."""
    data = await _tg("copyMessage", chat_id=chat_id, from_chat_id=from_chat_id, message_id=message_id)
    return bool(data.get("result"))


async def _get_bot_id() -> int:
    data = await _tg("getMe")
    return data.get("result", {}).get("id", 0)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _extract_units(text: str) -> list[str]:
    return list(set(_UNIT_RE.findall(text or "")))


def _all_group_ids(db: Session) -> list[str]:
    return [g.chat_id for g in db.query(ToniGroup).filter(ToniGroup.active == True).all()]


# ─── Main update handler ──────────────────────────────────────────────────────

async def handle_update(update: dict):
    db = SessionLocal()
    try:
        # New post in the private database channel
        if channel_post := update.get("channel_post"):
            cid = str(channel_post.get("chat", {}).get("id", ""))
            if DB_CHANNEL_ID and cid == DB_CHANNEL_ID:
                await _handle_channel_post(channel_post, db)
            return

        message = update.get("message")
        if not message:
            return

        chat = message.get("chat", {})
        chat_id = str(chat.get("id", ""))
        chat_type = chat.get("type", "")

        # Bot was added to a group
        for m in message.get("new_chat_members", []):
            if m.get("id") == await _get_bot_id():
                await _on_added_to_group(chat_id, chat.get("title", ""), db)
                return

        if chat_type in ("group", "supergroup"):
            # Messages from the database group → index files, not treat as agent query
            if DB_CHANNEL_ID and chat_id == DB_CHANNEL_ID:
                await _handle_channel_post(message, db)
            else:
                await _handle_group_message(message, chat_id, chat.get("title", ""), db)
    except Exception:
        logger.exception("Toni handle_update error")
    finally:
        db.close()


# ─── Bot added to a group ─────────────────────────────────────────────────────

async def _on_added_to_group(chat_id: str, title: str, db: Session):
    existing = db.query(ToniGroup).filter(ToniGroup.chat_id == chat_id).first()
    if existing:
        existing.active = True
    else:
        db.add(ToniGroup(chat_id=chat_id, title=title, active=True))
    db.commit()
    await _send(chat_id, GREETING)
    logger.info(f"Toni added to group {chat_id} ({title})")


# ─── New file in database channel ─────────────────────────────────────────────

async def _handle_channel_post(message: dict, db: Session):
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
        return  # text-only posts are not indexed

    if not file_unique_id:
        return

    if db.query(ToniFile).filter(ToniFile.file_unique_id == file_unique_id).first():
        return  # already indexed

    units = list(set(_extract_units(file_name) + _extract_units(caption)))
    db.add(ToniFile(
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

    date_str = datetime.utcnow().strftime("%d.%m.%Y")
    label = caption if caption else file_name
    announcement = f"🆕 Новое обновление {date_str}: {label}. Доступно уже сейчас!"

    for gid in _all_group_ids(db):
        await _send(gid, announcement)
        await _copy(gid, chat_id, message_id)


# ─── Group message handler ────────────────────────────────────────────────────

async def _handle_group_message(message: dict, chat_id: str, chat_title: str, db: Session):
    text = (message.get("text") or "").strip()
    if not text:
        return

    # Register group if first encounter
    if not db.query(ToniGroup).filter(ToniGroup.chat_id == chat_id).first():
        db.add(ToniGroup(chat_id=chat_id, title=chat_title, active=True))
        db.commit()

    ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    resp = await ai.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=_SYSTEM,
        messages=[{"role": "user", "content": text}],
    )

    try:
        raw = resp.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        parsed = json.loads(raw.strip())
    except Exception:
        logger.warning(f"Toni: failed to parse AI response: {resp.content[0].text[:100]}")
        return

    intent = parsed.get("intent", "off_topic")
    unit_numbers: list[str] = parsed.get("unit_numbers") or []
    project_name: str = parsed.get("project_name") or ""

    if intent == "unit_query" and unit_numbers:
        await _respond_unit(chat_id, unit_numbers, db)
    elif intent == "brochure_request":
        await _respond_brochure(chat_id, project_name, db)
    elif intent == "property_search":
        keywords: list[str] = parsed.get("keywords") or []
        await _respond_property_search(chat_id, keywords, db)
    elif intent == "direct_question":
        reply = (parsed.get("reply") or "").strip()
        if reply:
            await _send(chat_id, reply)
    # silent → stay out of the conversation


# ─── Unit query response ──────────────────────────────────────────────────────

async def _respond_unit(chat_id: str, unit_numbers: list[str], db: Session):
    all_files = db.query(ToniFile).all()
    found: list[ToniFile] = []
    not_found: list[str] = []

    for unit in unit_numbers:
        matches = [f for f in all_files if unit in (f.unit_numbers or [])]
        if matches:
            found.extend(matches)
        else:
            not_found.append(unit)

    for f in found[:3]:
        await _copy(chat_id, f.channel_chat_id, f.message_id)

    if not_found:
        units_str = ", ".join(not_found)
        await _send(
            chat_id,
            f"К сожалению, юнит {units_str} недоступен. "
            f"Напишите напрямую {UMAR_CONTACT} для уточнения."
        )


# ─── Brochure request response ────────────────────────────────────────────────

async def _respond_brochure(chat_id: str, project_name: str, db: Session):
    all_files = db.query(ToniFile).all()

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
            await _copy(chat_id, f.channel_chat_id, f.message_id)
    else:
        await _send(
            chat_id,
            f"Брошюра не найдена в базе. Обратитесь напрямую к {UMAR_CONTACT}."
        )


# ─── Property search response ────────────────────────────────────────────────

async def _respond_property_search(chat_id: str, keywords: list[str], db: Session):
    """Search files by keywords from description, send matches or ask to specify."""
    all_files = db.query(ToniFile).all()
    matched = []

    if keywords:
        for f in all_files:
            searchable = f"{f.file_name or ''} {f.caption or ''}".lower()
            if any(kw.lower() in searchable for kw in keywords):
                matched.append(f)

    if matched:
        for f in matched[:3]:
            await _copy(chat_id, f.channel_chat_id, f.message_id)
    else:
        await _send(
            chat_id,
            f"Bazada mos variant topilmadi. "
            f"Aniq loyiha yoki unit raqamini yozing — tezroq topamiz. "
            f"Yoki {UMAR_CONTACT} bilan bog'laning."
        )


# ─── Morning report (called by scheduler at 9:00) ────────────────────────────

async def send_morning_report():
    db = SessionLocal()
    try:
        props = (
            db.query(Property)
            .filter(Property.status == "active")
            .order_by(Property.id.desc())
            .all()
        )
        if not props:
            text = "📊 Доброе утро! На данный момент нет активных объектов в базе."
        else:
            lines = ["📊 Доброе утро! Актуальный инвентарь на сегодня:\n"]
            for p in props[:20]:
                if p.listing_type == "rent" and p.rent_price:
                    price = f"{p.rent_price:,.0f} $/мес".replace(",", " ")
                elif p.price:
                    price = f"{p.price:,.0f} $".replace(",", " ")
                else:
                    price = "цена по запросу"
                rooms = f"{p.rooms}к " if p.rooms else ""
                lines.append(f"• {rooms}{p.title} — {price}")
            text = "\n".join(lines)

        for gid in _all_group_ids(db):
            await _send(gid, text)
    finally:
        db.close()

