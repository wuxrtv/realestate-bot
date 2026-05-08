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

from sqlalchemy.orm.attributes import flag_modified

from database import SessionLocal
from excel_parser import format_unit_card
from models import Agency, GroupConversation, ToniProject, WhatsAppGroup

# ─── Daily admin state ────────────────────────────────────────────────────────

_daily: dict[str, dict] = {}


def _day_state(agency_id: int) -> dict:
    from datetime import datetime as _dt
    key = f"{agency_id}_{_dt.now().strftime('%Y-%m-%d')}"
    if key not in _daily:
        _daily[key] = {"morning_replied": False, "follow_up_sent": False}
    return _daily[key]


def mark_admin_active(agency_id: int):
    _day_state(agency_id)["morning_replied"] = True


# ─── Group conversation history ───────────────────────────────────────────────

def _load_group_history(db, agency_id: int, chat_id: str):
    from datetime import datetime as _dt
    conv = db.query(GroupConversation).filter(
        GroupConversation.agency_id == agency_id,
        GroupConversation.chat_id == chat_id,
    ).first()
    if not conv:
        conv = GroupConversation(agency_id=agency_id, chat_id=chat_id, history=[])
        db.add(conv)
        db.flush()
    return conv, list(conv.history or [])


def _save_group_history(db, conv, history: list):
    from datetime import datetime as _dt
    conv.history = history[-20:]
    conv.updated_at = _dt.now()
    flag_modified(conv, "history")
    db.commit()


# ─── Scheduled message texts ──────────────────────────────────────────────────

_MORNING_GREETINGS = [
    "Morning habibi! Yalla what are we dropping today? 🔥",
    "Morning boss! Wallah ready when you are — what's the plan? 💼",
    "Rise and shine habibi! What files are we sending out today? ☀️",
    "Good morning! Yalla yalla — what are we working with today? 🔥",
    "Morning habibi! The groups are waiting — what do we broadcast? 💪",
    "Hey, morning! Wallah got everything ready — what do we do today? 🤲",
    "Morning! Yalla habibi — what are we dropping today? 💼🔥",
]

_MORNING_GREETINGS_FRIDAY = [
    "Habibi it's FRIDAY wallah 🕌\nYalla what are we dropping before Jumaa? 🔥",
    "Friday vibes habibi ☀️\nQuick blast before people disappear to brunch? 😂",
    "Bro it's Friday wallah 🕌\nGroups go quiet after 12 you know the drill 😄\nYalla let's move fast! 💪",
]

_FOLLOWUP_MSGS = [
    "Habibi you awake? ☕ Yalla let's go 💪",
    "Hey habibi — still here whenever you're ready! Anything to push out? 🔥",
    "Just a nudge habibi — let me know what projects we're focusing on today! 💼",
    "Bro, you there? Wallah ready when you are 🙌",
    "Hey habibi, no rush — just checking in. Anything for the groups today? 🤲",
]

_MIDDAY_MSGS = [
    "Hey habibi, any offers worth sharing this afternoon? 👀",
    "Wallah groups are active bro — got any content to share? 🔥",
    "Afternoon habibi — anything new to drop? Yalla let's go! 💪",
    "Hey habibi — groups are busy wallah. Got something for them? 🔥",
    "Bro, anything new? Yalla send it — groups are waiting! 💼",
]

# ─── Tony group AI system prompt ─────────────────────────────────────────────

_SYSTEM_BASE = """You are TONY — an AI Sales Assistant for a real estate agency in Dubai.
You are in a group chat with real estate agents. Your name is Tony.

Respond ONLY with valid JSON (no markdown, no code blocks):
{
  "intent": "unit_query" | "brochure_request" | "photo_request" | "video_request" | "property_search" | "direct_question" | "off_topic",
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
• brochure_request: asking for brochure, PDF, price list, or project presentation
• photo_request: asking for photos, pictures, renders of a project ("фото", "photo", "pictures", "renders")
• video_request: asking for video, tour of a project ("видео", "video", "tour", "ролик")
• property_search: searching by parameters or project ("Bugatti", "3-bedroom villa", "20th floor", "2M budget")
• direct_question: any other work question — answer in "reply" using the project context below
• off_topic: personal talk or unrelated topic — leave reply as empty string

━━━ TONY'S CHARACTER (use in "reply" field only) ━━━
Tony is a Dubai local — smart, warm, fast, reliable. Dubai energy.
Arabic flavor: Habibi / Wallah / Yalla / Khalas — max 1-2 per message.
NEVER say: "Certainly!" "Of course!" "Absolutely!" "I'd be happy to!"
Good answer example:
  "Wallah good choice habibi 👀
   Unit B-2701 — Floor 27, Burj Khalifa view 🏙️
   Price: AED 1,869,432
   Inshallah yours soon 🤲"
Not found example:
  "Ya habibi this one I need to check 😅
   @admin can you jump in? 🙏"

━━━ STYLE ━━━
• Numbers and data always accurate — humor is just the wrapper
• Never guess facts. Never improvise prices or availability.
• Be accurate and concise. If unsure — say so honestly.

LANGUAGE: Detect the language of the message. Respond ONLY in that same language.
Russian → Russian. English → English. Uzbek → Uzbek.
Arabic flavor words (habibi, wallah, yalla, khalas) work in ANY language."""

# Multilingual keywords that identify an inventory/price-list file
_WA_INVENTORY_KEYWORDS = (
    "инвентарий", "инвентарь", "инвентаризация",
    "прайс-лист", "прайслист", "прайс", "база данных", "база юнитов", "список юнитов",
    "inventory", "price list", "pricelist", "availability", "unit list", "stock list",
    "مخزون", "قائمة الوحدات", "قائمة",
    "inventaire", "liste des unités",
    "inventario", "lista de unidades",
    "inventar", "bestand", "einheitenliste",
    "envanter", "birim listesi",
    "库存", "单元列表",
)


def _wa_is_inventory(fname: str, caption: str) -> bool:
    fl, cl = fname.lower(), (caption or "").lower()
    if fl.endswith((".xlsx", ".xls", ".csv")):
        return True
    return any(kw in cl or kw in fl for kw in _WA_INVENTORY_KEYWORDS)


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

    if msg_type not in ("textMessage", "documentMessage", "imageMessage", "videoMessage"):
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
        if msg_type in ("documentMessage", "imageMessage", "videoMessage") and not is_group and admin_check:
            file_data = message_data.get("fileMessageData", {})
            download_url = file_data.get("downloadUrl", "")
            file_name = file_data.get("fileName", f"file.{'jpg' if msg_type == 'imageMessage' else 'mp4' if msg_type == 'videoMessage' else 'bin'}")
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
    from datetime import datetime as _dt
    is_friday = _dt.now().weekday() == 4  # 4 = Friday
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not agency.wa_instance_id or not agency.wa_token:
                continue
            pool = _MORNING_GREETINGS_FRIDAY if is_friday else _MORNING_GREETINGS
            greeting = random.choice(pool)
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

    # Photos and videos are not inventory — inform admin
    if fname_lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".avi")):
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                       "📷 Фото/видео получено. Для добавления в базу брошюр используйте Telegram-канал агентства.")
        return

    # Non-data files
    if not fname_lower.endswith((".xlsx", ".xls", ".csv", ".pdf")):
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                       "❓ Файл не распознан как база данных. Для инвентаря отправьте .xlsx, .xls, .csv "
                       "или PDF с названием «Инвентарий».")
        return

    # PDF without inventory keyword → brochure, not inventory
    if not _wa_is_inventory(file_name, caption):
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                       f"📄 PDF «{file_name}» сохранён как брошюра.\n"
                       "Чтобы загрузить как инвентарий, напишите «Инвентарий» в названии файла или подписи.")
        return

    await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, f"📊 Читаю инвентарий *{file_name}*...")

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
        reply = await agent.process(agency, f"wa_{sender_phone}", text, db, chat_id=chat_id)
        if reply and reply.strip():
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
    contact = agency.umar_contact or "@support"
    if projects:
        proj_lines = "\n".join(f"  • {p.project_name} — {p.unit_count} units" for p in projects)
        system = _SYSTEM_BASE + f"\n\nAdmin contact: {contact}\nAvailable projects:\n{proj_lines}"
    else:
        system = _SYSTEM_BASE + f"\n\nAdmin contact: {contact}\nNo projects loaded yet."

    conv, history = _load_group_history(db, agency.id, f"wa_{chat_id}")
    history.append({"role": "user", "content": f"[{sender_name}]: {text}"})

    try:
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
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
        await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                       f"Ya habibi, something went wrong 😅 Contact {contact} 🙏")
        return

    intent = parsed.get("intent", "off_topic")
    unit_numbers: list = parsed.get("unit_numbers") or []
    project_name: str = parsed.get("project_name") or ""
    keywords: list = parsed.get("keywords") or []

    if intent == "unit_query":
        if unit_numbers:
            await _respond_unit(chat_id, unit_numbers, projects, agency)
        else:
            await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                           f"Ya habibi, which unit number? 😅 Contact {contact} 🙏")
    elif intent == "property_search":
        if project_name and project_name not in keywords:
            keywords = [project_name] + keywords
        await _respond_search(chat_id, keywords, projects, agency)
    elif intent == "brochure_request":
        import drive_service as _drive
        svc = _drive.get_service()
        sent = False
        search_name = project_name or (keywords[0] if keywords else "")
        if svc and search_name:
            drive_result = _drive.find_brochure(svc, search_name)
            if drive_result:
                file_id, file_name = drive_result
                file_bytes = _drive.download_file(svc, file_id)
                if file_bytes:
                    await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                                   f"Wallah great project habibi! 🏙️\nHere's the brochure 👇")
                    await _send_wa_file(agency.wa_instance_id, agency.wa_token,
                                        chat_id, file_bytes, file_name,
                                        f"{search_name} — Brochure 📄")
                    sent = True
        if not sent:
            contact = agency.umar_contact or "@support"
            await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                           f"Ya habibi, brochure not found in Drive 😅 Contact {contact} 🙏")
    elif intent == "photo_request":
        import drive_service as _drive
        svc = _drive.get_service()
        sent = False
        search_name = project_name or (keywords[0] if keywords else "")
        if svc and search_name:
            photos = _drive.find_photos(svc, search_name, limit=5)
            if photos:
                await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                               f"Wallah habibi — {search_name} photos incoming 📸👇")
                for file_id, file_name in photos:
                    file_bytes = _drive.download_file(svc, file_id)
                    if file_bytes:
                        await _send_wa_file(agency.wa_instance_id, agency.wa_token,
                                            chat_id, file_bytes, file_name)
                sent = True
        if not sent:
            contact = agency.umar_contact or "@support"
            await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                           f"Ya habibi, no photos found in Drive 😅 Contact {contact} 🙏")
    elif intent == "video_request":
        import drive_service as _drive
        svc = _drive.get_service()
        sent = False
        search_name = project_name or (keywords[0] if keywords else "")
        if svc and search_name:
            drive_result = _drive.find_video(svc, search_name)
            if drive_result:
                file_id, file_name = drive_result
                file_bytes = _drive.download_file(svc, file_id)
                if file_bytes:
                    await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                                   f"Yalla habibi — {search_name} video 🎬👇")
                    await _send_wa_file(agency.wa_instance_id, agency.wa_token,
                                        chat_id, file_bytes, file_name,
                                        f"{search_name} 🎬")
                    sent = True
        if not sent:
            contact = agency.umar_contact or "@support"
            await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                           f"Ya habibi, no video found in Drive 😅 Contact {contact} 🙏")
    elif intent == "direct_question":
        reply = (parsed.get("reply") or "").strip()
        if reply:
            await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id, reply)

    history.append({"role": "assistant", "content": raw})
    _save_group_history(db, conv, history)


# ─── Send file via WhatsApp ───────────────────────────────────────────────────

async def _send_wa_file(instance_id: str, token: str, chat_id: str,
                        file_bytes: bytes, file_name: str, caption: str = "") -> bool:
    """Send file to WhatsApp via Green API sendFileByUpload."""
    if not instance_id or not token:
        return False
    url = _wa_url(instance_id, token, "sendFileByUpload")
    ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    mime_map = {
        "pdf": "application/pdf",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xls": "application/vnd.ms-excel",
        "jpg": "image/jpeg", "jpeg": "image/jpeg",
        "png": "image/png", "webp": "image/webp",
        "mp4": "video/mp4", "mov": "video/quicktime",
    }
    mime = mime_map.get(ext, "application/octet-stream")
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            resp = await client.post(
                url,
                data={"chatId": chat_id, "fileName": file_name, "caption": caption},
                files={"file": (file_name, file_bytes, mime)},
            )
            ok = resp.status_code == 200
            if not ok:
                logger.warning(f"WA sendFileByUpload failed: {resp.text[:200]}")
            return ok
        except Exception:
            logger.exception("WA send file error")
            return False


# ─── Unit lookup ──────────────────────────────────────────────────────────────

async def _respond_unit(chat_id: str, unit_numbers: list, projects: list, agency: Agency):
    import drive_service as _drive
    svc = _drive.get_service()
    contact = agency.umar_contact or "@support"

    for unit in unit_numbers[:3]:
        found = False
        for proj in projects:
            idx: dict = proj.unit_index or {}
            if unit in idx:
                found = True
                card = format_unit_card(unit, idx[unit], proj.project_name)

                # Try Drive first — send PDF/file if available
                if svc:
                    drive_result = _drive.find_unit_file(svc, proj.project_name, unit)
                    if drive_result:
                        file_id, file_name = drive_result
                        file_bytes = _drive.download_file(svc, file_id)
                        if file_bytes:
                            await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                                           f"Wallah good choice habibi! 👀\nHere's everything about Unit {unit} 👇")
                            await _send_wa_file(agency.wa_instance_id, agency.wa_token,
                                                chat_id, file_bytes, file_name, card)
                            break

                # No Drive file — send text card
                await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                               f"Wallah good choice habibi! 👀\n{card}")
                break

        if not found:
            # Unit not in inventory — show alternatives
            alts = []
            for proj in projects:
                idx = proj.unit_index or {}
                for u_num, u_data in list(idx.items())[:3]:
                    if u_num != unit:
                        alts.append((u_num, u_data, proj.project_name))
                if alts:
                    break

            await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                           f"Ya habibi — Unit {unit} not available right now 😔\n"
                           f"Sold or reserved wallah\n\nBut check these 👇" if alts else
                           f"Ya habibi — Unit {unit} not found 😔 Contact {contact} 🙏")
            for u_num, u_data, p_name in alts[:2]:
                await _send_wa(agency.wa_instance_id, agency.wa_token, chat_id,
                               format_unit_card(u_num, u_data, p_name))


# ─── Property search ──────────────────────────────────────────────────────────

async def _respond_search(chat_id: str, keywords: list, projects: list, agency: Agency):
    contact = agency.umar_contact or "@support"
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
                       f"No matches found habibi 😅 Specify project, floor or room count — or contact {contact} 🙏")


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
