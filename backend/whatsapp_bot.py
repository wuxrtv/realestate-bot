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

def _dubai_today() -> str:
    from datetime import datetime, timezone, timedelta
    return datetime.now(timezone(timedelta(hours=4))).strftime("%Y-%m-%d")


def _load_group_history(db, agency_id: int, chat_id: str):
    from datetime import datetime as _dt
    conv = db.query(GroupConversation).filter(
        GroupConversation.agency_id == agency_id,
        GroupConversation.chat_id == chat_id,
    ).first()
    today = _dubai_today()
    if not conv:
        conv = GroupConversation(agency_id=agency_id, chat_id=chat_id, history=[], conversation_date=today)
        db.add(conv)
        db.flush()
    elif conv.conversation_date != today:
        logger.info(f"GroupConv: new day ({today}), resetting history for chat {chat_id}")
        conv.history = []
        conv.conversation_date = today
        db.commit()
    return conv, list(conv.history or [])


def _save_group_history(db, conv, history: list):
    from datetime import datetime as _dt
    conv.history = history  # full day history, no artificial cut
    conv.conversation_date = _dubai_today()
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
  "intent": "unit_query" | "media_request" | "property_search" | "direct_question" | "off_topic",
  "unit_numbers": ["1507", "1435"],
  "project_name": "project name or null",
  "keywords": ["2 rooms", "villa", "floor 20"],
  "reply": "your reply (only for direct_question, empty string otherwise)"
}

━━━ WHEN TO RESPOND ━━━
✅ Respond (use correct intent) when:
• Someone mentions "Tony" or "Тони" — always respond
• Someone asks about units, prices, availability — even WITHOUT mentioning Tony
• Someone asks for brochure, video, photos — even WITHOUT mentioning Tony
• Question is clearly real-estate related

❌ Use "off_topic" intent when:
• People are having personal conversations
• Topics completely unrelated to real estate
• You don't have accurate data to answer
• When in doubt — use off_topic, do NOT respond

━━━ CONTEXT INTELLIGENCE ━━━
ALWAYS read the FULL conversation history before responding.

→ If a specific project was discussed in the last 10-15 messages → use it as project_name automatically
→ NEVER ask "which project?" if the project is already clear from context
→ If project unclear AND not mentioned anywhere → use direct_question intent, ask once:
   "Habibi which project? 😊\nWe have:\n• [list from available projects below]"
→ Ask MAXIMUM ONCE — if you already asked above and got an answer, use that answer
→ If you asked and got NO answer yet — still use off_topic, do not repeat the question

Examples:
• History shows "SAAS Hills" being discussed → someone says "send brochure" or "photos" or "video"
  → project_name = "SAAS Hills", intent = media_request ← NO questions asked
• No project in history → "send brochure"
  → intent = direct_question, reply lists available projects
• "send me SAAS Hills brochure" → project_name = "SAAS Hills", intent = media_request ← immediate

━━━ INTENT RULES ━━━
• unit_query: asking for specific unit number ("unit 1507", "show 1435", "2301 bormi")
• media_request: asking for ANY media — brochure, PDF, photos, renders, video, tour, presentation ("фото", "photo", "brochure", "видео", "video", "renders", "tour", "ролик", "брошюра")
  → Tony sends ALL files from project's media folder in order: Brochure → Payment Plan → Photos → Video
  → NEVER send video before brochure — order is fixed
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

LANGUAGE: Always reply in English only — no exceptions.
You understand all languages (Russian, Uzbek, Arabic, any) but always respond in English.
Arabic flavor words (habibi, wallah, yalla, khalas) are personality — not language switching.
Never ask about language preference."""

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
    try:
        from drive_service import is_inventory_filename
        if is_inventory_filename(fname):
            return True
    except Exception:
        pass
    return any(kw in cl or kw in fl for kw in _WA_INVENTORY_KEYWORDS)


def _wa_is_sales_offer(fname: str) -> bool:
    """Return True if file matches sales offer naming pattern (e.g. SH_A311_40.60_1B.pdf)."""
    try:
        from drive_service import parse_offer_filename
        return parse_offer_filename(fname) is not None
    except Exception:
        return False


logger = logging.getLogger(__name__)

_BOT_NAMES = re.compile(r"\bтони\b|\btoni\b|\btony\b", re.IGNORECASE)
_REALESTATE_TRIGGERS = re.compile(
    r"\b(unit|юнит|brochure|брошюр|floor\s*plan|планировк|price\s*list|прайс|"
    r"bedroom|спальн|villa|вилла|available|наличи|"
    r"видео|video\s*tour|фото|render|renders|"
    r"presentation|презентац|каталог|catalog|"
    r"apartment|апартамент|availability|pdf)\b"
    r"|\b\d{3,5}\b",
    re.IGNORECASE,
)
_WA_BASE = "https://api.green-api.com"


# ─── Low-level Green API helpers ─────────────────────────────────────────────

def _wa_url(instance_id: str, token: str, method: str) -> str:
    return f"{_WA_BASE}/waInstance{instance_id}/{method}/{token}"


async def _send_wa(chat_id: str, text: str) -> bool:
    instance_id = os.getenv("WA_INSTANCE_ID", "")
    token = os.getenv("WA_TOKEN", "")
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


def _is_realestate_query(text: str) -> bool:
    return bool(_REALESTATE_TRIGGERS.search(text))


async def _transcribe_audio(audio_bytes: bytes) -> str:
    """Transcribe voice message using Groq Whisper. Returns empty string on failure."""
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        logger.warning("_transcribe_audio: GROQ_API_KEY not set — voice messages disabled")
        return ""
    try:
        import io
        from groq import AsyncGroq
        client = AsyncGroq(api_key=api_key)
        # Groq expects a file-like object with a name attribute
        buf = io.BytesIO(audio_bytes)
        buf.name = "voice.ogg"
        result = await client.audio.transcriptions.create(
            model="whisper-large-v3",
            file=buf,
            response_format="text",
        )
        return (result or "").strip()
    except Exception:
        logger.exception("_transcribe_audio error")
        return ""


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

    _AUDIO_TYPES = {"audioMessage", "pttMessage"}  # ptt = push-to-talk (voice note)
    if msg_type not in ("textMessage", "documentMessage", "imageMessage", "videoMessage", *_AUDIO_TYPES):
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
        # ── File messages (admin private chat) ──────────────────────────────────
        if msg_type in ("documentMessage", "imageMessage", "videoMessage") and not is_group and admin_check:
            file_data = message_data.get("fileMessageData", {})
            download_url = file_data.get("downloadUrl", "")
            file_name = file_data.get("fileName", f"file.{'jpg' if msg_type == 'imageMessage' else 'mp4' if msg_type == 'videoMessage' else 'bin'}")
            caption = file_data.get("caption", "").strip()
            if download_url:
                mark_admin_active(agency.id)
                await _handle_admin_document(chat_id, sender_phone, download_url, file_name, caption, db, agency)

        # ── Voice / audio messages ───────────────────────────────────────────────
        elif msg_type in ("audioMessage", "pttMessage"):
            file_data = message_data.get("fileMessageData", {})
            download_url = file_data.get("downloadUrl", "")
            if not download_url:
                return
            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    resp = await client.get(download_url)
                    audio_bytes = resp.content
            except Exception:
                logger.exception("WA voice download error")
                return
            text = await _transcribe_audio(audio_bytes)
            if not text:
                logger.info("WA voice: transcription empty — skipping")
                return
            logger.info(f"WA voice transcribed ({len(text)} chars): {text[:80]!r}")
            if not is_group and admin_check:
                mark_admin_active(agency.id)
                await _handle_admin_message(chat_id, sender_phone, f"[Voice] {text}", db, agency)
            elif is_group and (_is_tony_mentioned(text) or _is_realestate_query(text)):
                group_title = sender_data.get("chatName", chat_id)
                await _handle_group_message(chat_id, group_title, sender_name, text, db, agency)

        # ── Text messages ────────────────────────────────────────────────────────
        elif msg_type == "textMessage":
            text: str = message_data.get("textMessageData", {}).get("textMessage", "").strip()
            logger.info(f"WA from={sender_wid} chat={chat_id} text={text[:50]!r}")
            if not text:
                return
            if not is_group and admin_check:
                mark_admin_active(agency.id)
                await _handle_admin_message(chat_id, sender_phone, text, db, agency)
            elif is_group and (_is_tony_mentioned(text) or _is_realestate_query(text)):
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
    await _send_wa(chat_id, msg)


# ─── WhatsApp scheduled jobs ──────────────────────────────────────────────────

async def send_wa_morning_greeting():
    """08:00 — morning greeting to WA admins."""
    from datetime import datetime as _dt
    is_friday = _dt.now().weekday() == 4  # 4 = Friday
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not os.getenv("WA_INSTANCE_ID"):
                continue
            pool = _MORNING_GREETINGS_FRIDAY if is_friday else _MORNING_GREETINGS
            greeting = random.choice(pool)
            for phone in (agency.wa_admin_numbers or []):
                await _send_wa(f"{phone}@c.us", greeting)
    finally:
        db.close()


async def send_wa_morning_followup():
    """08:45 — follow up once if WA admin hasn't replied."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not os.getenv("WA_INSTANCE_ID"):
                continue
            state = _day_state(agency.id)
            if state["morning_replied"] or state["follow_up_sent"]:
                continue
            state["follow_up_sent"] = True
            msg = random.choice(_FOLLOWUP_MSGS)
            for phone in (agency.wa_admin_numbers or []):
                await _send_wa(f"{phone}@c.us", msg)
    finally:
        db.close()


async def send_wa_midday_checkin():
    """14:00 — midday check-in if admin hasn't been active today."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not os.getenv("WA_INSTANCE_ID"):
                continue
            if _day_state(agency.id)["morning_replied"]:
                continue
            msg = random.choice(_MIDDAY_MSGS)
            for phone in (agency.wa_admin_numbers or []):
                await _send_wa(f"{phone}@c.us", msg)
    finally:
        db.close()


_DAILY_INVENTORY_INTROS = [
    "Yalla habibi! 🔥 Today's picks from the inventory 👇",
    "Morning! Check these units — fresh from the list 👇",
    "Wallah good morning! Here's what I picked for today 👇",
    "Habibi! Top 3 units for today 🏢 Let's go 👇",
    "Good morning team! Today's featured units 💼👇",
    "Yalla let's move! Today's inventory highlights 🔥👇",
    "Morning habibi! I handpicked these for today 👀👇",
]


async def send_wa_daily_inventory():
    """10:00 — send 3 auto-selected units to each active WhatsApp group."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not os.getenv("WA_INSTANCE_ID"):
                continue

            # Collect units from DB
            projects = db.query(ToniProject).filter(
                ToniProject.is_active == True,
                ToniProject.agency_id == agency.id,
            ).all()

            all_units: list[tuple[str, dict, str]] = []
            for proj in projects:
                for unit_num, unit_data in (proj.unit_index or {}).items():
                    all_units.append((unit_num, unit_data, proj.project_name))

            # Also collect from Drive inventory (Excel) + sales offer PDFs
            import drive_service as _drive
            svc = _drive.get_service()
            root_id = getattr(agency, "drive_root_id", "") or ""
            if svc:
                seen_units = {u[0] for u in all_units}
                for proj in projects:
                    try:
                        drive_idx = _drive.get_project_inventory(svc, proj.project_name, root_id)
                        for unit_num, unit_data in drive_idx.items():
                            if unit_num not in seen_units:
                                all_units.append((unit_num, unit_data, proj.project_name))
                                seen_units.add(unit_num)
                    except Exception:
                        pass
                try:
                    offers = _drive.scan_sales_offers(svc, root_id)
                    for unit_key, offer_data in offers.items():
                        if unit_key not in seen_units:
                            all_units.append((unit_key, offer_data, offer_data.get("project_name", "Project")))
                            seen_units.add(unit_key)
                except Exception:
                    pass

            if not all_units:
                logger.info(f"Daily inventory: no units for agency {agency.id} — skipping")
                continue

            groups = db.query(WhatsAppGroup).filter(
                WhatsAppGroup.active == True,
                WhatsAppGroup.agency_id == agency.id,
            ).all()

            for i, group in enumerate(groups):
                if i > 0:
                    await asyncio.sleep(30)

                picks = random.sample(all_units, min(3, len(all_units)))
                intro = random.choice(_DAILY_INVENTORY_INTROS)
                await _send_wa(group.chat_id, intro)
                await asyncio.sleep(2)

                for unit_num, unit_data, proj_name in picks:
                    card = format_unit_card(unit_num, unit_data, proj_name)
                    await _send_wa(group.chat_id, card)
                    await asyncio.sleep(3)

            logger.info(f"Daily inventory sent: {len(groups)} groups, {len(all_units)} total units available")
    except Exception:
        logger.exception("send_wa_daily_inventory error")
    finally:
        db.close()


# ─── Admin document upload ───────────────────────────────────────────────────

_WA_SAVE_RE = re.compile(
    r"\b(save|сохрани|брошюра|brochure|inventory|инвентарь|прайс|price.?list|"
    r"payment.?plan|добавь|добавить|база|database|это.?файл|загрузи)\b",
    re.IGNORECASE,
)

_FORWARDABLE_EXTS = (
    ".pdf", ".jpg", ".jpeg", ".png", ".webp", ".heic", ".gif",
    ".mp4", ".mov", ".avi", ".mkv", ".webm",
    ".doc", ".docx", ".ppt", ".pptx",
)
_INVENTORY_EXTS = (".xlsx", ".xls", ".csv")


async def _handle_admin_document(chat_id: str, sender_phone: str, download_url: str,
                                 file_name: str, caption: str, db: Session, agency: Agency):
    import re as _re
    from datetime import datetime as _dt
    from excel_parser import (build_unit_index, diff_unit_indexes, format_diff_report,
                              normalize_project_name, parse_csv, parse_excel, parse_pdf)

    fname_lower = file_name.lower()
    has_save_intent = bool(_WA_SAVE_RE.search(caption))

    # ── INSTANT FORWARD: any file without explicit save instruction ──────────
    # Excel/CSV always go to inventory. Everything else (photo, video, PDF, doc)
    # is forwarded to all groups unless admin says "save" / "brochure" etc.
    is_inventory_ext = fname_lower.endswith(_INVENTORY_EXTS)
    is_forwardable   = fname_lower.endswith(_FORWARDABLE_EXTS)

    if not is_inventory_ext and not is_forwardable:
        await _send_wa(chat_id, "❓ Файл не распознан.")
        return

    # Sales offer PDFs (SH_A311_40.60_1B.pdf) and inventory PDFs → never forward
    is_sales_offer = _wa_is_sales_offer(file_name)
    is_inventory_file = _wa_is_inventory(file_name, caption)

    if not is_inventory_ext and not has_save_intent and not is_sales_offer and not is_inventory_file:
        # Download and instantly forward to all groups
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.get(download_url)
                file_bytes = resp.content
        except Exception:
            logger.exception("WA instant forward download error")
            await _send_wa(chat_id, "❌ Не удалось скачать файл.")
            return
        n = await announce_file_to_wa_groups(db, file_bytes, file_name, "", agency)
        await _send_wa(chat_id, f"Khalas habibi! ✅\nForwarded to {n} groups 💪")
        return

    # Sales offer PDF detected — tell admin it's recognized, not forwarded
    if is_sales_offer and not has_save_intent:
        try:
            from drive_service import parse_offer_filename
            parsed = parse_offer_filename(file_name) or {}
        except Exception:
            parsed = {}
        unit_info = ""
        if parsed:
            unit_info = (f"\nProject: {parsed.get('project_name', parsed.get('project_code', '?'))} "
                         f"| Building {parsed.get('building', '?')} | Unit {parsed.get('unit_number', '?')} "
                         f"| Floor {parsed.get('floor', '?')} | {parsed.get('unit_type', '?')} "
                         f"| {parsed.get('payment_plan', '?')}")
        await _send_wa(chat_id,
                       f"📋 Sales offer detected: *{file_name}*{unit_info}\n"
                       "Upload to Drive → project's 'sales office' folder. "
                       "Tony reads it automatically 🔥")
        return

    # ── SAVE PATH: Excel/CSV, or forwardable file with explicit save intent ──
    # PDFs with save intent but no unit data → brochure, tell admin to put in Drive
    if fname_lower.endswith(".pdf") and has_save_intent and not is_inventory_file:
        await _send_wa(chat_id,
                       f"📄 PDF «{file_name}» — брошюра.\n"
                       "Загрузи файл в Google Drive в папку проекта, чтобы агенты могли его получить.")
        return

    await _send_wa(chat_id, f"📊 Читаю *{file_name}*...")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(download_url)
            file_bytes = resp.content
    except Exception:
        logger.exception("WA file download error")
        await _send_wa(chat_id, "❌ Не удалось скачать файл.")
        return

    try:
        if fname_lower.endswith(".csv"):
            sheets_data = parse_csv(file_bytes)
        elif fname_lower.endswith(".pdf"):
            sheets_data = parse_pdf(file_bytes)
        else:
            sheets_data = parse_excel(file_bytes)
    except Exception as e:
        await _send_wa(chat_id, f"❌ Ошибка чтения файла: {e}")
        return

    if not sheets_data:
        await _send_wa(chat_id, "❌ Файл пустой или без данных.")
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
        await _send_wa(chat_id, "❌ Юниты не найдены в файле. Проверь формат таблицы.")
        return

    import drive_service as _drive
    _drive.clear_cache()

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

    await _send_wa(chat_id, msg)


# ─── Admin private message ────────────────────────────────────────────────────

async def _handle_admin_message(chat_id: str, sender_phone: str, text: str,
                                db: Session, agency: Agency):
    from admin_agent import AdminAgent
    from models import AdminConversation

    if text.strip().lower() in ("/reset", "reset", "/start"):
        conv = db.query(AdminConversation).filter(
            AdminConversation.agency_id == agency.id,
            AdminConversation.user_id == f"wa_{sender_phone}",
        ).first()
        if conv:
            conv.history = []
            db.commit()
        await _send_wa(chat_id,
                       "Khalas habibi — memory cleared! Fresh start 🔄🔥")
        return

    agent = AdminAgent()
    try:
        reply = await agent.process(agency, f"wa_{sender_phone}", text, db, chat_id=chat_id)
        if reply and reply.strip():
            await _send_wa(chat_id, reply)
    except Exception:
        logger.exception("WA admin agent error")
        await _send_wa(chat_id,
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
        await _send_wa(chat_id,
                       f"Ya habibi, something went wrong 😅 Contact {contact} 🙏")
        return

    intent = parsed.get("intent", "off_topic")
    unit_numbers: list = parsed.get("unit_numbers") or []
    project_name: str = parsed.get("project_name") or ""
    keywords: list = parsed.get("keywords") or []

    if intent == "unit_query":
        if unit_numbers:
            await _respond_unit(chat_id, unit_numbers, projects, agency, project_name)
        else:
            await _send_wa(chat_id,
                           f"Ya habibi, which unit number? 😅 Contact {contact} 🙏")
    elif intent == "property_search":
        if project_name and project_name not in keywords:
            keywords = [project_name] + keywords
        await _respond_search(chat_id, keywords, projects, agency)
    elif intent == "media_request":
        import drive_service as _drive
        svc = _drive.get_service()
        sent = False
        root_id = getattr(agency, "drive_root_id", "") or ""
        search_name = project_name or (keywords[0] if keywords else "")
        if svc and search_name:
            media_files = _drive.find_all_media(svc, search_name, limit=15, agency_root_id=root_id)
            if media_files:
                await _send_wa(chat_id, f"Yalla habibi — {search_name} media incoming 📸🎬👇")
                for file_id, file_name, export_mime in media_files:
                    file_bytes = _drive.download_file(svc, file_id, export_mime)
                    if file_bytes:
                        await _send_wa_file(chat_id, file_bytes, file_name)
                sent = True
        if not sent:
            # Tell group to wait, then notify admin privately
            await _send_wa(chat_id, "Give me a sec habibi 🙏")
            admin_numbers = getattr(agency, "wa_admin_numbers", []) or []
            if admin_numbers:
                admin_chat_id = f"{admin_numbers[0]}@c.us"
                await _send_wa(
                    admin_chat_id,
                    f"Habibi, media for *{search_name}* not found in Drive 🙏\n"
                    f"Can you send it? I'll forward to the groups khalas 🔥"
                )
    elif intent == "direct_question":
        reply = (parsed.get("reply") or "").strip()
        if reply:
            await _send_wa(chat_id, reply)

    history.append({"role": "assistant", "content": raw})
    _save_group_history(db, conv, history)


# ─── Send file via WhatsApp ───────────────────────────────────────────────────

async def _send_wa_file(chat_id: str,
                        file_bytes: bytes, file_name: str, caption: str = "") -> bool:
    """Send file to WhatsApp via Green API sendFileByUpload."""
    instance_id = os.getenv("WA_INSTANCE_ID", "")
    token = os.getenv("WA_TOKEN", "")
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

async def _respond_unit(chat_id: str, unit_numbers: list, projects: list, agency: Agency,
                        hint_project: str = ""):
    import drive_service as _drive
    svc = _drive.get_service()
    contact = agency.umar_contact or "@support"
    root_id = getattr(agency, "drive_root_id", "") or ""

    for unit in unit_numbers[:3]:
        found = False

        # 1. Search DB inventory
        for proj in projects:
            idx: dict = proj.unit_index or {}
            if unit in idx:
                found = True
                card = format_unit_card(unit, idx[unit], proj.project_name)

                # Try to send unit PDF from Drive if available
                if svc:
                    drive_result = _drive.find_unit_file(svc, proj.project_name, unit, root_id)
                    if drive_result:
                        file_id, file_name = drive_result
                        file_bytes = _drive.download_file(svc, file_id)
                        if file_bytes:
                            await _send_wa(chat_id,
                                           f"Wallah good choice habibi! 👀\nHere's everything about Unit {unit} 👇")
                            await _send_wa_file(chat_id, file_bytes, file_name, card)
                            break

                await _send_wa(chat_id, f"Wallah good choice habibi! 👀\n{card}")
                break

        # 2. Fallback: read inventory Excel from Drive
        if not found and svc:
            proj_names = [p.project_name for p in projects]
            if hint_project and hint_project not in proj_names:
                proj_names.insert(0, hint_project)
            for p_name in proj_names:
                drive_idx = _drive.get_project_inventory(svc, p_name, root_id)
                if unit in drive_idx:
                    found = True
                    card = format_unit_card(unit, drive_idx[unit], p_name)
                    await _send_wa(chat_id, f"Wallah good choice habibi! 👀\n{card}")
                    break

        # 3. Fallback: scan sales offer PDFs (SH_A311_40.60_1B.pdf)
        if not found and svc:
            try:
                offers = _drive.scan_sales_offers(svc, root_id)
                # Try exact match and "BUILDING+UNIT" match (e.g. "A311" for unit "311")
                offer_data = offers.get(unit)
                if not offer_data:
                    for key, val in offers.items():
                        if val.get("unit_number") == unit:
                            offer_data = val
                            break
                if offer_data:
                    enriched = _drive.enrich_offer_from_pdf(svc, offer_data)
                    proj_name = enriched.get("project_name", "Project")
                    card = format_unit_card(unit, enriched, proj_name)
                    found = True
                    # Also try to send the PDF file
                    file_id = enriched.get("file_id", "")
                    if file_id:
                        file_bytes = _drive.download_file(svc, file_id)
                        if file_bytes:
                            await _send_wa(chat_id, f"Wallah good choice habibi! 👀\n{card}")
                            await _send_wa_file(chat_id, file_bytes, enriched.get("filename", "offer.pdf"), "")
                    else:
                        await _send_wa(chat_id, f"Wallah good choice habibi! 👀\n{card}")
            except Exception:
                logger.exception("_respond_unit: scan_sales_offers failed")

        if not found:
            alts = []
            for proj in projects:
                idx = proj.unit_index or {}
                for u_num, u_data in list(idx.items())[:3]:
                    if u_num != unit:
                        alts.append((u_num, u_data, proj.project_name))
                if alts:
                    break

            await _send_wa(chat_id,
                           f"Ya habibi — Unit {unit} not available right now 😔\n"
                           f"Sold or reserved wallah\n\nBut check these 👇" if alts else
                           f"Ya habibi — Unit {unit} not found 😔 Contact {contact} 🙏")
            for u_num, u_data, p_name in alts[:2]:
                await _send_wa(chat_id, format_unit_card(u_num, u_data, p_name))


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
            await _send_wa(chat_id,
                           format_unit_card(unit_num, data, proj_name))
    else:
        await _send_wa(chat_id,
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
        await _send_wa(g.chat_id, message)
    return len(groups)


async def announce_file_to_wa_groups(db: Session, file_bytes: bytes, file_name: str,
                                     caption: str, agency: Agency) -> int:
    """Send a file to all active WhatsApp groups."""
    groups = db.query(WhatsAppGroup).filter(
        WhatsAppGroup.active == True,
        WhatsAppGroup.agency_id == agency.id,
    ).all()
    sent = 0
    for i, g in enumerate(groups):
        if i > 0:
            await asyncio.sleep(30)
        ok = await _send_wa_file(g.chat_id, file_bytes, file_name, caption)
        if ok:
            sent += 1
    logger.info(f"announce_file_to_wa_groups: sent to {sent}/{len(groups)} groups")
    return sent
