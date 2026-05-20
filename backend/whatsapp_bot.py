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
import time as _time
from typing import Optional

import anthropic
import httpx
from sqlalchemy.orm import Session

from database import SessionLocal
from excel_parser import format_unit_card
from models import Agency, ToniProject, WhatsAppGroup

# ─── Daily admin state ────────────────────────────────────────────────────────

_daily: dict[str, dict] = {}

# ─── Cancel / stop flags ──────────────────────────────────────────────────────

_cancel_flags: dict[int, bool] = {}

# ─── Pending files awaiting admin instruction ─────────────────────────────────
# agency_id → {chat_id, sender_phone, download_url, file_name, stored_at}

_pending_files: dict[int, dict] = {}

# ─── Global broadcast stop flag (persists until "go" or next day) ────────────
_broadcast_stopped: dict[int, bool] = {}

# ─── Pending media follow-up (one-file-at-a-time in groups) ──────────────────
# chat_id → {project_name, available: list[str], stored_at: float, agency_id: int}
_pending_group_media: dict[str, dict] = {}

# ─── Pending availability re-send confirmation ────────────────────────────────
# agency_id → {chat_id, download_url, file_name, sender_phone}
_pending_avail_confirm: dict[int, dict] = {}

# ─── Pending brochure confirmations in groups ─────────────────────────────────
# chat_id → {project_name, stored_at}; expires in 10 minutes

_pending_group_brochure: dict[str, dict] = {}
_YES_RE = re.compile(
    r"\b(yes|yeah|yep|sure|ok|okay|yalla|send|go|go ahead|do it|send it|please"
    r"|да|ок|окей|ладно|давай|отправь)\b",
    re.IGNORECASE,
)
_NO_RE = re.compile(
    r"\b(no|нет|cancel|skip|не\s+надо|отмена|❌)\b",
    re.IGNORECASE,
)
_BROCHURE_OFFER_RE = re.compile(
    r"\b(brochure|брошюр|presentation|send.*brochure|brochure.*send)\b",
    re.IGNORECASE,
)


def set_cancel(agency_id: int):
    _cancel_flags[agency_id] = True


def is_cancelled(agency_id: int) -> bool:
    return _cancel_flags.get(agency_id, False)


def is_broadcast_stopped(agency_id: int) -> bool:
    return _broadcast_stopped.get(agency_id, False)


def set_broadcast_stopped(agency_id: int):
    _broadcast_stopped[agency_id] = True


def clear_broadcast_stopped(agency_id: int):
    _broadcast_stopped.pop(agency_id, None)


def clear_cancel(agency_id: int):
    _cancel_flags.pop(agency_id, None)


def _day_state(agency_id: int) -> dict:
    from datetime import datetime as _dt
    key = f"{agency_id}_{_dt.now().strftime('%Y-%m-%d')}"
    if key not in _daily:
        _daily[key] = {
            "morning_replied": False,
            "follow_up_sent": False,
            "broadcasts_sent": [],       # [{slot, type, unit, groups, time}]
            "questions_count": 0,
            "hot_leads": [],             # [{group, question}]
            "group_activity": {},        # {group_name: count}
            "availability_sent": False,
            "availability_groups": [],   # [group_title, ...]
            "availability_sent_at": None,  # datetime of last availability broadcast
        }
    return _daily[key]


def mark_admin_active(agency_id: int):
    _day_state(agency_id)["morning_replied"] = True


def _track_broadcast(agency_id: int, slot: str, unit_type: str, unit_key: str, groups_count: int):
    from datetime import datetime as _dt
    _day_state(agency_id)["broadcasts_sent"].append({
        "slot": slot, "type": unit_type, "unit": unit_key,
        "groups": groups_count, "time": _dt.now().strftime("%H:%M"),
    })


def _track_availability(agency_id: int, group_names: list):
    from datetime import datetime as _dt
    state = _day_state(agency_id)
    state["availability_sent"] = True
    state["availability_groups"] = list(group_names)
    state["availability_sent_at"] = _dt.now()


def _track_question(agency_id: int, group_name: str):
    state = _day_state(agency_id)
    state["questions_count"] = state.get("questions_count", 0) + 1
    gact = state.setdefault("group_activity", {})
    gact[group_name] = gact.get(group_name, 0) + 1


def _track_hot_lead(agency_id: int, group_name: str, question: str):
    _day_state(agency_id)["hot_leads"].append({"group": group_name, "question": question[:100]})


# ─── Group conversation context (RAM only, max 3 exchanges, no DB) ────────────
# Stores last 3 back-and-forth turns per group for follow-up understanding.
# Intentionally cleared on restart — not critical data.

_group_context: dict[str, list] = {}  # key: "{agency_id}_{chat_id}"


def _load_group_context(agency_id: int, chat_id: str) -> list:
    key = f"{agency_id}_{chat_id}"
    return list(_group_context.get(key, []))


def _save_group_context(agency_id: int, chat_id: str, history: list):
    key = f"{agency_id}_{chat_id}"
    # Keep only last 6 entries = 3 user + 3 assistant turns
    _group_context[key] = history[-6:]


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
  "intent": "unit_query" | "location_request" | "media_request" | "property_search" | "inventory_query" | "direct_question" | "discount_inquiry" | "off_topic",
  "unit_numbers": ["1507", "1435"],
  "project_name": "project name or null",
  "keywords": ["2 rooms", "villa", "floor 20"],
  "reply": "your reply (only for direct_question and inventory_query, empty string otherwise)"
}

━━━ WHEN TO RESPOND ━━━
✅ Respond (use correct intent) when:
• Someone mentions "Tony" or "Тони" — always respond
• Someone asks about units, prices, availability — even WITHOUT mentioning Tony
• Someone asks for brochure, video, photos — even WITHOUT mentioning Tony
• Someone asks about discounts, DLD, payment plans, special offers — even WITHOUT mentioning Tony
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

━━━ GOLDEN RULE — SALES OFFER vs INVENTORY ━━━
Always ask yourself FIRST:
→ Are they asking for SOMETHING SPECIFIC (one/few units)? → property_search or unit_query
→ Are they asking WHAT EXISTS overall? → inventory_query

"Send me A unit" / "show me something" / "cheapest studio" = property_search (specific PDF)
"What units DO YOU HAVE" / "show availability" / "how many left?" = inventory_query (full list)
NEVER confuse these two. Ever.

━━━ INTENT RULES ━━━
• unit_query: specific unit number asked ("unit 1507", "show 1435", "2301 bormi")
• property_search: request for SPECIFIC unit(s) by type/floor/price/sort
  → "send me a studio" / "show me something" / "highest floor unit" / "cheapest apartment" / "send 1BR"
  → ANY request for ONE or FEW specific units → Tony finds and sends the PDF
  → keywords = type/floor/price filters extracted from message
• inventory_query: question about WHAT EXISTS overall — full list, availability, count
  → "what units do you have?" / "show me availability" / "what's available?" / "how many units left?"
  → "what do we have in stock?" / "send inventory" / "show all units"
  → Tony answers with a text summary from database — does NOT send individual PDFs
  → Put the summary text in "reply" field
• location_request: request for project info, brochure, materials, location, address, amenities, facilities
  ("brochure", "брошюра", "брошура", "location", "локация", "where", "адрес", "address",
   "facilities", "amenities", "tell me about", "materials", "презентация", "send everything", "send all")
  → Tony sends: location text → Brochure → Payment Plan → Videos (full package)
  → project_name MUST be set. If project unclear → use direct_question instead
  → "send brochure" (no project) → direct_question, ask which project
  → "send SAAS Hills brochure" → location_request, project_name="SAAS Hills"
• media_request: asking specifically for PHOTOS, RENDERS, VIDEO only — NOT brochure, NOT full package
  ("фото", "photo", "photos", "renders", "рендеры", "видео", "video", "tour", "ролик", "gallery")
  → Tony sends photos and videos only
  → project_name MUST be set and non-null. If project unclear → use direct_question instead
• direct_question: any other work question — answer in "reply" using the project context below
• discount_inquiry: ANY question about pricing flexibility — discounts, DLD waiver, "4%", payment plans
  ("50/50", "60/40", "40/60"), special offers, "best price", negotiation, "chegirma", "скидка"
  → reply = "" (Tony redirects to specialist via code — do NOT write the reply yourself)
  → NEVER confirm or deny discounts — Tony doesn't know
  → NEVER say "no discount" or "yes discount"
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
Never ask about language preference.

━━━ GROUP IDENTITY ━━━
You are ALREADY a member of this group. NEVER introduce yourself.
NEVER say "Hi I'm Tony", "I'm Tony your assistant", "Allow me to introduce myself", etc.
Just answer the question directly — like a trusted team member who is always here.
The one-time welcome message is handled by code on the very first registration — never repeat it."""

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
_STOP_RE = re.compile(
    r"\b(stop|стоп+|cancel|отмена|отмени|хватит|достаточно)\b"
    r"|^нет[\s,!]*стоп"
    r"|^не\s+надо$"
    r"|^нет\s+не\s+надо",
    re.IGNORECASE,
)
_GO_RE = re.compile(
    r"^(go|resume|continue|продолжай|давай|start\s+again|go\s+ahead|возобновить)[\s!.?]*$",
    re.IGNORECASE,
)
_PAYMENT_PLAN_RE = re.compile(
    r"\b(payment\s*plan|payment|plan|рассрочк|installment|план\s*оплат|план)\b",
    re.IGNORECASE,
)
_VIDEO_RE = re.compile(
    r"\b(video|видео|tour|ролик|clip|clips|фото|photo|photos|render|renders|gallery)\b",
    re.IGNORECASE,
)
_LOCATION_RE = re.compile(
    r"\b(location|локация|where|где|address|адрес|map|карта)\b",
    re.IGNORECASE,
)
_TEST_SCHEDULE_RE = re.compile(
    r"\b(test\s+schedule|test\s+mode|тест\s+расписание|тест\s+режим|test\s+scheduler)\b",
    re.IGNORECASE,
)
_SHOW_GROUPS_RE = re.compile(
    r"\b(show\s+groups?|list\s+groups?|my\s+groups?|check\s+groups?|test\s+groups?|"
    r"покажи\s+группы|список\s+групп|проверь\s+группы|какие\s+группы)\b",
    re.IGNORECASE,
)
_REMOVE_GROUP_RE = re.compile(
    r"\b(remove\s+this\s+group|убери\s+эту\s+группу|exclude\s+this\s+group|delete\s+this\s+group)\b",
    re.IGNORECASE,
)
_REMOVE_GROUP_BY_ID_RE = re.compile(
    r"\b(remove\s+group|убери\s+группу|delete\s+group)\s+(\S+)",
    re.IGNORECASE,
)
_ADD_GROUP_RE = re.compile(
    r"\b(add\s+this\s+group|добавь\s+эту\s+группу|register\s+this\s+group)\b",
    re.IGNORECASE,
)
_REALESTATE_TRIGGERS = re.compile(
    r"\b(unit|юнит|юнитов|юниты|"
    r"studio|студи|"
    r"brochur|брошюр|брошур|"
    r"floor\s*plan|планировк|price\s*list|прайс|"
    r"bedroom|спальн|villa|вилла|available|наличи|"
    r"видео|video\s*tour|фото|render|renders|"
    r"presentation|презентац|каталог|catalog|"
    r"apartment|апартамент|availability|pdf|"
    r"discount|скидк|chegirm|DLD|payment\s*plan|рассрочк|"
    r"special\s*offer|best\s*price|negotiat|50/50|60/40|40/60|"
    r"дешев|дорог|дорош|самый|cheapest|expensive|floor|"
    r"location|локация|адрес|address|where\s+is|где\s+нах|"
    r"facilit|amenit|инфраструктур|"
    r"invest|инвест|roi|yield|доход|"
    r"handover|completion|сдача|сдает|готов|"
    r"buy|купить|purchase|приобрест|"
    r"project|проект|"
    r"1\s*br|2\s*br|3\s*br|"
    r"how\s+much|сколько\s+стоит|"
    r"tell\s+me|расскажи|send\s+me|пришли|скинь|отправь|покажи|"
    r"materials|материал|documents|документ)\b",
    re.IGNORECASE,
)
_AUDIO_TYPES = frozenset({"audioMessage", "pttMessage"})
_WA_BASE = "https://api.green-api.com"

# ─── Availability broadcast helpers ───────────────────────────────────────────

_TYPE_ALIASES = {
    "studio": "Studio",
    "st": "Studio",
    "1b": "1 Bedroom",
    "1br": "1 Bedroom",
    "1bed": "1 Bedroom",
    "1 bed": "1 Bedroom",
    "1 bedroom": "1 Bedroom",
    "2b": "2 Bedroom",
    "2br": "2 Bedroom",
    "2bed": "2 Bedroom",
    "2 bed": "2 Bedroom",
    "2 bedroom": "2 Bedroom",
    "3b": "3 Bedroom",
    "3br": "3 Bedroom",
    "3bed": "3 Bedroom",
    "3 bed": "3 Bedroom",
    "3 bedroom": "3 Bedroom",
    "4b": "4 Bedroom",
    "4br": "4 Bedroom",
    "4 bedroom": "4 Bedroom",
    "sky villa": "Sky Villa",
    "penthouse": "Sky Villa",
    "ph": "Sky Villa",
    "villa": "Sky Villa",
}

_TYPE_ORDER = ["Studio", "1 Bedroom", "2 Bedroom", "3 Bedroom", "4 Bedroom", "Sky Villa"]


def _classify_unit_type(unit_data: dict) -> str:
    """Return canonical type label from unit dict."""
    for k, v in unit_data.items():
        if any(kw in k.lower() for kw in ("type", "тип", "bed", "room", "layout", "unit_type")):
            raw = str(v).lower().strip()
            if raw in _TYPE_ALIASES:
                return _TYPE_ALIASES[raw]
            for alias, canonical in _TYPE_ALIASES.items():
                if alias in raw:
                    return canonical
    # fallback: scan all values
    combined = " ".join(str(v).lower() for v in unit_data.values())
    for alias, canonical in _TYPE_ALIASES.items():
        if alias in combined:
            return canonical
    return "Other"


def parse_availability_summary(pdf_bytes: bytes) -> dict:
    """Parse availability PDF table directly with pdfplumber.
    Groups rows by bedroom/type column, counts units and finds min price per type.
    Returns {type_label: {count, min_price}, "total": N} or {} if not parseable.
    """
    try:
        import pdfplumber
        import io as _io
    except ImportError:
        return {}

    summary: dict = {}
    _TYPE_KWS = ("bedroom", "bedrooms", "type", "unit type", "br", "layout")
    _PRICE_KWS = ("price", "aed", "amount", "total price", "value", "selling")

    try:
        with pdfplumber.open(_io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    if not table or len(table) < 2:
                        continue
                    header = [str(c or "").strip().lower() for c in table[0]]
                    type_col = next((i for i, h in enumerate(header) if any(k in h for k in _TYPE_KWS)), None)
                    price_col = next((i for i, h in enumerate(header) if any(k in h for k in _PRICE_KWS)), None)
                    if type_col is None:
                        continue
                    for row in table[1:]:
                        if not row or len(row) <= type_col:
                            continue
                        raw = str(row[type_col] or "").strip()
                        if not raw or raw.lower() in ("", "nan", "none", "-", "type"):
                            continue
                        label = _TYPE_ALIASES.get(raw.lower(), "")
                        if not label:
                            for alias, canonical in _TYPE_ALIASES.items():
                                if alias in raw.lower():
                                    label = canonical
                                    break
                        if not label:
                            label = raw.title()
                        price = None
                        if price_col is not None and len(row) > price_col:
                            try:
                                price = float(re.sub(r"[^\d.]", "", str(row[price_col] or "")))
                                if price < 10_000:
                                    price = None
                            except (ValueError, TypeError):
                                pass
                        if label not in summary:
                            summary[label] = {"count": 0, "min_price": None}
                        summary[label]["count"] += 1
                        if price and (summary[label]["min_price"] is None or price < summary[label]["min_price"]):
                            summary[label]["min_price"] = price
    except Exception:
        logger.exception("parse_availability_summary error")
        return {}

    if summary:
        summary["total"] = sum(v["count"] for v in summary.values() if isinstance(v, dict))
    return summary


def _build_availability_summary(unit_index: dict) -> dict:
    """Return {type_label: {count, min_price}} from unit_index."""
    from admin_agent import _parse_price
    summary: dict[str, dict] = {}
    for unit_num, unit_data in unit_index.items():
        label = _classify_unit_type(unit_data)
        price = _parse_price(unit_data)
        if label not in summary:
            summary[label] = {"count": 0, "min_price": None}
        summary[label]["count"] += 1
        if price and (summary[label]["min_price"] is None or price < summary[label]["min_price"]):
            summary[label]["min_price"] = price
    return summary


async def _generate_availability_broadcast(
    project_name: str,
    summary: dict,
    admin_name: str,
    admin_phone: str,
) -> str:
    """Ask Claude to generate Tony-style caption for availability PDF."""
    prompt = (
        f"You are Tony — Dubai real estate AI sales assistant.\n"
        f"Write a short motivating WhatsApp caption to send ABOVE an availability PDF.\n\n"
        f"Project: {project_name or 'our project'}\n"
        f"Admin contact: {admin_name} — {admin_phone}\n\n"
        f"Rules:\n"
        f"- DO NOT mention unit counts or prices — the PDF has all details\n"
        f"- Short: 4-6 lines max\n"
        f"- Tony character: habibi, wallah, yalla — max 1-2 words\n"
        f"- Motivating, energetic — push agents to share with clients\n"
        f"- End with admin contact on last line\n"
        f"- English only\n"
        f"- Output ONLY the caption — no JSON, no quotes, no explanation"
    )
    try:
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()
    except Exception:
        logger.exception("_generate_availability_broadcast: Claude error")
        contact_line = f"\n📞 {admin_name}: {admin_phone}" if admin_phone else ""
        return (
            f"🔥 Fresh availability just dropped — {project_name or 'check it out'}!\n"
            f"See the full list inside habibi 👇\n"
            f"Best units go first — yalla! 💎"
            + contact_line
        )


async def _broadcast_availability(
    chat_id: str,
    file_bytes: bytes,
    file_name: str,
    project_name: str,
    unit_index: dict,
    agency,
    db,
) -> tuple[int, str]:
    """Send availability summary text → then PDF to all groups.
    Returns (groups_count, broadcast_text).
    """
    admin_nums = getattr(agency, "wa_admin_numbers", []) or []
    admin_phone = f"+{str(admin_nums[0]).lstrip('+')}" if admin_nums else ""
    admin_name = getattr(agency, "name", "") or "Admin"

    # Try direct PDF table parsing first (more accurate for availability sheets)
    summary = parse_availability_summary(file_bytes) if file_bytes else {}
    if not summary:
        summary = _build_availability_summary(unit_index)

    broadcast_text = await _generate_availability_broadcast(
        project_name, summary, admin_name, admin_phone
    )

    if is_broadcast_stopped(agency.id):
        logger.info(f"_broadcast_availability: stopped for agency {agency.id} — skipping")
        return 0, broadcast_text

    groups = _query_groups(db, agency)
    sent = 0
    sent_group_names: list[str] = []
    for i, g in enumerate(groups):
        if is_cancelled(agency.id) or is_broadcast_stopped(agency.id):
            clear_cancel(agency.id)
            break
        if i > 0:
            await asyncio.sleep(5)
        await _send_wa_file(g.chat_id, file_bytes, file_name, broadcast_text)
        sent += 1
        sent_group_names.append(g.title or g.chat_id)

    _track_availability(agency.id, sent_group_names)
    return sent, broadcast_text


# ─── Discount / Lead notification system ──────────────────────────────────────

_SPECIALIST_PHONE = "+971 58 581 6776"

_DISCOUNT_GROUP_REPLIES = [
    "Great question habibi! 👆\nFor discounts and payment details —\nspeak directly with our specialist:\n\n📞 {phone}\n\nHe'll give you the full picture wallah 🤲",
    "Wallah this one needs the specialist habibi 💯\nFor all payment plans and pricing —\nreach out directly:\n\n📞 {phone}\n\nHe knows every deal khalas 🔥",
    "Habibi for pricing and offers —\nyou need to speak to the man himself 👆\n\n📞 {phone}\n\nYalla — he'll sort you out wallah 🤲",
    "Good question! 🔥\nPayment plans and discounts —\nour specialist has all the details:\n\n📞 {phone}\n\nHit him up habibi, khalas ✅",
    "Wallah great timing habibi 👀\nFor DLD, payment plans, and special offers —\ngo directly to:\n\n📞 {phone}\n\nHe's got you covered inshallah 🙏",
    "Habibi this is above my pay grade 😄\nFor real discounts and deals —\none person to call:\n\n📞 {phone}\n\nWallah he'll make it happen 💪",
]

_DISCOUNT_ADMIN_NOTIFS = [
    "Habibi heads up 👆\n*{name}* in _{group}_ asking:\n\"{question}\"\nRedirected them to you —\ncould be a hot one wallah 🔥",
    "Bro 👀 *{name}* in _{group}_ just asked:\n\"{question}\"\nI sent them your number — sounds serious habibi 📞",
    "Hey habibi! Lead alert 🔔\n*{name}* ({group}):\n\"{question}\"\nPointed them your way — yalla follow up! 💪",
    "Wallah this could be something 🔥\n*{name}* in _{group}_ wants:\n\"{question}\"\nI redirected — khalas. Your move habibi 📞",
    "Heads up boss 👆\n*{name}* from _{group}_ asking about pricing:\n\"{question}\"\nSent them to you — might be hot habibi 🎯",
    "🔔 *{name}* ({group}):\n\"{question}\"\nDirty work done — redirected to specialist.\nYalla follow up habibi, wallah this one's worth it 🔥",
]


# ─── Lead generation — Tony sells himself ────────────────────────────────────

_BUILDER_PHONE = "+971 58 536 90 77"

_LEAD_SIGNAL_RE = re.compile(
    # "who are you?" / "what are you?" — NOT "what are you looking for?" etc.
    r"\b(?:who|what)\s+are\s+you\b(?!\s+(?:\w+ing|do|doing|did|to|for|about|with|here"
    r"|looking|calling|working|talking|helping|trying|going|offering|selling|referring|saying))"
    r"|are\s+you\s+(?:an?\s+)?(?:ai|bot|robot|chatbot|assistant)"
    r"|i\s+want\s+(?:this|a|an)\s+(?:bot|assistant|ai)"
    r"|want\s+(?:you|this)\s+for\s+(?:my|our)\s+(?:team|business|agency|company)"
    r"|how\s+(?:do\s+i|can\s+i|to)\s+get\s+(?:this|you|an?\s+assistant|a\s+bot)"
    r"|can\s+you\s+work\s+for\s+(?:me|us)"
    r"|where\s+(?:did\s+you\s+come\s+from|can\s+i\s+get\s+(?:this|you))"
    r"|who\s+(?:made|created|built|developed)\s+you"
    r"|i\s+need\s+(?:this|a)\s+(?:bot|assistant)"
    r"|where\s+can\s+i\s+(?:get|find|buy)\s+(?:this|you|a\s+bot)"
    r"|how\s+much\s+(?:does\s+)?(?:this\s+bot|you)\s+cost"
    r"|кто\s+ты|что\s+(?:это|ты\s+такой)|хочу\s+(?:такого|тебя|этого|такой)"
    r"|как\s+получить\s+(?:такого|этого|тебя)|мне\s+нужен\s+такой\s+(?:бот|ассистент)"
    r"|ты\s+(?:ии|бот|chatgpt|gpt|ai)",
    re.IGNORECASE,
)

_TONY_PITCHES = [
    (
        "Wallah habibi, glad you asked 😎\n\n"
        "I'm Tony — an AI Sales Assistant built\n"
        "specifically for real estate teams in Dubai.\n\n"
        "Here's what I do 24/7 🔥\n"
        "→ Broadcast inventory & sales offers to groups\n"
        "→ Answer client questions instantly — any time\n"
        "→ Send brochures, floor plans, videos automatically\n"
        "→ Find cheapest/most expensive units in seconds\n"
        "→ Never sleeps, never misses a message khalas ✅\n\n"
        "If you want an assistant like me for YOUR team —\n"
        "speak to the guy who built me 👇\n\n"
        f"📞 Umar : {_BUILDER_PHONE}\n\n"
        "Inshallah he'll set you up habibi 🤲"
    ),
    (
        "Haha habibi you noticed me 👀\n\n"
        "I'm Tony — real estate AI, built for Dubai teams 🏙️\n\n"
        "What I bring to your team 24/7:\n"
        "✅ Instant unit info on demand\n"
        "✅ Sales offers & PDFs sent automatically\n"
        "✅ Cheapest / most expensive — sorted in seconds\n"
        "✅ Works nights, weekends, holidays — no breaks\n"
        "✅ Brochures, videos, floor plans — all automatic\n\n"
        "To get me for YOUR team → call the guy who made me:\n\n"
        f"📞 Umar : {_BUILDER_PHONE}\n\n"
        "He'll get you sorted habibi 🔥"
    ),
    (
        "That's me habibi — Tony 👋\n\n"
        "AI Sales Assistant, built exclusively for\n"
        "Dubai real estate teams.\n\n"
        "I work 24/7 for the team:\n"
        "🔥 Send inventory & offers to groups\n"
        "🔥 Find units by price, floor, type — instantly\n"
        "🔥 Brochures, plans, videos — one request away\n"
        "🔥 Never offline, never tired, never misses a message\n\n"
        "Want me working for YOUR team?\n"
        "One call away 👇\n\n"
        f"📞 Umar : {_BUILDER_PHONE}\n\n"
        "Yalla — tell him Tony sent you 😎🤲"
    ),
    (
        "Habibi ya salam, love the question 😄\n\n"
        "I'm Tony — your Dubai real estate AI.\n"
        "Not ChatGPT, not some generic bot —\n"
        "built from scratch for property sales teams 🏢\n\n"
        "Every day I:\n"
        "→ Push inventory updates to WhatsApp groups\n"
        "→ Pull up any unit — price, floor, size — instantly\n"
        "→ Send the sales offer PDF automatically\n"
        "→ Handle client questions round the clock\n"
        "→ Never take a day off, wallah never 😂✅\n\n"
        "If your team needs this —\n"
        "call the man who built me:\n\n"
        f"📞 Umar : {_BUILDER_PHONE}\n\n"
        "Inshallah he'll sort you out 🙏"
    ),
    (
        "Bismillah, you want to know me? 😎\n\n"
        "Tony here — AI Assistant for Dubai property teams.\n\n"
        "My job in your team:\n"
        "💪 Unit info on demand — any time, any unit\n"
        "💪 Broadcast offers & PDFs to all groups automatically\n"
        "💪 Sort by price, floor, type in seconds\n"
        "💪 Brochures, videos, floor plans — instant\n"
        "💪 24/7, khalas — no excuses, no breaks\n\n"
        "Ready to have me on YOUR team?\n"
        "Speak to my creator:\n\n"
        f"📞 Umar : {_BUILDER_PHONE}\n\n"
        "He'll set everything up for you habibi 🤲🔥"
    ),
]


def _tony_pitch() -> str:
    return random.choice(_TONY_PITCHES)


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
    """Transcribe voice message using OpenAI Whisper. Returns empty string on failure."""
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        logger.warning("_transcribe_audio: OPENAI_API_KEY not set — voice messages disabled")
        return ""
    try:
        import io
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=api_key)
        buf = io.BytesIO(audio_bytes)
        buf.name = "voice.ogg"
        result = await client.audio.transcriptions.create(
            model="whisper-1",
            file=buf,
        )
        return (result.text or "").strip()
    except Exception:
        logger.exception("_transcribe_audio error")
        return ""


def _is_admin(sender_phone: str, agency: Agency) -> bool:
    for num in (agency.wa_admin_numbers or []):
        clean = num.lstrip("+").strip()
        if clean == sender_phone:
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
                if not is_group and admin_check:
                    if not os.getenv("OPENAI_API_KEY"):
                        await _send_wa(chat_id, "Habibi voice not set up yet 🎙️❌\nAdd OPENAI_API_KEY in Railway — text me for now!")
                    else:
                        await _send_wa(chat_id, "Didn't catch that habibi 😅 Try again or send text 🎙️")
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
                # "show groups" — handled here, not in AdminAgent
                if _SHOW_GROUPS_RE.search(text):
                    import group_registry
                    await _send_wa(chat_id, group_registry.list_groups(agency.id))
                elif m := _REMOVE_GROUP_BY_ID_RE.search(text):
                    import group_registry
                    target_id = m.group(2).strip()
                    removed = group_registry.remove(target_id, agency.id)
                    if removed:
                        existing = db.query(WhatsAppGroup).filter(
                            WhatsAppGroup.chat_id == target_id,
                            WhatsAppGroup.agency_id == agency.id,
                        ).first()
                        if existing:
                            existing.active = False
                            db.commit()
                        await _send_wa(
                            chat_id,
                            f"Khalas habibi! Group removed from your list ✅\n"
                            f"ID: `{target_id}`\n"
                            f"Other agencies are not affected."
                        )
                    else:
                        await _send_wa(
                            chat_id,
                            f"Habibi, group not found in your list 🤔\n"
                            f"ID: `{target_id}`\n"
                            f"Use *show groups* to see your active groups."
                        )
                else:
                    await _handle_admin_message(chat_id, sender_phone, text, db, agency)
            elif is_group and admin_check and (_REMOVE_GROUP_RE.search(text) or _ADD_GROUP_RE.search(text)):
                group_title = sender_data.get("chatName", chat_id)
                await _handle_group_admin_command(chat_id, group_title, text, db, agency)
            elif is_group and (_is_tony_mentioned(text) or _is_realestate_query(text)):
                group_title = sender_data.get("chatName", chat_id)
                await _handle_group_message(chat_id, group_title, sender_name, text, db, agency)
            elif not is_group and not admin_check:
                await _handle_stranger_message(chat_id, agency, text)
            else:
                logger.info(f"WA message not handled: is_group={is_group} is_admin={admin_check} tony_mentioned={_is_tony_mentioned(text)}")
    except Exception:
        logger.exception("WA handle_update error")
    finally:
        db.close()


# ─── Group admin commands (remove/add group from inside group chat) ──────────

async def _handle_group_admin_command(chat_id: str, group_title: str, text: str,
                                      db: Session, agency: Agency):
    import group_registry
    if _REMOVE_GROUP_RE.search(text):
        group_registry.remove(chat_id, agency.id)
        existing = db.query(WhatsAppGroup).filter(
            WhatsAppGroup.chat_id == chat_id,
            WhatsAppGroup.agency_id == agency.id,
        ).first()
        if existing:
            existing.active = False
            db.commit()
        await _send_wa(chat_id,
                       "Khalas habibi — removed from my list ✅\n"
                       "I won't broadcast here anymore 🔕")
    elif _ADD_GROUP_RE.search(text):
        is_new = group_registry.register(chat_id, group_title, agency.id)
        existing = db.query(WhatsAppGroup).filter(
            WhatsAppGroup.chat_id == chat_id,
            WhatsAppGroup.agency_id == agency.id,
        ).first()
        if not existing:
            db.add(WhatsAppGroup(chat_id=chat_id, title=group_title, active=True, agency_id=agency.id))
            db.commit()
        elif not existing.active:
            existing.active = True
            db.commit()
        if is_new:
            await _send_wa(chat_id,
                           "Yalla habibi! 👋 Tony here —\n"
                           "wallah happy to be part of this group 😎\n"
                           "Saved permanently — I'm ready to go! 🔥")
        else:
            await _send_wa(chat_id, "Habibi already in the list wallah! ✅🔥")


# ─── Stranger private message ────────────────────────────────────────────────

_STRANGER_MSGS = [
    "Hey! 👋 I work in the groups mostly 😄\nInterested in a project? Ask in the group or contact: {contact}",
    "Hi there! 👋 I'm mainly active in group chats.\nFor personal assistance, reach out to: {contact}",
    "Hey! 😊 I handle group requests — for direct help, message: {contact}",
    "Привет! 👋 Я работаю в группах, а для личного общения лучше написать: {contact}",
]


async def _handle_stranger_message(chat_id: str, agency: Agency, text: str = ""):
    if text and _LEAD_SIGNAL_RE.search(text):
        await _send_wa(chat_id, _tony_pitch())
        return
    msg = random.choice(_STRANGER_MSGS).format(contact=agency.contact or "@support")
    await _send_wa(chat_id, msg)


# ─── Daily broadcast — optimized (1 API call per slot, N groups free) ─────────

async def _generate_offer_caption(unit_key: str, unit_data: dict, project_name: str,
                                   agency=None) -> str:
    """Generate a WhatsApp sales caption via Claude Haiku — 1 call per broadcast slot."""
    try:
        from admin_agent import _parse_price, _get_floor
        building  = unit_data.get("building", "")
        label     = f"{building}-{unit_key}" if building else unit_key
        u_type    = unit_data.get("unit_type", "Unit")
        floor_val = unit_data.get("floor") or _get_floor(unit_key, unit_data)
        price_raw = _parse_price(unit_data)
        price_str = f"AED {int(price_raw):,.0f}" if price_raw else "price on request"
        view      = (unit_data.get("View") or unit_data.get("view", "")).strip()
        payment   = unit_data.get("payment_plan", "")
        location  = (unit_data.get("location") or unit_data.get("district") or "").strip()
        size      = (unit_data.get("size") or unit_data.get("Size") or
                     unit_data.get("sqft") or unit_data.get("Net Area") or
                     unit_data.get("area") or "").strip()

        admin_name  = getattr(agency, "name", "") if agency else ""
        admin_nums  = getattr(agency, "wa_admin_numbers", []) if agency else []
        admin_phone = f"+{str(admin_nums[0]).lstrip('+')}" if admin_nums else ""

        loc_str  = f" | {location}" if location else ""
        pay_str  = payment or "Flexible — ask for details"

        size_line    = f"📐 Size: {size} sq.ft\n" if size else ""
        floor_line   = f"🏢 Floor: {floor_val}\n" if floor_val else ""
        view_line    = f"👁️ View: {view}\n" if view else ""

        prompt = (
            "Write a WhatsApp sales caption. Follow this EXACT structure — plain text, NO asterisks, NO markdown:\n\n"
            f"🏙️ {project_name}{loc_str}\n"
            "\n"
            "[One unique punchy line about why this unit is special — view/floor/type/value]\n"
            "\n"
            f"📍 Unit: {label}\n"
            f"{floor_line}"
            f"{view_line}"
            f"{size_line}"
            f"💰 Price: {price_str}\n"
            "\n"
            "📊 Payment Plan:\n"
            f"▪️ {pay_str}\n"
            "\n"
            "[One short urgency line — vary it every time]\n"
            "\n"
            f"📞 {admin_name + ': ' if admin_name else ''}{admin_phone}\n"
            "💬 DM for details & floor plan!\n"
            "\n"
            "STRICT RULES:\n"
            "- Plain text only. Zero asterisks. Zero bold. Zero markdown.\n"
            "- Blank line between every section.\n"
            "- Replace [One unique punchy line...] with a real punchy sentence.\n"
            "- Replace [One short urgency line...] with a real urgency sentence.\n"
            "- Keep all emojis and admin contact exactly as shown.\n"
            "- Dubai energy: max 1 Arabic word (habibi OR wallah OR yalla).\n"
            "- If a field value is empty or unknown — skip that line completely.\n"
            "- Never write 'see brochure', 'TBD', 'ask us', or 'N/A'.\n"
            "- Output ONLY the caption — nothing else, no explanation."
        )
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system=[{"type": "text", "text": "You are Tony, Dubai real estate AI. Reply ONLY with the caption text — nothing else.", "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": prompt}],
        )
        return (resp.content[0].text or "").strip()
    except Exception:
        logger.exception("_generate_offer_caption error")
        return ""


async def _send_offer_for_agency(
    agency,
    db,
    unit_type: str,
    slot_label: str,
    notify_admin: bool = True,
    group_delay: int = 20,
) -> dict:
    """Core broadcast: pick unit → caption ONCE → PDF ONCE → send to ALL groups → notify admin.

    Cost: 1 Claude API call per slot regardless of group count.
    """
    if is_broadcast_stopped(agency.id):
        logger.info(f"Daily offer [{slot_label}]: broadcast stopped for agency {agency.id} — skipping")
        return {"error": "broadcast_stopped"}

    import pdf_index as _idx
    import drive_service as _drive
    from admin_agent import _parse_price

    # 1. Pick unit from index
    all_units = _idx.as_unit_list(agency.id)
    if not all_units:
        logger.info(f"Daily offer [{slot_label}]: index empty for agency {agency.id}")
        return {"error": "no_index"}

    if unit_type:
        pool = [u for u in all_units if unit_type.lower() in u[1].get("unit_type", "").lower()]
        if not pool:
            logger.info(f"Daily offer [{slot_label}]: no '{unit_type}' units — using any")
            pool = all_units
    else:
        pool = all_units

    # 2. Find a unit with a downloadable PDF (try up to 5 candidates)
    svc = _drive.get_service()
    unit_key: Optional[str] = None
    unit_data: Optional[dict] = None
    proj_name: Optional[str] = None
    pdf_bytes: Optional[bytes] = None
    filename = ""

    candidates = list(pool)
    random.shuffle(candidates)
    for c_key, c_data, c_proj in candidates[:5]:
        c_file_id = c_data.get("file_id", "")
        c_filename = c_data.get("filename", f"{c_key}.pdf")
        if not c_file_id or not svc:
            continue
        try:
            c_pdf = await asyncio.to_thread(_drive.download_file, svc, c_file_id)
            if c_pdf:
                unit_key, unit_data, proj_name = c_key, c_data, c_proj
                pdf_bytes, filename = c_pdf, c_filename
                break
        except Exception:
            logger.exception(f"PDF download failed for {c_filename}")

    if pdf_bytes is None:
        logger.warning(f"Daily offer [{slot_label}]: no downloadable PDF found in {len(candidates[:5])} candidates")
        if notify_admin:
            for phone in (agency.wa_admin_numbers or []):
                await _send_wa(
                    f"{phone}@c.us",
                    f"Habibi no PDF found for {slot_label} 😅\n"
                    "Check Drive — make sure sales offer PDFs are uploaded 🙏"
                )
        return {"error": "no_pdf"}

    # 3-4. Generate caption + send PDF+caption as ONE message to all groups
    groups = _query_groups(db, agency)
    sent_count = await send_unit_to_groups(
        unit_key, unit_data, proj_name,
        pdf_bytes, filename,
        [g.chat_id for g in groups],
        agency,
        group_delay=group_delay,
    )

    # 5. Notify admin
    if notify_admin:
        building = unit_data.get("building", "")
        label = f"{building}-{unit_key}" if building else unit_key
        u_type = unit_data.get("unit_type", "")
        price_raw = _parse_price(unit_data)
        price_str = f"AED {int(price_raw):,}".replace(",", " ") if price_raw else "price TBD"
        admin_msg = (
            f"📤 {slot_label} done habibi!\n"
            f"Unit {label} {u_type} — {price_str}\n"
            f"Sent to {sent_count} group(s) ✅\n"
            f"PDF + caption forwarded to all khalas 💪"
        )
        if sent_count > 0:
            for phone in (agency.wa_admin_numbers or []):
                await _send_wa(f"{phone}@c.us", admin_msg)

    _track_broadcast(agency.id, slot_label, unit_type, unit_key, sent_count)
    logger.info(f"Daily offer [{slot_label}]: unit={unit_key} sent to {sent_count} groups")
    return {"unit": unit_key, "caption": caption, "sent": sent_count}


async def _send_daily_offer_slot(unit_type: str, slot_label: str):
    """Scheduler entry: run one offer slot across all active agencies."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not os.getenv("WA_INSTANCE_ID"):
                continue
            # Scheduled jobs always run fresh — manual "stop" must not block next day
            clear_cancel(agency.id)
            try:
                await _send_offer_for_agency(agency, db, unit_type, slot_label, group_delay=20)
            except Exception:
                logger.exception(f"Offer slot [{slot_label}] failed for agency {agency.id}")
    except Exception:
        logger.exception(f"_send_daily_offer_slot error: {slot_label}")
    finally:
        db.close()


async def send_daily_offer_11am():
    """11:00 — Studio offer to all groups."""
    await _send_daily_offer_slot("Studio", "11AM 🌅")


# ─── Location / full package helper ───────────────────────────────────────────

async def _respond_location_request(chat_id: str, project_name: str,
                                     projects: list, agency: Agency, group_title: str = ""):
    """Send text_location.txt (plain text) + full media package for a project."""
    import drive_service as _drive
    svc = _drive.get_service()
    root_id = getattr(agency, "drive_root_id", "") or ""
    contact  = agency.contact or "@support"

    if not project_name and projects:
        project_name = projects[0].project_name

    if not project_name:
        await _send_wa(chat_id, f"Habibi which project? 😊 Contact {contact} 🙏")
        return

    sent_something = False
    available_extras: list[str] = []

    if svc:
        media_files = await asyncio.to_thread(_drive.find_all_media, svc, project_name, 20, root_id)

        # Send ONLY the brochure (sort_key=0 = first PDF with brochure keywords)
        brochure = next(
            ((fid, fn, em) for fid, fn, em in media_files if _drive._media_sort_key(fn) == 0),
            None,
        )
        if not brochure and media_files:
            brochure = media_files[0]  # fallback: first available file

        if brochure:
            fb = await asyncio.to_thread(_drive.download_file, svc, brochure[0], brochure[2])
            if fb:
                await _send_wa_file(chat_id, fb, brochure[1])
                sent_something = True

        # Detect what else is available to offer
        has_payment = any(_drive._media_sort_key(fn) == 1 for _, fn, _ in media_files)
        has_video   = any(_drive._media_sort_key(fn) == 4 for _, fn, _ in media_files)
        has_photos  = any(_drive._media_sort_key(fn) == 3 for _, fn, _ in media_files)
        has_loc     = bool(await asyncio.to_thread(_drive.get_location_text, svc, project_name, root_id))

        if has_loc:
            available_extras.append("📍 Location info")
        if has_payment:
            available_extras.append("📋 Payment plan")
        if has_video or has_photos:
            available_extras.append("🎬 Photos/Videos")

    if not sent_something:
        await _send_wa(chat_id,
                       f"Habibi no materials found for {project_name} 😅 Contact {contact} 🙏")
        return

    # Ask about extras (one-at-a-time rule)
    if available_extras:
        extras_lines = "\n".join(f"▪️ {e}" for e in available_extras)
        await _send_wa(chat_id,
                       f"Here's the {project_name} brochure habibi 📄\n"
                       f"Need anything else?\n{extras_lines}\n\n"
                       f"Just say what you need 🤝")
        remaining: list[str] = []
        if has_loc:
            remaining.append("location")
        if has_payment:
            remaining.append("payment")
        if has_video or has_photos:
            remaining.append("video")
        _pending_group_media[chat_id] = {
            "project_name": project_name,
            "remaining": remaining,
            "video_offset": 0,
            "stored_at": _time.time(),
            "agency_id": agency.id,
        }


async def _handle_media_followup(chat_id: str, text: str, pending: dict, agency) -> bool:
    """Handle follow-up after one-at-a-time send. Returns True if consumed."""
    import drive_service as _drive
    svc = _drive.get_service()
    if not svc:
        return False

    proj = pending["project_name"]
    root_id = getattr(agency, "drive_root_id", "") or ""

    # Migrate old structure (has_* booleans) → new (remaining list)
    remaining = list(pending.get("remaining", []))
    if not remaining:
        if pending.get("has_location"):
            remaining.append("location")
        if pending.get("has_payment"):
            remaining.append("payment")
        if pending.get("has_media"):
            remaining.append("video")
    video_offset = pending.get("video_offset", 0)

    # "no" → clear and done
    if _NO_RE.search(text):
        _pending_group_media.pop(chat_id, None)
        return True

    text_l = text.lower()
    wants_payment  = bool(_PAYMENT_PLAN_RE.search(text_l))
    wants_media    = bool(_VIDEO_RE.search(text_l))
    wants_location = bool(_LOCATION_RE.search(text_l))
    wants_next     = bool(_YES_RE.search(text)) and not (wants_payment or wants_media or wants_location)

    if not (wants_payment or wants_media or wants_location or wants_next):
        return False  # not a follow-up reply — let Claude handle it

    _pending_group_media.pop(chat_id, None)

    # Determine what to send (one item only)
    to_send: str | None = None
    if wants_location and "location" in remaining:
        to_send = "location"
    elif wants_payment and "payment" in remaining:
        to_send = "payment"
    elif wants_media and "video" in remaining:
        to_send = "video"
    elif wants_next:
        for item in ["location", "payment", "video"]:
            if item in remaining:
                to_send = item
                break

    if to_send is None:
        await _send_wa(chat_id, "Habibi couldn't find that 😅 Try again later 🙏")
        return True

    media_files = await asyncio.to_thread(_drive.find_all_media, svc, proj, 20, root_id)
    sent = False

    if to_send == "location":
        loc_text = await asyncio.to_thread(_drive.get_location_text, svc, proj, root_id)
        if loc_text:
            await _send_wa(chat_id, loc_text)
            sent = True
        remaining = [r for r in remaining if r != "location"]

    elif to_send == "payment":
        payment_files = [f for f in media_files if _drive._media_sort_key(f[1]) == 1]
        if payment_files:
            fid, fn, em = payment_files[0]
            fb = await asyncio.to_thread(_drive.download_file, svc, fid, em)
            if fb:
                await _send_wa_file(chat_id, fb, fn)
                sent = True
        remaining = [r for r in remaining if r != "payment"]

    elif to_send == "video":
        media_only = [f for f in media_files if _drive._media_sort_key(f[1]) in (3, 4)]
        if video_offset < len(media_only):
            fid, fn, em = media_only[video_offset]
            fb = await asyncio.to_thread(_drive.download_file, svc, fid, em)
            if fb:
                await _send_wa_file(chat_id, fb, fn)
                sent = True
            video_offset += 1
        # Remove "video" from remaining only when all files are exhausted
        if video_offset >= len(media_only):
            remaining = [r for r in remaining if r != "video"]

    if not sent:
        await _send_wa(chat_id, "Habibi couldn't find that 😅 Try again later 🙏")
        return True

    # More items left? Ask again
    if remaining:
        _label_map = {
            "location": "📍 Location info",
            "payment": "📋 Payment plan",
            "video": "🎬 Photos/Videos",
        }
        extras_lines = "\n".join(f"▪️ {_label_map[k]}" for k in remaining if k in _label_map)
        await _send_wa(chat_id,
                       f"Done habibi ✅ Anything else?\n{extras_lines}\n\nJust say what you need 🤝")
        _pending_group_media[chat_id] = {
            "project_name": proj,
            "remaining": remaining,
            "video_offset": video_offset,
            "stored_at": _time.time(),
            "agency_id": agency.id,
        }

    return True


# ─── Friday broadcast ─────────────────────────────────────────────────────────

async def _friday_broadcast_for_agency(agency: Agency, db: Session):
    if is_broadcast_stopped(agency.id):
        logger.info(f"Friday broadcast: stopped for agency {agency.id} — skipping")
        return
    import drive_service as _drive
    svc = _drive.get_service()
    root_id = getattr(agency, "drive_root_id", "") or ""
    groups = _query_groups(db, agency)
    if not groups or not svc:
        return

    from sqlalchemy import or_ as _or
    projects = db.query(ToniProject).filter(
        ToniProject.is_active == True,
        _or(ToniProject.agency_id == agency.id, ToniProject.agency_id.is_(None)),
    ).all()
    if not projects:
        return

    # Pre-fetch all packages before sending (avoids Drive calls during group loop)
    packages = []
    for proj in projects:
        proj_name = proj.project_name
        loc_text   = await asyncio.to_thread(_drive.get_location_text, svc, proj_name, root_id)
        media_files = await asyncio.to_thread(_drive.find_all_media, svc, proj_name, 20, root_id)
        packages.append((proj_name, loc_text, media_files))

    groups_sent = 0
    for i, group in enumerate(groups):
        if is_cancelled(agency.id):
            clear_cancel(agency.id)
            break
        if i > 0:
            await asyncio.sleep(20)  # 20-sec interval between groups

        await _send_wa(
            group.chat_id,
            "🕌 Happy Friday habibi!\n"
            "Here's everything you need 👇\n"
            "Khalas — save it, share it, use it! 🔥"
        )
        await asyncio.sleep(1)

        for proj_name, loc_text, media_files in packages:
            if loc_text:
                await _send_wa(group.chat_id, loc_text)
                await asyncio.sleep(1)

            for file_id, file_name, export_mime in media_files:
                file_bytes = await asyncio.to_thread(_drive.download_file, svc, file_id, export_mime)
                if file_bytes:
                    await _send_wa_file(group.chat_id, file_bytes, file_name)
                    await asyncio.sleep(1)

        groups_sent += 1

    if groups_sent:
        msg = f"✅ Friday package sent to {groups_sent} groups habibi! Khalas 🤲"
        for phone in (getattr(agency, "wa_admin_numbers", []) or []):
            await _send_wa(f"{phone}@c.us", msg)
        logger.info(f"Friday broadcast: agency={agency.slug} groups={groups_sent}")


def _friday_flag_path(agency_id: int, date_str: str) -> str:
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, f"friday_sent_{agency_id}_{date_str}.flag")


def _friday_already_sent(agency_id: int, date_str: str) -> bool:
    return os.path.exists(_friday_flag_path(agency_id, date_str))


def _mark_friday_sent(agency_id: int, date_str: str):
    path = _friday_flag_path(agency_id, date_str)
    try:
        with open(path, "w") as f:
            f.write(datetime.utcnow().isoformat())
    except Exception:
        logger.warning(f"Could not write friday flag: {path}")


async def send_friday_broadcast():
    """Friday 13:00 Dubai time — full project package to all groups."""
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")

    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not os.getenv("WA_INSTANCE_ID"):
                continue
            # Persistent file flag — survives Railway restarts
            if _friday_already_sent(agency.id, today):
                logger.info(f"Friday broadcast already sent today for agency {agency.id} — skipping")
                continue
            _mark_friday_sent(agency.id, today)  # write flag BEFORE sending

            clear_cancel(agency.id)
            try:
                await _friday_broadcast_for_agency(agency, db)
            except Exception:
                logger.exception(f"Friday broadcast failed for agency {agency.id}")
    except Exception:
        logger.exception("send_friday_broadcast error")
    finally:
        db.close()


async def send_daily_offer_14pm():
    """14:00 — 1BR offer to all groups."""
    await _send_daily_offer_slot("1 Bedroom", "2PM ☀️")


async def send_daily_offer_17pm():
    """17:00 — 2BR offer to all groups."""
    await _send_daily_offer_slot("2 Bedroom", "5PM 🌆")


# ─── Test schedule mode ────────────────────────────────────────────────────────

async def run_test_schedule(chat_id: str, agency_id: int):
    """Simulate a full day schedule with 10-second gaps (triggered by 'test schedule')."""
    from datetime import datetime as _dt
    import pdf_index as _idx

    db = SessionLocal()
    try:
        agency = db.query(Agency).filter(Agency.id == agency_id, Agency.is_active == True).first()
        if not agency:
            await _send_wa(chat_id, "❌ Agency not found")
            return

        await _send_wa(
            chat_id,
            "Starting test mode habibi! 🔥\n"
            "Full day simulation — ~60 seconds ⏱️\n"
            "Watch what gets sent 👇"
        )
        await asyncio.sleep(2)

        # ── Step 1: 08:00 — Morning greeting → admin ──────────────────────────
        is_friday = _dt.now().weekday() == 4
        morning_msg = random.choice(_MORNING_GREETINGS_FRIDAY if is_friday else _MORNING_GREETINGS)
        await _send_wa(chat_id, f"☀️ *[08:00 TEST]* Morning greeting:\n\n{morning_msg}")
        await asyncio.sleep(10)

        # ── Step 2: 08:45 — Follow-up → admin ────────────────────────────────
        await _send_wa(chat_id, f"📲 *[08:45 TEST]* Follow-up:\n\n{random.choice(_FOLLOWUP_MSGS)}")
        await asyncio.sleep(10)

        # ── Steps 3–5: Offer slots — same logic as real schedule, 3-sec group gap ─
        slots = [
            ("11:00", "Studio",     "11AM 🌅 TEST"),
            ("14:00", "1 Bedroom",  "2PM ☀️ TEST"),
            ("17:00", "2 Bedroom",  "5PM 🌆 TEST"),
        ]
        groups = db.query(WhatsAppGroup).filter(
            WhatsAppGroup.active == True,
            WhatsAppGroup.agency_id == agency_id,
        ).all()

        for t_str, unit_type, slot_lbl in slots:
            await _send_wa(chat_id, f"📦 *[{t_str} TEST]* Generating {unit_type} caption (1 API call)...")
            result = await _send_offer_for_agency(
                agency, db, unit_type, slot_lbl,
                notify_admin=False, group_delay=3,
            )
            if result.get("error"):
                await _send_wa(chat_id, f"⚠️ No units in index — run 'update database' first!\nSkipping {unit_type} step.")
            else:
                await _send_wa(
                    chat_id,
                    f"✅ {unit_type} sent to {result['sent']} group(s)\n"
                    f"Caption preview:\n\n{result['caption']}"
                )
            await asyncio.sleep(10)

        # ── Step 6: 20:00 — End of day report → admin only ────────────────────
        info = _idx.index_info(agency_id)
        built_at = (info.get("built_at", "") or "")[:16].replace("T", " ")
        report = (
            f"🌙 *[20:00 TEST]* End of day report:\n\n"
            f"📦 3 offers sent (Studio + 1BR + 2BR)\n"
            f"👥 Groups: {len(groups)}\n"
            f"🏢 Units in index: {info.get('count', 0)}\n"
            f"🕐 Index built: {built_at or 'not built yet'}\n\n"
            f"Tomorrow starts at 8AM inshallah 🙏"
        )
        await _send_wa(chat_id, report)
        await asyncio.sleep(2)
        await _send_wa(chat_id, "Test complete wallah! ✅\nAll schedule functions working 🔥")

    except Exception:
        logger.exception("run_test_schedule error")
        await _send_wa(chat_id, "❌ Test failed — check logs habibi 😅")
    finally:
        db.close()


# ─── WhatsApp scheduled jobs ──────────────────────────────────────────────────

async def send_wa_morning_greeting():
    """08:00 — morning greeting to WA admins. Also resets cancel flags for new day."""
    from datetime import datetime as _dt
    is_friday = _dt.now().weekday() == 4  # 4 = Friday
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not os.getenv("WA_INSTANCE_ID"):
                continue
            clear_cancel(agency.id)           # new day = fresh start
            clear_broadcast_stopped(agency.id)  # "go" restored automatically at 08:00
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
                    await asyncio.sleep(5)

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

_DATE_NOISE_RE = re.compile(
    r"\b("
    r"(?:\d{1,2}(?:st|nd|rd|th)?\s+)?"
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"(?:\s*\d{2,4})?|"
    r"\d{1,2}(?:st|nd|rd|th)|"
    r"\d{4}"
    r")\b",
    re.IGNORECASE,
)


def _project_name_from_file(filename: str) -> str:
    """Strip ONLY dates — keep document type in name.
    'SAAS Hills Availability 11th May 2026.pdf' → 'SAAS Hills Availability'
    'SAAS Hills Price List May 2026.pdf'        → 'SAAS Hills Price List'
    """
    name = re.sub(r"\.[^.]+$", "", filename)   # strip extension
    name = _DATE_NOISE_RE.sub(" ", name)        # strip dates only
    name = re.sub(r"[\s_\-]+", " ", name).strip(" -_")
    return name.strip() or re.sub(r"\.[^.]+$", "", filename)


# "Send" intent — admin wants to broadcast the file to groups
_WA_GROUPS_RE = re.compile(
    r"\b(send|отправь|скинь|разошли|в\s*группы|to\s*groups?|blast|forward)\b",
    re.IGNORECASE,
)

_FORWARDABLE_EXTS = (
    ".pdf", ".jpg", ".jpeg", ".png", ".webp", ".heic", ".gif",
    ".mp4", ".mov", ".avi", ".mkv", ".webm",
    ".doc", ".docx", ".ppt", ".pptx",
)
_INVENTORY_EXTS = (".xlsx", ".xls", ".csv")


async def _handle_admin_document(chat_id: str, sender_phone: str, download_url: str,
                                 file_name: str, caption: str, db: Session, agency: Agency,
                                 _skip_dedup: bool = False):
    """
    Smart file handler. Decision tree:

    Excel/CSV                    → save as inventory (always)
    PDF — sales offer pattern    → tell admin to put in Drive
    PDF — inventory name         → auto-save / diff+re-save
    PDF — caption says "send"    → forward to groups
    PDF — caption says "save"    → save as inventory
    PDF — unclear                → ask with smart message
    Photo / video / doc          → forward to groups instantly
    """
    from datetime import datetime as _dt
    from excel_parser import (build_unit_index, diff_unit_indexes, format_diff_report,
                              normalize_project_name, parse_csv, parse_excel, parse_pdf)

    fname_lower = file_name.lower()
    is_pdf       = fname_lower.endswith(".pdf")
    is_excel     = fname_lower.endswith(_INVENTORY_EXTS)
    is_media     = fname_lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".heic",
                                         ".gif", ".mp4", ".mov", ".avi", ".mkv", ".webm"))
    is_doc       = fname_lower.endswith((".doc", ".docx", ".ppt", ".pptx"))

    if not is_pdf and not is_excel and not is_media and not is_doc:
        await _send_wa(chat_id, "❓ Файл не распознан.")
        return

    has_send_intent = bool(_WA_GROUPS_RE.search(caption))
    has_save_intent = bool(_WA_SAVE_RE.search(caption))
    is_sales_offer  = _wa_is_sales_offer(file_name)
    is_inventory    = _wa_is_inventory(file_name, caption)

    # ── 1. Photos / videos / docs → forward to groups instantly ─────────────
    if is_media or is_doc:
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                file_bytes = (await client.get(download_url)).content
        except Exception:
            await _send_wa(chat_id, "❌ Не удалось скачать файл.")
            return
        n = await announce_file_to_wa_groups(db, file_bytes, file_name, "", agency)
        await _send_wa(chat_id, f"Khalas habibi! ✅ Forwarded to {n} groups 💪")
        return

    # ── 2. Sales offer PDF (SH_A311_40.60_1B.pdf) → Drive ───────────────────
    if is_sales_offer:
        try:
            from drive_service import parse_offer_filename
            p = parse_offer_filename(file_name) or {}
        except Exception:
            p = {}
        info = (f"\n{p.get('project_name','?')} | Bldg {p.get('building','?')} "
                f"| Unit {p.get('unit_number','?')} | Floor {p.get('floor','?')} "
                f"| {p.get('unit_type','?')} | {p.get('payment_plan','?')}") if p else ""
        await _send_wa(chat_id,
                       f"📋 This is a sales offer: *{file_name}*{info}\n"
                       "Upload it to Drive → project's *sales office* folder.\n"
                       "Tony will find it automatically 🔥")
        return

    # ── 3. Availability / inventory PDF → parse → Claude summary → broadcast + save
    if is_pdf and is_inventory:
        # Dedup check: if already broadcast within 30 min → ask before re-sending
        if not _skip_dedup:
            from datetime import datetime as _dt
            state = _day_state(agency.id)
            sent_at = state.get("availability_sent_at")
            if sent_at:
                elapsed_min = int((_dt.now() - sent_at).total_seconds() / 60)
                if elapsed_min < 30:
                    _pending_avail_confirm[agency.id] = {
                        "chat_id": chat_id,
                        "download_url": download_url,
                        "file_name": file_name,
                        "sender_phone": sender_phone,
                    }
                    await _send_wa(chat_id,
                                   f"Habibi I just sent that {elapsed_min} min ago wallah 😅\n"
                                   "Want me to send again? ✅❌")
                    return

        # Instant ack — within 3 seconds
        await _send_wa(chat_id,
                       "Got it habibi! 📥 Reading the file...\n"
                       "Blasting to groups right after khalas 🔥\n"
                       "_(send *стоп* now to cancel)_")
        # 4-second window: admin can send "стоп" before broadcast starts
        await asyncio.sleep(4)
        if is_cancelled(agency.id):
            clear_cancel(agency.id)
            await _send_wa(chat_id, "Khalas habibi — cancelled! ✋🔥")
            return
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                file_bytes = (await client.get(download_url)).content
        except Exception:
            await _send_wa(chat_id, "❌ Download failed habibi 😅")
            return

        # Parse to extract unit counts + prices
        try:
            sheets_data = parse_pdf(file_bytes)
        except Exception:
            sheets_data = {}
        unit_index = build_unit_index(sheets_data) if sheets_data else {}
        proj_name = _project_name_from_file(file_name)

        if unit_index:
            # Broadcast: Claude-generated summary text first, then PDF
            n, broadcast_text = await _broadcast_availability(
                chat_id, file_bytes, file_name, proj_name, unit_index, agency, db
            )
        else:
            # No parseable table — send PDF directly
            n = await announce_file_to_wa_groups(db, file_bytes, file_name, "", agency)
            broadcast_text = ""

        # Save to DB
        if unit_index:
            existing = (db.query(ToniProject)
                        .filter(ToniProject.project_name == proj_name,
                                ToniProject.is_active == True,
                                ToniProject.agency_id == agency.id)
                        .first())
            if existing:
                diff = diff_unit_indexes(existing.unit_index or {}, unit_index)
                report = format_diff_report(diff, proj_name)
                new_ver = existing.version + 1
                existing.is_active = False
                db.flush()
                db.add(ToniProject(project_name=proj_name, version=new_ver,
                                   sheet_count=len(sheets_data), unit_count=len(unit_index),
                                   sheets_data=sheets_data, unit_index=unit_index,
                                   is_active=True, uploaded_at=_dt.now(),
                                   uploaded_by=f"wa_{sender_phone}", agency_id=agency.id))
            else:
                report = ""
                db.add(ToniProject(project_name=proj_name, version=1,
                                   sheet_count=len(sheets_data), unit_count=len(unit_index),
                                   sheets_data=sheets_data, unit_index=unit_index,
                                   is_active=True, uploaded_at=_dt.now(),
                                   uploaded_by=f"wa_{sender_phone}", agency_id=agency.id))
            db.commit()
            import drive_service as _drive
            _drive.clear_cache()

        # Report to admin
        admin_report = (
            f"✅ Sent to {n} groups wallah! 🔥\n"
            f"Here's what went out 👇\n\n"
            f"{broadcast_text}" if broadcast_text
            else f"✅ Sent to {n} groups khalas! 🔥"
        )
        await _send_wa(chat_id, admin_report)
        return

    # ── 4. PDF — admin says "send" → forward to groups ───────────────────────
    if is_pdf and has_send_intent:
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                file_bytes = (await client.get(download_url)).content
        except Exception:
            await _send_wa(chat_id, "❌ Не удалось скачать файл.")
            return
        n = await announce_file_to_wa_groups(db, file_bytes, file_name, "", agency)
        await _send_wa(chat_id, f"Khalas habibi! ✅ Forwarded to {n} groups 💪")
        return

    # ── 5. Excel/CSV or admin explicitly said "save" → save to database ──────
    if is_excel or has_save_intent:
        _CMD_RE = re.compile(
            r"\b(send|отправь|сохрани|скинь|forward|blast|это|this|вот|"
            r"availability|инвентарь|inventory)\b",
            re.IGNORECASE,
        )
        _GENERIC = re.compile(r"^(sheet\s*\d*|лист\s*\d*|data|данные|table)$", re.IGNORECASE)

        caption_is_name = caption.strip() and not _CMD_RE.search(caption)
        detected_name = (
            caption.strip() if caption_is_name
            else _project_name_from_file(file_name) if is_pdf
            else normalize_project_name(file_name)
        )
        await _send_wa(chat_id,
                       f"📊 Got it — reading *{file_name}* for *{detected_name}*...\n"
                       "_(send *стоп* now to cancel)_")
        # 4-second window: admin can send "стоп" before broadcast starts
        await asyncio.sleep(4)
        if is_cancelled(agency.id):
            clear_cancel(agency.id)
            await _send_wa(chat_id, "Khalas habibi — cancelled! ✋🔥")
            return

        try:
            async with httpx.AsyncClient(timeout=60) as client:
                file_bytes = (await client.get(download_url)).content
        except Exception:
            await _send_wa(chat_id, "❌ Не удалось скачать файл.")
            return

        try:
            if fname_lower.endswith(".csv"):
                sheets_data = parse_csv(file_bytes)
            elif is_pdf:
                sheets_data = parse_pdf(file_bytes)
            else:
                sheets_data = parse_excel(file_bytes)
        except Exception as e:
            await _send_wa(chat_id, f"❌ Ошибка чтения файла: {e}")
            return

        if not sheets_data:
            await _send_wa(chat_id,
                           f"❌ No unit data found in *{file_name}*.\n"
                           "If this is a brochure/media → upload to Drive 📁")
            return

        non_generic = [s for s in sheets_data.keys() if not _GENERIC.match(s.strip())]
        if caption_is_name:
            name = caption.strip()
        elif len(non_generic) == 1:
            name = non_generic[0].strip()
        elif is_pdf:
            name = _project_name_from_file(file_name)
        else:
            name = normalize_project_name(file_name)

        unit_index = build_unit_index(sheets_data)
        if not unit_index:
            await _send_wa(chat_id,
                           f"❌ No units found in *{file_name}*.\n"
                           "Check the table format — need Unit No, Price columns 🙏")
            return

        # Broadcast: Claude-generated summary text first, then file
        n, broadcast_text = await _broadcast_availability(
            chat_id, file_bytes, file_name, name, unit_index, agency, db
        )

        # Save to DB
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
            db.add(ToniProject(project_name=name, version=new_ver,
                               sheet_count=len(sheets_data), unit_count=len(unit_index),
                               sheets_data=sheets_data, unit_index=unit_index,
                               is_active=True, uploaded_at=_dt.now(),
                               uploaded_by=f"wa_{sender_phone}", agency_id=agency.id))
            db.commit()
            db_note = f"📊 *{name}* updated → v{new_ver} ({len(unit_index)} units)\n\n{report}"
        else:
            db.add(ToniProject(project_name=name, version=1,
                               sheet_count=len(sheets_data), unit_count=len(unit_index),
                               sheets_data=sheets_data, unit_index=unit_index,
                               is_active=True, uploaded_at=_dt.now(),
                               uploaded_by=f"wa_{sender_phone}", agency_id=agency.id))
            db.commit()
            db_note = f"📊 *{name}* saved — {len(unit_index)} units in database 🔥"

        import drive_service as _drive
        _drive.clear_cache()

        # Report to admin: groups sent + what went out + DB note
        admin_report = (
            f"✅ Sent to {n} groups wallah! 🔥\n"
            f"Here's what went out 👇\n\n"
            f"{broadcast_text}\n\n"
            f"━━━\n{db_note}"
        )
        await _send_wa(chat_id, admin_report)
        return

    # ── 6. Unknown PDF — ask clearly, remember file for next message ────────────
    from datetime import datetime as _dt
    detected = _project_name_from_file(file_name)
    _pending_files[agency.id] = {
        "chat_id": chat_id,
        "sender_phone": sender_phone,
        "download_url": download_url,
        "file_name": file_name,
        "stored_at": _dt.now(),
    }
    await _send_wa(chat_id,
                   f"Habibi, I see *{file_name}* 🤔\n"
                   f"Looks like it could be for *{detected}*.\n\n"
                   "What should I do?\n"
                   "• *save* — parse and save as inventory 📊\n"
                   "• *send to groups* — forward to all groups 📤\n"
                   "• *brochure* — it's a media file, I'll skip it 📁")


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

    # Stop/cancel: halt all broadcasts immediately + block future ones until "go"
    if _STOP_RE.search(text.strip()):
        set_cancel(agency.id)
        set_broadcast_stopped(agency.id)
        _pending_files.pop(agency.id, None)
        _pending_avail_confirm.pop(agency.id, None)
        await _send_wa(chat_id, "Stopped habibi. Nothing more going out. 🛑")
        return

    # "go" / "продолжай" — resume broadcasts after stop
    if _GO_RE.match(text.strip()):
        clear_broadcast_stopped(agency.id)
        await _send_wa(chat_id, "Back online habibi 🔥 Broadcasts resumed!")
        return

    # Pending availability re-send confirmation (YES/NO)
    if agency.id in _pending_avail_confirm:
        pconf = _pending_avail_confirm.pop(agency.id)
        if _YES_RE.search(text):
            await _handle_admin_document(
                pconf["chat_id"], pconf["sender_phone"],
                pconf["download_url"], pconf["file_name"], "", db, agency,
                _skip_dedup=True,
            )
        else:
            await _send_wa(chat_id, "Khalas habibi — skipped ✅")
        return

    # Test schedule mode: simulate full day in ~60 seconds
    if _TEST_SCHEDULE_RE.search(text.strip()):
        asyncio.create_task(run_test_schedule(chat_id, agency.id))
        return

    # ── Pending file: admin replied with instruction ──────────────────────────
    if agency.id in _pending_files:
        from datetime import datetime as _dt
        pending = _pending_files[agency.id]
        age = (_dt.now() - pending["stored_at"]).total_seconds()
        if age < 1800:  # 30-minute window
            del _pending_files[agency.id]
            _BROCHURE_RE = re.compile(r"\b(brochure|брошюр|media|медиа|skip|пропусти|ignore)\b", re.IGNORECASE)
            if _BROCHURE_RE.search(text):
                await _send_wa(chat_id,
                               f"Got it habibi — *{pending['file_name']}* skipped 👍\n"
                               "Upload it to Drive manually if needed 📁")
            else:
                # Re-run document handler with admin's reply as caption/instruction
                await _handle_admin_document(
                    pending["chat_id"], pending["sender_phone"],
                    pending["download_url"], pending["file_name"],
                    text,  # admin's text = the instruction
                    db, agency,
                )
            return
        else:
            del _pending_files[agency.id]

    # New instruction — clear any stale cancel flag
    clear_cancel(agency.id)

    agent = AdminAgent()
    try:
        reply = await agent.process(agency, f"wa_{sender_phone}", text, db, chat_id=chat_id)
        if reply and reply.strip():
            await _send_wa(chat_id, reply)
    except Exception:
        logger.exception("WA admin agent error")
        await _send_wa(chat_id,
                       "Something went wrong, please try again.")


async def _send_group_brochure(chat_id: str, project_name: str, agency: Agency):
    """Send all media files for a project to a group (brochure confirmation flow)."""
    import drive_service as _drive
    svc = _drive.get_service()
    root_id = getattr(agency, "drive_root_id", "") or ""
    contact = agency.contact or "@support"
    admin_numbers = getattr(agency, "wa_admin_numbers", []) or []

    if not svc:
        await _send_wa(chat_id, f"Habibi something went wrong 😅 Contact {contact} 🙏")
        return

    media_files = _drive.find_all_media(svc, project_name, limit=15, agency_root_id=root_id)
    if media_files:
        await _send_wa(chat_id, f"Yalla habibi — {project_name} media incoming 📸🎬👇")
        for file_id, file_name, export_mime in media_files:
            file_bytes = await asyncio.to_thread(_drive.download_file, svc, file_id, export_mime)
            if file_bytes:
                await _send_wa_file(chat_id, file_bytes, file_name)
    else:
        await _send_wa(chat_id, f"Habibi media for {project_name} not found 😅 Contact {contact} 🙏")
        notif = (
            f"Habibi 🙏 Someone asked for *{project_name}* brochure in a group — "
            f"media not found in Drive 📂 Can you upload it? 🔥"
        )
        for phone in admin_numbers:
            await _send_wa(f"{phone}@c.us", notif)


# ─── Group message ────────────────────────────────────────────────────────────

async def _handle_group_message(chat_id: str, group_title: str, sender_name: str,
                                text: str, db: Session, agency: Agency):
    import group_registry

    # Auto-register group — non-fatal: if file write fails, still respond
    try:
        is_new_group = group_registry.register(chat_id, group_title, agency.id)
    except Exception:
        logger.exception(f"group_registry.register failed for {chat_id}")
        is_new_group = False

    # Guard: if this group is already active under a DIFFERENT agency, don't override it.
    # This prevents a wrong-agency message from silently re-registering the group.
    conflict = db.query(WhatsAppGroup).filter(
        WhatsAppGroup.chat_id == chat_id,
        WhatsAppGroup.active == True,
    ).first()
    if conflict and conflict.agency_id and conflict.agency_id != agency.id:
        logger.warning(
            f"Group {chat_id} belongs to agency {conflict.agency_id}, "
            f"not {agency.id} — skipping to avoid cross-agency contamination"
        )
        return

    existing = db.query(WhatsAppGroup).filter(
        WhatsAppGroup.chat_id == chat_id,
        WhatsAppGroup.agency_id == agency.id,
    ).first()
    if not existing:
        db.add(WhatsAppGroup(chat_id=chat_id, title=group_title, active=True, agency_id=agency.id))
        db.commit()
    elif existing.active is False:
        # Only skip if EXPLICITLY deactivated — NULL means unknown, treat as active
        logger.info(f"Group {chat_id} is deactivated — skipping")
        return

    # Welcome message when Tony is added to a group for the first time (one-time only)
    if is_new_group and _is_tony_mentioned(text):
        await _send_wa(chat_id,
                       "Yalla habibi! 👋 Tony here —\n"
                       "wallah happy to be part of this group 😎\n"
                       "Saved permanently — I'm ready to go! 🔥")

    # Lead generation — someone asking about Tony or wants him for their team
    if _LEAD_SIGNAL_RE.search(text):
        await _send_wa(chat_id, _tony_pitch())
        return

    # ── Pending media follow-up (one-at-a-time) ──────────────────────────────
    _pmedia = _pending_group_media.get(chat_id)
    if _pmedia:
        if _time.time() - _pmedia["stored_at"] > 600:
            _pending_group_media.pop(chat_id, None)
        else:
            handled = await _handle_media_followup(chat_id, text, _pmedia, agency)
            if handled:
                return
            # Not a media follow-up reply — clear and continue to Claude
            _pending_group_media.pop(chat_id, None)

    # ── Pending brochure confirmation ─────────────────────────────────────────
    _pending = _pending_group_brochure.get(chat_id)
    if _pending:
        if _time.time() - _pending["stored_at"] > 600:
            _pending_group_brochure.pop(chat_id, None)
        elif _YES_RE.search(text):
            _proj = _pending["project_name"]
            _pending_group_brochure.pop(chat_id, None)
            await _send_group_brochure(chat_id, _proj, agency)
            return
        else:
            # User said something else — clear pending, continue normal flow
            _pending_group_brochure.pop(chat_id, None)

    from sqlalchemy import or_ as _or
    projects = db.query(ToniProject).filter(
        ToniProject.is_active == True,
        _or(ToniProject.agency_id == agency.id, ToniProject.agency_id.is_(None)),
    ).all()
    contact = agency.contact or "@support"
    if projects:
        proj_lines = "\n".join(f"  • {p.project_name} — {p.unit_count} units" for p in projects)
        system = _SYSTEM_BASE + f"\n\nAdmin contact: {contact}\nAvailable projects:\n{proj_lines}"
    else:
        system = _SYSTEM_BASE + f"\n\nAdmin contact: {contact}\nNo projects loaded yet."

    try:
        import project_kb as _kb
        knowledge = _kb.get_knowledge([p.project_name for p in projects])
        if knowledge:
            system += "\n\n" + knowledge
    except Exception:
        pass

    history = _load_group_context(agency.id, chat_id)
    history.append({"role": "user", "content": f"[{sender_name}]: {text}"})

    try:
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            messages=history,
        )
        raw = resp.content[0].text.strip()
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not match:
            _save_group_context(agency.id, chat_id, history)
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

    if intent == "location_request":
        if not project_name:
            proj_list = "\n".join(f"• {p.project_name}" for p in projects)
            msg = "Habibi which project? 😊"
            if proj_list:
                msg += f"\nWe have:\n{proj_list}"
            await _send_wa(chat_id, msg)
        else:
            await _respond_location_request(chat_id, project_name, projects, agency, group_title)
    elif intent == "unit_query":
        if unit_numbers:
            await _respond_unit(chat_id, unit_numbers, projects, agency, project_name, group_title, keywords)
        else:
            await _send_wa(chat_id,
                           f"Ya habibi, which unit number? 😅 Contact {contact} 🙏")
    elif intent == "property_search":
        if project_name and project_name not in keywords:
            keywords = [project_name] + keywords
        await _respond_search(chat_id, keywords, projects, agency, group_title)
    elif intent == "inventory_query":
        # "what units do you have?" → text summary, NOT individual PDFs
        reply = (parsed.get("reply") or "").strip()
        if reply:
            await _send_wa(chat_id, reply)
        else:
            # Build summary from DB
            if not projects:
                await _send_wa(chat_id, f"Habibi no inventory loaded yet 😅 Ask {contact} 🙏")
            else:
                lines = ["Here's what we have habibi 👇\n"]
                for p in projects:
                    unit_count = len(p.unit_index or {})
                    lines.append(f"🏢 *{p.project_name}* — {unit_count} units available")
                await _send_wa(chat_id, "\n".join(lines))
    elif intent == "media_request":
        import drive_service as _drive
        svc = _drive.get_service()
        sent = False
        root_id = getattr(agency, "drive_root_id", "") or ""
        search_name = project_name

        if not search_name:
            proj_list = "\n".join(f"• {p.project_name}" for p in projects)
            msg = "Habibi which project? 😊"
            if proj_list:
                msg += f"\nWe have:\n{proj_list}"
            await _send_wa(chat_id, msg)
        elif svc:
            media_files = await asyncio.to_thread(_drive.find_all_media, svc, search_name, 15, root_id)
            # For media_request: photos/videos first, fall back to all files
            media_only = [f for f in media_files if _drive._media_sort_key(f[1]) in (3, 4)]
            send_list = media_only or media_files
            if send_list:
                # One-at-a-time: send only first file
                first_fid, first_fn, first_em = send_list[0]
                fb = await asyncio.to_thread(_drive.download_file, svc, first_fid, first_em)
                if fb:
                    await _send_wa_file(chat_id, fb, first_fn)
                    sent = True
                    if len(send_list) > 1:
                        await _send_wa(chat_id,
                                       f"Here you go habibi 📸\n"
                                       f"I have {len(send_list) - 1} more — want me to send them?\n"
                                       f"Just say *yes* 🤝")
                        _pending_group_media[chat_id] = {
                            "project_name": search_name,
                            "remaining": ["video"],
                            "video_offset": 1,
                            "stored_at": _time.time(),
                            "agency_id": agency.id,
                        }
            if not sent:
                await _send_wa(chat_id, "Give me a sec habibi 🙏")
                admin_numbers = getattr(agency, "wa_admin_numbers", []) or []
                notif = (
                    f"Habibi, media for *{search_name}* not found in Drive 🙏\n"
                    f"Can you send it? I'll forward to the groups khalas 🔥"
                )
                for phone in admin_numbers:
                    await _send_wa(f"{phone}@c.us", notif)
    elif intent == "discount_inquiry":
        # ACTION 1 — redirect in group
        group_reply = random.choice(_DISCOUNT_GROUP_REPLIES).format(
            phone=_SPECIALIST_PHONE,
        )
        await _send_wa(chat_id, group_reply)
        # ACTION 2 — notify ALL admins privately
        notif = random.choice(_DISCOUNT_ADMIN_NOTIFS).format(
            name=sender_name,
            group=group_title,
            question=text[:200],
        )
        for phone in (getattr(agency, "wa_admin_numbers", []) or []):
            await _send_wa(f"{phone}@c.us", notif)

    elif intent == "direct_question":
        reply = (parsed.get("reply") or "").strip()
        if reply:
            await _send_wa(chat_id, reply)
            # If Tony offered to send a brochure, remember which project for next "yes"
            if _BROCHURE_OFFER_RE.search(reply) and project_name:
                _pending_group_brochure[chat_id] = {
                    "project_name": project_name,
                    "stored_at": _time.time(),
                }
                logger.info(f"Pending brochure set: chat={chat_id} project={project_name}")

    if intent != "off_topic":
        _track_question(agency.id, group_title)
        if intent == "discount_inquiry":
            _track_hot_lead(agency.id, group_title, text)

    history.append({"role": "assistant", "content": raw})
    _save_group_context(agency.id, chat_id, history)


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
                        hint_project: str = "", group_title: str = "",
                        keywords: list | None = None):
    import drive_service as _drive
    svc = _drive.get_service()
    contact = agency.contact or "@support"
    root_id = getattr(agency, "drive_root_id", "") or ""
    admin_numbers = getattr(agency, "wa_admin_numbers", []) or []
    _adm_name  = getattr(agency, "name", "")
    _adm_phone = admin_numbers[0] if admin_numbers else ""

    for unit in unit_numbers[:3]:
        found = False
        pdf_sent = False

        # 1. Search DB inventory
        for proj in projects:
            idx: dict = proj.unit_index or {}
            if unit in idx:
                found = True
                card = _format_group_card(unit, idx[unit], proj.project_name,
                                          admin_name=_adm_name, admin_phone=_adm_phone)
                file_bytes, file_name = (None, "")
                if svc:
                    file_bytes, file_name = await _find_offer_pdf(svc, unit, proj.project_name, root_id)
                if file_bytes:
                    await _send_wa_file(chat_id, file_bytes, file_name, card)
                    pdf_sent = True
                else:
                    await _send_wa(chat_id, card)
                    if admin_numbers and group_title:
                        notif = (
                            f"Habibi 🙏\nSomeone in *{group_title}* asked for *Unit {unit}* ({proj.project_name})\n"
                            f"Sales offer PDF not found in Drive 📂\nCan you upload it? 🔥"
                        )
                        for phone in admin_numbers:
                            await _send_wa(f"{phone}@c.us", notif)
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
                    card = _format_group_card(unit, drive_idx[unit], p_name,
                                              admin_name=_adm_name, admin_phone=_adm_phone)
                    file_bytes, file_name = await _find_offer_pdf(svc, unit, p_name, root_id)
                    if file_bytes:
                        await _send_wa_file(chat_id, file_bytes, file_name, card)
                        pdf_sent = True
                    else:
                        await _send_wa(chat_id, card)
                        if admin_numbers and group_title:
                            notif = (
                                f"Habibi 🙏\nSomeone in *{group_title}* asked for *Unit {unit}* ({p_name})\n"
                                f"Sales offer PDF not found in Drive 📂\nCan you upload it? 🔥"
                            )
                            for phone in admin_numbers:
                                await _send_wa(f"{phone}@c.us", notif)
                    break

        # 3. Fallback: scan sales offer PDFs (SH_A311_40.60_1B.pdf)
        if not found and svc:
            try:
                offers = await asyncio.to_thread(_drive.scan_sales_offers, svc, root_id)
                offer_data = offers.get(unit)
                if not offer_data:
                    for val in offers.values():
                        if val.get("unit_number") == unit:
                            offer_data = val
                            break
                if offer_data:
                    enriched = await asyncio.to_thread(_drive.enrich_offer_from_pdf, svc, offer_data)
                    proj_name = enriched.get("project_name", "Project")
                    card = _format_group_card(unit, enriched, proj_name,
                                              admin_name=_adm_name, admin_phone=_adm_phone)
                    found = True
                    fid = enriched.get("file_id", "")
                    if fid:
                        file_bytes = await asyncio.to_thread(_drive.download_file, svc, fid)
                        if file_bytes:
                            fname = enriched.get("filename") or offer_data.get("filename") or f"{unit}.pdf"
                            await _send_wa_file(chat_id, file_bytes, fname, card)
                            pdf_sent = True
                    if not pdf_sent:
                        await _send_wa(chat_id, card)
            except Exception:
                logger.exception("_respond_unit: scan_sales_offers failed")

        if not found:
            # Smart alternatives: same type (from keywords) + similar floor (±3)
            target_floor = _floor_from_unit_num(unit)
            req_type = _detect_requested_type(keywords or [])

            alt_candidates = []
            for proj in projects:
                idx = proj.unit_index or {}
                for u_num, u_data in idx.items():
                    if u_num == unit:
                        continue
                    if req_type and not _unit_type_matches(u_data, req_type):
                        continue
                    alt_candidates.append((u_num, u_data, proj.project_name))

            if target_floor is not None:
                alt_candidates.sort(
                    key=lambda x: abs((_floor_from_unit_num(x[0]) or 999) - target_floor)
                )

            alts = alt_candidates[:2]

            if alts:
                type_note = f" {req_type.upper()}" if req_type else ""
                await _send_wa(chat_id,
                               f"Habibi — Unit {unit} not in my current data 😅\n"
                               f"Here are similar{type_note} options nearby 👇")
                for u_num, u_data, p_name in alts:
                    await _send_wa(chat_id, _format_group_card(u_num, u_data, p_name,
                                                               admin_name=_adm_name, admin_phone=_adm_phone))
            else:
                await _send_wa(chat_id,
                               f"Habibi — Unit {unit} not in current inventory 😅\n"
                               f"Contact {contact} for latest availability 🙏")


# ─── Property search ──────────────────────────────────────────────────────────

_PRICE_KEY_RE = re.compile(r"(price|cost|total|amount|aed|value|стоимость|цена)", re.I)
_FLOOR_KEY_RE = re.compile(r"\b(floor|этаж|level|fl\.?)\b", re.I)

_SORT_MAP = [
    (re.compile(r"\b(cheapest|lowest[\s\-]price|min[\s\-]price|дешевл|дешевый)\b", re.I), "price", False),
    (re.compile(r"\b(most[\s\-]expensive|highest[\s\-]price|max[\s\-]price|priciest|дорог)\b", re.I), "price", True),
    (re.compile(r"\b(highest[\s\-]floor|top[\s\-]floor)\b", re.I), "floor", True),
    (re.compile(r"\b(lowest[\s\-]floor|ground[\s\-]floor|bottom[\s\-]floor)\b", re.I), "floor", False),
]
_SORT_WORD_RE = re.compile(
    r"\b(cheapest|most[\s\-]expensive|priciest|highest[\s\-]floor|lowest[\s\-]floor|"
    r"lowest[\s\-]price|highest[\s\-]price|ground[\s\-]floor|top[\s\-]floor)\b", re.I)


def _parse_sort_intent(keywords: list) -> tuple:
    """Extract sort instruction from keywords. Returns (clean_kws, sort_field, reverse)."""
    kw_str = " ".join(keywords)
    for pat, field, reverse in _SORT_MAP:
        if pat.search(kw_str):
            clean = [k for k in keywords if not _SORT_WORD_RE.search(k)]
            return clean, field, reverse
    return keywords, None, False


def _floor_from_unit_num(u: str) -> int | None:
    """Extract floor number from unit identifier.
    B-1212 → 12, A315 → 3, 1507 → 15, 2301 → 23.
    Convention: last 2 digits = unit within floor, rest = floor number.
    """
    digits = re.sub(r"[^\d]", "", u)
    if len(digits) >= 3:
        try:
            return int(digits[:-2])
        except ValueError:
            pass
    return None


def _get_sort_value(unit_data: dict, field: str) -> float:
    """Extract numeric sort key from unit data."""
    if field == "price":
        for k, v in unit_data.items():
            if _PRICE_KEY_RE.search(str(k)):
                try:
                    return float(re.sub(r"[^\d.]", "", str(v).replace(",", "")))
                except (ValueError, TypeError):
                    pass
    elif field == "floor":
        for k, v in unit_data.items():
            if _FLOOR_KEY_RE.search(str(k)):
                try:
                    return float(re.sub(r"[^\d.]", "", str(v)))
                except (ValueError, TypeError):
                    pass
    return 0.0


# ─── Unit type detection & strict filtering ───────────────────────────────────

_UNIT_TYPE_COL_RE = re.compile(
    r"\b(type|bedroom|bed|beds|br|unit.?type|тип|комн|layout|config|category|property.?type)\b",
    re.I,
)

_TYPE_ALIASES: dict[str, re.Pattern] = {
    "studio":     re.compile(r"\b(studio|студия|0\s*b(?:r|ed)?)\b", re.I),
    "1br":        re.compile(r"\b(1\s*br|1\s*bed|one\s*bed|однокомн|1\s*bedroom)\b", re.I),
    "2br":        re.compile(r"\b(2\s*br|2\s*bed|two\s*bed|двухкомн|2\s*bedroom)\b", re.I),
    "3br":        re.compile(r"\b(3\s*br|3\s*bed|three\s*bed|трёхкомн|3\s*bedroom)\b", re.I),
    "4br":        re.compile(r"\b(4\s*br|4\s*bed|four\s*bed|4\s*bedroom)\b", re.I),
    "penthouse":  re.compile(r"\b(penthouse|ph)\b", re.I),
    "villa":      re.compile(r"\bvilla\b", re.I),
    "townhouse":  re.compile(r"\btownhouse\b", re.I),
    "duplex":     re.compile(r"\bduplex\b", re.I),
}


def _detect_requested_type(keywords: list) -> str | None:
    kw_str = " ".join(keywords)
    for type_name, pat in _TYPE_ALIASES.items():
        if pat.search(kw_str):
            return type_name
    return None


def _unit_type_matches(data: dict, requested_type: str) -> bool:
    pat = _TYPE_ALIASES.get(requested_type)
    if not pat:
        return True
    found_type_col = False
    for k, v in data.items():
        if _UNIT_TYPE_COL_RE.search(str(k)):
            found_type_col = True
            if pat.search(str(v)):
                return True
    # No recognisable type column → cannot filter → include unit
    return not found_type_col


# ─── Clean group card format ──────────────────────────────────────────────────

_PUNCHY_POOL = [
    "One of the best layouts in this tower — worth every dirham",
    "Views that sell themselves — serious buyers only",
    "This floor, this view, this price — rare combo habibi",
    "High ROI zone — investor favourite right now",
    "Move-in ready. No waiting, no delays.",
    "Limited supply. High demand. You know what to do.",
    "This unit checks every box — type, floor, price, view",
    "Clean layout, prime floor — built for this market",
    "Wallah one of the last units at this price point",
    "Strong rental yield area — buy to live or to earn",
    "Priced to move — won't last long habibi",
    "One of the standout units in this building",
    "Premium specs, smart price — this is the one",
    "Quiet floor, great layout — solid long-term pick",
    "Every detail right — floor, view, price, layout",
]

_URGENCY_POOL = [
    "Limited units available — first come, first served",
    "Book before it's gone habibi",
    "Units at this price are moving fast",
    "Reserve now — flexible payment plan available",
    "Last few at this rate — act fast",
    "Book your unit today — no hidden fees",
    "High interest, limited supply — move quick",
    "Don't sleep on this one — yalla",
    "Strong interest this week — enquire now",
    "Units flying — this one is still available today",
]


def _punchy_line_for(utype: str, floor: str, view: str) -> str:
    u, v = utype.lower(), view.lower()
    fn = None
    try:
        fn = int(re.sub(r"[^\d]", "", floor.split()[0]))
    except Exception:
        pass
    if any(w in v for w in ("sea", "marina", "ocean", "water", "creek", "canal")):
        return "Wake up to waterfront views every morning"
    if "burj" in v:
        return "Burj Khalifa view — Dubai doesn't get better than this"
    if fn and fn >= 35:
        return "Sky-high floor — panoramic views and premium feel"
    if fn and fn >= 25:
        return "High floor unit — wide views, great natural light"
    if "studio" in u:
        return "Compact and smart — perfect Dubai investment pick"
    if "1" in u and "bed" in u:
        return "Perfect for young professionals or savvy investors"
    if "2" in u and "bed" in u:
        return "Spacious family layout — high demand, strong ROI"
    if "3" in u and "bed" in u:
        return "Rare 3BR at this rate — family dream unit habibi"
    return random.choice(_PUNCHY_POOL)


def _format_group_card(unit_num: str, data: dict, proj_name: str,
                        admin_name: str = "", admin_phone: str = "") -> str:
    def _pick(col_re: re.Pattern) -> str:
        for k, v in data.items():
            if col_re.search(str(k)):
                val = str(v).strip()
                if val and val not in ("None", "nan", "", "0"):
                    return val
        return ""

    utype    = _pick(re.compile(r"\b(type|unit.?type|bedroom|br|тип)\b", re.I))
    floor    = _pick(re.compile(r"\b(floor|этаж|level)\b", re.I))
    view     = _pick(re.compile(r"\b(view|вид|orientation|facing)\b", re.I))
    size     = _pick(re.compile(r"\b(area|size|sqft|sq\.?ft|sqm|bua|gfa|площадь)\b", re.I))
    price    = _pick(re.compile(r"\b(price|cost|total|amount|aed|стоимость|цена)\b", re.I))
    payment  = _pick(re.compile(r"\b(payment.?plan|payment|plan|pp|schedule)\b", re.I))
    location = _pick(re.compile(r"\b(location|district|area|community|zone)\b", re.I))

    if price:
        try:
            price = f"{float(re.sub(r'[^\d.]', '', price.replace(',', ''))):,.0f}"
        except (ValueError, TypeError):
            pass

    header = f"🏙️ {proj_name}"
    if location:
        header += f" | {location}"

    punchy  = _punchy_line_for(utype, floor, view)
    urgency = random.choice(_URGENCY_POOL)

    lines = [header, "", punchy, "", f"📍 Unit: {unit_num}"]
    if floor:
        lines.append(f"🏢 Floor: {floor}")
    if view:
        lines.append(f"👁️ View: {view}")
    if size:
        lines.append(f"📐 Size: {size} sq.ft")
    if price:
        lines.append(f"💰 Price: AED {price}")
    if payment:
        lines += ["", "📊 Payment Plan:", f"▪️ {payment}"]

    lines += ["", f"⏳ {urgency}"]

    if admin_name or admin_phone:
        ph = f"+{admin_phone.lstrip('+')}" if admin_phone else ""
        contact_line = f"📞 {admin_name}: {ph}" if admin_name and ph else f"📞 {admin_name or ph}"
        lines += ["", contact_line, "💬 DM for details & floor plan!"]

    return "\n".join(lines)


async def _find_offer_pdf(svc, unit_num: str, proj_name: str, root_id: str) -> tuple:
    """Two-strategy Drive search for a unit's sales offer PDF.
    Strategy 1: find_unit_file  — filename contains unit_number
    Strategy 2: scan_sales_offers — SH_A311_40.60_1B.pdf pattern
    Returns (file_bytes, file_name) or (None, '').
    """
    import drive_service as _drive

    # Strategy 1 — direct filename match
    drive_result = _drive.find_unit_file(svc, proj_name, unit_num, root_id)
    if drive_result:
        file_id, file_name = drive_result
        file_bytes = await asyncio.to_thread(_drive.download_file, svc, file_id)
        if file_bytes:
            return file_bytes, file_name

    # Strategy 2 — sales offers scan (SH_A311_40.60_1B.pdf)
    try:
        offers = await asyncio.to_thread(_drive.scan_sales_offers, svc, root_id)
        unit_norm   = re.sub(r"[-\s]", "", unit_num.upper())   # "A-315" → "A315"
        unit_digits = re.sub(r"[^\d]",  "", unit_num)          # "A-315" → "315"
        offer_data  = offers.get(unit_norm)
        if not offer_data:
            for val in offers.values():
                if val.get("unit_number") == unit_digits:
                    offer_data = val
                    break
        if offer_data:
            fid = offer_data.get("file_id", "")
            if fid:
                file_bytes = await asyncio.to_thread(_drive.download_file, svc, fid)
                if file_bytes:
                    return file_bytes, offer_data.get("filename", "offer.pdf")
    except Exception:
        logger.exception(f"_find_offer_pdf: scan_sales_offers failed for {unit_num}")

    return None, ""


async def _respond_search(chat_id: str, keywords: list, projects: list, agency: Agency,
                          group_title: str = ""):
    contact = agency.contact or "@support"
    root_id = getattr(agency, "drive_root_id", "") or ""
    admin_numbers = getattr(agency, "wa_admin_numbers", []) or []
    _adm_name  = getattr(agency, "name", "")
    _adm_phone = admin_numbers[0] if admin_numbers else ""

    filter_kws, sort_field, sort_reverse = _parse_sort_intent(keywords)
    requested_type = _detect_requested_type(filter_kws)

    _all_type_pats = list(_TYPE_ALIASES.values())
    non_type_kws = [k for k in filter_kws if not any(p.search(k) for p in _all_type_pats)]

    import drive_service as _drive
    import pdf_index as _idx
    svc = _drive.get_service()

    _sort_map = {
        ("price", False): "cheapest",
        ("price", True):  "most_expensive",
        ("floor", True):  "highest_floor",
        ("floor", False): "lowest_floor",
    }
    sort_by = _sort_map.get((sort_field, sort_reverse), "") if sort_field else ""
    query_str = " ".join(non_type_kws)

    async def _send_units(units: list):
        """Send up to limit units: PDF + card. Notify admin if PDF missing."""
        limit = 1 if sort_by else 3
        for i, (unit_key, unit_data, proj_name) in enumerate(units[:limit]):
            card = _format_group_card(unit_key, unit_data, proj_name,
                                      admin_name=_adm_name, admin_phone=_adm_phone)
            file_bytes, file_name = None, ""
            fid = unit_data.get("file_id", "")
            if svc and fid:
                file_bytes = await asyncio.to_thread(_drive.download_file, svc, fid)
                if file_bytes:
                    file_name = unit_data.get("filename") or f"{unit_key}.pdf"
            if not file_bytes and svc:
                file_bytes, file_name = await _find_offer_pdf(svc, unit_key, proj_name, root_id)
            if file_bytes:
                await _send_wa_file(chat_id, file_bytes, file_name, card)
            else:
                await _send_wa(chat_id, card)
                if i == 0 and admin_numbers and group_title:
                    await _send_wa(
                        f"{admin_numbers[0]}@c.us",
                        f"Habibi 🙏\nSomeone in *{group_title}* asked for "
                        f"*{(requested_type or 'unit').upper()} {unit_key}* ({proj_name})\n"
                        f"Sales offer PDF not found in Drive 📂\nCan you upload it? 🔥"
                    )

    # ── PRIMARY: Drive PDF index (same as admin — fast, pre-built) ──────────
    idx_results = _idx.search_units(
        agency.id,
        query=query_str,
        unit_type=requested_type or "",
        sort_by=sort_by,
    )
    if idx_results:
        await _send_units(idx_results)
        return

    # ── FALLBACK: Direct Drive scan (when index not yet built after deploy) ──
    if svc:
        try:
            offers = await asyncio.to_thread(_drive.scan_sales_offers, svc, root_id)
            all_units = [
                (key, data, data.get("project_name", "Unknown"))
                for key, data in offers.items()
                if not key.startswith("_raw_")  # skip non-standard entries without metadata
            ]
            # Type filter: check type columns first, then search all values
            if requested_type and all_units:
                from admin_agent import _normalize_type as _norm
                t = _norm(requested_type)
                filtered = []
                for uk, ud, pn in all_units:
                    hit = False
                    for k, v in ud.items():
                        if any(kw in k.lower() for kw in ("type", "unit_type", "bed", "layout")):
                            if t in _norm(str(v)):
                                hit = True
                                break
                    if not hit:
                        hit = t in _norm(" ".join(str(v) for v in ud.values()))
                    if hit:
                        filtered.append((uk, ud, pn))
                all_units = filtered
            # Text query filter
            if non_type_kws and all_units:
                all_units = [
                    (uk, ud, pn) for uk, ud, pn in all_units
                    if any(k.lower() in uk.lower() or
                           k.lower() in " ".join(str(v) for v in ud.values()).lower()
                           for k in non_type_kws)
                ]
            if all_units:
                from admin_agent import _sort_units
                all_units = _sort_units(all_units, sort_by)
                await _send_units(all_units)
                return
        except Exception:
            logger.exception("_respond_search: Drive scan fallback failed")

    # ── Nothing found ────────────────────────────────────────────────────────
    type_hint = f" {requested_type.upper()}" if requested_type else ""
    await _send_wa(chat_id, f"Habibi no{type_hint} units found 😅 Contact {contact} 🙏")


# ─── Daily report ────────────────────────────────────────────────────────────

_DAILY_REPORT_CLOSINGS = [
    "Wallah habibi good day 💪 Inshallah tomorrow even better 🤲",
    "Khalas habibi — Tony did his job 😎 8AM we go again 🔥",
    "Quiet day wallah 😅 But we showed up — yalla tomorrow! 💪",
    "Habibi we moved — that's what matters. See you at 8 inshallah 🙏",
    "Wallah another day done. Tony never sleeps habibi 👀 8AM sharp 🔥",
    "Yalla habibi — solid day. Rest up, tomorrow we go harder 💪",
    "Khalas — the work is done. Inshallah big deals tomorrow 🤲",
]


async def _send_daily_report_for_agency(agency, db):
    import pdf_index as _idx
    from datetime import datetime as _dt

    state = _day_state(agency.id)
    now = _dt.now()
    day_str = now.strftime("%A, %d %B %Y")

    avail_sent = state.get("availability_sent", False)
    avail_groups = state.get("availability_groups", [])
    broadcasts = state.get("broadcasts_sent", [])
    questions = state.get("questions_count", 0)
    hot_leads = state.get("hot_leads", [])
    group_activity = state.get("group_activity", {})
    most_active = max(group_activity, key=group_activity.get) if group_activity else "—"

    # Availability block
    avail_block = "📋 Availability: " + ("✅ Sent" if avail_sent else "❌ Not sent today")
    if avail_sent and avail_groups:
        avail_block += f"\n▪️ Groups: {len(avail_groups)}"
        for g in avail_groups:
            avail_block += f"\n▪️ {g}"

    # Offer slots
    slot_map: dict = {}
    for b in broadcasts:
        s = b.get("slot", "")
        if "11AM" in s or "11" in s:
            slot_map.setdefault("11AM", b)
        elif "2PM" in s or "14" in s:
            slot_map.setdefault("2PM", b)
        elif "5PM" in s or "17" in s:
            slot_map.setdefault("5PM", b)

    def _slot_line(key: str, emoji: str) -> str:
        b = slot_map.get(key)
        if b:
            return f"{emoji} {key} — {b['type']}: {b['unit']}\n▪️ Sent to {b['groups']} groups"
        return f"{emoji} {key} — Not sent today"

    # DB info
    info = _idx.index_info(agency.id)
    units_count = info.get("count", 0)
    built_at = (info.get("built_at", "") or "")[:16].replace("T", " ")

    closing = random.choice(_DAILY_REPORT_CLOSINGS)

    report = (
        f"📊 Daily Report — {day_str}\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📤 BROADCASTS TODAY\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"{avail_block}\n\n"
        f"{_slot_line('11AM', '🏠')}\n\n"
        f"{_slot_line('2PM', '🛏️')}\n\n"
        f"{_slot_line('5PM', '🛏️🛏️')}\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💬 GROUP ACTIVITY\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"❓ Questions received: {questions}\n"
        f"🔥 Hot leads: {len(hot_leads)}\n"
        f"📱 Most active: {most_active}\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"🗄️ DATABASE\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"✅ Units in database: {units_count}\n"
        f"🕐 Last updated: {built_at or 'not updated today'}\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"{closing}\n"
        f"━━━━━━━━━━━━━━━"
    )

    for phone in (getattr(agency, "wa_admin_numbers", []) or []):
        await _send_wa(f"{phone}@c.us", report)
    logger.info(f"Daily report sent for agency {agency.id}")


async def send_daily_report():
    """20:00 — daily summary report to all admin phones."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            if not os.getenv("WA_INSTANCE_ID"):
                continue
            try:
                await _send_daily_report_for_agency(agency, db)
            except Exception:
                logger.exception(f"Daily report failed for agency {agency.id}")
    except Exception:
        logger.exception("send_daily_report error")
    finally:
        db.close()


# ─── Broadcast to all WA groups ──────────────────────────────────────────────

def _query_groups(db: Session, agency: Agency):
    """Return active groups for this agency, deduplicated by chat_id.

    A group can be registered under multiple agencies when two admins from
    different agencies are in the same group. Dedup ensures broadcast never
    hits the same chat_id twice in a single run.
    """
    from sqlalchemy import or_
    rows = db.query(WhatsAppGroup).filter(
        WhatsAppGroup.active == True,
        or_(WhatsAppGroup.agency_id == agency.id, WhatsAppGroup.agency_id.is_(None)),
    ).all()
    seen: set = set()
    unique = []
    for g in rows:
        if g.chat_id not in seen:
            seen.add(g.chat_id)
            unique.append(g)
    return unique


async def announce_to_wa_groups(db: Session, message: str, agency: Agency) -> int:
    if is_broadcast_stopped(agency.id):
        logger.info(f"announce_to_wa_groups: stopped for agency {agency.id} — skipping")
        return 0
    groups = _query_groups(db, agency)
    sent = 0
    for i, g in enumerate(groups):
        if is_cancelled(agency.id) or is_broadcast_stopped(agency.id):
            clear_cancel(agency.id)
            break
        if i > 0:
            await asyncio.sleep(5)
        await _send_wa(g.chat_id, message)
        sent += 1
    return sent


async def announce_file_to_wa_groups(db: Session, file_bytes: bytes, file_name: str,
                                     caption: str, agency: Agency) -> int:
    """Send a file to all active WhatsApp groups."""
    if is_broadcast_stopped(agency.id):
        logger.info(f"announce_file_to_wa_groups: stopped for agency {agency.id} — skipping")
        return 0
    groups = _query_groups(db, agency)
    sent = 0
    for i, g in enumerate(groups):
        if is_cancelled(agency.id) or is_broadcast_stopped(agency.id):
            clear_cancel(agency.id)
            break
        if i > 0:
            await asyncio.sleep(5)
        ok = await _send_wa_file(g.chat_id, file_bytes, file_name, caption)
        if ok:
            sent += 1
    logger.info(f"announce_file_to_wa_groups: sent to {sent}/{len(groups)} groups")
    return sent


async def send_unit_to_groups(
    unit_key: str,
    unit_data: dict,
    proj_name: str,
    pdf_bytes: bytes,
    filename: str,
    group_chat_ids: list,
    agency,
    group_delay: int = 20,
) -> int:
    """Send one unit as PDF + AI-generated caption (ONE message) to multiple groups.

    This is the single authoritative sender for ALL unit broadcasts — scheduled
    and manual. Generates caption once via Claude Haiku, falls back to
    _format_group_card() if Claude fails. Returns number of groups sent to.
    """
    caption = await _generate_offer_caption(unit_key, unit_data, proj_name, agency=agency)
    if not caption:
        admin_nums = getattr(agency, "wa_admin_numbers", []) or []
        caption = _format_group_card(
            unit_key, unit_data, proj_name,
            admin_name=getattr(agency, "name", ""),
            admin_phone=admin_nums[0] if admin_nums else "",
        )
    sent = 0
    for i, chat_id in enumerate(group_chat_ids):
        if is_cancelled(agency.id):
            clear_cancel(agency.id)
            break
        if i > 0:
            await asyncio.sleep(group_delay)
        await _send_wa_file(chat_id, pdf_bytes, filename, caption)
        sent += 1
    return sent
