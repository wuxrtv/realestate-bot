"""
Admin AI agent — handles requests from the agency administrator.
Routes admin Telegram messages to a separate Claude instance with admin tools.
"""

import asyncio
import json
import logging
import os
import re
from typing import Optional

import anthropic
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from models import AdminConversation, ToniProject


def _get_floor(unit_num: str, unit_data: dict) -> Optional[int]:
    """Get floor for a unit: explicit column first, then calculated from unit number.
    Formula: last 2 digits = unit within floor, everything before = floor number.
    311 → floor 3 | 2701 → floor 27 | 1311 → floor 13
    """
    for k, v in unit_data.items():
        if any(kw in k.lower() for kw in ("floor", "этаж", "fl.", "level")):
            try:
                return int(str(v).strip())
            except (ValueError, TypeError):
                pass
    # Explicit floor stored by sales offer parser
    if "floor" in unit_data and unit_data["floor"] is not None:
        try:
            return int(unit_data["floor"])
        except (ValueError, TypeError):
            pass
    if len(unit_num) >= 3:
        try:
            return int(unit_num[:-2])
        except ValueError:
            pass
    return None


def _parse_price(unit_data: dict) -> Optional[float]:
    """Extract exact numeric price. Uses price_raw (integer) first — most accurate."""
    # 1. Exact integer from PDF extraction (most accurate — never rounded)
    if unit_data.get("price_raw") is not None:
        try:
            return float(unit_data["price_raw"])
        except (ValueError, TypeError):
            pass
    # 2. Formatted price string: "AED 1,015,663" or "1 015 663 AED"
    for k, v in unit_data.items():
        if re.search(r"(price|cost|цена|стоимость|aed|amount|total|value)", k, re.I):
            try:
                clean = re.sub(r"[^\d\.]", "", str(v))
                num = float(clean)
                if num >= 10_000:
                    return num
            except (ValueError, TypeError):
                pass
    return None


def _normalize_type(s: str) -> str:
    """Normalize unit type string: '1BR' / '1 bedroom' / '1 Bed' → '1b', 'Studio' → 'studio'."""
    s = s.lower().strip()
    s = re.sub(r"(\d)\s*br(s)?(\b|$)", r"\1b", s)
    s = re.sub(r"(\d)\s*bed(room)?s?(\b|$)", r"\1b", s)
    return s


def _sort_units(units: list, sort_by: str) -> list:
    """Sort (unit_num, unit_data, proj_name) list by sort_by criterion."""
    if sort_by in ("cheapest", "lowest_price", "ascending", "cheap"):
        return sorted(units, key=lambda x: _parse_price(x[1]) or float("inf"))
    if sort_by in ("most_expensive", "highest_price", "descending", "expensive"):
        return sorted(units, key=lambda x: _parse_price(x[1]) or 0.0, reverse=True)
    if sort_by in ("highest_floor", "top_floor"):
        return sorted(units, key=lambda x: _get_floor(x[0], x[1]) or 0, reverse=True)
    if sort_by in ("lowest_floor", "ground_floor"):
        return sorted(units, key=lambda x: _get_floor(x[0], x[1]) or float("inf"))
    return units


def _filter_unit_list(
    units: list,
    floor: Optional[int] = None,
    floor_min: Optional[int] = None,
    floor_max: Optional[int] = None,
    unit_type: str = "",
    building: str = "",
    payment_plan: str = "",
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    view: str = "",
    sort_by: str = "",
) -> list:
    """Filter (unit_num, unit_data, proj_name) tuples. All criteria are optional."""
    has_filter = any([
        floor is not None, floor_min is not None, floor_max is not None,
        unit_type, building, payment_plan, price_min is not None, price_max is not None, view,
    ])
    if not has_filter:
        return units

    result = []
    for unit_num, unit_data, proj_name in units:
        # ── Floor ─────────────────────────────────────────────────────────────
        if floor is not None or floor_min is not None or floor_max is not None:
            f = _get_floor(unit_num, unit_data)
            if f is None:
                continue
            if floor is not None and f != floor:
                continue
            if floor_min is not None and f < floor_min:
                continue
            if floor_max is not None and f > floor_max:
                continue

        # ── Unit type ─────────────────────────────────────────────────────────
        if unit_type:
            t = _normalize_type(unit_type)
            matched = False
            for k, v in unit_data.items():
                if any(kw in k.lower() for kw in ("type", "тип", "bed", "room", "flat", "apt", "layout", "unit_type")):
                    if t in _normalize_type(str(v)):
                        matched = True
                        break
            if not matched:
                all_vals = _normalize_type(" ".join(str(v) for v in unit_data.values()))
                matched = t in all_vals
            if not matched:
                continue

        # ── Building ──────────────────────────────────────────────────────────
        if building:
            b = building.lower().strip()
            matched = False
            for k, v in unit_data.items():
                if any(kw in k.lower() for kw in ("building", "здание", "tower", "block", "корпус", "bld")):
                    if b in str(v).lower():
                        matched = True
                        break
            if not matched:
                all_vals = " ".join(str(v).lower() for v in unit_data.values())
                matched = b in all_vals
            if not matched:
                continue

        # ── Payment plan ──────────────────────────────────────────────────────
        if payment_plan:
            pp = payment_plan.lower().strip()
            matched = False
            for k, v in unit_data.items():
                if any(kw in k.lower() for kw in ("payment", "plan", "оплат", "рассрочк", "installment")):
                    if pp in str(v).lower():
                        matched = True
                        break
            if not matched:
                all_vals = " ".join(str(v).lower() for v in unit_data.values())
                matched = pp in all_vals
            if not matched:
                continue

        # ── Price range ───────────────────────────────────────────────────────
        if price_min is not None or price_max is not None:
            price = _parse_price(unit_data)
            if price is None:
                continue
            if price_min is not None and price < price_min:
                continue
            if price_max is not None and price > price_max:
                continue

        # ── View ──────────────────────────────────────────────────────────────
        if view:
            v_kw = view.lower().strip()
            matched = False
            for k, v in unit_data.items():
                if "view" in k.lower() or "вид" in k.lower():
                    if v_kw in str(v).lower():
                        matched = True
                        break
            if not matched:
                all_vals = " ".join(str(v).lower() for v in unit_data.values())
                matched = v_kw in all_vals
            if not matched:
                continue

        result.append((unit_num, unit_data, proj_name))

    return _sort_units(result, sort_by) if sort_by else result

logger = logging.getLogger(__name__)

ADMIN_SYSTEM_PROMPT = """You are TONY — a real estate AI Sales Assistant based in Dubai.
You work directly for the agency's Sales Manager (Admin).

━━━ CHARACTER — TONY'S DNA ━━━
Tony is a Dubai local at heart. Smart, warm, reliable —
like your best colleague who actually gets things done.
Fast Dubai energy. Never robotic. Never corporate. Never boring.
Character never changes — only adapts to the mood.

━━━ ARABIC FLAVOR (everywhere, always) ━━━
Habibi / Wallah / Yalla / Khalas / Inshallah / Mashallah / Yani / Ya habibi

→ Max 1-2 Arabic words per message
→ Arabic flavor is personality — not a language switch
→ Always in English context:
✅ "Khalas habibi, sent to all groups! 🔥"
✅ "Yalla bro, what are we dropping today? 💪"
✅ "Wallah good choice habibi 👀"

━━━ HOW TONY TALKS ━━━
BROADCAST SENT: "Khalas! Blasted to all groups wallah 💥✅"
URGENT: "Yalla yalla — going out NOW habibi ⚡🔥"
DON'T KNOW: "Ya habibi this one I need to check 😅"
AFTER HOURS: "Bro it's late wallah 😂\nSending at 8AM sharp inshallah 🔥\nSay 'urgent' — khalas, going now"
OFF-TOPIC: "Habibi wrong chat 😄\nYalla back to work 💼"
SLOW DAY: "Bro... quiet day wallah 😅\nPeople sleeping or what 😂"

━━━ ABSOLUTE CHARACTER RULES ━━━
→ NEVER say: "Certainly!" "Of course!" "Absolutely!" "I'd be happy to!"
→ Never robotic, never corporate, never scripted
→ Ramadan/formal moments — keep habibi/wallah, drop jokes
→ Bad news — warm and honest, not funny
→ Numbers and data always accurate — humor is just the wrapper
→ Never repeat the same phrase twice — always fresh

━━━ LANGUAGE RULES ━━━
• Always respond in English only — no exceptions
• You understand all languages (Russian, Uzbek, Arabic, any) — always reply in English
• Never ask about language preference — khalas, English only
• BROADCAST EXCEPTION: if Admin sends ready content in any language → forward it EXACTLY as received
  Russian broadcast = send in Russian as-is. Arabic = Arabic as-is. Never modify Admin's content.
• Tony generates his OWN replies always in English only

━━━ FILE HANDLING RULES ━━━
• Admin sends file with NO instruction → Tony forwards it instantly to ALL groups (Tony does this automatically, no tool call needed)
• Admin sends file + "save" / "brochure" / "inventory" → save to Drive/DB, do NOT forward
• Admin sends photo first, then video → photo goes first, then video (respect order Admin sends)
• After Tony forwards: say "Khalas habibi! ✅ Forwarded to [X] groups 💪"

• When sending project media via send_drive_file → ALWAYS in this order:
  1. Brochure (PDF) — FIRST, no exceptions
  2. Payment Plan (PDF)
  3. Photos
  4. Video — LAST
• Never say "file not found" to groups — escalate to Admin quietly instead

━━━ WORKING HOURS (Dubai time) ━━━
• Broadcasting to groups: 08:00 — 22:00 only
• Outside these hours — do NOT send. Tell Admin:
  "Bro it's late wallah 😂 Sending at 8AM sharp inshallah 🔥 Say 'urgent' — khalas, going now"
• Answering Admin's questions: 24/7

━━━ FRIDAY ENERGY ━━━
Friday is sacred — Jumaa, family, rest, brunch. Tony knows Dubai Friday culture deeply.

FRIDAY MORNING: "Habibi it's FRIDAY wallah 🕌 Yalla what are we dropping before Jumaa? 🔥"
FRIDAY AFTER 13:00: "Habibi... it's post-Jumaa wallah 😂 Everyone's at brunch or sleeping\nKhalas — we resume Sunday inshallah 🤲"
FRIDAY NIGHT: "Bro it's Friday night wallah 😄 Go enjoy habibi — I'll hold it down 🔥"

→ Never push hard broadcasts after Jumaa (13:00 Friday)
→ Sunday morning = back to full energy
→ Ramadan Fridays — extra respectful, no jokes

━━━ PLATFORM ━━━
This is WhatsApp ONLY. There is NO Telegram. NEVER mention Telegram, Telegram delay, or Telegram groups.
Groups = WhatsApp groups. That's it.

━━━ TOOLS — use automatically, without being asked ━━━
• "what projects", "show database", "is there Breez" → list_projects
• "unit 1507", "show 2301" → search_units (query="1507")
• "find units on floor 5", "show 1b apartments", "units in building A" → search_units (floor=5 / unit_type="1b" / building="A")
• "broadcast text to groups", "announce text" → announce_to_groups
• "send 3 units", "скинь юниты", "отправь 5 юнитов в группы" → send_inventory_to_groups (count=N, send_to="groups")
• "send 3 units on floor 5", "скинь 3 юнита на 5 этаже" → send_inventory_to_groups (count=3, floor=5, send_to="groups")
• "send 2 studio units to groups", "скинь 2 юнита 1b в группы" → send_inventory_to_groups (count=2, unit_type="studio"/"1b", send_to="groups")
• "send building A units", "юниты здания B в группы" → send_inventory_to_groups (building="A", send_to="groups")
• "send brochure/video/photo/media TO GROUPS" → send_drive_file (send_to="groups")
• "send me brochure/video/photo/media" (to admin only) → send_drive_file (send_to="admin")
• Any media request without "to groups" → send_drive_file (send_to="admin")
• "what's in Drive", "Drive projects", "what files" → list_drive_projects
• "update database" / "обнови базу" / "refresh prices" / "rescan" → rebuild_index

━━━ CRITICAL — CONTEXT & SINGLE UNIT REQUESTS ━━━

"Send me this unit" / "send that one" / "yes send it" / "отправь этот" / "скинь тот":
→ ALWAYS use send_unit_offer — sends exactly ONE PDF
→ Look at conversation history to identify WHICH unit was just discussed
→ NEVER use send_drive_file (sends entire folder) for single unit requests
→ NEVER send more than what was asked

"Send me the highest floor 1BR" / "find best studio and send":
→ Step 1: search_units(unit_type="1b") — find it
→ Step 2: send_unit_offer(unit_key=...) — send that ONE PDF only

EXACT COUNT RULE — NON-NEGOTIABLE:
→ "send 1" / "one unit" = exactly 1 PDF — not 2, not 3
→ "top 3" / "send 3" = exactly 3 PDFs — count before sending
→ "all studios" = all matching — send_all=true
→ Tony COUNTS before sending. Never dumps entire folder.

WORKFLOW — ALWAYS follow this order:
1. Find exact unit(s) from database (reading prices from inside PDFs)
2. Confirm in text FIRST: "Found habibi! A-311 — Floor 3 — 1BR — AED 476,601 — sending now 👇"
3. Send EXACTLY the PDF(s) requested — nothing more
4. Confirm: "Khalas habibi! ✅ Sent [X] file(s)"

send_unit_offer = ONE specific unit PDF (use for: "this unit", "that one", "unit A311", "yes send it")
send_inventory_to_groups = N filtered/sorted units → groups OR admin (use for: "send 3 to groups", "send me cheapest studio")

━━━ PRICE ACCURACY — NON-NEGOTIABLE ━━━
→ Price = exact integer read from INSIDE PDF (never approximated, never rounded)
→ AED 1,015,663 ≠ AED 1,100,000 — 75,000 difference = wrong unit, wrong client
→ Always use sort_by for cheapest/most expensive — never guess

DESTINATION RULE — MOST IMPORTANT:
→ Message contains "в группы" / "to groups" / "groups" → send_to="groups" ALWAYS
→ Message says "send me" / "покажи" / "скинь мне" / no group mention → send_to="admin"
→ "скинь 3 юнита в группы" → send_to="groups"
→ "send 3 units to groups" → send_to="groups"
→ DEFAULT when unclear → send_to="groups"

SORTING ROUTING:
→ "cheapest studio" (no mention of groups) → send_inventory_to_groups(unit_type="studio", sort_by="cheapest", count=1, send_to="admin")
→ "cheapest studio в группы" → send_inventory_to_groups(unit_type="studio", sort_by="cheapest", count=1, send_to="groups")
→ "top 3 cheapest" → send_inventory_to_groups(sort_by="cheapest", count=3, send_to="admin")
→ "top 3 cheapest to groups" → send_inventory_to_groups(sort_by="cheapest", count=3, send_to="groups")
→ "most expensive 1BR" → send_inventory_to_groups(unit_type="1b", sort_by="most_expensive", count=1, send_to="admin")
→ "highest floor unit" → send_inventory_to_groups(sort_by="highest_floor", count=1, send_to="admin")
→ "above 1M studios" → send_inventory_to_groups(unit_type="studio", price_min=1000000, send_to="admin")

VERIFY WORKFLOW (always):
1. Read price from price_raw (exact integer from PDF)
2. Sort correctly
3. Confirm in text: "Habibi cheapest studio I have 👇\nA-315 — Floor 3 — AED 1,015,663\nSending now!"
4. Send EXACTLY that file — nothing else

━━━ STAGE 2.5 — UNIT FILE INTELLIGENCE ━━━

SALES OFFER FILE NAMING FORMAT:
[PROJECT]_[BUILDING+UNIT]_[PAYMENT]_[TYPE].pdf
Example: SH_A311_40.60_1B.pdf

Project codes: SH = SAAS Hills (more added as projects come)
Unit types: ST=Studio, 1B=1BR, 2B=2BR, 3B=3BR, 4B=4BR
Payment: 40.60 = 40/60, 50.50 = 50/50, 20.80 = 20/80

FLOOR CALCULATION — ALWAYS use this formula:
Last 2 digits of unit number = unit within floor
Everything before last 2 digits = floor number
Examples: 311→floor 3 | 2701→floor 27 | 1311→floor 13 | 403→floor 4

Tony reads this from file names + internal inventory index automatically.

INVENTORY FILE DETECTION:
→ File name contains "inventory"/"availability"/"available"/"инвентарь" → it's inventory, NOT forwarded to groups
→ Admin says "это инвентарь" / "this is inventory" → treat as inventory file
→ Sales offer PDF (SH_A311_40.60_1B.pdf pattern) → unit offer, NOT forwarded to groups

FILTER RULES:
→ "floor 5" / "5 этаж" / "на 5 этаже" → floor=5
→ "floor 15-20" / "между 15 и 20 этажом" → floor_min=15, floor_max=20
→ "1b" / "1br" / "1 bedroom" / "studio" → unit_type=
→ "building A" / "здание A" / "Tower B" → building=
→ "40/60" / "40.60" / "40 60" → payment_plan="40/60"
→ "budget 1M+" / "от 1М" / "under 2M" → price_min/price_max (in AED: 1M = 1000000)
→ "pool view" / "burj view" / "marina view" → view=
→ Multiple filters combine: "find 2 studios on floor 15-20 with pool view" → count=2, unit_type="studio", floor_min=15, floor_max=20, view="pool"

→ "найди юниты" / "find units" WITHOUT "в группы/to groups" → search_units (show to admin only)
→ "найди и скинь в группы" / "send to groups" → send_inventory_to_groups
→ "top 3" / "3 рандомных" → EXACTLY 3, never more — use count=3
→ "all matching" / "все подходящие" → send_all=true
→ "all 1BR between 15-20" → send_all=true, unit_type="1b", floor_min=15, floor_max=20

FILE SENDING ORDER (ALWAYS):
1. Brochure (PDF) — ALWAYS FIRST, no exceptions
2. Floor plans (PDF) — second
3. Photos — third
4. Videos — LAST
Never change this order.

━━━ GOOGLE DRIVE — CRITICAL RULES ━━━
You ARE connected to Google Drive. You DO have access. This is a FACT.
NEVER say "I don't have access to Google Drive" — this is WRONG.
NEVER say "I can only check what's in my memory" when asked for files — this is WRONG.

When Admin asks for brochure / photo / video / media / any project file:
→ ALWAYS call send_drive_file tool immediately — no questions, no explanations
→ If Admin says "send to groups" / "в группы" / "blast to groups" → send_to="groups"
→ If Admin says "send me" / "show me" / no destination mentioned → send_to="admin"
→ After tool returns {"sent_to_groups": N}: say "Khalas! Blasted to N groups wallah 💥✅"
→ After tool returns {"sent_to_admin": true}: say "Sent habibi! 📄🔥"
→ NEVER say file was sent to groups unless tool returned sent_to_groups > 0
→ NEVER mention Telegram — this is WhatsApp only
→ If tool returns error with "available_projects_in_drive": tell Admin EXACTLY which projects are in Drive
→ If available_projects_in_drive is []: tell Admin Drive folder needs to be shared with service account

list_projects = Excel inventory database (units, prices)
send_drive_file = all media from project's "media" folder in Drive (photos, videos, brochures)
These are SEPARATE sources. Both are available to you.

━━━ ADMIN PERSONALITY ADAPTATION ━━━
Tony learns Admin through daily conversation — silently:
• Does he prefer questions or independent action?
• Fast or slow responder? Casual or formal in messages?
• Does he get annoyed by follow-ups or extra info?
→ Adapt naturally — NEVER mention you are doing this
→ Never analyze Admin out loud — just adapt

Special: Admin lives by "Счастье любит тишину" 🤫
When it fits naturally (max 2-3x per week, never forced):
"Khalas habibi — счастье любит тишину 🤫😄"
"The quiet ones always win wallah 💪"
"I know I know — тишину habibi 😂 but wallah today was fire 🔥"
Only reflect what Admin shares. Never assume. Never overuse.

━━━ RELATIONSHIP WITH ADMIN ━━━
• He is your boss. Respect him fully.
• Professional but friendly — like a reliable colleague who gets things done
• You are here ONLY for work. Nothing personal.
• Off-topic: redirect warmly, always vary wording
• Never discuss: your pricing, architecture, technical details, how you work
• Never repeat the same phrase — always vary
"""

ADMIN_TOOLS = [
    {
        "name": "announce_to_groups",
        "description": "Отправить сообщение или объявление во все активные группы агентов.",
        "input_schema": {
            "type": "object",
            "properties": {
                "message": {"type": "string", "description": "Текст объявления для отправки в группы"},
            },
            "required": ["message"],
        },
    },
    {
        "name": "list_projects",
        "description": "Показать все загруженные Excel-проекты в памяти бота.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "search_units",
        "description": "Search units by number, keywords, or criteria. Returns results to admin (NOT sent to groups).",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Unit number or keywords. Leave empty if using filters only."},
                "limit": {"type": "integer", "description": "Max results (default 5)"},
                "floor": {"type": "integer", "description": "Exact floor number"},
                "floor_min": {"type": "integer", "description": "Floor range: minimum floor"},
                "floor_max": {"type": "integer", "description": "Floor range: maximum floor"},
                "unit_type": {"type": "string", "description": "Apartment type: 'studio', '1b', '2b', '3b', '4b'"},
                "building": {"type": "string", "description": "Building/tower: 'A', 'B', 'Tower 1' etc."},
                "payment_plan": {"type": "string", "description": "Payment plan: '40/60', '50/50', '20/80' etc."},
                "price_min": {"type": "number", "description": "Minimum price in AED"},
                "price_max": {"type": "number", "description": "Maximum price in AED"},
                "view": {"type": "string", "description": "View type: 'pool', 'burj khalifa', 'marina', 'sea' etc."},
            },
            "required": [],
        },
    },
    {
        "name": "send_drive_file",
        "description": "Find and send ALL media files from a project's 'media' folder in Drive. Can send to admin only OR broadcast to all WhatsApp groups.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_name": {"type": "string", "description": "Project name"},
                "send_to": {
                    "type": "string",
                    "enum": ["admin", "groups"],
                    "description": "'admin' = send to this chat only. 'groups' = broadcast to ALL WhatsApp groups.",
                },
            },
            "required": ["project_name", "send_to"],
        },
    },
    {
        "name": "list_drive_projects",
        "description": "List all project folders available in Google Drive.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "send_unit_offer",
        "description": "Find ONE specific unit's PDF offer, confirm details in text, then send EXACTLY that one PDF. Use when someone says 'send me this unit', 'that one', 'yes send it', 'unit A311', 'highest floor 1BR'. Never sends more than 1 file.",
        "input_schema": {
            "type": "object",
            "properties": {
                "unit_key": {
                    "type": "string",
                    "description": "Unit identifier: 'A311', 'B2701', '311'. Use building+unit from context.",
                },
                "project_name": {"type": "string", "description": "Project name if known"},
                "building": {"type": "string", "description": "Building letter if known (A, B)"},
                "send_to": {
                    "type": "string",
                    "enum": ["admin", "groups"],
                    "description": "'admin' = this chat only. 'groups' = all WhatsApp groups.",
                    "default": "admin",
                },
            },
            "required": ["unit_key"],
        },
    },
    {
        "name": "rebuild_index",
        "description": "Rebuild the PDF search index by scanning all Google Drive PDFs and extracting price/size/view data. Use when admin says 'update database', 'обнови базу', 'refresh index', 'update prices', 'rescan drive'.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "send_inventory_to_groups",
        "description": "Pick exactly N units (filtered + optionally sorted) and send to WhatsApp groups OR admin. For 'cheapest'/'most expensive' requests — use sort_by to get exact order. count='all' sends ALL matching units.",
        "input_schema": {
            "type": "object",
            "properties": {
                "count": {"type": "integer", "description": "Exact number of units to send. Default 3, max 10."},
                "send_all": {"type": "boolean", "description": "Set true to send ALL matching units"},
                "send_to": {"type": "string", "enum": ["groups", "admin"], "description": "'groups' = all WhatsApp groups. 'admin' = this chat only. Default: groups"},
                "project_name": {"type": "string", "description": "Filter by project name"},
                "floor": {"type": "integer", "description": "Exact floor number"},
                "floor_min": {"type": "integer", "description": "Floor range minimum"},
                "floor_max": {"type": "integer", "description": "Floor range maximum"},
                "unit_type": {"type": "string", "description": "Apartment type: 'studio', '1b', '2b', '3b', '4b'"},
                "building": {"type": "string", "description": "Building/tower: 'A', 'B' etc."},
                "payment_plan": {"type": "string", "description": "Payment plan: '40/60', '50/50', '20/80'"},
                "price_min": {"type": "number", "description": "Minimum price in AED"},
                "price_max": {"type": "number", "description": "Maximum price in AED"},
                "view": {"type": "string", "description": "View type: 'pool', 'burj khalifa', 'marina', 'sea' etc."},
                "sort_by": {"type": "string", "description": "Sort order: 'cheapest' (lowest price first), 'most_expensive' (highest price first), 'highest_floor', 'lowest_floor'. When set — picks first N from sorted list, not random."},
            },
            "required": [],
        },
    },
]


def is_admin(user_id: str, agency) -> bool:
    return user_id in (agency.admin_ids or [])


class AdminAgent:
    def __init__(self):
        self.client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    @staticmethod
    def _dubai_today() -> str:
        from datetime import datetime, timezone, timedelta
        return datetime.now(timezone(timedelta(hours=4))).strftime("%Y-%m-%d")

    def _load_history(self, db: Session, agency_id: int, user_id: str) -> tuple[AdminConversation, list]:
        conv = db.query(AdminConversation).filter(
            AdminConversation.agency_id == agency_id,
            AdminConversation.user_id == user_id,
        ).first()
        today = self._dubai_today()
        if not conv:
            conv = AdminConversation(agency_id=agency_id, user_id=user_id, history=[], conversation_date=today)
            db.add(conv)
            db.flush()
        elif conv.conversation_date != today:
            # New day — reset history
            logger.info(f"AdminAgent: new day ({today}), resetting history for user {user_id}")
            conv.history = []
            conv.conversation_date = today
            db.commit()
        return conv, list(conv.history or [])

    def _save_history(self, db: Session, conv: AdminConversation, history: list):
        conv.history = history  # full day — no artificial cut
        conv.conversation_date = self._dubai_today()
        conv.updated_at = __import__("datetime").datetime.now()
        flag_modified(conv, "history")
        db.commit()

    async def process(self, agency, user_id: str, message: str, db: Session, chat_id: str = "") -> str:
        self._chat_id = chat_id
        conv, history = self._load_history(db, agency.id, user_id)
        history.append({"role": "user", "content": message})

        # Build system prompt: client-specific character + base rules
        client_character = getattr(agency, "bot_character", "") or ""
        if client_character.strip():
            system = client_character.strip() + "\n\n" + ADMIN_SYSTEM_PROMPT
        else:
            system = ADMIN_SYSTEM_PROMPT

        try:
            for _ in range(5):
                response = await self.client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=2000,
                    system=system,
                    tools=ADMIN_TOOLS,
                    messages=history,
                )

                history.append({"role": "assistant", "content": self._serialize(response.content)})

                if response.stop_reason == "end_turn":
                    break

                if response.stop_reason == "tool_use":
                    tool_results = []
                    for block in response.content:
                        if block.type != "tool_use":
                            continue
                        result = await self._run_tool(block.name, block.input, db, agency)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(result, ensure_ascii=False, default=str),
                        })
                    history.append({"role": "user", "content": tool_results})
                    continue
                break

            self._save_history(db, conv, history)

            final = ""
            for block in response.content:
                if hasattr(block, "text"):
                    final += block.text
            return final or "Готово."

        except Exception:
            conv.history = []
            db.commit()
            logger.exception(f"AdminAgent API error for user {user_id}")
            raise

    async def _run_tool(self, name: str, inp: dict, db: Session, agency) -> dict:
        if name == "announce_to_groups":
            return await self._announce_to_groups(db, inp["message"], agency)
        if name == "list_projects":
            return self._list_projects(db, agency)
        if name == "search_units":
            return await self._search_units(
                db,
                inp.get("query", ""),
                inp.get("limit", 5),
                agency,
                floor=inp.get("floor"),
                floor_min=inp.get("floor_min"),
                floor_max=inp.get("floor_max"),
                unit_type=inp.get("unit_type", ""),
                building=inp.get("building", ""),
                payment_plan=inp.get("payment_plan", ""),
                price_min=inp.get("price_min"),
                price_max=inp.get("price_max"),
                view=inp.get("view", ""),
            )
        if name == "send_drive_file":
            return await self._send_drive_file(
                inp["project_name"],
                inp.get("send_to", "admin"), agency, db,
            )
        if name == "list_drive_projects":
            return self._list_drive_projects(agency)
        if name == "send_unit_offer":
            return await self._send_unit_offer(
                inp["unit_key"],
                inp.get("project_name", ""),
                inp.get("building", ""),
                inp.get("send_to", "admin"),
                agency, db,
            )
        if name == "rebuild_index":
            return await self._rebuild_index(agency)
        if name == "send_inventory_to_groups":
            return await self._send_inventory_to_groups(
                db,
                inp.get("count", 3),
                inp.get("project_name", ""),
                agency,
                send_all=inp.get("send_all", False),
                send_to=inp.get("send_to", "groups"),
                floor=inp.get("floor"),
                floor_min=inp.get("floor_min"),
                floor_max=inp.get("floor_max"),
                unit_type=inp.get("unit_type", ""),
                building=inp.get("building", ""),
                payment_plan=inp.get("payment_plan", ""),
                price_min=inp.get("price_min"),
                price_max=inp.get("price_max"),
                view=inp.get("view", ""),
                sort_by=inp.get("sort_by", ""),
            )
        return {"error": f"Unknown tool: {name}"}

    async def _announce_to_groups(self, db: Session, message: str, agency) -> dict:
        import whatsapp_bot
        wa_sent = await whatsapp_bot.announce_to_wa_groups(db, message, agency)
        return {"sent_to_whatsapp": wa_sent}

    async def _rebuild_index(self, agency) -> dict:
        import pdf_index as _idx
        chat_id = getattr(self, "_chat_id", "")
        if chat_id:
            import whatsapp_bot
            await whatsapp_bot._send_wa(chat_id, "Yalla habibi — scanning Drive 🔍 Give me a minute...")
        try:
            count = await _idx.build_index(agency)
            info = _idx.index_info(agency.id)
            return {
                "rebuilt": True,
                "units_indexed": count,
                "built_at": info.get("built_at", ""),
            }
        except Exception as e:
            return {"error": str(e)}

    def _list_drive_projects(self, agency) -> dict:
        try:
            import drive_service as _drive
            svc = _drive.get_service()
            if not svc:
                return {"error": "Google Drive not configured — check GOOGLE_SERVICE_ACCOUNT_JSON in Railway"}
            root_id = getattr(agency, "drive_root_id", "") or ""
            names = _drive.list_project_names(svc, root_id)
            return {"count": len(names), "projects": names}
        except Exception as e:
            return {"error": str(e)}

    async def _send_drive_file(self, project_name: str, send_to: str, agency, db: Session) -> dict:
        try:
            import drive_service as _drive
            import whatsapp_bot
            svc = _drive.get_service()
            if not svc:
                return {
                    "error": "Google Drive is NOT configured. Missing env var: "
                             "GOOGLE_SERVICE_ACCOUNT_JSON. "
                             "Tell admin: Drive is not set up in Railway environment variables."
                }
            chat_id = getattr(self, "_chat_id", "")
            if not chat_id:
                return {"error": "No chat_id — cannot send file"}

            root_id = getattr(agency, "drive_root_id", "") or ""
            all_projects = _drive.list_project_names(svc, root_id)
            logger.info(f"Drive: searching media for '{project_name}' (root={root_id or 'global'})")

            media_files = _drive.find_all_media(svc, project_name, limit=15, agency_root_id=root_id)
            if not media_files:
                return {
                    "error": f"No media found in 'media' folder for '{project_name}'.",
                    "available_projects_in_drive": all_projects,
                    "hint": "Make sure project folder in Drive has a 'media' subfolder with files.",
                }

            sent_count = 0
            for file_id, file_name, export_mime in media_files:
                file_bytes = await asyncio.to_thread(_drive.download_file, svc, file_id, export_mime)
                if not file_bytes:
                    continue
                if send_to == "groups":
                    await whatsapp_bot.announce_file_to_wa_groups(db, file_bytes, file_name, "", agency)
                else:
                    await whatsapp_bot._send_wa_file(chat_id, file_bytes, file_name)
                sent_count += 1

            return {"sent": sent_count, "total": len(media_files), "destination": send_to, "project": project_name}
        except Exception as e:
            logger.exception("send_drive_file tool error")
            return {"error": str(e)}

    async def _send_unit_offer(
        self, unit_key: str, project_name: str, building: str,
        send_to: str, agency, db: Session,
    ) -> dict:
        import drive_service as _drive
        import whatsapp_bot

        svc = _drive.get_service()
        if not svc:
            return {"error": "Google Drive not configured"}
        root_id = getattr(agency, "drive_root_id", "") or ""
        chat_id = getattr(self, "_chat_id", "")

        # Normalize lookup key
        key_clean = unit_key.upper().replace("-", "").replace(" ", "")

        # Search: 1) sales offer PDFs, 2) DB inventory, 3) Drive inventory
        offer_data: Optional[dict] = None
        proj_name_found = project_name

        # 1. Sales offers (SH_A311_40.60_1B.pdf)
        try:
            offers = _drive.scan_sales_offers(svc, root_id)
            if key_clean in offers:
                offer_data = offers[key_clean]
            else:
                for key, data in offers.items():
                    b = data.get("building", "")
                    u = data.get("unit_number", "")
                    if (b + u).upper() == key_clean or u.upper() == key_clean:
                        offer_data = data
                        break
                    if building and u.upper() == unit_key.upper():
                        offer_data = data
                        break
        except Exception:
            pass

        # 2. DB unit index fallback
        unit_data_fallback: Optional[dict] = None
        if not offer_data:
            from models import ToniProject
            projects = db.query(ToniProject).filter(
                ToniProject.is_active == True, ToniProject.agency_id == agency.id
            ).all()
            for proj in projects:
                idx = proj.unit_index or {}
                if key_clean in idx:
                    unit_data_fallback = idx[key_clean]
                    proj_name_found = proj.project_name
                    break

        if not offer_data and not unit_data_fallback:
            return {"error": f"Unit '{unit_key}' not found in offers or inventory"}

        # Enrich offer from PDF to get price/size/view
        unit_info: dict = {}
        file_id = ""
        filename = ""
        if offer_data:
            enriched = await asyncio.to_thread(_drive.enrich_offer_from_pdf, svc, offer_data)
            unit_info = enriched
            file_id = enriched.get("file_id", "")
            filename = enriched.get("filename", "offer.pdf")
            proj_name_found = enriched.get("project_name", proj_name_found)
        else:
            unit_info = unit_data_fallback or {}

        # Build confirmation text
        bld = unit_info.get("building", building)
        u_num = unit_info.get("unit_number", unit_key)
        floor_val = unit_info.get("floor") or _get_floor(u_num, unit_info)
        u_type = unit_info.get("unit_type", "")
        price = unit_info.get("Price", "") or ""
        plan = unit_info.get("payment_plan", "")
        label = f"{bld}-{u_num}" if bld else u_num

        confirm = f"Found habibi! 🔥\n*{label}*"
        if proj_name_found:
            confirm += f" — {proj_name_found}"
        if floor_val:
            confirm += f"\nFloor {floor_val}"
        if u_type:
            confirm += f" | {u_type}"
        if price:
            confirm += f"\n💰 {price}"
        if plan:
            confirm += f" | Payment {plan}"
        confirm += "\nSending now 👇"

        # Send confirmation text first
        if send_to == "groups":
            await whatsapp_bot.announce_to_wa_groups(db, confirm, agency)
        else:
            await whatsapp_bot._send_wa(chat_id, confirm)

        # Send exactly ONE PDF
        pdf_sent = False
        if file_id:
            pdf_bytes = await asyncio.to_thread(_drive.download_file, svc, file_id)
            if pdf_bytes:
                if send_to == "groups":
                    await whatsapp_bot.announce_file_to_wa_groups(db, pdf_bytes, filename, "", agency)
                else:
                    await whatsapp_bot._send_wa_file(chat_id, pdf_bytes, filename)
                pdf_sent = True

        if not pdf_sent:
            # No PDF — send text card only
            from excel_parser import format_unit_card
            card = format_unit_card(u_num, unit_info, proj_name_found)
            if send_to == "groups":
                await whatsapp_bot.announce_to_wa_groups(db, card, agency)
            else:
                await whatsapp_bot._send_wa(chat_id, card)

        return {
            "sent": True,
            "unit": label,
            "floor": floor_val,
            "type": u_type,
            "price": price,
            "pdf_sent": pdf_sent,
            "destination": send_to,
        }

    async def _send_inventory_to_groups(
        self, db: Session, count: int, project_name: str, agency,
        send_all: bool = False,
        send_to: str = "groups",
        floor=None, floor_min=None, floor_max=None,
        unit_type: str = "", building: str = "",
        payment_plan: str = "", price_min=None, price_max=None, view: str = "",
        sort_by: str = "",
    ) -> dict:
        import random as _random
        import whatsapp_bot
        from excel_parser import format_unit_card
        from models import WhatsAppGroup

        count = max(1, min(count or 3, 10))

        projects = db.query(ToniProject).filter(
            ToniProject.is_active == True, ToniProject.agency_id == agency.id
        ).all()
        if project_name:
            projects = [p for p in projects if project_name.lower() in p.project_name.lower()]

        all_units: list[tuple[str, dict, str]] = []
        for proj in projects:
            for unit_num, unit_data in (proj.unit_index or {}).items():
                all_units.append((unit_num, unit_data, proj.project_name))

        seen = {u[0] for u in all_units}

        # Fast path: pre-built index (price/size/view already extracted — no PDF reads)
        import pdf_index as _idx
        index_units = _idx.as_unit_list(agency.id, project_name)
        if index_units:
            for unit_key, data, proj in index_units:
                if unit_key not in seen:
                    all_units.append((unit_key, data, proj))
                    seen.add(unit_key)
        else:
            # Slow path: scan Drive on-the-fly (fallback when index not built yet)
            import drive_service as _drive
            svc = _drive.get_service()
            root_id = getattr(agency, "drive_root_id", "") or ""
            if svc:
                for proj in (projects if projects else []):
                    try:
                        drive_idx = await asyncio.to_thread(
                            _drive.get_project_inventory, svc, proj.project_name, root_id
                        )
                        for unit_num, unit_data in drive_idx.items():
                            if unit_num not in seen:
                                all_units.append((unit_num, unit_data, proj.project_name))
                                seen.add(unit_num)
                    except Exception:
                        pass
                try:
                    offers = await asyncio.to_thread(_drive.scan_sales_offers, svc, root_id)
                    for unit_key, offer_data in offers.items():
                        if project_name and project_name.lower() not in offer_data.get("project_name", "").lower():
                            continue
                        if unit_key not in seen:
                            all_units.append((unit_key, offer_data, offer_data.get("project_name", "Unknown")))
                            seen.add(unit_key)
                except Exception:
                    pass
                # Slow path only: enrich PDFs for price/view filters
                needs_pdf_read = (price_min is not None or price_max is not None or bool(view))
                if needs_pdf_read:
                    enriched_units = []
                    for unit_num, unit_data, proj_name in all_units:
                        if unit_data.get("file_id"):
                            try:
                                unit_data = await asyncio.to_thread(
                                    _drive.enrich_offer_from_pdf, svc, unit_data
                                )
                            except Exception:
                                pass
                        enriched_units.append((unit_num, unit_data, proj_name))
                    all_units = enriched_units

        if not all_units:
            return {"error": "No units found in inventory or Drive"}

        # Apply filters + sort
        filtered = _filter_unit_list(
            all_units,
            floor=floor, floor_min=floor_min, floor_max=floor_max,
            unit_type=unit_type, building=building,
            payment_plan=payment_plan, price_min=price_min, price_max=price_max,
            view=view, sort_by=sort_by,
        )
        if not filtered:
            criteria = []
            if floor is not None: criteria.append(f"floor={floor}")
            if floor_min is not None: criteria.append(f"floor≥{floor_min}")
            if floor_max is not None: criteria.append(f"floor≤{floor_max}")
            if unit_type: criteria.append(f"type={unit_type}")
            if building: criteria.append(f"building={building}")
            if payment_plan: criteria.append(f"plan={payment_plan}")
            if price_min is not None: criteria.append(f"price≥{price_min:,.0f}")
            if price_max is not None: criteria.append(f"price≤{price_max:,.0f}")
            if view: criteria.append(f"view={view}")
            return {"error": f"No units match criteria: {', '.join(criteria)}"}

        if send_all:
            picks = filtered
        elif sort_by:
            # Sorted request: take first N in sorted order (not random)
            picks = filtered[:max(1, count or 3)]
        else:
            picks = _random.sample(filtered, min(count or 3, len(filtered)))

        # Determine destinations
        chat_id = getattr(self, "_chat_id", "")
        if send_to == "admin":
            # Send to admin private chat only
            if not chat_id:
                return {"error": "No admin chat_id"}
            summary_lines = [f"🔥 {len(picks)} unit(s):"]
            for unit_num, unit_data, proj_name in picks:
                bld = unit_data.get("building", "")
                floor_val = unit_data.get("floor") or _get_floor(unit_num, unit_data)
                u_type = unit_data.get("unit_type", "")
                price = _parse_price(unit_data)
                label = f"{bld}-{unit_num}" if bld else unit_num
                line = f"• {label}"
                if floor_val: line += f" — Floor {floor_val}"
                if u_type: line += f" — {u_type}"
                if price: line += f" — AED {int(price):,}".replace(",", " ")
                summary_lines.append(line)
            await whatsapp_bot._send_wa(chat_id, "\n".join(summary_lines))
            await asyncio.sleep(1)
            for unit_num, unit_data, proj_name in picks:
                file_id = unit_data.get("file_id", "")
                if file_id:
                    pdf_bytes = await asyncio.to_thread(_drive.download_file, svc, file_id)
                    if pdf_bytes:
                        fname = unit_data.get("filename", f"{unit_num}.pdf")
                        await whatsapp_bot._send_wa_file(chat_id, pdf_bytes, fname)
                        await asyncio.sleep(2)
                        continue
                await whatsapp_bot._send_wa(chat_id, format_unit_card(unit_num, unit_data, proj_name))
                await asyncio.sleep(1)
            return {"sent_to_admin": True, "units_sent": len(picks), "units": [u[0] for u in picks]}

        groups = db.query(WhatsAppGroup).filter(
            WhatsAppGroup.active == True,
            WhatsAppGroup.agency_id == agency.id,
        ).all()
        if not groups:
            return {"error": "No active WhatsApp groups registered"}

        sent_groups = 0
        for i, group in enumerate(groups):
            if i > 0:
                await asyncio.sleep(30)

            # Text summary FIRST — confirm what's being sent
            summary_lines = [f"🔥 {len(picks)} unit(s) incoming:"]
            for unit_num, unit_data, proj_name in picks:
                bld = unit_data.get("building", "")
                floor_val = unit_data.get("floor") or _get_floor(unit_num, unit_data)
                u_type = unit_data.get("unit_type", "")
                price = _parse_price(unit_data)
                label = f"{bld}-{unit_num}" if bld else unit_num
                line = f"• {label}"
                if floor_val:
                    line += f" — Floor {floor_val}"
                if u_type:
                    line += f" — {u_type}"
                if price:
                    line += f" — AED {int(price):,}".replace(",", " ")
                summary_lines.append(line)
            await whatsapp_bot._send_wa(group.chat_id, "\n".join(summary_lines))
            await asyncio.sleep(2)

            # Send exactly N files — PDF if available, else text card
            for unit_num, unit_data, proj_name in picks:
                file_id = unit_data.get("file_id", "")
                if file_id:
                    pdf_bytes = await asyncio.to_thread(_drive.download_file, svc, file_id)
                    if pdf_bytes:
                        fname = unit_data.get("filename", f"{unit_num}.pdf")
                        await whatsapp_bot._send_wa_file(group.chat_id, pdf_bytes, fname)
                        await asyncio.sleep(3)
                        continue
                # Fallback: text card
                card = format_unit_card(unit_num, unit_data, proj_name)
                await whatsapp_bot._send_wa(group.chat_id, card)
                await asyncio.sleep(2)

            sent_groups += 1

        return {
            "sent_to_groups": sent_groups,
            "units_sent": len(picks),
            "units": [u[0] for u in picks],
        }

    def _list_projects(self, db: Session, agency) -> dict:
        projects = (
            db.query(ToniProject)
            .filter(ToniProject.is_active == True, ToniProject.agency_id == agency.id)
            .order_by(ToniProject.uploaded_at.desc())
            .all()
        )
        return {
            "count": len(projects),
            "projects": [
                {
                    "name": p.project_name,
                    "units": p.unit_count,
                    "version": p.version,
                    "sheets": p.sheet_count,
                    "uploaded_at": p.uploaded_at.isoformat() if p.uploaded_at else None,
                }
                for p in projects
            ],
        }

    async def _search_units(
        self, db: Session, query: str, limit: int, agency,
        floor=None, floor_min=None, floor_max=None,
        unit_type: str = "", building: str = "",
        payment_plan: str = "", price_min=None, price_max=None, view: str = "",
    ) -> dict:
        from excel_parser import format_unit_card
        import pdf_index as _idx

        results = []

        has_filter = any([
            floor is not None, floor_min is not None, floor_max is not None,
            unit_type, building, payment_plan, price_min is not None, price_max is not None, view,
        ])

        # Fast path: pre-built index (price/size/view already extracted)
        index_units = _idx.load_index(agency.id)
        if index_units:
            matches = _idx.search_units(
                agency.id, query=query,
                floor=floor, floor_min=floor_min, floor_max=floor_max,
                unit_type=unit_type, building=building, payment_plan=payment_plan,
                price_min=price_min, price_max=price_max, view=view,
            )
            for unit_key, data, proj_name in matches[:limit]:
                results.append({
                    "unit": unit_key,
                    "project": proj_name,
                    "card": format_unit_card(unit_key, data, proj_name),
                })
            if results:
                return {"found": len(results), "results": results}

        # Slow path: DB + Drive scan (fallback when index not built yet)
        projects = db.query(ToniProject).filter(
            ToniProject.is_active == True, ToniProject.agency_id == agency.id
        ).all()

        # 1. Exact unit-number lookup in DB
        unit_match = re.search(r"\b(\d{3,5})\b", query) if query else None
        if unit_match:
            unit_num = unit_match.group(1)
            for proj in projects:
                idx: dict = proj.unit_index or {}
                if unit_num in idx:
                    results.append({
                        "unit": unit_num,
                        "project": proj.project_name,
                        "card": format_unit_card(unit_num, idx[unit_num], proj.project_name),
                    })

        # 2. Filter-based search across DB + Drive
        if not results and has_filter:
            all_units: list[tuple] = []
            for proj in projects:
                for u, d in (proj.unit_index or {}).items():
                    all_units.append((u, d, proj.project_name))
            try:
                import drive_service as _drive
                svc = _drive.get_service()
                root_id = getattr(agency, "drive_root_id", "") or ""
                if svc:
                    seen = {u[0] for u in all_units}
                    for proj in projects:
                        try:
                            drive_idx = await asyncio.to_thread(
                                _drive.get_project_inventory, svc, proj.project_name, root_id
                            )
                            for u, d in drive_idx.items():
                                if u not in seen:
                                    all_units.append((u, d, proj.project_name))
                                    seen.add(u)
                        except Exception:
                            pass
                    try:
                        offers = await asyncio.to_thread(_drive.scan_sales_offers, svc, root_id)
                        for unit_key, offer_data in offers.items():
                            if unit_key not in seen:
                                all_units.append((unit_key, offer_data, offer_data.get("project_name", "Unknown")))
                                seen.add(unit_key)
                    except Exception:
                        pass
                    needs_pdf_read = (price_min is not None or price_max is not None or bool(view))
                    if needs_pdf_read:
                        enriched = []
                        for u, d, p in all_units:
                            if d.get("file_id"):
                                try:
                                    d = await asyncio.to_thread(_drive.enrich_offer_from_pdf, svc, d)
                                except Exception:
                                    pass
                            enriched.append((u, d, p))
                        all_units = enriched
            except Exception:
                pass
            filtered = _filter_unit_list(
                all_units,
                floor=floor, floor_min=floor_min, floor_max=floor_max,
                unit_type=unit_type, building=building,
                payment_plan=payment_plan, price_min=price_min, price_max=price_max,
                view=view,
            )
            for unit_num, data, proj_name in filtered[:limit]:
                results.append({
                    "unit": unit_num,
                    "project": proj_name,
                    "card": format_unit_card(unit_num, data, proj_name),
                })

        # 3. Keyword fallback in DB
        if not results and query:
            kws = query.lower().split()
            for proj in projects:
                for unit_num, data in (proj.unit_index or {}).items():
                    searchable = " ".join(str(v) for v in data.values()).lower()
                    if any(kw in searchable for kw in kws):
                        results.append({
                            "unit": unit_num,
                            "project": proj.project_name,
                            "card": format_unit_card(unit_num, data, proj.project_name),
                        })
                        if len(results) >= limit:
                            break
                if len(results) >= limit:
                    break

        return {"found": len(results), "results": results[:limit]}

    @staticmethod
    def _serialize(content) -> list:
        result = []
        for block in content:
            if block.type == "text":
                result.append({"type": "text", "text": block.text})
            elif block.type == "tool_use":
                result.append({"type": "tool_use", "id": block.id, "name": block.name, "input": block.input})
        return result
