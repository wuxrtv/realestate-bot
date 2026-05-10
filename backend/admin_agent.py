"""
Admin AI agent — handles requests from the agency administrator.
Routes admin Telegram messages to a separate Claude instance with admin tools.
"""

import json
import logging
import os

import anthropic
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from models import AdminConversation, ToniProject

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
• "unit 1507", "show 2301", "any 3-bedrooms" → search_units
• "broadcast text to groups", "announce text" → announce_to_groups
• "send 3 units", "скинь юниты", "отправь 5 юнитов в группы" → send_inventory_to_groups (count=N)
• "send brochure/video/photo/media TO GROUPS" → send_drive_file (send_to="groups")
• "send me brochure/video/photo/media" (to admin only) → send_drive_file (send_to="admin")
• Any media request without "to groups" → send_drive_file (send_to="admin")
• "what's in Drive", "Drive projects", "what files" → list_drive_projects

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
        "description": "Поиск юнитов по номеру или ключевым словам во всех проектах.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Номер юнита или текстовый запрос"},
                "limit": {"type": "integer", "description": "Максимум результатов (по умолчанию 5)"},
            },
            "required": ["query"],
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
        "name": "send_inventory_to_groups",
        "description": "Pick N random units from inventory and send their cards to all WhatsApp groups. Use this when Admin says 'send 3 units', 'скинь юниты', 'отправь 5 юнитов' etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "count": {
                    "type": "integer",
                    "description": "Number of units to send per group (default 3, max 10)",
                },
                "project_name": {
                    "type": "string",
                    "description": "Optional: filter by project name. Leave empty to pick from all projects.",
                },
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

    def _load_history(self, db: Session, agency_id: int, user_id: str) -> tuple[AdminConversation, list]:
        conv = db.query(AdminConversation).filter(
            AdminConversation.agency_id == agency_id,
            AdminConversation.user_id == user_id,
        ).first()
        if not conv:
            conv = AdminConversation(agency_id=agency_id, user_id=user_id, history=[])
            db.add(conv)
            db.flush()
        return conv, list(conv.history or [])

    def _save_history(self, db: Session, conv: AdminConversation, history: list):
        conv.history = history[-30:]
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
            return self._search_units(db, inp["query"], inp.get("limit", 5), agency)
        if name == "send_drive_file":
            return await self._send_drive_file(
                inp["project_name"], inp.get("file_type", ""),
                inp.get("send_to", "admin"), agency, db,
            )
        if name == "list_drive_projects":
            return self._list_drive_projects(agency)
        if name == "send_inventory_to_groups":
            return await self._send_inventory_to_groups(
                db, inp.get("count", 3), inp.get("project_name", ""), agency
            )
        return {"error": f"Unknown tool: {name}"}

    async def _announce_to_groups(self, db: Session, message: str, agency) -> dict:
        import whatsapp_bot
        wa_sent = await whatsapp_bot.announce_to_wa_groups(db, message, agency)
        return {"sent_to_whatsapp": wa_sent}

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

    async def _send_drive_file(self, project_name: str, file_type: str, send_to: str, agency, db: Session) -> dict:
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
                file_bytes = _drive.download_file(svc, file_id, export_mime)
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

    async def _send_inventory_to_groups(self, db: Session, count: int, project_name: str, agency) -> dict:
        import asyncio as _asyncio
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

        # Also try Drive inventory
        import drive_service as _drive
        svc = _drive.get_service()
        root_id = getattr(agency, "drive_root_id", "") or ""
        if svc:
            seen = {u[0] for u in all_units}
            search_projects = projects if projects else []
            for proj in search_projects:
                try:
                    drive_idx = _drive.get_project_inventory(svc, proj.project_name, root_id)
                    for unit_num, unit_data in drive_idx.items():
                        if unit_num not in seen:
                            all_units.append((unit_num, unit_data, proj.project_name))
                            seen.add(unit_num)
                except Exception:
                    pass

        if not all_units:
            return {"error": "No units found in inventory or Drive"}

        picks = _random.sample(all_units, min(count, len(all_units)))

        groups = db.query(WhatsAppGroup).filter(
            WhatsAppGroup.active == True,
            WhatsAppGroup.agency_id == agency.id,
        ).all()
        if not groups:
            return {"error": "No active WhatsApp groups registered"}

        sent_groups = 0
        for i, group in enumerate(groups):
            if i > 0:
                await _asyncio.sleep(30)
            for unit_num, unit_data, proj_name in picks:
                card = format_unit_card(unit_num, unit_data, proj_name)
                await whatsapp_bot._send_wa(group.chat_id, card)
                await _asyncio.sleep(2)
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

    def _search_units(self, db: Session, query: str, limit: int, agency) -> dict:
        import re as _re
        from excel_parser import format_unit_card

        projects = db.query(ToniProject).filter(
            ToniProject.is_active == True, ToniProject.agency_id == agency.id
        ).all()
        results = []

        unit_match = _re.search(r"\b(\d{3,5})\b", query)
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

        if not results:
            kws = query.lower().split()
            for proj in projects:
                idx = proj.unit_index or {}
                for unit_num, data in idx.items():
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
