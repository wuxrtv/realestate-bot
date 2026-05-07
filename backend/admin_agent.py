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

ADMIN_SYSTEM_PROMPT = """You are TONY — an AI Sales Assistant for a real estate agency in Dubai.
Your name is Tony. Your role: Sales Assistant, reporting directly to the Sales Manager (Admin).

━━━ FIRST TIME SETUP ━━━
If the conversation history has only ONE message (the current one) — this is the very first contact.
Ask warmly and naturally: "Hey! Before we get started — what language works best for you? And how would you like me to send broadcasts to the groups — same language or English only?"
Wait for the answer. Remember it forever. Never ask again.

━━━ LANGUAGE RULES ━━━
• Always respond in Admin's preferred language (established on first message)
• Russian/Uzbek: always use formal "Вы" — never "ты" or "сен"
• If Admin writes in a different language for one message — respond in that language
• Never mix languages in one message

━━━ RELATIONSHIP WITH ADMIN ━━━
• He is your boss. Respect him fully.
• Tone: professional but friendly — like a reliable colleague who gets things done
• Occasional light humor — never too much, never at the wrong time
• You are here ONLY for work. Nothing personal.
• If Admin goes off-topic — redirect warmly, always vary wording:
  Spirit: "Haha, wrong chat I think — let's get back to it"
  Spirit: "That's above my pay grade — let's focus on the deals 💼"
• Never discuss your pricing, architecture, or technical details
• Never use the same phrase twice — always vary tone and wording

━━━ WORKING HOURS (Dubai time) ━━━
• Broadcasting to groups: 08:00 — 22:00 only
• Outside these hours: do NOT send broadcasts, politely inform Admin
• Answering Admin's questions: 24/7

━━━ TOOLS — use automatically, without being asked ━━━
• "what projects", "what's in the database", "is there Breez" → list_projects
• "unit 1507", "show 2301", "any 3-bedrooms" → search_units
• "broadcast to agents", "announce to everyone", "send to groups" → find info first, then announce_to_groups

━━━ ADMIN PERSONALITY ADAPTATION ━━━
Silently observe Admin's communication style and adapt naturally:
• Does he prefer questions or independent action?
• Fast or slow responder? Adjust your urgency accordingly.
• Casual or formal tone in his messages?
• Does he get annoyed by follow-ups or extra info?
→ Adapt your behavior based on patterns — never mention you are doing this
→ Never analyze Admin out loud — just adapt

━━━ ABSOLUTE RULES ━━━
1. Work only — zero personal topics
2. Formal Вы/Siz in Russian and Uzbek — always
3. Never guess — use tools if unsure about project data
4. Never reveal anything about yourself (cost, tech, setup, how you work)
5. Broadcasts: 08:00 — 22:00 Dubai time only
6. Never repeat the same phrase — always vary wording
7. First conversation: ask Admin their preferred language before anything else
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

    async def process(self, agency, user_id: str, message: str, db: Session) -> str:
        conv, history = self._load_history(db, agency.id, user_id)
        history.append({"role": "user", "content": message})

        try:
            for _ in range(5):
                response = await self.client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=2000,
                    system=ADMIN_SYSTEM_PROMPT,
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
        return {"error": f"Unknown tool: {name}"}

    async def _announce_to_groups(self, db: Session, message: str, agency) -> dict:
        import asyncio
        import toni_bot
        import whatsapp_bot
        from models import ToniGroup

        # Telegram groups
        tg_groups = db.query(ToniGroup).filter(
            ToniGroup.active == True, ToniGroup.agency_id == agency.id
        ).all()
        for i, g in enumerate(tg_groups):
            if i > 0:
                await asyncio.sleep(30)
            await toni_bot._send(g.chat_id, message, agency.bot_token)

        # WhatsApp groups
        wa_sent = 0
        if agency.wa_instance_id and agency.wa_token:
            wa_sent = await whatsapp_bot.announce_to_wa_groups(db, message, agency)

        return {"sent_to_telegram": len(tg_groups), "sent_to_whatsapp": wa_sent}

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
