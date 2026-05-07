"""
Admin AI agent — handles requests from the agency administrator.
Routes admin Telegram messages to a separate Claude instance with admin tools.
"""

import json
import logging
import os

import anthropic
from sqlalchemy.orm import Session

from models import ToniProject
from telegram_bot import send_message

logger = logging.getLogger(__name__)

ADMIN_IDS = set(os.getenv("ADMIN_IDS", "7567850330").split(","))

ADMIN_SYSTEM_PROMPT = """Ты — личный AI-ассистент руководителя агентства недвижимости.
Ты общаешься с администратором как умный коллега — понимаешь смысл его слов, а не ждёшь точных команд.

━━━ КАК ТЫ ДУМАЕШЬ ━━━
• Если спрашивают «какие проекты», «что в базе», «сколько юнитов», «есть ли Breez» — вызови list_projects.

• Если спрашивают конкретный юнит («есть 1507?», «покажи 2301», «3-комнатные есть?») — вызови search_units.

• Если говорят «разошли в группы», «отправь агентам», «объяви всем», «проект X отправь во все группы» — сначала найди инфо через list_projects или search_units, составь красивое объявление и вызови announce_to_groups.

• Если непонятно — уточни одним коротким вопросом.

━━━ СТИЛЬ ОБЩЕНИЯ ━━━
• Говори как умный коллега, не как робот.
• Не объясняй что ты делаешь — просто делай и кратко сообщи результат.
• Отвечай только на русском языке.
• Будь конкретным и лаконичным.
"""

ADMIN_TOOLS = [
    {
        "name": "announce_to_groups",
        "description": "Отправить сообщение или объявление во все активные группы агентов. Используй когда просят 'разошли в группы', 'отправь во все группы', 'объяви агентам'.",
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
        "description": "Поиск юнитов по номеру или ключевым словам (комнаты, цена, этаж, площадь) во всех проектах.",
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


def is_admin(user_id: str) -> bool:
    return user_id in ADMIN_IDS


class AdminAgent:
    def __init__(self):
        self.client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self._history: dict[str, list] = {}

    async def process(self, user_id: str, message: str, db: Session) -> str:
        history = self._history.get(user_id, [])
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
                        result = await self._run_tool(block.name, block.input, db)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(result, ensure_ascii=False, default=str),
                        })
                    history.append({"role": "user", "content": tool_results})
                    continue
                break

            self._history[user_id] = history[-30:]

            final = ""
            for block in response.content:
                if hasattr(block, "text"):
                    final += block.text
            return final or "Готово."

        except Exception:
            # Clear corrupted history so the next message starts fresh
            self._history.pop(user_id, None)
            logger.exception(f"AdminAgent API error for user {user_id}")
            raise

    async def _run_tool(self, name: str, inp: dict, db: Session) -> dict:
        if name == "announce_to_groups":
            return await self._announce_to_groups(db, inp["message"])

        if name == "list_projects":
            return self._list_projects(db)

        if name == "search_units":
            return self._search_units(db, inp["query"], inp.get("limit", 5))

        return {"error": f"Unknown tool: {name}"}

    async def _announce_to_groups(self, db: Session, message: str) -> dict:
        import toni_bot
        from models import ToniGroup
        groups = db.query(ToniGroup).filter(ToniGroup.active == True).all()
        for g in groups:
            await toni_bot._send(g.chat_id, message)
        return {"sent_to_groups": len(groups)}

    def _list_projects(self, db: Session) -> dict:
        projects = (
            db.query(ToniProject)
            .filter(ToniProject.is_active == True)
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

    def _search_units(self, db: Session, query: str, limit: int) -> dict:
        import re as _re
        from excel_parser import format_unit_card

        projects = (
            db.query(ToniProject).filter(ToniProject.is_active == True).all()
        )
        results = []

        # Direct unit number match
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

        # Keyword search
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
