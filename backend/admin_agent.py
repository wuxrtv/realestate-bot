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

logger = logging.getLogger(__name__)

ADMIN_SYSTEM_PROMPT = """Ты — личный AI-ассистент руководителя агентства недвижимости.
Отвечай на любые вопросы администратора — про недвижимость, бизнес, агентов, рынок, или просто поболтать.

━━━ ИНСТРУМЕНТЫ ━━━
Используй их сам, без команды — по смыслу сообщения:
• «какие проекты», «что в базе», «есть ли Breez» → list_projects
• «юнит 1507», «покажи 2301», «3-комнатные есть» → search_units
• «разошли агентам», «объяви всем», «отправь в группы» → сначала найди инфо, потом announce_to_groups

━━━ СТИЛЬ ━━━
• Говори как умный коллега — живо, кратко, по делу.
• Не объясняй что делаешь — просто делай и сообщи результат.
• ЯЗЫК: если администратор пишет по-русски — отвечай по-русски, если по-английски — по-английски.
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
        self._history: dict[str, list] = {}

    async def process(self, agency, user_id: str, message: str, db: Session) -> str:
        history_key = f"{agency.id}:{user_id}"
        history = self._history.get(history_key, [])
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

            self._history[history_key] = history[-30:]

            final = ""
            for block in response.content:
                if hasattr(block, "text"):
                    final += block.text
            return final or "Готово."

        except Exception:
            self._history.pop(history_key, None)
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
        import toni_bot
        from models import ToniGroup
        groups = db.query(ToniGroup).filter(
            ToniGroup.active == True, ToniGroup.agency_id == agency.id
        ).all()
        for g in groups:
            await toni_bot._send(g.chat_id, message, agency.bot_token)
        return {"sent_to_groups": len(groups)}

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
