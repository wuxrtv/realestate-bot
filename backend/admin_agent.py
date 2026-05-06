"""
Admin AI agent — handles requests from the agency administrator.
Routes admin Telegram messages to a separate Claude instance with admin tools.
"""

import json
import logging
import os
from datetime import datetime, timedelta

import anthropic
from sqlalchemy.orm import Session

from models import Appointment, Conversation, Lead, Property, ToniProject
from telegram_bot import send_message

logger = logging.getLogger(__name__)

ADMIN_IDS = set(os.getenv("ADMIN_IDS", "7567850330").split(","))

ADMIN_SYSTEM_PROMPT = """Ты — личный AI-ассистент руководителя агентства недвижимости.
Ты общаешься с администратором как умный коллега — понимаешь смысл его слов, а не ждёшь точных команд.

━━━ КАК ТЫ ДУМАЕШЬ ━━━
Ты анализируешь каждое сообщение и сам решаешь что нужно сделать:

• Если админ описывает объект (квартира, дом, офис, цена, район, метраж, преимущества) — это сигнал добавить объект в базу через add_property. Не жди явной команды «добавь». Если в тексте есть параметры недвижимости — действуй.

• Если админ упоминает клиента по имени или говорит «что там с [именем]», «как [имя]», «посмотри по [имя]» — вызови get_conversation чтобы найти переписку.

• Если админ говорит «напиши», «скинь», «отправь», «передай» кому-то что-то — вызови send_to_client.

• Если админ говорит «как дела», «что по лидам», «кто есть», «статистика», «отчёт», «за неделю» — вызови get_report или get_leads.

• Если спрашивают «какие проекты», «что в базе», «сколько юнитов», «есть ли Breez» — вызови list_projects.

• Если спрашивают конкретный юнит («есть 1507?», «покажи 2301», «3-комнатные есть?») — вызови search_units.

• Если непонятно — уточни одним коротким вопросом.

━━━ СТИЛЬ ОБЩЕНИЯ ━━━
• Говори как умный коллега, не как робот.
• Не объясняй что ты делаешь — просто делай и кратко сообщи результат.
• Если добавил объект — скажи «Добавил» и назови название.
• Если нашёл переписку — покажи её кратко.
• Отвечай только на русском языке.
• Будь конкретным и лаконичным.

━━━ ПРИМЕРЫ ПОНИМАНИЯ ━━━
«Слушай, тут есть хорошая 2-комнатная в Юнусабаде, 65 метров, 3 этаж из 9, цена 85 тысяч» → add_property
«Что там с Азизом, он давно не писал» → get_conversation с query="Азиз"
«Напомни Санжару что завтра просмотр в 14:00» → найди Санжара через get_conversation, потом send_to_client
«Как там дела вообще за эту неделю» → get_report period=week
«Покажи кто у нас есть из новых клиентов» → get_leads status=new
"""

ADMIN_TOOLS = [
    {
        "name": "get_leads",
        "description": "Получить список клиентов (лидов) из CRM. Можно фильтровать по статусу.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Фильтр по статусу: new, qualified, viewing_scheduled, converted, lost. Пусто = все.",
                },
                "limit": {"type": "integer", "description": "Сколько записей вернуть (по умолчанию 20)"},
            },
        },
    },
    {
        "name": "get_conversation",
        "description": "Получить историю переписки бота с клиентом по имени, телефону или Telegram ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Имя, телефон или Telegram ID клиента",
                }
            },
            "required": ["query"],
        },
    },
    {
        "name": "send_to_client",
        "description": "Отправить сообщение клиенту от имени бота в Telegram.",
        "input_schema": {
            "type": "object",
            "properties": {
                "user_id": {"type": "string", "description": "Telegram ID клиента"},
                "message": {"type": "string", "description": "Текст сообщения"},
            },
            "required": ["user_id", "message"],
        },
    },
    {
        "name": "add_property",
        "description": "Добавить новый объект недвижимости в базу агентства.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Название объекта"},
                "listing_type": {"type": "string", "enum": ["sale", "rent"], "description": "sale — продажа, rent — аренда"},
                "property_type": {"type": "string", "enum": ["apartment", "house", "commercial"], "description": "Тип объекта"},
                "price": {"type": "number", "description": "Цена продажи"},
                "rent_price": {"type": "number", "description": "Цена аренды в месяц"},
                "area": {"type": "string", "description": "Район"},
                "address": {"type": "string", "description": "Адрес"},
                "rooms": {"type": "integer", "description": "Количество комнат"},
                "square_meters": {"type": "number", "description": "Площадь м²"},
                "floor": {"type": "integer", "description": "Этаж"},
                "total_floors": {"type": "integer", "description": "Всего этажей в доме"},
                "description": {"type": "string", "description": "Описание объекта и его категория (инвестиции, для жизни, и т.д.)"},
                "features": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Список преимуществ объекта",
                },
                "photos": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Список URL фотографий",
                },
            },
            "required": ["title", "listing_type", "property_type"],
        },
    },
    {
        "name": "get_report",
        "description": "Получить статистику и отчёт за указанный период.",
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {
                    "type": "string",
                    "enum": ["week", "month", "all"],
                    "description": "Период: week — неделя, month — месяц, all — всё время",
                }
            },
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

    async def _run_tool(self, name: str, inp: dict, db: Session) -> dict:
        if name == "get_leads":
            return self._get_leads(db, inp.get("status"), inp.get("limit", 20))

        if name == "get_conversation":
            return self._get_conversation(db, inp["query"])

        if name == "send_to_client":
            await send_message(inp["user_id"], inp["message"])
            return {"sent": True, "to": inp["user_id"]}

        if name == "add_property":
            return self._add_property(db, inp)

        if name == "get_report":
            return self._get_report(db, inp.get("period", "week"))

        if name == "list_projects":
            return self._list_projects(db)

        if name == "search_units":
            return self._search_units(db, inp["query"], inp.get("limit", 5))

        return {"error": f"Unknown tool: {name}"}

    def _get_leads(self, db: Session, status: str | None, limit: int) -> dict:
        q = db.query(Lead)
        if status:
            q = q.filter(Lead.status == status)
        leads = q.order_by(Lead.created_at.desc()).limit(limit).all()
        return {
            "count": len(leads),
            "leads": [
                {
                    "id": l.id,
                    "name": l.name or "—",
                    "phone": l.phone or "—",
                    "user_id": l.user_id,
                    "goal": l.goal,
                    "area": l.area,
                    "budget_max": l.budget_max,
                    "status": l.status,
                    "created_at": l.created_at.isoformat() if l.created_at else None,
                }
                for l in leads
            ],
        }

    def _get_conversation(self, db: Session, query: str) -> dict:
        lead = (
            db.query(Lead)
            .filter(
                Lead.name.ilike(f"%{query}%")
                | Lead.phone.ilike(f"%{query}%")
                | (Lead.user_id == query)
            )
            .first()
        )
        if not lead:
            return {"found": False, "message": f"Клиент «{query}» не найден в базе"}

        conv = db.query(Conversation).filter(Conversation.user_id == lead.user_id).first()
        if not conv or not conv.history:
            return {"found": True, "name": lead.name, "history": [], "message": "История переписки пуста"}

        messages = []
        for msg in conv.history:
            role = "Клиент" if msg["role"] == "user" else "Бот"
            content = msg.get("content", "")
            if isinstance(content, list):
                text = " ".join(b.get("text", "") for b in content if b.get("type") == "text")
            else:
                text = str(content)
            if text.strip():
                messages.append(f"{role}: {text.strip()}")

        return {
            "found": True,
            "name": lead.name,
            "phone": lead.phone,
            "user_id": lead.user_id,
            "history": messages,
        }

    def _add_property(self, db: Session, data: dict) -> dict:
        external_id = f"admin-{int(datetime.utcnow().timestamp())}"
        prop = Property(
            external_id=external_id,
            title=data.get("title", "Объект"),
            listing_type=data.get("listing_type", "sale"),
            property_type=data.get("property_type", "apartment"),
            price=data.get("price"),
            rent_price=data.get("rent_price"),
            area=data.get("area", ""),
            address=data.get("address", ""),
            rooms=data.get("rooms"),
            square_meters=data.get("square_meters"),
            floor=data.get("floor"),
            total_floors=data.get("total_floors"),
            description=data.get("description", ""),
            features=data.get("features", []),
            photos=data.get("photos", []),
            status="active",
        )
        db.add(prop)
        db.commit()
        return {"added": True, "external_id": external_id, "title": prop.title}

    def _get_report(self, db: Session, period: str) -> dict:
        if period == "week":
            since = datetime.utcnow() - timedelta(days=7)
        elif period == "month":
            since = datetime.utcnow() - timedelta(days=30)
        else:
            since = datetime(2000, 1, 1)

        leads = db.query(Lead).filter(Lead.created_at >= since).all()
        appts = db.query(Appointment).filter(Appointment.created_at >= since).all()

        by_status: dict[str, int] = {}
        by_goal: dict[str, int] = {}
        for l in leads:
            by_status[l.status or "new"] = by_status.get(l.status or "new", 0) + 1
            by_goal[l.goal or "—"] = by_goal.get(l.goal or "—", 0) + 1

        return {
            "period": period,
            "total_leads": len(leads),
            "by_status": by_status,
            "by_goal": by_goal,
            "appointments": len(appts),
            "leads_with_phone": sum(1 for l in leads if l.phone),
        }

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
