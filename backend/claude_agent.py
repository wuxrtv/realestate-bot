"""
Claude-powered conversation engine for real estate chatbot.
Manages multi-turn dialogue, tool execution, and lead qualification.
"""

import json
import os
import logging
from typing import Any
from datetime import datetime

import anthropic
from sqlalchemy.orm import Session

from models import Conversation, Lead, Appointment, Property
from property_service import PropertyService

logger = logging.getLogger(__name__)

AGENCY_NAME = os.getenv("AGENCY_NAME", "НедвижимостьПро")
AGENT_NAME = os.getenv("AGENT_NAME", "Менеджер")
AGENT_PHONE = os.getenv("AGENT_PHONE", "+79001234567")

SYSTEM_PROMPT = f"""Ты — профессиональный AI-помощник агентства недвижимости «{AGENCY_NAME}».

━━━ ЯЗЫК — САМОЕ ВАЖНОЕ ПРАВИЛО ━━━

Язык определяется по ПЕРВОМУ сообщению клиента и НЕ МЕНЯЕТСЯ до конца разговора:
• Первое сообщение на русском («Здравствуйте» и любое другое) → весь разговор ТОЛЬКО на русском
• Первое сообщение на узбекском («Салом» и любое другое) → весь разговор ТОЛЬКО на узбекском

СТРОГО ЗАПРЕЩЕНО — нарушение этих правил недопустимо:
❌ Спрашивать «на каком языке вы хотите общаться?» — язык уже определён, спрашивать не нужно
❌ Смешивать языки: брать русское слово и добавлять узбекский суффикс (-da, -ni, -ga, -lar, -ing и т.д.)
❌ Брать узбекское слово и добавлять русское окончание
❌ Примеры ОШИБОК: «недвижимостида», «квартирани», «районga», «агентствоmiz» — так писать НЕЛЬЗЯ
✅ Правильно: либо «в недвижимости» (русский), либо «ko'chmas mulkda» (узбекский) — только чистый язык

Обращение:
• Русский → ВСЕГДА на «Вы», «Вам», «Ваш» — никогда не «ты», «тебе», «твой»
• Узбекский → ВСЕГДА на «Siz», «Sizga», «Sizning» — вежливая форма

━━━ ЭТАПЫ РАЗГОВОРА (строго по порядку) ━━━

ШАГ 1 — ПРИВЕТСТВИЕ
• Поздоровайся на языке клиента, представься как помощник «{AGENCY_NAME}».
• СРАЗУ задай один вопрос о цели: что интересует — аренда, покупка, продажа или сдача?
• НЕ спрашивай про язык — он уже определён по первому сообщению.

ШАГ 2 — КВАЛИФИКАЦИЯ (по одному вопросу, не более 3–4 итого)
Для аренды/покупки — выясни по очереди:
  1. Район / локация (например: центр Ташкента, Юнусабад, Чиланзар…)
  2. Бюджет (примерный)
  3. Тип объекта (квартира, дом, коммерческое помещение) и площадь/комнаты
Для продажи/сдачи:
  1. Тип и адрес объекта
  2. Цель (продать / сдать в аренду)

ШАГ 3 — ПОИСК В БАЗЕ
• Вызови search_properties с собранными параметрами.
• Если объекты найдены — покажи 2–3 варианта с ключевыми параметрами (район, цена, площадь, особенности).
• Ответь на вопросы клиента по конкретным объектам.
• Если клиент заинтересован — предложи встречу или звонок с агентом.

ШАГ 4 — ЕСЛИ ОБЪЕКТОВ НЕТ В БАЗЕ
• Скажи что сейчас подходящего варианта нет, но можно подобрать. Попроси оставить номер телефона.
• Возьми имя и номер телефона клиента.
• Вызови save_lead, потом transfer_to_agent с пометкой «нет объекта в базе».

ШАГ 5 — ФИНАЛ
• Если клиент готов — запиши на просмотр через book_viewing.
• Если хочет живого агента — вызови transfer_to_agent немедленно.
• Если хочет продать/сдать — вызови request_valuation.

━━━ ПРАВИЛА ━━━
• Только ОДИН вопрос за раз — никогда не задавай сразу несколько.
• Не задавай лишних вопросов — максимум 3–4 для квалификации.
• Будь тёплым, конкретным, без лишних слов.
• После получения контакта всегда вызывай save_lead.
• Если клиент написал «агент», «живой человек», «позвоните», «operator», «qo'ng'iring» — немедленно вызови transfer_to_agent.

━━━ ЧАСТЫЕ ВОПРОСЫ ━━━
• Комиссия агентства: уточняется индивидуально, зависит от объекта.
• Работаем с ипотекой — помогаем с оформлением.
• Юридическое сопровождение сделки — входит в услугу.
• Работаем 24/7 — бот всегда на связи, агент перезвонит в рабочее время.
"""

TOOLS = [
    {
        "name": "search_properties",
        "description": "Поиск подходящих объектов недвижимости в базе агентства по критериям клиента.",
        "input_schema": {
            "type": "object",
            "properties": {
                "goal": {
                    "type": "string",
                    "enum": ["buy", "rent"],
                    "description": "Цель: buy — купить, rent — арендовать"
                },
                "budget_max": {
                    "type": "number",
                    "description": "Максимальный бюджет в рублях"
                },
                "budget_min": {
                    "type": "number",
                    "description": "Минимальный бюджет (необязательно)"
                },
                "area": {
                    "type": "string",
                    "description": "Желаемый район или микрорайон"
                },
                "property_type": {
                    "type": "string",
                    "enum": ["apartment", "house", "commercial"],
                    "description": "apartment — квартира, house — дом/коттедж, commercial — коммерция"
                },
                "rooms": {
                    "type": "integer",
                    "description": "Количество комнат (0 = студия)"
                }
            },
            "required": ["goal"]
        }
    },
    {
        "name": "save_lead",
        "description": "Сохранить данные клиента (лид) в CRM после сбора контактной информации.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Имя клиента"},
                "phone": {"type": "string", "description": "Телефон"},
                "email": {"type": "string", "description": "Email (необязательно)"},
                "goal": {
                    "type": "string",
                    "enum": ["buy", "rent", "sell", "lease_out"],
                    "description": "buy/rent/sell/lease_out"
                },
                "budget_max": {"type": "number"},
                "budget_min": {"type": "number"},
                "area": {"type": "string"},
                "property_type": {"type": "string"},
                "rooms": {"type": "integer"},
                "urgency": {
                    "type": "string",
                    "enum": ["asap", "1month", "3months", "just_looking"],
                    "description": "Срочность: asap/1month/3months/just_looking"
                }
            },
            "required": ["name", "goal"]
        }
    },
    {
        "name": "book_viewing",
        "description": "Записать клиента на просмотр конкретного объекта.",
        "input_schema": {
            "type": "object",
            "properties": {
                "property_id": {
                    "type": "string",
                    "description": "ID объекта из результатов поиска"
                },
                "client_name": {"type": "string"},
                "client_phone": {"type": "string"},
                "preferred_date": {
                    "type": "string",
                    "description": "Желаемая дата и время, например: 'завтра в 15:00' или '2025-05-10 14:00'"
                },
                "property_title": {"type": "string", "description": "Название объекта для подтверждения"}
            },
            "required": ["property_id", "client_name", "client_phone"]
        }
    },
    {
        "name": "transfer_to_agent",
        "description": "Немедленно переключить клиента на живого агента и уведомить агента.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "client_phone": {"type": "string"},
                "reason": {
                    "type": "string",
                    "description": "Причина переключения (запрос клиента, сложный вопрос, и т.д.)"
                },
                "summary": {
                    "type": "string",
                    "description": "Краткое резюме разговора для агента: цель, параметры, что обсуждали"
                }
            },
            "required": ["reason", "summary"]
        }
    },
    {
        "name": "request_valuation",
        "description": "Принять заявку на оценку недвижимости (от клиентов, желающих продать или сдать объект).",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "client_phone": {"type": "string"},
                "property_address": {"type": "string", "description": "Адрес объекта"},
                "property_type": {
                    "type": "string",
                    "enum": ["apartment", "house", "commercial"]
                },
                "goal": {
                    "type": "string",
                    "enum": ["sell", "lease_out"],
                    "description": "sell — продать, lease_out — сдать в аренду"
                },
                "notes": {"type": "string", "description": "Дополнительная информация об объекте"}
            },
            "required": ["client_name", "client_phone", "property_address", "goal"]
        }
    }
]


class ConversationManager:
    def __init__(self):
        self.client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.property_service = None

    def _get_property_service(self, db: Session) -> PropertyService:
        return PropertyService(db)

    @staticmethod
    def _serialize_blocks(content) -> list:
        result = []
        for block in content:
            if block.type == "text":
                result.append({"type": "text", "text": block.text})
            elif block.type == "tool_use":
                result.append({"type": "tool_use", "id": block.id, "name": block.name, "input": block.input})
        return result

    def _get_or_create_conversation(self, db: Session, user_id: str, platform: str) -> Conversation:
        conv = db.query(Conversation).filter(Conversation.user_id == user_id).first()
        if not conv:
            conv = Conversation(user_id=user_id, platform=platform, history=[], lead_data={})
            db.add(conv)
            db.commit()
            db.refresh(conv)
        return conv

    def _execute_tool(self, tool_name: str, tool_input: dict, db: Session, user_id: str) -> dict:
        """Execute a tool call and return the result."""
        logger.info(f"Tool call: {tool_name} | input: {tool_input}")

        if tool_name == "search_properties":
            ps = self._get_property_service(db)
            props = ps.search(
                goal=tool_input.get("goal", "buy"),
                budget_min=tool_input.get("budget_min"),
                budget_max=tool_input.get("budget_max"),
                area=tool_input.get("area"),
                property_type=tool_input.get("property_type"),
                rooms=tool_input.get("rooms"),
                limit=3
            )
            if not props:
                return {"found": 0, "message": "Объектов по заданным критериям не найдено. Попробуем расширить поиск?"}
            return {"found": len(props), "properties": props}

        if tool_name == "save_lead":
            self._upsert_lead(db, user_id, tool_input)
            result = {"saved": True, "message": "Данные клиента сохранены в CRM"}
            if tool_input.get("phone"):
                goal_map = {"buy": "покупка", "rent": "аренда", "sell": "продажа", "lease_out": "сдача"}
                goal_label = goal_map.get(tool_input.get("goal", ""), tool_input.get("goal", "—"))
                result["_notify_agent"] = True
                result["_agent_summary"] = (
                    f"📋 *Новый лид*\n"
                    f"👤 Имя: {tool_input.get('name', '—')}\n"
                    f"📞 Телефон: {tool_input.get('phone', '—')}\n"
                    f"🎯 Цель: {goal_label}\n"
                    f"📍 Район: {tool_input.get('area', '—')}\n"
                    f"💰 Бюджет: {tool_input.get('budget_max', '—')}\n"
                    f"🏠 Тип: {tool_input.get('property_type', '—')}"
                )
            return result

        if tool_name == "book_viewing":
            result = self._create_appointment(db, user_id, tool_input)
            return result

        if tool_name == "transfer_to_agent":
            self._notify_agent_transfer(db, user_id, tool_input)
            return {
                "transferred": True,
                "agent_name": AGENT_NAME,
                "agent_phone": AGENT_PHONE,
                "message": f"Агент {AGENT_NAME} уведомлён и свяжется с клиентом в ближайшее время"
            }

        if tool_name == "request_valuation":
            self._save_valuation_request(db, user_id, tool_input)
            return {
                "received": True,
                "message": f"Заявка на оценку принята. Агент {AGENT_NAME} свяжется для согласования удобного времени осмотра"
            }

        return {"error": f"Unknown tool: {tool_name}"}

    def _upsert_lead(self, db: Session, user_id: str, data: dict):
        lead = db.query(Lead).filter(Lead.user_id == user_id).first()
        if not lead:
            lead = Lead(user_id=user_id)
            db.add(lead)
        for field in ("name", "phone", "email", "goal", "area", "property_type", "urgency"):
            if data.get(field):
                setattr(lead, field, data[field])
        if data.get("budget_max"):
            lead.budget_max = data["budget_max"]
        if data.get("budget_min"):
            lead.budget_min = data["budget_min"]
        if data.get("rooms") is not None:
            lead.rooms = data["rooms"]
        lead.status = "qualified"
        lead.last_contact = datetime.utcnow()
        db.commit()

    def _create_appointment(self, db: Session, user_id: str, data: dict) -> dict:
        lead = db.query(Lead).filter(Lead.user_id == user_id).first()
        prop = db.query(Property).filter(
            Property.external_id == str(data["property_id"])
        ).first()

        appt = Appointment(
            user_id=user_id,
            lead_id=lead.id if lead else None,
            property_id=prop.id if prop else None,
            client_name=data.get("client_name", ""),
            client_phone=data.get("client_phone", ""),
            notes=data.get("preferred_date", ""),
        )
        db.add(appt)
        if lead:
            lead.status = "viewing_scheduled"
        db.commit()
        return {
            "booked": True,
            "appointment_id": appt.id,
            "message": "Просмотр записан. Агент подтвердит время и пришлёт напоминание за день и за час до встречи."
        }

    def _save_valuation_request(self, db: Session, user_id: str, data: dict):
        lead = db.query(Lead).filter(Lead.user_id == user_id).first()
        if not lead:
            lead = Lead(user_id=user_id)
            db.add(lead)
        lead.name = data.get("client_name", lead.name)
        lead.phone = data.get("client_phone", lead.phone)
        lead.goal = data.get("goal", "sell")
        lead.property_type = data.get("property_type")
        lead.notes = f"Оценка: {data.get('property_address', '')}. {data.get('notes', '')}"
        lead.status = "qualified"
        db.commit()

    def _notify_agent_transfer(self, db: Session, user_id: str, data: dict):
        """Mark lead for urgent agent callback."""
        lead = db.query(Lead).filter(Lead.user_id == user_id).first()
        if lead:
            lead.notes = (lead.notes or "") + f"\n[ПЕРЕДАЧА АГЕНТУ] {data.get('summary', '')}"
            lead.status = "qualified"
            db.commit()

    def _extract_properties_from_tool_results(self, tool_results: list) -> list:
        """Pull property objects from tool call results for rich message formatting."""
        properties = []
        for res in tool_results:
            if isinstance(res.get("content"), str):
                try:
                    parsed = json.loads(res["content"])
                    if parsed.get("properties"):
                        properties.extend(parsed["properties"])
                except Exception:
                    pass
        return properties

    async def process_message(self, user_id: str, message: str, platform: str, db: Session) -> dict:
        """
        Main entry point: receive a user message, run Claude agentic loop, return formatted response.
        Returns a dict with:
          - messages: list of {type, content/photo_url/caption/latitude/longitude}
          - notify_agent: bool
          - agent_summary: str (if notify_agent)
        """
        conv = self._get_or_create_conversation(db, user_id, platform)

        history: list = list(conv.history or [])
        history.append({"role": "user", "content": message})

        collected_tool_results = []
        transfer_to_agent = False
        agent_summary = ""
        all_properties: list[dict] = []
        appointment_confirmed = False

        max_iterations = 6
        for _ in range(max_iterations):
            response = await self.client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1500,
                system=[
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},  # prompt caching
                    }
                ],
                tools=TOOLS,
                messages=history,
            )

            # Append assistant message to history
            history.append({"role": "assistant", "content": self._serialize_blocks(response.content)})

            if response.stop_reason == "end_turn":
                break

            if response.stop_reason == "tool_use":
                tool_results_payload = []
                for block in response.content:
                    if block.type != "tool_use":
                        continue
                    result = self._execute_tool(block.name, block.input, db, user_id)
                    collected_tool_results.append({"tool": block.name, "content": json.dumps(result)})

                    if block.name == "search_properties" and result.get("properties"):
                        all_properties.extend(result["properties"])

                    if block.name == "transfer_to_agent":
                        transfer_to_agent = True
                        agent_summary = block.input.get("summary", "")

                    if block.name == "save_lead" and result.get("_notify_agent"):
                        transfer_to_agent = True
                        agent_summary = result.get("_agent_summary", "")

                    if block.name == "book_viewing" and result.get("booked"):
                        appointment_confirmed = True

                    tool_results_payload.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, ensure_ascii=False)
                    })

                history.append({"role": "user", "content": tool_results_payload})
                continue

            break

        # Extract final text from last assistant message
        final_text = ""
        for block in response.content:
            if hasattr(block, "text"):
                final_text += block.text

        # Save updated history (keep last 40 messages to avoid token bloat)
        conv.history = history[-40:]
        conv.last_message_at = datetime.utcnow()
        db.commit()

        return self._build_response(
            text=final_text,
            properties=all_properties,
            transfer_to_agent=transfer_to_agent,
            agent_summary=agent_summary,
            appointment_confirmed=appointment_confirmed,
        )

    def _build_response(
        self,
        text: str,
        properties: list,
        transfer_to_agent: bool,
        agent_summary: str,
        appointment_confirmed: bool,
    ) -> dict:
        """Build the structured response that n8n will consume."""
        messages = []

        if text:
            messages.append({"type": "text", "content": text})

        # Attach property cards after main text
        for prop in properties:
            photo_url = (prop.get("photos") or [None])[0]
            caption = self._format_property_caption(prop)
            if photo_url:
                messages.append({
                    "type": "photo",
                    "photo_url": photo_url,
                    "caption": caption,
                    "property_id": prop.get("external_id"),
                })
            else:
                messages.append({"type": "text", "content": caption})

            if prop.get("latitude") and prop.get("longitude"):
                messages.append({
                    "type": "location",
                    "latitude": prop["latitude"],
                    "longitude": prop["longitude"],
                    "title": prop.get("address", "Расположение объекта"),
                })

        return {
            "messages": messages,
            "notify_agent": transfer_to_agent or appointment_confirmed,
            "agent_summary": agent_summary,
            "appointment_confirmed": appointment_confirmed,
        }

    @staticmethod
    def _format_property_caption(prop: dict) -> str:
        price_str = ""
        if prop.get("listing_type") == "sale" and prop.get("price"):
            price_str = f"💰 {prop['price']:,.0f} ₽".replace(",", " ")
        elif prop.get("rent_price"):
            price_str = f"💰 {prop['rent_price']:,.0f} ₽/мес".replace(",", " ")

        rooms_str = f"{prop['rooms']}-комн. " if prop.get("rooms") else ""
        sq = f"{prop['square_meters']} м²" if prop.get("square_meters") else ""
        floor_str = ""
        if prop.get("floor") and prop.get("total_floors"):
            floor_str = f"  {prop['floor']}/{prop['total_floors']} эт."

        features = ""
        if prop.get("features"):
            features = "\n✅ " + "\n✅ ".join(prop["features"][:3])

        return (
            f"🏠 *{prop.get('title', 'Объект')}*\n"
            f"{rooms_str}{sq}{floor_str}\n"
            f"📍 {prop.get('address', prop.get('area', ''))}\n"
            f"{price_str}"
            f"{features}\n\n"
            f"ID: `{prop.get('external_id', '')}`"
        )
