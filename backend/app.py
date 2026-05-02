"""
FastAPI application — main entrypoint.
Telegram webhook + APScheduler for reminders/follow-ups. No n8n needed.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session

from admin_agent import AdminAgent, is_admin
from claude_agent import ConversationManager
from database import SessionLocal, get_db, init_db
from models import Appointment, Lead, Property
from property_service import PropertyService
from scheduler_service import (
    get_appointment_reminders,
    get_followup_leads,
    get_price_drop_notifications,
)
from telegram_bot import (
    answer_callback_query,
    dispatch_response,
    send_message,
    send_message_with_keyboard,
    send_typing,
    set_webhook,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conversation_manager = ConversationManager()
admin_agent = AdminAgent()
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


# ─── Scheduled jobs ──────────────────────────────────────────────────────────

async def job_reminders():
    db = SessionLocal()
    try:
        reminders = get_appointment_reminders(db)
        for r in reminders:
            if r["type"] == "day_reminder":
                text = (
                    f"⏰ *Напоминание о просмотре*\n\n"
                    f"Здравствуйте, {r['client_name']}!\n\n"
                    f"Завтра состоится просмотр объекта:\n"
                    f"🏠 *{r['property_title']}*\n"
                    f"📍 {r['property_address']}\n\n"
                    f"Если нужно перенести — просто напишите нам."
                )
            else:
                text = (
                    f"🔔 *Просмотр через 1 час!*\n\n"
                    f"{r['client_name']}, напоминаем — скоро просмотр:\n"
                    f"🏠 *{r['property_title']}*\n"
                    f"📍 {r['property_address']}\n\n"
                    f"Агент будет вас ждать. Удачного просмотра!"
                )
            await send_message(r["user_id"], text)
    finally:
        db.close()


async def job_followups():
    db = SessionLocal()
    try:
        leads = get_followup_leads(db)
        for lead in leads:
            goal_text = "аренду" if lead["goal"] == "rent" else "покупку"
            text = (
                f"👋 Здравствуйте, {lead['name']}!\n\n"
                f"Вы недавно интересовались {goal_text} недвижимости.\n\n"
                f"Есть новые подходящие варианты — хотите посмотреть? "
                f"Просто напишите 🏠"
            )
            await send_message(lead["user_id"], text)
    finally:
        db.close()


async def job_price_drops():
    db = SessionLocal()
    try:
        notifications = get_price_drop_notifications(db)
        for n in notifications:
            old = f"{n['old_price']:,.0f}".replace(",", " ")
            new = f"{n['new_price']:,.0f}".replace(",", " ")
            text = (
                f"📉 *Снижение цены!*\n\n"
                f"Здравствуйте, {n['client_name']}!\n\n"
                f"На интересный вам объект снизилась цена на *{n['drop_pct']}%*:\n\n"
                f"🏠 *{n['property_title']}*\n"
                f"📍 {n['property_address']}\n"
                f"~~{old} ₽~~ → *{new} ₽*\n\n"
                f"Хотите записаться на просмотр? Напишите нам!"
            )
            await send_message(n["user_id"], text)
    finally:
        db.close()


# ─── App lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Init DB and seed properties
    init_db()
    db = SessionLocal()
    try:
        PropertyService(db).load_sample_data()
    finally:
        db.close()

    # Register Telegram webhook
    railway_url = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("PUBLIC_URL")
    if railway_url and os.getenv("TELEGRAM_BOT_TOKEN"):
        webhook_url = f"https://{railway_url}/telegram/webhook"
        await set_webhook(webhook_url)
        logger.info(f"Telegram webhook set: {webhook_url}")

    # Start background scheduler
    scheduler.add_job(job_reminders, "interval", minutes=15, id="reminders")
    scheduler.add_job(job_followups, "interval", hours=1, id="followups")
    scheduler.add_job(job_price_drops, "cron", hour=9, minute=0, id="price_drops")
    scheduler.start()
    logger.info("Scheduler started")

    yield

    scheduler.shutdown()


app = FastAPI(
    title="Real Estate Bot API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Telegram webhook endpoint ────────────────────────────────────────────────

WELCOME_KEYBOARD = [
    [
        {"text": "🇷🇺 Здравствуйте", "callback_data": "greet_ru"},
        {"text": "🇺🇿 Салом", "callback_data": "greet_uz"},
    ]
]

WELCOME_TEXT = (
    "👋 Добро пожаловать в *{agency}*!\n"
    "Xush kelibsiz *{agency}*ga!\n\n"
    "Выберите язык / Tilni tanlang:"
)


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    """Telegram sends every user message here."""
    data = await request.json()

    # ── Handle inline button presses ──────────────────────────────────────────
    if callback := data.get("callback_query"):
        user_id = str(callback["from"]["id"])
        first_name = callback["from"].get("first_name", "")
        cb_data = callback.get("data", "")
        await answer_callback_query(callback["id"])

        text_map = {
            "greet_ru": "Здравствуйте",
            "greet_uz": "Салом",
        }
        text = text_map.get(cb_data)
        if not text:
            return {"ok": True}

        await send_typing(user_id)
        try:
            result = await conversation_manager.process_message(
                user_id=user_id,
                message=text,
                platform="telegram",
                db=db,
            )
            await dispatch_response(user_id, result, client_name=first_name)
        except Exception as e:
            logger.exception(f"Callback error for user {user_id}: {e}")
        return {"ok": True}

    # ── Handle regular messages ────────────────────────────────────────────────
    message = data.get("message") or data.get("edited_message")
    if not message:
        return {"ok": True}

    user_id = str(message["from"]["id"])
    text = message.get("text") or message.get("caption", "")
    first_name = message["from"].get("first_name", "")

    if not text:
        return {"ok": True}

    # /start — show language selection buttons (only for regular users)
    if text.strip() == "/start" and not is_admin(user_id):
        agency = os.getenv("AGENCY_NAME", "Агентство недвижимости")
        await send_message_with_keyboard(
            user_id,
            WELCOME_TEXT.format(agency=agency),
            WELCOME_KEYBOARD,
        )
        return {"ok": True}

    await send_typing(user_id)

    # ── Admin route ───────────────────────────────────────────────────────────
    if is_admin(user_id):
        try:
            reply = await admin_agent.process(user_id=user_id, message=text, db=db)
            await send_message(user_id, reply)
        except Exception as e:
            logger.exception(f"Admin agent error: {e}")
            await send_message(user_id, f"Ошибка: {e}")
        return {"ok": True}

    # ── Regular client route ──────────────────────────────────────────────────
    try:
        result = await conversation_manager.process_message(
            user_id=user_id,
            message=text,
            platform="telegram",
            db=db,
        )
        await dispatch_response(user_id, result, client_name=first_name)

        # When bot notifies admin about a new lead, inject that notification
        # into the admin agent's conversation history so the admin bot
        # remembers which client was just discussed.
        if result.get("notify_agent"):
            admin_id = os.getenv("AGENT_TELEGRAM_ID", "7567850330")
            summary = result.get("agent_summary", "")
            notification_text = (
                f"🔔 *Новый лид / нужен звонок*\n\n"
                f"👤 Клиент: {first_name}\n"
                f"🆔 Telegram ID: `{user_id}`\n\n"
                f"{summary}\n\n"
                f"📞 Свяжитесь с клиентом как можно скорее!"
            )
            hist = admin_agent._history.setdefault(admin_id, [])
            hist.append({
                "role": "assistant",
                "content": [{"type": "text", "text": notification_text}],
            })
            admin_agent._history[admin_id] = hist[-30:]
    except Exception as e:
        logger.exception(f"Error for user {user_id}: {e}")
        await send_message(
            user_id,
            "Извините, произошла ошибка. Попробуйте написать ещё раз.",
        )

    return {"ok": True}


# ─── Property management ──────────────────────────────────────────────────────

class PropertyUpdateRequest(BaseModel):
    external_id: str
    new_price: Optional[float] = None
    new_rent_price: Optional[float] = None
    status: Optional[str] = None


@app.put("/properties/update")
async def update_property(payload: PropertyUpdateRequest, db: Session = Depends(get_db)):
    prop = db.query(Property).filter(Property.external_id == payload.external_id).first()
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")
    if payload.new_price is not None and payload.new_price != prop.price:
        prop.previous_price = prop.price
        prop.price = payload.new_price
    if payload.new_rent_price is not None and payload.new_rent_price != prop.rent_price:
        prop.previous_price = prop.rent_price
        prop.rent_price = payload.new_rent_price
    if payload.status:
        prop.status = payload.status
    db.commit()
    return {"ok": True}


@app.get("/properties/search")
async def search_properties(
    goal: str = "buy",
    budget_max: Optional[float] = None,
    area: Optional[str] = None,
    property_type: Optional[str] = None,
    rooms: Optional[int] = None,
    db: Session = Depends(get_db),
):
    ps = PropertyService(db)
    results = ps.search(goal=goal, budget_max=budget_max, area=area,
                        property_type=property_type, rooms=rooms)
    return {"properties": results, "count": len(results)}


# ─── Admin ────────────────────────────────────────────────────────────────────

@app.get("/admin/leads")
async def list_leads(status: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(Lead)
    if status:
        q = q.filter(Lead.status == status)
    leads = q.order_by(Lead.created_at.desc()).limit(50).all()
    return {"leads": [
        {
            "id": l.id, "name": l.name, "phone": l.phone,
            "goal": l.goal, "area": l.area, "budget_max": l.budget_max,
            "status": l.status,
            "created_at": l.created_at.isoformat() if l.created_at else None,
        }
        for l in leads
    ]}


@app.get("/admin/appointments")
async def list_appointments(db: Session = Depends(get_db)):
    appts = db.query(Appointment).order_by(Appointment.created_at.desc()).limit(50).all()
    return {"appointments": [
        {
            "id": a.id, "client_name": a.client_name, "client_phone": a.client_phone,
            "scheduled_at": a.scheduled_at.isoformat() if a.scheduled_at else None,
            "status": a.status,
        }
        for a in appts
    ]}


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}
