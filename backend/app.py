"""
FastAPI application — Toni bot for real estate agent groups.
Routes Telegram updates: groups/channel → Toni, private admin chat → AdminAgent.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from admin_agent import AdminAgent, is_admin
from database import SessionLocal, get_db, init_db
import toni_bot
from telegram_bot import (
    answer_callback_query,
    send_message,
    send_typing,
    set_webhook,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

admin_agent = AdminAgent()
scheduler = AsyncIOScheduler(timezone="Asia/Tashkent")


# ─── App lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    railway_url = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("PUBLIC_URL")
    if railway_url and os.getenv("TELEGRAM_BOT_TOKEN"):
        webhook_url = f"https://{railway_url}/telegram/webhook"
        await set_webhook(webhook_url)
        logger.info(f"Webhook set: {webhook_url}")

    # Daily 9:00 morning report to all agent groups
    scheduler.add_job(toni_bot.send_morning_report, "cron", hour=9, minute=0, id="toni_morning")
    scheduler.start()
    logger.info("Scheduler started")

    yield
    scheduler.shutdown()


app = FastAPI(title="Toni Bot", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Telegram webhook ─────────────────────────────────────────────────────────

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    data = await request.json()

    # ── New file/post in the private database channel ─────────────────────────
    if data.get("channel_post"):
        await toni_bot.handle_update(data)
        return {"ok": True}

    # ── Inline button press ───────────────────────────────────────────────────
    if callback := data.get("callback_query"):
        await answer_callback_query(callback["id"])
        return {"ok": True}

    message = data.get("message") or data.get("edited_message")
    if not message:
        return {"ok": True}

    chat_type = message.get("chat", {}).get("type", "")
    chat_id = str(message.get("chat", {}).get("id", ""))
    user_id = str(message.get("from", {}).get("id", ""))
    text = (message.get("text") or "").strip()

    # ── Group / supergroup → Toni ─────────────────────────────────────────────
    if chat_type in ("group", "supergroup"):
        await toni_bot.handle_update(data)
        return {"ok": True}

    # ── Private chat — admin only ─────────────────────────────────────────────
    if not text or not is_admin(user_id):
        return {"ok": True}

    await send_typing(user_id)

    # /tonigroups — list registered agent groups
    if text == "/tonigroups":
        from models import ToniGroup
        groups = db.query(ToniGroup).filter(ToniGroup.active == True).all()
        if not groups:
            await send_message(user_id, "Бот ещё не добавлен ни в одну группу.")
        else:
            lines = [f"📋 Групп зарегистрировано: {len(groups)}\n"]
            for g in groups:
                lines.append(f"• {g.title or '—'} (`{g.chat_id}`)")
            await send_message(user_id, "\n".join(lines))
        return {"ok": True}

    # /tonifiles — list indexed files
    if text == "/tonifiles":
        from models import ToniFile
        files = db.query(ToniFile).order_by(ToniFile.id.desc()).limit(10).all()
        if not files:
            await send_message(user_id, "База файлов пуста. Загрузи файлы в канал-базу данных.")
        else:
            lines = [f"📁 Последние {len(files)} файлов в базе:\n"]
            for f in files:
                units = ", ".join(f.unit_numbers) if f.unit_numbers else "—"
                lines.append(f"• {f.file_name} | юниты: {units}")
            await send_message(user_id, "\n".join(lines))
        return {"ok": True}

    # /toniannounce <текст> — ручная рассылка во все группы
    if text.startswith("/toniannounce "):
        msg_text = text[len("/toniannounce "):].strip()
        from models import ToniGroup
        groups = db.query(ToniGroup).filter(ToniGroup.active == True).all()
        for g in groups:
            await toni_bot._send(g.chat_id, msg_text)
        await send_message(user_id, f"✅ Отправлено в {len(groups)} групп(ы).")
        return {"ok": True}

    # All other admin messages → AdminAgent
    try:
        reply = await admin_agent.process(user_id=user_id, message=text, db=db)
        await send_message(user_id, reply)
    except Exception as e:
        logger.exception(f"Admin agent error: {e}")
        await send_message(user_id, f"Ошибка: {e}")

    return {"ok": True}


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}
