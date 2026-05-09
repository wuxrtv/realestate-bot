"""
FastAPI application — Tony real estate bot.
One WhatsApp bot, multiple clients via clients/ config files.
"""

import logging
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from database import SessionLocal, init_db
from models import Agency, ToniProject
import whatsapp_bot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="Asia/Dubai")

_OWNER_PASSWORD = os.getenv("ADMIN_PASSWORD", "toni2024")


# ─── App lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    # Set ONE global WhatsApp webhook
    public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("PUBLIC_URL")
    wa_instance = os.getenv("WA_INSTANCE_ID", "")
    wa_token = os.getenv("WA_TOKEN", "")
    if public_domain and wa_instance and wa_token:
        wa_url = f"https://{public_domain}/whatsapp/webhook/default"
        await whatsapp_bot.set_wa_webhook(wa_instance, wa_token, wa_url)
        logger.info(f"WA webhook set: {wa_url}")

    scheduler.add_job(whatsapp_bot.send_wa_morning_greeting,  "cron", hour=8,  minute=0,  id="wa_morning")
    scheduler.add_job(whatsapp_bot.send_wa_morning_followup,  "cron", hour=8,  minute=45, id="wa_followup")
    scheduler.add_job(whatsapp_bot.send_wa_midday_checkin,    "cron", hour=14, minute=0,  id="wa_midday")
    scheduler.start()
    logger.info("Scheduler started")

    yield
    scheduler.shutdown()


app = FastAPI(title="Tony Bot", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── Owner dashboard (read-only) ──────────────────────────────────────────────

def _check_owner(request: Request) -> bool:
    import base64
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(auth[6:]).decode()
        _, pwd = decoded.split(":", 1)
        return secrets.compare_digest(pwd.encode(), _OWNER_PASSWORD.encode())
    except Exception:
        return False


@app.get("/admin", response_class=HTMLResponse)
async def owner_dashboard(request: Request):
    if not _check_owner(request):
        return HTMLResponse(
            content="Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Tony Admin"'},
        )

    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).order_by(Agency.name).all()
        rows = ""
        for a in agencies:
            count = db.query(ToniProject).filter(
                ToniProject.agency_id == a.id, ToniProject.is_active == True
            ).count()
            drive = "✅" if getattr(a, "drive_root_id", "") else "—"
            rows += f"<tr><td><b>{a.name}</b></td><td>{a.slug}</td><td>{count}</td><td>{drive}</td></tr>"
    finally:
        db.close()

    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Tony — Admin</title>
<style>
body{{font-family:sans-serif;background:#f5f5f5;padding:32px}}
h1{{color:#1a1a2e}}
table{{background:#fff;border-collapse:collapse;width:100%;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.1)}}
th{{background:#1a1a2e;color:#fff;padding:12px 16px;text-align:left}}
td{{padding:11px 16px;border-bottom:1px solid #eee}}
tr:last-child td{{border:none}}
</style></head>
<body>
<h1>🤖 Tony — Clients</h1>
<table>
<thead><tr><th>Client</th><th>Slug</th><th>Projects</th><th>Drive</th></tr></thead>
<tbody>{rows or '<tr><td colspan="4" style="color:#aaa;text-align:center">No clients yet</td></tr>'}</tbody>
</table>
</body></html>"""
    return HTMLResponse(html)


# ─── WhatsApp webhook ─────────────────────────────────────────────────────────

def _resolve_agency(data: dict, db) -> Agency | None:
    """Find the right agency for an incoming message by sender phone or group."""
    import client_registry
    sender_data = data.get("senderData", {})
    sender_wid  = sender_data.get("sender", "")
    sender_phone = sender_wid.split("@")[0]

    if sender_phone:
        cfg = client_registry.find_by_phone(sender_phone)
        if cfg:
            return db.query(Agency).filter(Agency.slug == cfg.slug, Agency.is_active == True).first()

    chat_id = sender_data.get("chatId", "")
    if chat_id.endswith("@g.us"):
        from models import WhatsAppGroup
        group = db.query(WhatsAppGroup).filter(WhatsAppGroup.chat_id == chat_id).first()
        if group and group.agency_id:
            return db.query(Agency).filter(Agency.id == group.agency_id, Agency.is_active == True).first()

    logger.warning(f"_resolve_agency: no client found for sender={sender_phone} chat={chat_id} — ignoring")
    return None


async def _bg_webhook(data: dict, agency_id: int):
    db = SessionLocal()
    try:
        agency = db.query(Agency).filter(Agency.id == agency_id, Agency.is_active == True).first()
        if agency:
            await whatsapp_bot.handle_update(data, agency)
    except Exception:
        logger.exception("WA webhook error")
    finally:
        db.close()


@app.post("/whatsapp/webhook/{slug}")
async def whatsapp_webhook(slug: str, request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    db = SessionLocal()
    try:
        agency = _resolve_agency(data, db)
        agency_id = agency.id if agency else None
    finally:
        db.close()
    if agency_id:
        background_tasks.add_task(_bg_webhook, data, agency_id)
    return {"ok": True}


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}
