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

import asyncio

from database import SessionLocal, init_db
from models import Agency, ToniProject
import client_registry
import pdf_index
import whatsapp_bot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="Asia/Dubai")

_OWNER_PASSWORD = os.getenv("ADMIN_PASSWORD", "toni2024")


# ─── App lifespan ─────────────────────────────────────────────────────────────

async def _rebuild_all_indexes():
    """Rebuild PDF index for every active agency. Runs at 07:00 Dubai time."""
    db = SessionLocal()
    try:
        agencies = db.query(Agency).filter(Agency.is_active == True).all()
        for agency in agencies:
            try:
                count = await pdf_index.build_index(agency)
                logger.info(f"Index rebuilt: agency={agency.slug} units={count}")
            except Exception:
                logger.exception(f"Index rebuild failed for agency {agency.slug}")
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    client_registry.load_all()
    client_registry.sync_to_db()

    # Restore groups from persistent groups.json → DB (survives redeployments)
    import group_registry
    db = SessionLocal()
    try:
        restored = group_registry.sync_to_db(db)
        if restored:
            logger.info(f"Restored {restored} groups from groups.json")
        else:
            # First run: migrate existing DB groups → groups.json
            migrated = group_registry.sync_from_db(db)
            if migrated:
                logger.info(f"Migrated {migrated} existing groups to groups.json")
    finally:
        db.close()

    # Set ONE global WhatsApp webhook
    public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("PUBLIC_URL")
    wa_instance = os.getenv("WA_INSTANCE_ID", "")
    wa_token = os.getenv("WA_TOKEN", "")
    if public_domain and wa_instance and wa_token:
        wa_url = f"https://{public_domain}/whatsapp/webhook/default"
        await whatsapp_bot.set_wa_webhook(wa_instance, wa_token, wa_url)
        logger.info(f"WA webhook set: {wa_url}")

    scheduler.add_job(whatsapp_bot.send_wa_morning_greeting,  "cron", hour=8,  minute=0,  id="wa_morning",           coalesce=True, max_instances=1)
    scheduler.add_job(whatsapp_bot.send_wa_morning_followup,  "cron", hour=8,  minute=45, id="wa_followup",          coalesce=True, max_instances=1)
    scheduler.add_job(whatsapp_bot.send_wa_midday_checkin,    "cron", hour=14, minute=0,  id="wa_midday",            coalesce=True, max_instances=1)
    scheduler.add_job(whatsapp_bot.send_daily_offer_11am,     "cron", hour=11, minute=0,  id="wa_offer_11am",        coalesce=True, max_instances=1)
    scheduler.add_job(whatsapp_bot.send_daily_offer_14pm,     "cron", hour=14, minute=0,  id="wa_offer_14pm",        coalesce=True, max_instances=1)
    scheduler.add_job(whatsapp_bot.send_daily_offer_17pm,     "cron", hour=17, minute=0,  id="wa_offer_17pm",        coalesce=True, max_instances=1)
    scheduler.add_job(whatsapp_bot.send_friday_broadcast,     "cron", day_of_week="fri", hour=13, minute=0, id="wa_friday_broadcast", coalesce=True, max_instances=1, misfire_grace_time=60)
    scheduler.add_job(_rebuild_all_indexes,                    "cron", hour=7,  minute=0,  id="rebuild_index",        coalesce=True, max_instances=1)
    scheduler.add_job(whatsapp_bot.send_daily_report,          "cron", hour=20, minute=0,  id="wa_daily_report",      coalesce=True, max_instances=1)
    scheduler.start()
    logger.info("Scheduler started")

    # Build PDF index sequentially with delay to avoid concurrent SSL conflicts in httplib2
    async def _build_indexes_sequential():
        db2 = SessionLocal()
        try:
            agencies = db2.query(Agency).filter(Agency.is_active == True).all()
            for i, agency in enumerate(agencies):
                if i > 0:
                    await asyncio.sleep(5)  # stagger to avoid concurrent SSL errors
                try:
                    count = await pdf_index.build_index(agency)
                    logger.info(f"Index built: agency={agency.slug} units={count}")
                except Exception:
                    logger.exception(f"Index build failed for agency {agency.slug}")
        finally:
            db2.close()

    asyncio.create_task(_build_indexes_sequential())

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
    """Find the right agency for an incoming message.

    Groups  → group_registry (file-backed, per-agency) → DB (active only, latest entry)
              → sender phone (first ever message in new group).
    Private → sender phone only.
    Fallback → only when exactly one agency is active.
    """
    import client_registry
    from models import WhatsAppGroup
    sender_data = data.get("senderData", {})
    sender_wid  = sender_data.get("sender", "")
    sender_phone = sender_wid.split("@")[0]
    chat_id = sender_data.get("chatId", "")

    if chat_id.endswith("@g.us"):
        # ── Group: group_registry is the authoritative source ─────────────────
        # It is per-agency and file-backed — survives restarts and avoids DB
        # race conditions where the same chat_id may appear under two agencies.
        import group_registry as _gr
        for ag in db.query(Agency).filter(Agency.is_active == True).all():
            if any(g["id"] == chat_id for g in _gr.get_groups(ag.id)):
                return ag

        # Not in registry yet — fall back to DB (active rows, newest first)
        group = (
            db.query(WhatsAppGroup)
            .filter(WhatsAppGroup.chat_id == chat_id, WhatsAppGroup.active == True)
            .order_by(WhatsAppGroup.id.desc())
            .first()
        )
        if group and group.agency_id:
            return db.query(Agency).filter(Agency.id == group.agency_id, Agency.is_active == True).first()

        # Brand-new group — resolve by sender phone (first message from a known admin)
        if sender_phone:
            cfg = client_registry.find_by_phone(sender_phone)
            if cfg:
                return db.query(Agency).filter(Agency.slug == cfg.slug, Agency.is_active == True).first()

        logger.warning(f"_resolve_agency: unregistered group {chat_id} from unknown sender {sender_phone} — ignoring")
        return None
    else:
        # ── Private chat: agency from sender's phone ───────────────────────────
        if sender_phone:
            cfg = client_registry.find_by_phone(sender_phone)
            if cfg:
                return db.query(Agency).filter(Agency.slug == cfg.slug, Agency.is_active == True).first()

    # Fallback: only safe with exactly one active agency (single-instance setup).
    active_agencies = db.query(Agency).filter(Agency.is_active == True).all()
    if len(active_agencies) == 1:
        logger.info(f"_resolve_agency: single-agency fallback={active_agencies[0].slug} sender={sender_phone} chat={chat_id}")
        return active_agencies[0]

    logger.warning(f"_resolve_agency: unrecognised sender={sender_phone} chat={chat_id} — ignoring (multi-agency setup)")
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
    webhook_type = data.get("typeWebhook", "?")
    sender = data.get("senderData", {}).get("sender", "?")
    logger.info(f"WEBHOOK IN: type={webhook_type} sender={sender}")
    db = SessionLocal()
    try:
        agency = _resolve_agency(data, db)
        agency_id = agency.id if agency else None
        logger.info(f"WEBHOOK agency={'found:'+str(agency_id) if agency_id else 'NOT FOUND'}")
    finally:
        db.close()
    if agency_id:
        background_tasks.add_task(_bg_webhook, data, agency_id)
    return {"ok": True}


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}
