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

    scheduler.add_job(whatsapp_bot.send_wa_morning_greeting,  "cron", hour=8,  minute=0,  id="wa_morning")
    scheduler.add_job(whatsapp_bot.send_wa_morning_followup,  "cron", hour=8,  minute=45, id="wa_followup")
    scheduler.add_job(whatsapp_bot.send_wa_midday_checkin,    "cron", hour=14, minute=0,  id="wa_midday")
    scheduler.add_job(whatsapp_bot.send_daily_offer_11am,     "cron", hour=11, minute=0,  id="wa_offer_11am")
    scheduler.add_job(whatsapp_bot.send_daily_offer_14pm,     "cron", hour=14, minute=0,  id="wa_offer_14pm")
    scheduler.add_job(whatsapp_bot.send_daily_offer_17pm,     "cron", hour=17, minute=0,  id="wa_offer_17pm")
    scheduler.add_job(_rebuild_all_indexes,                    "cron", hour=7,  minute=0,  id="rebuild_index")
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


_ADMIN_CSS = """
body{font-family:sans-serif;background:#f5f5f5;padding:32px;max-width:960px;margin:0 auto}
h1,h2{color:#1a1a2e}
h2{font-size:16px;margin:0 0 16px 0}
.card{background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.1);padding:24px;margin-bottom:24px}
table{border-collapse:collapse;width:100%}
th{background:#1a1a2e;color:#fff;padding:11px 16px;text-align:left;font-size:13px}
td{padding:11px 16px;border-bottom:1px solid #eee}
tr:last-child td{border:none}
tr.clickable{cursor:pointer}
tr.clickable:hover td{background:#f0f4ff}
a{color:#1a1a2e}
.back{text-decoration:none;display:inline-block;margin-bottom:20px;font-size:14px}
.field{margin-bottom:16px}
.label{color:#888;font-size:12px;margin-bottom:4px;text-transform:uppercase;letter-spacing:.5px}
input,textarea{width:100%;padding:9px 12px;border:1px solid #ddd;border-radius:6px;font-size:14px;box-sizing:border-box;font-family:inherit}
textarea{resize:vertical}
.btn{background:#1a1a2e;color:#fff;border:none;padding:10px 24px;border-radius:6px;cursor:pointer;font-size:14px}
.btn:hover{background:#2d2d5e}
.info-row{display:flex;gap:8px;align-items:flex-start;padding:9px 0;border-bottom:1px solid #eee}
.info-row:last-child{border:none}
.info-label{color:#888;width:150px;font-size:13px;flex-shrink:0;padding-top:1px}
.info-val{font-size:14px}
.saved{background:#e8f5e9;color:#2e7d32;padding:12px 16px;border-radius:6px;margin-bottom:16px;font-size:14px}
"""


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
            rows += (
                f'<tr class="clickable" onclick="location.href=\'/admin/client/{a.slug}\'">'
                f"<td><b>{a.name}</b></td><td>{a.slug}</td><td>{count}</td><td>{drive}</td></tr>"
            )
    finally:
        db.close()

    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Tony — Admin</title>
<style>{_ADMIN_CSS}</style></head>
<body>
<h1>Tony — Clients</h1>
<div class="card" style="padding:0;overflow:hidden">
<table>
<thead><tr><th>Client</th><th>Slug</th><th>Projects</th><th>Drive</th></tr></thead>
<tbody>{rows or '<tr><td colspan="4" style="color:#aaa;text-align:center;padding:24px">No clients yet</td></tr>'}</tbody>
</table>
</div>
</body></html>"""
    return HTMLResponse(html)


@app.get("/admin/client/{slug}", response_class=HTMLResponse)
async def client_detail(slug: str, request: Request):
    if not _check_owner(request):
        return HTMLResponse(
            content="Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Tony Admin"'},
        )

    db = SessionLocal()
    try:
        from models import WhatsAppGroup
        agency = db.query(Agency).filter(Agency.slug == slug, Agency.is_active == True).first()
        if not agency:
            return HTMLResponse("<p>Client not found</p>", status_code=404)

        projects = db.query(ToniProject).filter(
            ToniProject.agency_id == agency.id, ToniProject.is_active == True
        ).order_by(ToniProject.uploaded_at.desc()).all()

        groups = db.query(WhatsAppGroup).filter(
            WhatsAppGroup.agency_id == agency.id
        ).order_by(WhatsAppGroup.title).all()

        proj_rows = "".join(
            f"<tr><td><b>{p.project_name}</b></td><td>{p.unit_count}</td>"
            f"<td>{str(p.uploaded_at)[:10] if p.uploaded_at else '—'}</td></tr>"
            for p in projects
        )
        grp_rows = "".join(
            f"<tr><td>{g.title or g.chat_id}</td><td style='font-size:12px;color:#888'>{g.chat_id}</td>"
            f"<td>{'✅ Active' if g.active else '⛔ Paused'}</td></tr>"
            for g in groups
        )

        idx_info = pdf_index.index_info(agency.id)
        if idx_info["exists"]:
            built = idx_info["built_at"][:10] if idx_info["built_at"] else "—"
            idx_text = f"{idx_info['count']} units &nbsp;·&nbsp; built {built}"
        else:
            idx_text = "Not built yet"

        phones_str = ", ".join(agency.wa_admin_numbers) if agency.wa_admin_numbers else "—"
        drive_val = agency.drive_root_id or ""
        drive_display = (
            f'<a href="https://drive.google.com/drive/folders/{drive_val}" target="_blank">{drive_val}</a>'
            if drive_val else "—"
        )

        saved_banner = '<div class="saved">✅ Данные сохранены</div>' if request.query_params.get("saved") else ""

        projects_section = (
            f'<table><thead><tr><th>Проект</th><th>Юнитов</th><th>Загружен</th></tr></thead>'
            f'<tbody>{proj_rows}</tbody></table>'
            if projects else '<p style="color:#aaa;margin:0">Нет загруженных проектов</p>'
        )
        groups_section = (
            f'<table><thead><tr><th>Название</th><th>Chat ID</th><th>Статус</th></tr></thead>'
            f'<tbody>{grp_rows}</tbody></table>'
            if groups else '<p style="color:#aaa;margin:0">Нет групп</p>'
        )

        bot_char_escaped = (agency.bot_character or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Tony — {agency.name}</title>
<style>{_ADMIN_CSS}</style></head>
<body>
<a href="/admin" class="back">← Все клиенты</a>
<h1>{agency.name}</h1>

<div class="card">
<h2>Информация</h2>
<div class="info-row"><span class="info-label">Slug</span><span class="info-val">{agency.slug}</span></div>
<div class="info-row"><span class="info-label">Телефоны</span><span class="info-val">{phones_str}</span></div>
<div class="info-row"><span class="info-label">Google Drive</span><span class="info-val">{drive_display}</span></div>
<div class="info-row"><span class="info-label">PDF индекс</span><span class="info-val">{idx_text}</span></div>
<div class="info-row"><span class="info-label">Проекты</span><span class="info-val">{len(projects)}</span></div>
<div class="info-row"><span class="info-label">WA группы</span><span class="info-val">{len(groups)}</span></div>
</div>

<div class="card">
<h2>Настройки клиента</h2>
{saved_banner}
<form method="POST" action="/admin/client/{agency.slug}">
<div class="field">
<div class="label">Drive Root ID</div>
<input name="drive_root_id" value="{drive_val}">
</div>
<div class="field">
<div class="label">Контакт (когда юнит не найден)</div>
<input name="umar_contact" value="{agency.umar_contact or ''}">
</div>
<div class="field">
<div class="label">Характер Tony (bot character)</div>
<textarea name="bot_character" rows="6">{bot_char_escaped}</textarea>
</div>
<button type="submit" class="btn">Сохранить</button>
</form>
</div>

<div class="card">
<h2>Проекты ({len(projects)})</h2>
{projects_section}
</div>

<div class="card">
<h2>WhatsApp группы ({len(groups)})</h2>
{groups_section}
</div>

</body></html>"""
        return HTMLResponse(html)
    finally:
        db.close()


@app.post("/admin/client/{slug}")
async def client_detail_save(slug: str, request: Request):
    if not _check_owner(request):
        return HTMLResponse("Unauthorized", status_code=401)

    form = await request.form()
    drive_root_id = (form.get("drive_root_id") or "").strip()
    umar_contact  = (form.get("umar_contact") or "").strip()
    bot_character = (form.get("bot_character") or "").strip()

    db = SessionLocal()
    try:
        agency = db.query(Agency).filter(Agency.slug == slug).first()
        if agency:
            agency.drive_root_id = drive_root_id
            agency.umar_contact  = umar_contact
            agency.bot_character = bot_character
            db.commit()
    finally:
        db.close()

    from fastapi.responses import RedirectResponse
    return RedirectResponse(f"/admin/client/{slug}?saved=1", status_code=303)


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

    # Fallback: use first active agency ordered by id.
    # Covers unregistered groups and members whose phone isn't in client config.
    # Safe when all agencies share the same WhatsApp instance (single-instance setup).
    first = db.query(Agency).filter(Agency.is_active == True).order_by(Agency.id).first()
    if first:
        logger.info(f"_resolve_agency: fallback agency={first.slug} for sender={sender_phone} chat={chat_id}")
        return first

    logger.warning(f"_resolve_agency: no active agency found — ignoring sender={sender_phone} chat={chat_id}")
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
