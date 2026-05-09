"""
FastAPI application — Toni SaaS for real estate agencies.
Multi-tenant: each agency has its own WhatsApp bot and isolated data.
Super admin panel at /superadmin to manage agencies.
Per-agency admin panel at /admin/{slug}.
WhatsApp webhook at /whatsapp/webhook/{slug}.
"""

import logging
import os
import re
import secrets
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, Depends, FastAPI, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy.orm import Session

from admin_agent import AdminAgent
from database import SessionLocal, get_db, init_db
from excel_parser import build_unit_index, diff_unit_indexes, format_diff_report
from models import Agency, ToniProject
import whatsapp_bot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

admin_agent = AdminAgent()
scheduler = AsyncIOScheduler(timezone="Asia/Dubai")
http_basic = HTTPBasic()

_SUPER_PASSWORD = os.getenv("SUPER_ADMIN_PASSWORD", "superadmin2024")


# ─── App lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("PUBLIC_URL")
    if public_domain:
        db = SessionLocal()
        try:
            agencies = db.query(Agency).filter(Agency.is_active == True).all()
            for agency in agencies:
                if agency.wa_instance_id and agency.wa_token:
                    wa_url = f"https://{public_domain}/whatsapp/webhook/{agency.slug}"
                    await whatsapp_bot.set_wa_webhook(agency.wa_instance_id, agency.wa_token, wa_url)
                    logger.info(f"WA webhook set for '{agency.slug}': {wa_url}")
        finally:
            db.close()

    scheduler.add_job(whatsapp_bot.send_wa_morning_greeting, "cron", hour=8, minute=0, id="wa_morning_admin")
    scheduler.add_job(whatsapp_bot.send_wa_morning_followup, "cron", hour=8, minute=45, id="wa_followup")
    scheduler.add_job(whatsapp_bot.send_wa_midday_checkin, "cron", hour=14, minute=0, id="wa_midday")
    scheduler.start()
    logger.info("Scheduler started")

    yield
    scheduler.shutdown()


app = FastAPI(title="Toni SaaS", version="4.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Auth helpers ─────────────────────────────────────────────────────────────

def _verify_super_admin(credentials: HTTPBasicCredentials = Depends(http_basic)):
    ok = (
        secrets.compare_digest(credentials.username.encode(), b"admin")
        and secrets.compare_digest(credentials.password.encode(), _SUPER_PASSWORD.encode())
    )
    if not ok:
        raise HTTPException(status_code=401, detail="Unauthorized",
                            headers={"WWW-Authenticate": "Basic"})


def _check_agency_auth(request: Request, agency: Agency) -> bool:
    import base64 as _b64
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Basic "):
        return False
    try:
        decoded = _b64.b64decode(auth_header[6:]).decode()
        username, password = decoded.split(":", 1)
        return (
            secrets.compare_digest(username.encode(), b"admin")
            and secrets.compare_digest(password.encode(), (agency.admin_password or "toni2024").encode())
        )
    except Exception:
        return False


# ─── Slug helper ──────────────────────────────────────────────────────────────

def _make_slug(name: str, db: Session) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "agency"
    slug = base
    i = 2
    while db.query(Agency).filter(Agency.slug == slug).first():
        slug = f"{base}-{i}"
        i += 1
    return slug


# ─── Shared CSS ───────────────────────────────────────────────────────────────

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f0f2f5; color: #1a1a2e; }
.header { background: #1a1a2e; color: #fff; padding: 18px 32px;
          display: flex; align-items: center; gap: 12px; }
.header h1 { font-size: 1.3rem; }
.header span { opacity: .6; font-size: .85rem; }
.container { max-width: 960px; margin: 28px auto; padding: 0 20px; }
.card { background: #fff; border-radius: 12px; padding: 24px;
        margin-bottom: 20px; box-shadow: 0 1px 6px rgba(0,0,0,.08); }
.card h2 { font-size: 1rem; font-weight: 600; margin-bottom: 16px;
           color: #1a1a2e; border-bottom: 1px solid #e8eaf0; padding-bottom: 10px; }
.hint { background: #f0f4ff; border: 1px solid #c3d0f0; border-radius: 8px;
        padding: 14px 18px; font-size: .9rem; color: #2d3a8c; line-height: 1.6; }
table { width: 100%; border-collapse: collapse; font-size: .88rem; }
th { text-align: left; padding: 9px 14px; background: #f7f9fc;
     border-bottom: 2px solid #e8eaf0; color: #4a5568; font-weight: 600; }
td { padding: 9px 14px; border-bottom: 1px solid #edf0f7; vertical-align: top; }
tr:last-child td { border-bottom: none; }
.badge { display: inline-block; padding: 2px 9px; border-radius: 999px;
         font-size: .75rem; font-weight: 500; }
.badge-blue { background: #ebf4ff; color: #2b6cb0; }
.badge-green { background: #f0fff4; color: #276749; }
.badge-red { background: #fff5f5; color: #c53030; }
.sheet-list { font-size: .8rem; color: #718096; }
.empty { text-align: center; padding: 28px; color: #a0aec0; font-size: .9rem; }
.form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.field { display: flex; flex-direction: column; gap: 5px; }
.field label { font-size: .82rem; font-weight: 600; color: #4a5568; }
.field input { padding: 9px 12px; border: 1px solid #d1d5db; border-radius: 8px;
               font-size: .9rem; outline: none; }
.field input:focus { border-color: #667eea; box-shadow: 0 0 0 3px rgba(102,126,234,.15); }
.field-full { grid-column: 1 / -1; }
.btn { padding: 10px 22px; border: none; border-radius: 8px; font-size: .9rem;
       font-weight: 600; cursor: pointer; }
.btn-primary { background: #1a1a2e; color: #fff; }
.btn-primary:hover { background: #2d2d4e; }
a { color: #2b6cb0; text-decoration: none; }
a:hover { text-decoration: underline; }
"""


# ─── Super admin panel ────────────────────────────────────────────────────────

def _super_html(agencies: list, message: str = "") -> str:
    rows = ""
    for a in agencies:
        dt = a.created_at.strftime("%d.%m.%Y") if a.created_at else "—"
        status = '<span class="badge badge-green">active</span>' if a.is_active else '<span class="badge badge-red">off</span>'
        drive = f'<span class="badge badge-blue">✓</span>' if getattr(a, "drive_root_id", "") else '<span class="badge badge-red">—</span>'
        toggle_label = "Деактивировать" if a.is_active else "Активировать"
        rows += f"""
        <tr>
          <td><strong>{a.name}</strong></td>
          <td><code>{a.slug}</code></td>
          <td>{status}</td>
          <td>{drive}</td>
          <td><a href="/admin/{a.slug}" target="_blank">/admin/{a.slug}</a></td>
          <td>{dt}</td>
          <td>
            <form method="post" action="/superadmin/agency/{a.id}/toggle" style="display:inline">
              <button class="btn" style="padding:4px 10px;font-size:.75rem;background:#e2e8f0" type="submit">{toggle_label}</button>
            </form>
          </td>
        </tr>"""

    msg_html = f'<div class="hint" style="margin-bottom:16px;background:#f0fff4;border-color:#9ae6b4;color:#276749">{message}</div>' if message else ""

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Toni SaaS — Агентства</title>
  <style>{_CSS}</style>
</head>
<body>
  <div class="header">
    <div>🤖</div>
    <div>
      <h1>Toni SaaS — Управление агентствами</h1>
      <span>Супер-администратор</span>
    </div>
  </div>
  <div class="container">
    {msg_html}
    <div class="card">
      <h2>➕ Добавить агентство</h2>
      <form method="post" action="/superadmin/agency">
        <div class="form-grid">
          <div class="field field-full">
            <label>Название агентства *</label>
            <input name="name" required placeholder="Alpha Real Estate">
          </div>
          <div class="field">
            <label>Пароль для панели /admin</label>
            <input name="admin_password" placeholder="toni2024">
          </div>
          <div class="field">
            <label>Контакт поддержки (когда юнит не найден)</label>
            <input name="umar_contact" placeholder="@manager">
          </div>
          <div class="field">
            <label>WhatsApp — Green API Instance ID *</label>
            <input name="wa_instance_id" required placeholder="1234567890">
          </div>
          <div class="field">
            <label>WhatsApp — Green API Token *</label>
            <input name="wa_token" required placeholder="abcdef1234567890abcdef">
          </div>
          <div class="field field-full">
            <label>WhatsApp номера админов * (через запятую, без +)</label>
            <input name="wa_admin_numbers" required placeholder="79001234567, 971501234567">
          </div>
          <div class="field">
            <label>Google Drive Root Folder ID (папка клиента)</label>
            <input name="drive_root_id" placeholder="1IhO3Gq6e9mNs7xWVqdmwdx_YUr81hFws">
          </div>
          <div class="field field-full">
            <button class="btn btn-primary" type="submit">Создать агентство</button>
          </div>
        </div>
      </form>
    </div>
    <div class="card">
      <h2>📋 Агентства ({len(agencies)})</h2>
      {'<div class="empty">Пока нет ни одного агентства.</div>' if not agencies else f'<table><thead><tr><th>Название</th><th>Slug</th><th>Статус</th><th>Drive</th><th>Панель</th><th>Создано</th><th></th></tr></thead><tbody>{rows}</tbody></table>'}
    </div>
  </div>
</body>
</html>"""


@app.get("/superadmin", response_class=HTMLResponse)
async def super_admin_page(
    _: None = Depends(_verify_super_admin),
    db: Session = Depends(get_db),
):
    agencies = db.query(Agency).order_by(Agency.created_at.desc()).all()
    return _super_html(agencies)


@app.post("/superadmin/agency")
async def super_admin_create_agency(
    _: None = Depends(_verify_super_admin),
    db: Session = Depends(get_db),
    name: str = Form(...),
    admin_password: str = Form("toni2024"),
    umar_contact: str = Form("@support"),
    wa_instance_id: str = Form(...),
    wa_token: str = Form(...),
    wa_admin_numbers: str = Form(...),
    drive_root_id: str = Form(""),
):
    slug = _make_slug(name, db)
    parsed_wa_admins = [n.strip().lstrip("+") for n in wa_admin_numbers.split(",") if n.strip()]
    agency = Agency(
        name=name.strip(),
        slug=slug,
        bot_token="",
        admin_ids=[],
        admin_password=admin_password.strip() or "toni2024",
        umar_contact=umar_contact.strip() or "@support",
        wa_instance_id=wa_instance_id.strip(),
        wa_token=wa_token.strip(),
        wa_admin_numbers=parsed_wa_admins,
        drive_root_id=drive_root_id.strip(),
    )
    db.add(agency)
    db.commit()
    db.refresh(agency)

    public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("PUBLIC_URL")
    if public_domain and agency.wa_instance_id and agency.wa_token:
        wa_url = f"https://{public_domain}/whatsapp/webhook/{slug}"
        await whatsapp_bot.set_wa_webhook(agency.wa_instance_id, agency.wa_token, wa_url)
        logger.info(f"WA webhook registered for '{slug}': {wa_url}")

    return RedirectResponse(url=f"/superadmin?created={slug}", status_code=303)


@app.post("/superadmin/agency/{agency_id}/toggle")
async def super_admin_toggle_agency(
    agency_id: int,
    _: None = Depends(_verify_super_admin),
    db: Session = Depends(get_db),
):
    agency = db.query(Agency).filter(Agency.id == agency_id).first()
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    agency.is_active = not agency.is_active
    db.commit()
    return RedirectResponse(url="/superadmin", status_code=303)


# ─── Per-agency admin panel ───────────────────────────────────────────────────

def _projects_table(projects: list) -> str:
    if not projects:
        return '<div class="empty">Проекты пока не загружены. Отправьте Excel-файл боту в личку.</div>'

    rows = ""
    for p in projects:
        dt = p.uploaded_at.strftime("%d.%m.%Y %H:%M") if p.uploaded_at else "—"
        sheets_html = ""
        if p.sheets_data:
            names = list(p.sheets_data.keys())
            sheets_html = ", ".join(names[:4])
            if len(names) > 4:
                sheets_html += f" +{len(names) - 4}"
        rows += f"""
        <tr>
          <td><strong>{p.project_name}</strong></td>
          <td><span class="badge badge-green">{p.unit_count} юн.</span></td>
          <td><span class="badge badge-blue">v{p.version}</span></td>
          <td class="sheet-list">{sheets_html or "—"}</td>
          <td>{dt}</td>
        </tr>"""

    return f"""
    <table>
      <thead><tr>
        <th>Проект</th><th>Юниты</th><th>Версия</th><th>Листы</th><th>Загружен</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def _agency_admin_html(agency: Agency, projects: list) -> str:
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Тони — {agency.name}</title>
  <style>{_CSS}</style>
</head>
<body>
  <div class="header">
    <div>🤖</div>
    <div>
      <h1>Тони — {agency.name}</h1>
      <span>WhatsApp бот • управление проектами</span>
    </div>
  </div>
  <div class="container">
    <div class="card">
      <h2>📲 Как загрузить инвентарий</h2>
      <div class="hint">
        Отправьте файл боту в личный WhatsApp чат:<br>
        • <strong>Excel (.xlsx, .xls)</strong> или <strong>CSV</strong> — автоматически распознаётся как инвентарий<br>
        • <strong>PDF</strong> — добавьте слово «Инвентарий» в название файла или подпись<br>
        • Бот прочитает все листы и сохранит как <strong>один проект</strong><br>
        • Если проект уже был загружен — бот покажет что изменилось
      </div>
    </div>
    <div class="card">
      <h2>💬 WhatsApp (Green API)</h2>
      {'<div class="hint" style="background:#f0fff4;border-color:#68d391">✅ Подключён — Instance ID: <strong>' + (agency.wa_instance_id or '') + '</strong></div>' if agency.wa_instance_id else '<div class="hint" style="background:#fff5f5;border-color:#fc8181">⚠️ WhatsApp не настроен</div>'}
      <form method="post" action="/admin/{agency.slug}/whatsapp" style="margin-top:16px">
        <div class="form-grid">
          <div class="field">
            <label>Green API Instance ID</label>
            <input name="wa_instance_id" value="{agency.wa_instance_id or ''}" placeholder="1234567890">
          </div>
          <div class="field">
            <label>Green API Token</label>
            <input name="wa_token" value="{agency.wa_token or ''}" placeholder="abcdef1234...">
          </div>
          <div class="field field-full">
            <label>Номера админов WhatsApp (через запятую, без +)</label>
            <input name="wa_admin_numbers" value="{', '.join(agency.wa_admin_numbers or [])}" placeholder="79001234567, 971501234567">
          </div>
          <div class="field field-full">
            <button class="btn btn-primary" type="submit">Сохранить WhatsApp настройки</button>
          </div>
        </div>
      </form>
    </div>
    <div class="card">
      <h2>📁 Google Drive</h2>
      {'<div class="hint" style="background:#f0fff4;border-color:#68d391">✅ Drive Root ID: <strong>' + (getattr(agency, "drive_root_id", "") or '') + '</strong></div>' if getattr(agency, "drive_root_id", "") else '<div class="hint" style="background:#fff5f5;border-color:#fc8181">⚠️ Google Drive не настроен — брошюры и фото недоступны</div>'}
      <form method="post" action="/admin/{agency.slug}/drive" style="margin-top:16px">
        <div class="form-grid">
          <div class="field field-full">
            <label>Google Drive Root Folder ID</label>
            <input name="drive_root_id" value="{getattr(agency, 'drive_root_id', '') or ''}" placeholder="1IhO3Gq6e9mNs7xWVqdmwdx_YUr81hFws">
          </div>
          <div class="field field-full" style="font-size:.82rem;color:#718096;line-height:1.6">
            Скопируйте ID из URL папки Google Drive: drive.google.com/drive/folders/<strong>ВОТ_ЭТО</strong><br>
            Папка должна быть расшарена с email сервисного аккаунта Google.
          </div>
          <div class="field field-full">
            <button class="btn btn-primary" type="submit">Сохранить Drive настройки</button>
          </div>
        </div>
      </form>
    </div>
    <div class="card">
      <h2>📋 Активные проекты в памяти бота ({len(projects)})</h2>
      {_projects_table(projects)}
    </div>
  </div>
</body>
</html>"""


@app.get("/admin/{slug}", response_class=HTMLResponse)
async def agency_admin_page(slug: str, request: Request, db: Session = Depends(get_db)):
    agency = db.query(Agency).filter(Agency.slug == slug, Agency.is_active == True).first()
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")

    if not _check_agency_auth(request, agency):
        return HTMLResponse(
            content="Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Toni Admin"'},
        )

    projects = (
        db.query(ToniProject)
        .filter(ToniProject.is_active == True, ToniProject.agency_id == agency.id)
        .order_by(ToniProject.uploaded_at.desc())
        .all()
    )
    return _agency_admin_html(agency, projects)


@app.post("/admin/{slug}/whatsapp")
async def agency_admin_save_whatsapp(
    slug: str,
    request: Request,
    db: Session = Depends(get_db),
    wa_instance_id: str = Form(""),
    wa_token: str = Form(""),
    wa_admin_numbers: str = Form(""),
):
    agency = db.query(Agency).filter(Agency.slug == slug, Agency.is_active == True).first()
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    if not _check_agency_auth(request, agency):
        return HTMLResponse(content="Unauthorized", status_code=401,
                            headers={"WWW-Authenticate": 'Basic realm="Toni Admin"'})

    agency.wa_instance_id = wa_instance_id.strip()
    agency.wa_token = wa_token.strip()
    agency.wa_admin_numbers = [n.strip().lstrip("+") for n in wa_admin_numbers.split(",") if n.strip()]
    db.commit()

    public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("PUBLIC_URL")
    if public_domain and agency.wa_instance_id and agency.wa_token:
        wa_url = f"https://{public_domain}/whatsapp/webhook/{agency.slug}"
        await whatsapp_bot.set_wa_webhook(agency.wa_instance_id, agency.wa_token, wa_url)

    return RedirectResponse(url=f"/admin/{slug}", status_code=303)


@app.post("/admin/{slug}/drive")
async def agency_admin_save_drive(
    slug: str,
    request: Request,
    db: Session = Depends(get_db),
    drive_root_id: str = Form(""),
):
    agency = db.query(Agency).filter(Agency.slug == slug, Agency.is_active == True).first()
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    if not _check_agency_auth(request, agency):
        return HTMLResponse(content="Unauthorized", status_code=401,
                            headers={"WWW-Authenticate": 'Basic realm="Toni Admin"'})
    agency.drive_root_id = drive_root_id.strip()
    db.commit()
    return RedirectResponse(url=f"/admin/{slug}", status_code=303)


@app.get("/admin", response_class=HTMLResponse)
async def admin_redirect(db: Session = Depends(get_db)):
    agency = db.query(Agency).filter(Agency.slug == "default").first()
    if agency:
        return RedirectResponse(url=f"/admin/{agency.slug}", status_code=302)
    raise HTTPException(status_code=404, detail="No default agency configured")


# ─── Project save helper (used by web admin) ─────────────────────────────────

_GENERIC_SHEET = re.compile(r"^(sheet\s*\d*|лист\s*\d*|data|данные|table)$", re.IGNORECASE)


async def _save_project(name: str, sheets: dict, user_id: str,
                        db: Session, agency_id: int) -> dict:
    unit_index = build_unit_index(sheets)
    if not unit_index:
        return {"status": "skipped", "name": name, "reason": "no units found"}

    existing = (
        db.query(ToniProject)
        .filter(
            ToniProject.project_name == name,
            ToniProject.is_active == True,
            ToniProject.agency_id == agency_id,
        )
        .first()
    )
    if existing:
        diff = diff_unit_indexes(existing.unit_index or {}, unit_index)
        report = format_diff_report(diff, name)
        new_version = existing.version + 1
        existing.is_active = False
        db.flush()
        db.add(ToniProject(
            project_name=name, version=new_version,
            sheet_count=len(sheets), unit_count=len(unit_index),
            sheets_data=sheets, unit_index=unit_index,
            is_active=True, uploaded_at=datetime.now(),
            uploaded_by=user_id, agency_id=agency_id,
        ))
        db.commit()
        return {"status": "updated", "name": name, "units": len(unit_index),
                "version": new_version, "diff_report": report}
    else:
        db.add(ToniProject(
            project_name=name, version=1,
            sheet_count=len(sheets), unit_count=len(unit_index),
            sheets_data=sheets, unit_index=unit_index,
            is_active=True, uploaded_at=datetime.now(),
            uploaded_by=user_id, agency_id=agency_id,
        ))
        db.commit()
        return {"status": "created", "name": name, "units": len(unit_index), "version": 1}


async def _bg_wa_webhook(data: dict, agency_id: int):
    """Background task — processes WhatsApp update with its own DB session."""
    db = SessionLocal()
    try:
        agency = db.query(Agency).filter(Agency.id == agency_id, Agency.is_active == True).first()
        if agency:
            await whatsapp_bot.handle_update(data, agency)
    except Exception:
        logger.exception("WA webhook background error")
    finally:
        db.close()


# ─── WhatsApp webhook (Green API) ────────────────────────────────────────────

@app.post("/whatsapp/webhook/{slug}")
async def whatsapp_webhook(slug: str, request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    db = SessionLocal()
    try:
        agency = db.query(Agency).filter(Agency.slug == slug, Agency.is_active == True).first()
        agency_id = agency.id if agency else None
    finally:
        db.close()
    if agency_id:
        background_tasks.add_task(_bg_wa_webhook, data, agency_id)
    return {"ok": True}


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}
