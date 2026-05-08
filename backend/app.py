"""
FastAPI application — Toni SaaS for real estate agencies.
Multi-tenant: each agency has its own bot token and isolated data.
Super admin panel at /superadmin to manage agencies.
Per-agency admin panel at /admin/{slug}.
Webhooks at /telegram/webhook/{slug}.
"""

import asyncio
import base64
import json
import logging
import os
import re
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import anthropic
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, Depends, FastAPI, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy.orm import Session

from admin_agent import AdminAgent, is_admin
from database import SessionLocal, get_db, init_db
from excel_parser import (
    build_unit_index,
    diff_unit_indexes,
    format_diff_report,
    normalize_project_name,
    parse_csv,
    parse_excel,
    parse_pdf,
)
from models import Agency, ToniFile, ToniGroup, ToniProject, WhatsAppGroup
import toni_bot
import whatsapp_bot
from telegram_bot import (
    answer_callback_query,
    edit_message_text,
    get_file_bytes,
    send_message,
    send_message_with_keyboard,
    send_typing,
    set_webhook,
)

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
                tg_url = f"https://{public_domain}/telegram/webhook/{agency.slug}"
                await set_webhook(tg_url, token=agency.bot_token)
                logger.info(f"Telegram webhook set for '{agency.slug}': {tg_url}")

                if agency.wa_instance_id and agency.wa_token:
                    wa_url = f"https://{public_domain}/whatsapp/webhook/{agency.slug}"
                    await whatsapp_bot.set_wa_webhook(agency.wa_instance_id, agency.wa_token, wa_url)
        finally:
            db.close()

    scheduler.add_job(toni_bot.send_morning_greeting_to_admin, "cron", hour=8, minute=0, id="toni_morning_admin")
    scheduler.add_job(toni_bot.send_morning_report, "cron", hour=8, minute=0, id="toni_morning_groups")
    scheduler.add_job(toni_bot.send_morning_followup, "cron", hour=8, minute=45, id="toni_followup")
    scheduler.add_job(toni_bot.send_midday_checkin, "cron", hour=14, minute=0, id="toni_midday")
    scheduler.add_job(toni_bot.send_end_of_day_report, "cron", hour=20, minute=0, id="toni_evening_report")
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
        rows += f"""
        <tr>
          <td><strong>{a.name}</strong></td>
          <td><code>{a.slug}</code></td>
          <td>{status}</td>
          <td><a href="/admin/{a.slug}" target="_blank">/admin/{a.slug}</a></td>
          <td>{dt}</td>
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
          <div class="field field-full">
            <label>Telegram Bot Token * (от @BotFather)</label>
            <input name="bot_token" required placeholder="1234567890:AAFxxxx...">
          </div>
          <div class="field">
            <label>Telegram ID администратора * (через запятую)</label>
            <input name="admin_ids" required placeholder="7567850330">
          </div>
          <div class="field">
            <label>Пароль для панели /admin</label>
            <input name="admin_password" placeholder="toni2024">
          </div>
          <div class="field">
            <label>Username бота (без @, для @упоминаний)</label>
            <input name="bot_username" placeholder="ToniRealtyBot">
          </div>
          <div class="field">
            <label>Контакт поддержки (когда юнит не найден)</label>
            <input name="umar_contact" placeholder="@manager">
          </div>
          <div class="field field-full">
            <label>ID канала-базы файлов (необязательно)</label>
            <input name="db_channel_id" placeholder="-1001234567890">
          </div>
          <div class="field">
            <label>WhatsApp — Green API Instance ID</label>
            <input name="wa_instance_id" placeholder="1234567890">
          </div>
          <div class="field">
            <label>WhatsApp — Green API Token</label>
            <input name="wa_token" placeholder="abcdef1234567890abcdef">
          </div>
          <div class="field field-full">
            <label>WhatsApp номера админов (через запятую, без +)</label>
            <input name="wa_admin_numbers" placeholder="79001234567, 971501234567">
          </div>
          <div class="field field-full">
            <button class="btn btn-primary" type="submit">Создать агентство</button>
          </div>
        </div>
      </form>
    </div>
    <div class="card">
      <h2>📋 Агентства ({len(agencies)})</h2>
      {'<div class="empty">Пока нет ни одного агентства.</div>' if not agencies else f'<table><thead><tr><th>Название</th><th>Slug</th><th>Статус</th><th>Панель</th><th>Создано</th></tr></thead><tbody>{rows}</tbody></table>'}
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
    bot_token: str = Form(...),
    admin_ids: str = Form(...),
    admin_password: str = Form("toni2024"),
    bot_username: str = Form(""),
    umar_contact: str = Form("@support"),
    db_channel_id: str = Form(""),
    wa_instance_id: str = Form(""),
    wa_token: str = Form(""),
    wa_admin_numbers: str = Form(""),
):
    slug = _make_slug(name, db)
    parsed_ids = [i.strip() for i in admin_ids.split(",") if i.strip()]
    parsed_wa_admins = [n.strip().lstrip("+") for n in wa_admin_numbers.split(",") if n.strip()]
    agency = Agency(
        name=name.strip(),
        slug=slug,
        bot_token=bot_token.strip(),
        admin_ids=parsed_ids,
        admin_password=admin_password.strip() or "toni2024",
        bot_username=bot_username.strip().lstrip("@"),
        umar_contact=umar_contact.strip() or "@support",
        db_channel_id=db_channel_id.strip(),
        wa_instance_id=wa_instance_id.strip(),
        wa_token=wa_token.strip(),
        wa_admin_numbers=parsed_wa_admins,
    )
    db.add(agency)
    db.commit()
    db.refresh(agency)

    # Register webhook for this agency's bot
    public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("PUBLIC_URL")
    if public_domain:
        webhook_url = f"https://{public_domain}/telegram/webhook/{slug}"
        await set_webhook(webhook_url, token=bot_token.strip())
        logger.info(f"Webhook registered for new agency '{slug}': {webhook_url}")

    return RedirectResponse(url=f"/superadmin?created={slug}", status_code=303)


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
      <span>Только просмотр • загрузка через Telegram</span>
    </div>
  </div>
  <div class="container">
    <div class="card">
      <h2>📲 Как загрузить проект</h2>
      <div class="hint">
        Отправьте Excel-файл (<strong>.xlsx</strong> или <strong>.xls</strong>) боту в личный чат в Telegram.<br>
        • Бот прочитает все листы и сохранит как <strong>один проект</strong>.<br>
        • Если проект уже был загружен — бот покажет что изменилось.<br>
        • Если название проекта не найдено в файле — бот спросит как его назвать.
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


@app.get("/admin", response_class=HTMLResponse)
async def admin_redirect(db: Session = Depends(get_db)):
    agency = db.query(Agency).filter(Agency.slug == "default").first()
    if agency:
        return RedirectResponse(url=f"/admin/{agency.slug}", status_code=302)
    raise HTTPException(status_code=404, detail="No default agency configured")


# ─── Admin helper utilities ──────────────────────────────────────────────────

async def _transcribe_voice(audio_bytes: bytes) -> str | None:
    key = os.getenv("GROQ_API_KEY")
    if not key:
        return None
    try:
        from groq import AsyncGroq
        client = AsyncGroq(api_key=key)
        transcription = await client.audio.transcriptions.create(
            file=("voice.ogg", audio_bytes),
            model="whisper-large-v3",
        )
        return transcription.text
    except Exception as e:
        logger.warning(f"Voice transcription failed: {e}")
        return None


async def _analyze_image(image_bytes: bytes, caption: str, media_type: str = "image/jpeg") -> str:
    try:
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        img_b64 = base64.standard_b64encode(image_bytes).decode()
        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": img_b64}},
                    {"type": "text", "text": (
                        f"Подпись администратора: «{caption}»\n\n"
                        "Опиши что изображено. Если это документ о недвижимости — извлеки: "
                        "название проекта, номера юнитов, цены, характеристики. "
                        "Отвечай на русском языке."
                    )},
                ],
            }],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.warning(f"Image analysis failed: {e}")
        return caption or "Изображение получено."


async def _detect_project_name_ai(sheets_data: dict, filename: str) -> str:
    try:
        first_sheet = next(iter(sheets_data.values()), [])
        headers = list(first_sheet[0].keys()) if first_sheet else []
        sample = first_sheet[:2]
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=60,
            messages=[{"role": "user", "content": (
                f"Файл: {filename}\nЛисты: {list(sheets_data.keys())}\n"
                f"Заголовки: {headers}\nПример строк: {json.dumps(sample, ensure_ascii=False, default=str)}\n"
                "Как называется этот проект недвижимости? Ответь только названием."
            )}],
        )
        name = resp.content[0].text.strip()
        return name if name and len(name) < 80 else normalize_project_name(filename)
    except Exception:
        return normalize_project_name(filename)


async def _detect_project_name_or_none(sheets_data: dict, filename: str) -> str | None:
    """Returns project name or None when Claude is not confident enough to name it."""
    try:
        first_sheet = next(iter(sheets_data.values()), [])
        headers = list(first_sheet[0].keys()) if first_sheet else []
        sample = first_sheet[:2]
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            messages=[{"role": "user", "content": (
                f"Файл: {filename}\nЛисты: {list(sheets_data.keys())}\n"
                f"Заголовки: {headers}\nПример строк: {json.dumps(sample, ensure_ascii=False, default=str)}\n"
                "Как называется этот проект недвижимости? "
                "Если НЕ УВЕРЕН — ответь только: UNKNOWN. "
                "Если уверен — ответь только названием (без кавычек и пояснений)."
            )}],
        )
        name = resp.content[0].text.strip()
        if not name or name.upper() == "UNKNOWN" or len(name) > 80:
            return None
        return name
    except Exception:
        return None


# ─── Telegram spreadsheet upload handler ─────────────────────────────────────

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


async def _process_excel_upload(
    agency: Agency,
    user_id: str,
    file_id: str,
    file_name: str,
    project_name_override: str,
    db: Session,
    sheets_data: dict | None = None,
):
    """Parse Excel/CSV and save ALL sheets as ONE project. If name unknown, ask admin."""
    tok = agency.bot_token

    if sheets_data is None:
        await send_typing(user_id, token=tok)
        await send_message(user_id, f"📊 Читаю файл *{file_name}*...", token=tok)

        file_bytes = await get_file_bytes(file_id, token=tok)
        if not file_bytes:
            await send_message(user_id, "❌ Не удалось скачать файл. Попробуй ещё раз.", token=tok)
            return

        try:
            fname_lower = file_name.lower()
            if fname_lower.endswith(".csv"):
                sheets_data = parse_csv(file_bytes)
            elif fname_lower.endswith(".pdf"):
                sheets_data = parse_pdf(file_bytes)
            else:
                sheets_data = parse_excel(file_bytes)
        except Exception as e:
            logger.exception("Spreadsheet parse error")
            await send_message(user_id, f"❌ Ошибка чтения файла: {e}", token=tok)
            return

        if not sheets_data:
            await send_message(user_id, "❌ Файл пустой или не содержит данных с заголовками.", token=tok)
            return

    # Determine project name — all sheets become ONE project
    if project_name_override.strip():
        name = project_name_override.strip()
    else:
        non_generic = [s for s in sheets_data.keys() if not _GENERIC_SHEET.match(s.strip())]
        if len(non_generic) == 1:
            name = non_generic[0].strip()
        else:
            name = await _detect_project_name_or_none(sheets_data, file_name)
            if name is None:
                # Ask admin to name the project
                _waiting_for_project_name[user_id] = {
                    "agency_id": agency.id,
                    "file_name": file_name,
                    "sheets_data": sheets_data,
                }
                sheet_list = ", ".join(list(sheets_data.keys())[:5])
                await send_message(
                    user_id,
                    f"📊 Файл прочитан. Листов: *{len(sheets_data)}* ({sheet_list})\n\n"
                    "Как назвать этот проект? Напишите название:",
                    token=tok,
                )
                return

    r = await _save_project(name, sheets_data, user_id, db, agency_id=agency.id)

    if r["status"] == "skipped":
        await send_message(user_id, "❌ В файле не найдены юниты (нет числовых номеров в данных).", token=tok)
        return

    if r["status"] == "updated":
        await send_message(
            user_id,
            f"✅ Проект *{r['name']}* обновлён → v{r['version']}\n"
            f"Листов: {len(sheets_data)}, юнитов: {r['units']}\n\n{r['diff_report']}",
            token=tok,
        )
    else:
        await send_message(
            user_id,
            f"✅ Проект *{r['name']}* сохранён!\nЛистов: {len(sheets_data)}, юнитов: {r['units']}",
            token=tok,
        )

    if r["status"] == "created":
        groups = db.query(ToniGroup).filter(
            ToniGroup.active == True, ToniGroup.agency_id == agency.id
        ).all()
        announce = f"📁 Новый проект добавлен: *{r['name']}*\nСпрашивайте по номеру юнита!"
        for g in groups:
            await toni_bot._send(g.chat_id, announce, agency.bot_token)


# ─── Pending broadcast state (in-memory, cleared on restart) ─────────────────

# pending[uid] = {agency_id, admin_id, from_chat_id, message_id, description}
_pending_broadcasts: dict[str, dict] = {}
# waiting_for_edit[admin_id] = uid  — admin is typing a new description
_waiting_for_edit: dict[str, str] = {}
# waiting_for_project_name[admin_id] = {agency_id, sheets_data, file_name}
_waiting_for_project_name: dict[str, dict] = {}


def _is_urgent(text: str) -> bool:
    t = (text or "").lower()
    return any(w in t for w in ("срочно", "urgent", "asap", "жон", "tez", "немедленно"))


def _wants_broadcast(caption: str) -> bool:
    c = (caption or "").lower()
    return any(w in c for w in ("скинь всем", "отправь всем", "send all", "broadcast",
                                 "разошли", "скинь", "отправь", "share"))


def _is_inventory_file(fname: str, caption: str) -> bool:
    f, c = fname.lower(), (caption or "").lower()
    if f.endswith((".xlsx", ".xls", ".csv")):
        return True
    inv_kw = ("инвентарь", "inventory", "прайс", "price list", "availability", "unit list")
    return any(w in c or w in f for w in inv_kw)


def _store_pending(agency_id: int, admin_id: str, message_id: int, description: str = "") -> str:
    uid = uuid.uuid4().hex[:8]
    _pending_broadcasts[uid] = {
        "agency_id": agency_id,
        "admin_id": admin_id,
        "from_chat_id": admin_id,
        "message_id": message_id,
        "description": description,
    }
    return uid


def _confirm_keyboard(uid: str, has_caption: bool = False) -> dict:
    """has_caption=True → 2 buttons (admin wrote own description, no AI edit needed)."""
    if has_caption:
        return {"inline_keyboard": [[
            {"text": "✅ Отправить", "callback_data": f"confirm:{uid}"},
            {"text": "❌ Отмена", "callback_data": f"cancel:{uid}"},
        ]]}
    return {"inline_keyboard": [
        [{"text": "✅ Отправить с описанием", "callback_data": f"confirm:{uid}"}],
        [
            {"text": "✏️ Изменить описание", "callback_data": f"edit:{uid}"},
            {"text": "❌ Отмена", "callback_data": f"cancel:{uid}"},
        ],
    ]}


def _save_brochure(db: Session, agency_id: int, file_id: str, fuid: str,
                   fname: str, caption: str, file_type: str,
                   message_id: int, from_chat_id: str) -> "ToniFile":
    key = fuid or f"admin_{from_chat_id}_{message_id}"
    existing = db.query(ToniFile).filter(ToniFile.file_unique_id == key).first()
    if existing:
        return existing
    units = list(set(toni_bot._extract_units(fname) + toni_bot._extract_units(caption)))
    tf = ToniFile(
        agency_id=agency_id, file_id=file_id, file_unique_id=key,
        file_name=fname, caption=caption, file_type=file_type,
        unit_numbers=units, project_name="",
        message_id=message_id, channel_chat_id=from_chat_id,
    )
    db.add(tf)
    db.commit()
    return tf


async def _analyze_brochure(file_bytes: bytes | None, fname: str, caption: str, file_type: str) -> str:
    """Read file content with Claude. Returns formatted description for agents."""
    fname_lower = fname.lower()
    try:
        ai = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        if file_bytes and fname_lower.endswith(".pdf"):
            import io as _io
            import pdfplumber
            text = ""
            with pdfplumber.open(_io.BytesIO(file_bytes)) as pdf:
                for page in pdf.pages[:8]:
                    text += (page.extract_text() or "") + "\n"
            if text.strip():
                resp = await ai.messages.create(
                    model="claude-haiku-4-5-20251001", max_tokens=600,
                    messages=[{"role": "user", "content": (
                        f"Файл: {fname}\n\nСодержимое PDF:\n{text[:4000]}\n\n"
                        "Составь краткое профессиональное описание (4-6 строк) для команды агентов Dubai. "
                        "Выдели: название проекта, цены, типы юнитов, ключевые особенности, ROI если есть. "
                        "Используй эмодзи. Ответь на русском языке."
                    )}],
                )
                return resp.content[0].text.strip()

        if file_bytes and file_type == "photo":
            return await _analyze_image(file_bytes, caption or "")

        resp = await ai.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=200,
            messages=[{"role": "user", "content": (
                f"Файл: {fname}\nПодпись: {caption or 'нет'}\n"
                "Составь 2-3 строки описания для агентов недвижимости. Ответ на русском."
            )}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.warning(f"Brochure analysis error: {e}")
        return caption or fname


async def _execute_broadcast(agency: Agency, from_chat_id: str, message_id: int,
                              description: str, db: Session, tok: str) -> tuple[list, list]:
    """Send description text → forward original file → footer to every active group.
    Returns (success_group_names, failed_group_names)."""
    groups = db.query(ToniGroup).filter(
        ToniGroup.active == True, ToniGroup.agency_id == agency.id
    ).all()
    footer = f"💬 Есть вопросы? Пишите — отвечу!\n📞 {agency.umar_contact}"
    success, failed = [], []

    for i, g in enumerate(groups):
        if i > 0:
            await asyncio.sleep(20)
        try:
            if description:
                await toni_bot._send(g.chat_id, description, tok)
            ok = await toni_bot._copy(g.chat_id, from_chat_id, message_id, tok)
            await toni_bot._send(g.chat_id, footer, tok)
            (success if ok else failed).append(g.title or g.chat_id)
        except Exception:
            failed.append(g.title or g.chat_id)

    return success, failed


async def _handle_brochure_file(agency: Agency, user_id: str, file_id: str, fuid: str,
                                 fname: str, caption: str, msg_id: int, file_type: str,
                                 db: Session, tok: str):
    """Core brochure flow: save → urgent/auto-send or analyze → link project → confirm."""
    _save_brochure(db, agency.id, file_id, fuid, fname, caption, file_type, msg_id, user_id)

    # ── URGENT: no confirmation, broadcast immediately ────────────────────────
    if _is_urgent(caption):
        success, failed = await _execute_broadcast(agency, user_id, msg_id, caption, db, tok)
        lines = [f"⚡ Отправлено {len(success)} группам мгновенно!"]
        if failed:
            lines.append(f"❌ Ошибка: {', '.join(failed)}")
        await send_message(user_id, "\n".join(lines), token=tok)
        return

    # ── Admin wrote own caption: use it, skip Claude ──────────────────────────
    has_caption = bool(caption.strip())
    if has_caption:
        description = caption
    else:
        fname_lower = fname.lower()
        is_video = fname_lower.endswith((".mp4", ".avi", ".mov", ".mkv"))
        file_bytes = None if is_video else await get_file_bytes(file_id, token=tok)
        await send_message(user_id, f"📖 Читаю *{fname}* через Claude и сохраняю в базу данных...", token=tok)
        description = await _analyze_brochure(file_bytes, fname, caption, file_type)
        await send_message(user_id, f"✅ Файл *{fname}* сохранён в базу данных.", token=tok)

    # ── "Скинь всем" in caption: auto-broadcast without confirmation ──────────
    if _wants_broadcast(caption):
        success, failed = await _execute_broadcast(agency, user_id, msg_id, description, db, tok)
        lines = [f"✅ Отправлено: {len(success)} групп"]
        if failed:
            lines.append(f"❌ Ошибка: {', '.join(failed)}")
        await send_message(user_id, "\n".join(lines), token=tok)
        return

    # ── Normal: store pending, ask which project, then confirm broadcast ──────
    uid = _store_pending(agency.id, user_id, msg_id, description)

    projects = (
        db.query(ToniProject)
        .filter(ToniProject.is_active == True, ToniProject.agency_id == agency.id)
        .order_by(ToniProject.uploaded_at.desc())
        .limit(8)
        .all()
    )

    if projects:
        buttons = [
            [{"text": f"📁 {p.project_name}", "callback_data": f"linkproj:{uid}:{p.id}"}]
            for p in projects
        ]
        buttons.append([{"text": "⏩ Без проекта", "callback_data": f"linkproj:{uid}:0"}])
        desc_preview = description[:200] + "…" if len(description) > 200 else description
        await send_message_with_keyboard(
            user_id,
            f"✅ *{fname}* сохранён в базу данных.\n\n📝 _{desc_preview}_\n\nК какому проекту относится этот материал?",
            {"inline_keyboard": buttons},
            token=tok,
        )
    else:
        # No projects yet — go straight to broadcast confirmation
        if has_caption:
            await send_message_with_keyboard(
                user_id,
                f"📎 *{fname}*\n\n_{description}_\n\nГотово. Отправляем?",
                _confirm_keyboard(uid, has_caption=True),
                token=tok,
            )
        else:
            await send_message(user_id, f"📝 *Описание для рассылки:*\n\n{description}", token=tok)
            await send_message_with_keyboard(
                user_id, "Отправляем с этим описанием?",
                _confirm_keyboard(uid, has_caption=False),
                token=tok,
            )


# ─── Telegram webhook (per-agency) ────────────────────────────────────────────

async def _handle_webhook(data: dict, agency: Agency, db: Session):
    """Core webhook logic shared between /telegram/webhook/{slug} and legacy route."""
    tok = agency.bot_token

    if data.get("channel_post"):
        await toni_bot.handle_update(data, agency)
        return

    if callback := data.get("callback_query"):
        cb_data = callback.get("data", "")
        cb_from_id = str(callback.get("from", {}).get("id", ""))
        cb_msg_id = callback.get("message", {}).get("message_id")
        await answer_callback_query(callback["id"], token=tok)

        if not is_admin(cb_from_id, agency):
            return

        if cb_data.startswith("confirm:"):
            uid = cb_data[8:]
            pending = _pending_broadcasts.pop(uid, None)
            if pending and pending["agency_id"] == agency.id:
                await edit_message_text(cb_from_id, cb_msg_id, "⏳ Рассылаю по группам...", token=tok)
                success, failed = await _execute_broadcast(
                    agency, pending["from_chat_id"], pending["message_id"],
                    pending.get("description", ""), db, tok,
                )
                lines = [f"✅ Отправлено: {len(success)} групп"]
                if failed:
                    lines.append(f"❌ Ошибка: {', '.join(failed)}")
                await edit_message_text(cb_from_id, cb_msg_id, "\n".join(lines), token=tok)
            else:
                await edit_message_text(cb_from_id, cb_msg_id, "❌ Запрос устарел.", token=tok)

        elif cb_data.startswith("edit:"):
            uid = cb_data[5:]
            if uid in _pending_broadcasts:
                _waiting_for_edit[cb_from_id] = uid
                await edit_message_text(
                    cb_from_id, cb_msg_id,
                    "✏️ Введите новое описание — я обновлю и спрошу снова:",
                    token=tok,
                )
            else:
                await edit_message_text(cb_from_id, cb_msg_id, "❌ Запрос устарел.", token=tok)

        elif cb_data.startswith("linkproj:"):
            # linkproj:{pending_uid}:{project_id} — link brochure to a project
            rest = cb_data[9:]
            uid, pid_str = rest.rsplit(":", 1)
            pid = int(pid_str)

            pending = _pending_broadcasts.get(uid)
            if not pending or pending["agency_id"] != agency.id:
                await edit_message_text(cb_from_id, cb_msg_id, "❌ Запрос устарел.", token=tok)
                return

            proj_name = ""
            if pid > 0:
                proj = db.query(ToniProject).filter(
                    ToniProject.id == pid, ToniProject.agency_id == agency.id
                ).first()
                if proj:
                    tf = db.query(ToniFile).filter(
                        ToniFile.message_id == pending["message_id"],
                        ToniFile.channel_chat_id == pending["from_chat_id"],
                        ToniFile.agency_id == agency.id,
                    ).first()
                    if tf:
                        tf.project_name = proj.project_name
                        db.commit()
                    proj_name = proj.project_name

            description = pending.get("description", "")
            if proj_name:
                header = f"✅ Привязан к проекту *{proj_name}*.\n\n"
            else:
                header = "✅ Сохранено без проекта.\n\n"
            desc_preview = description[:200] + "…" if len(description) > 200 else description
            await edit_message_text(
                cb_from_id, cb_msg_id,
                header + f"📝 _{desc_preview}_",
                token=tok,
            )
            await send_message_with_keyboard(
                cb_from_id,
                "Отправляем описание и файл в группы?",
                _confirm_keyboard(uid, has_caption=True),
                token=tok,
            )

        elif cb_data.startswith("cancel:"):
            uid = cb_data[7:]
            _pending_broadcasts.pop(uid, None)
            _waiting_for_edit.pop(cb_from_id, None)
            await edit_message_text(cb_from_id, cb_msg_id, "❌ Отменено.", token=tok)
        return

    if data.get("edited_message"):
        return

    message = data.get("message")
    if not message:
        return

    if message.get("from", {}).get("is_bot"):
        return

    chat_type = message.get("chat", {}).get("type", "")
    user_id = str(message.get("from", {}).get("id", ""))
    text = (message.get("text") or "").strip()

    if chat_type in ("group", "supergroup"):
        await toni_bot.handle_update(data, agency)
        return

    if not is_admin(user_id, agency):
        return

    toni_bot.mark_admin_active(agency.id)

    # ── 1. Voice ──────────────────────────────────────────────────────────────
    voice = message.get("voice") or message.get("audio")
    if voice and not text:
        await send_typing(user_id, token=tok)
        file_bytes = await get_file_bytes(voice["file_id"], token=tok)
        if file_bytes:
            transcript = await _transcribe_voice(file_bytes)
            if transcript:
                await send_message(user_id, f"🎤 _{transcript}_", token=tok)
                text = transcript
            else:
                await send_message(
                    user_id,
                    "❌ Не могу распознать голосовое — добавь GROQ\\_API\\_KEY в переменные окружения.",
                    token=tok,
                )
                return
        else:
            return

    # ── 2. Photo ──────────────────────────────────────────────────────────────
    photos = message.get("photo")
    if photos:
        photo = photos[-1]
        caption = (message.get("caption") or "").strip()
        msg_id = message.get("message_id")
        await send_typing(user_id, token=tok)

        await _handle_brochure_file(
            agency, user_id, photo["file_id"], photo.get("file_unique_id", ""),
            caption or "Фото", caption, msg_id, "photo", db, tok
        )
        return

    # ── 3. Document ───────────────────────────────────────────────────────────
    doc = message.get("document")
    if doc:
        fname = doc.get("file_name", "")
        caption = (message.get("caption") or "").strip()
        fname_lower = fname.lower()
        msg_id = message.get("message_id")
        fuid = doc.get("file_unique_id", "")
        await send_typing(user_id, token=tok)

        # Excel / CSV → inventory/project data
        if fname_lower.endswith((".xlsx", ".xls", ".csv")):
            proj_name_hint = caption if not _wants_broadcast(caption) else ""
            await _process_excel_upload(agency, user_id, doc["file_id"], fname, proj_name_hint, db)
            return

        # PDF → if explicit inventory keyword: project data. Otherwise: brochure
        if fname_lower.endswith(".pdf"):
            if _is_inventory_file(fname, caption) and any(
                kw in (caption or "").lower()
                for kw in ("инвентарь", "inventory", "прайс", "price list", "availability")
            ):
                await _process_excel_upload(agency, user_id, doc["file_id"], fname, caption, db)
            else:
                await _handle_brochure_file(
                    agency, user_id, doc["file_id"], fuid, fname, caption, msg_id, "document", db, tok
                )
            return

        # Image documents → brochure
        if fname_lower.endswith((".png", ".jpg", ".jpeg", ".webp")):
            await _handle_brochure_file(
                agency, user_id, doc["file_id"], fuid, fname, caption, msg_id, "photo", db, tok
            )
            return

        # Any other file (Word, video, etc.) → brochure
        await _handle_brochure_file(
            agency, user_id, doc["file_id"], fuid, fname, caption, msg_id, "document", db, tok
        )
        return

    if not text:
        return

    await send_typing(user_id, token=tok)

    # ── Admin is naming a newly-uploaded project ─────────────────────────────
    if user_id in _waiting_for_project_name:
        pending = _waiting_for_project_name.pop(user_id)
        if pending["agency_id"] == agency.id:
            name = text.strip()
            r = await _save_project(name, pending["sheets_data"], user_id, db, agency_id=agency.id)
            if r["status"] == "skipped":
                await send_message(user_id, "❌ В файле не найдены юниты.", token=tok)
            elif r["status"] == "updated":
                await send_message(
                    user_id,
                    f"✅ Проект *{r['name']}* обновлён → v{r['version']}\n"
                    f"Листов: {len(pending['sheets_data'])}, юнитов: {r['units']}\n\n{r['diff_report']}",
                    token=tok,
                )
            else:
                await send_message(
                    user_id,
                    f"✅ Проект *{r['name']}* сохранён!\n"
                    f"Листов: {len(pending['sheets_data'])}, юнитов: {r['units']}",
                    token=tok,
                )
                groups = db.query(ToniGroup).filter(
                    ToniGroup.active == True, ToniGroup.agency_id == agency.id
                ).all()
                for g in groups:
                    await toni_bot._send(g.chat_id, f"📁 Новый проект: *{r['name']}*\nСпрашивайте по номеру юнита!", agency.bot_token)
        return

    # ── Admin is editing a description (after clicking ✏️) ──────────────────
    if user_id in _waiting_for_edit:
        uid = _waiting_for_edit.pop(user_id)
        pending = _pending_broadcasts.get(uid)
        if pending and pending["agency_id"] == agency.id:
            # Update description and re-ask confirmation
            _pending_broadcasts[uid]["description"] = text
            await send_message(user_id, f"📝 *Новое описание:*\n\n{text}", token=tok)
            await send_message_with_keyboard(
                user_id, "Отправляем с обновлённым описанием?",
                _confirm_keyboard(uid, has_caption=True),
                token=tok,
            )
        else:
            await send_message(user_id, "❌ Файл не найден — отправьте его заново.", token=tok)
        return

    # ── Urgent text → execute pending broadcast immediately ──────────────────
    if _is_urgent(text):
        uid_found = next(
            (k for k, v in _pending_broadcasts.items()
             if v["agency_id"] == agency.id and v["admin_id"] == user_id),
            None,
        )
        if uid_found:
            pending = _pending_broadcasts.pop(uid_found)
            success, failed = await _execute_broadcast(
                agency, pending["from_chat_id"], pending["message_id"],
                pending.get("description", ""), db, tok,
            )
            lines = [f"⚡ Отправлено всем {len(success)} группам мгновенно!"]
            if failed:
                lines.append(f"❌ Ошибка: {', '.join(failed)}")
            await send_message(user_id, "\n".join(lines), token=tok)
            return

    if text == "/tonigroups":
        groups = db.query(ToniGroup).filter(
            ToniGroup.active == True, ToniGroup.agency_id == agency.id
        ).all()
        if not groups:
            await send_message(user_id, "Бот ещё не добавлен ни в одну группу.", token=tok)
        else:
            lines = [f"📋 Групп зарегистрировано: {len(groups)}\n"]
            for g in groups:
                lines.append(f"• {g.title or '—'} (`{g.chat_id}`)")
            await send_message(user_id, "\n".join(lines), token=tok)
        return

    if text == "/toniprojects":
        projs = db.query(ToniProject).filter(
            ToniProject.is_active == True, ToniProject.agency_id == agency.id
        ).all()
        if not projs:
            await send_message(user_id, f"Проекты не загружены. Откройте /admin/{agency.slug}", token=tok)
        else:
            lines = [f"📁 Проектов в базе: {len(projs)}\n"]
            for p in projs:
                lines.append(f"• {p.project_name} — {p.unit_count} юн., v{p.version}")
            await send_message(user_id, "\n".join(lines), token=tok)
        return

    if text == "/tonifiles":
        files = db.query(ToniFile).filter(
            ToniFile.agency_id == agency.id
        ).order_by(ToniFile.id.desc()).limit(10).all()
        if not files:
            await send_message(user_id, "База файлов пуста.", token=tok)
        else:
            lines = [f"📁 Последние {len(files)} файлов:\n"]
            for f in files:
                units = ", ".join(f.unit_numbers) if f.unit_numbers else "—"
                lines.append(f"• {f.file_name} | юниты: {units}")
            await send_message(user_id, "\n".join(lines), token=tok)
        return

    if text.startswith("/toniannounce "):
        msg_text = text[len("/toniannounce "):].strip()
        groups = db.query(ToniGroup).filter(
            ToniGroup.active == True, ToniGroup.agency_id == agency.id
        ).all()
        for g in groups:
            await toni_bot._send(g.chat_id, msg_text, agency.bot_token)
        await send_message(user_id, f"✅ Отправлено в {len(groups)} групп(ы).", token=tok)
        return

    try:
        reply = await admin_agent.process(agency=agency, user_id=user_id, message=text, db=db)
        await send_message(user_id, reply, token=tok)
    except Exception as e:
        logger.exception(f"Admin agent error: {e}")
        await send_message(user_id, f"Ошибка: {e}", token=tok)


async def _bg_telegram_webhook(data: dict, agency_id: int):
    """Background task — processes Telegram update with its own DB session."""
    db = SessionLocal()
    try:
        agency = db.query(Agency).filter(Agency.id == agency_id, Agency.is_active == True).first()
        if agency:
            await _handle_webhook(data, agency, db)
    except Exception:
        logger.exception("TG webhook background error")
    finally:
        db.close()


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


@app.post("/telegram/webhook/{slug}")
async def telegram_webhook_agency(slug: str, request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    db = SessionLocal()
    try:
        agency = db.query(Agency).filter(Agency.slug == slug, Agency.is_active == True).first()
        agency_id = agency.id if agency else None
    finally:
        db.close()
    if agency_id:
        background_tasks.add_task(_bg_telegram_webhook, data, agency_id)
    return {"ok": True}


@app.post("/telegram/webhook")
async def telegram_webhook_default(request: Request, background_tasks: BackgroundTasks):
    """Backward-compat route — uses the 'default' agency slug."""
    data = await request.json()
    db = SessionLocal()
    try:
        agency = db.query(Agency).filter(Agency.slug == "default", Agency.is_active == True).first()
        agency_id = agency.id if agency else None
    finally:
        db.close()
    if agency_id:
        background_tasks.add_task(_bg_telegram_webhook, data, agency_id)
    return {"ok": True}


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
