"""
FastAPI application — Toni bot for real estate agent groups.
Routes Telegram updates: groups/channel → Toni, private admin chat → AdminAgent.
Admin panel at /admin for uploading Excel project files.
"""

import logging
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy.orm import Session

from admin_agent import AdminAgent, is_admin
from database import get_db, init_db
from excel_parser import (
    build_unit_index,
    diff_unit_indexes,
    format_diff_report,
    normalize_project_name,
    parse_excel,
)
from models import ToniFile, ToniGroup, ToniProject
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
http_basic = HTTPBasic()


# ─── App lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    railway_url = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("PUBLIC_URL")
    if railway_url and os.getenv("TELEGRAM_BOT_TOKEN"):
        webhook_url = f"https://{railway_url}/telegram/webhook"
        await set_webhook(webhook_url)
        logger.info(f"Webhook set: {webhook_url}")

    scheduler.add_job(toni_bot.send_morning_report, "cron", hour=9, minute=0, id="toni_morning")
    scheduler.start()
    logger.info("Scheduler started")

    yield
    scheduler.shutdown()


app = FastAPI(title="Toni Bot", version="3.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Admin auth ───────────────────────────────────────────────────────────────

def _verify_admin(credentials: HTTPBasicCredentials = Depends(http_basic)):
    password = os.getenv("ADMIN_PASSWORD", "toni2024")
    ok = (
        secrets.compare_digest(credentials.username.encode(), b"admin")
        and secrets.compare_digest(credentials.password.encode(), password.encode())
    )
    if not ok:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )


# ─── Admin panel HTML ─────────────────────────────────────────────────────────

_ADMIN_CSS = """
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
.upload-zone { border: 2px dashed #c8d0e0; border-radius: 10px; padding: 28px 20px;
               text-align: center; cursor: pointer; transition: .2s; }
.upload-zone:hover, .upload-zone.drag { border-color: #4f6ef7; background: #f5f7ff; }
.upload-zone p { color: #718096; font-size: .9rem; margin-top: 6px; }
.upload-zone input { display: none; }
.name-row { display: flex; gap: 12px; margin-top: 14px; align-items: center; }
.name-row input[type=text] { flex: 1; border: 1px solid #d1d9e6; border-radius: 8px;
                              padding: 9px 14px; font-size: .9rem; outline: none; }
.name-row input[type=text]:focus { border-color: #4f6ef7; }
.btn { padding: 9px 22px; border: none; border-radius: 8px; cursor: pointer;
       font-size: .9rem; font-weight: 500; transition: .15s; }
.btn-blue { background: #4f6ef7; color: #fff; }
.btn-blue:hover { background: #3d5ce5; }
.alert { padding: 14px 18px; border-radius: 8px; margin-bottom: 18px;
         font-size: .9rem; white-space: pre-line; }
.alert-ok  { background: #f0fff4; border: 1px solid #9ae6b4; color: #276749; }
.alert-upd { background: #ebf8ff; border: 1px solid #90cdf4; color: #1a365d; }
.alert-err { background: #fff5f5; border: 1px solid #fc8181; color: #742a2a; }
table { width: 100%; border-collapse: collapse; font-size: .88rem; }
th { text-align: left; padding: 9px 14px; background: #f7f9fc;
     border-bottom: 2px solid #e8eaf0; color: #4a5568; font-weight: 600; }
td { padding: 9px 14px; border-bottom: 1px solid #edf0f7; vertical-align: top; }
tr:last-child td { border-bottom: none; }
.badge { display: inline-block; padding: 2px 9px; border-radius: 999px;
         font-size: .75rem; font-weight: 500; }
.badge-blue { background: #ebf4ff; color: #2b6cb0; }
.badge-green { background: #f0fff4; color: #276749; }
.sheet-list { font-size: .8rem; color: #718096; }
.empty { text-align: center; padding: 28px; color: #a0aec0; font-size: .9rem; }
"""

_ADMIN_JS = """
const zone = document.getElementById('zone');
const inp  = document.getElementById('file-inp');
const lbl  = document.getElementById('file-lbl');
zone.onclick = () => inp.click();
zone.ondragover = e => { e.preventDefault(); zone.classList.add('drag'); };
zone.ondragleave = () => zone.classList.remove('drag');
zone.ondrop = e => {
  e.preventDefault(); zone.classList.remove('drag');
  if (e.dataTransfer.files[0]) { inp.files = e.dataTransfer.files; updateLabel(); }
};
inp.onchange = updateLabel;
function updateLabel() {
  lbl.textContent = inp.files[0] ? '📄 ' + inp.files[0].name : '📁 Нажмите или перетащите файл';
}
"""


def _projects_table(projects: list) -> str:
    if not projects:
        return '<div class="empty">Проекты не загружены. Загрузите первый Excel-файл.</div>'

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
          <td><span class="badge badge-green">{p.unit_count} юнит.</span></td>
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


def _admin_html(projects: list, alert: str = "", alert_type: str = "ok") -> str:
    alert_html = ""
    if alert:
        alert_html = f'<div class="alert alert-{alert_type}">{alert}</div>'

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Тони — Admin Panel</title>
  <style>{_ADMIN_CSS}</style>
</head>
<body>
  <div class="header">
    <div>🤖</div>
    <div>
      <h1>Тони — Admin Panel</h1>
      <span>Управление проектами недвижимости</span>
    </div>
  </div>
  <div class="container">
    {alert_html}

    <div class="card">
      <h2>📤 Загрузить Excel-файл проекта</h2>
      <form method="post" action="/admin/upload" enctype="multipart/form-data">
        <div class="upload-zone" id="zone">
          <div style="font-size:2rem">📊</div>
          <strong id="file-lbl">📁 Нажмите или перетащите файл</strong>
          <p>Поддерживаются .xlsx и .xls с несколькими листами</p>
          <input type="file" name="file" id="file-inp" accept=".xlsx,.xls" required>
        </div>
        <div class="name-row">
          <input type="text" name="project_name" placeholder="Название проекта (необязательно — определится из файла)">
          <button type="submit" class="btn btn-blue">Загрузить и обработать</button>
        </div>
      </form>
    </div>

    <div class="card">
      <h2>📋 Активные проекты ({len(projects)})</h2>
      {_projects_table(projects)}
    </div>
  </div>
  <script>{_ADMIN_JS}</script>
</body>
</html>"""


# ─── Admin panel routes ───────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_db),
):
    projects = (
        db.query(ToniProject)
        .filter(ToniProject.is_active == True)
        .order_by(ToniProject.uploaded_at.desc())
        .all()
    )
    return _admin_html(projects)


@app.post("/admin/upload", response_class=HTMLResponse)
async def admin_upload(
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
    project_name: str = Form(default=""),
):
    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xls")):
        projects = db.query(ToniProject).filter(ToniProject.is_active == True).all()
        return _admin_html(projects, "❌ Загрузите файл Excel (.xlsx или .xls)", "err")

    try:
        content = await file.read()
        sheets_data = parse_excel(content)
    except Exception as e:
        logger.exception("Excel parse error")
        projects = db.query(ToniProject).filter(ToniProject.is_active == True).all()
        return _admin_html(projects, f"❌ Ошибка чтения файла: {e}", "err")

    if not sheets_data:
        projects = db.query(ToniProject).filter(ToniProject.is_active == True).all()
        return _admin_html(projects, "❌ Файл пустой или не содержит данных.", "err")

    name = (project_name.strip() or normalize_project_name(file.filename))
    unit_index = build_unit_index(sheets_data)

    # Check if project with same name already exists
    existing = (
        db.query(ToniProject)
        .filter(ToniProject.project_name == name, ToniProject.is_active == True)
        .first()
    )

    if existing:
        diff = diff_unit_indexes(existing.unit_index or {}, unit_index)
        report = format_diff_report(diff, name)
        new_version = existing.version + 1

        # Deactivate old version
        existing.is_active = False
        db.flush()

        proj = ToniProject(
            project_name=name,
            version=new_version,
            sheet_count=len(sheets_data),
            unit_count=len(unit_index),
            sheets_data=sheets_data,
            unit_index=unit_index,
            is_active=True,
            uploaded_at=datetime.now(),
            uploaded_by="web",
        )
        db.add(proj)
        db.commit()

        alert = f"✅ Проект «{name}» обновлён до версии {new_version}\n\n{report}"
        alert_type = "upd"
    else:
        proj = ToniProject(
            project_name=name,
            version=1,
            sheet_count=len(sheets_data),
            unit_count=len(unit_index),
            sheets_data=sheets_data,
            unit_index=unit_index,
            is_active=True,
            uploaded_at=datetime.now(),
            uploaded_by="web",
        )
        db.add(proj)
        db.commit()

        # Announce to all agent groups
        announce = (
            f"📁 Новый проект добавлен: *{name}*\n"
            f"Юнитов: {len(unit_index)}, листов: {len(sheets_data)}\n"
            f"Спрашивайте по номеру юнита!"
        )
        for group in db.query(ToniGroup).filter(ToniGroup.active == True).all():
            await toni_bot._send(group.chat_id, announce)

        alert = f"✅ Проект «{name}» добавлен: {len(unit_index)} юнитов, {len(sheets_data)} листов."
        alert_type = "ok"

    projects = (
        db.query(ToniProject)
        .filter(ToniProject.is_active == True)
        .order_by(ToniProject.uploaded_at.desc())
        .all()
    )
    return _admin_html(projects, alert, alert_type)


# ─── Telegram webhook ─────────────────────────────────────────────────────────

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    data = await request.json()

    if data.get("channel_post"):
        await toni_bot.handle_update(data)
        return {"ok": True}

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

    if chat_type in ("group", "supergroup"):
        await toni_bot.handle_update(data)
        return {"ok": True}

    if not text or not is_admin(user_id):
        return {"ok": True}

    await send_typing(user_id)

    if text == "/tonigroups":
        groups = db.query(ToniGroup).filter(ToniGroup.active == True).all()
        if not groups:
            await send_message(user_id, "Бот ещё не добавлен ни в одну группу.")
        else:
            lines = [f"📋 Групп зарегистрировано: {len(groups)}\n"]
            for g in groups:
                lines.append(f"• {g.title or '—'} (`{g.chat_id}`)")
            await send_message(user_id, "\n".join(lines))
        return {"ok": True}

    if text == "/toniprojects":
        projs = db.query(ToniProject).filter(ToniProject.is_active == True).all()
        if not projs:
            await send_message(user_id, "Проекты не загружены. Откройте /admin панель.")
        else:
            lines = [f"📁 Проектов в базе: {len(projs)}\n"]
            for p in projs:
                lines.append(f"• {p.project_name} — {p.unit_count} юн., v{p.version}")
            await send_message(user_id, "\n".join(lines))
        return {"ok": True}

    if text == "/tonifiles":
        files = db.query(ToniFile).order_by(ToniFile.id.desc()).limit(10).all()
        if not files:
            await send_message(user_id, "База файлов пуста.")
        else:
            lines = [f"📁 Последние {len(files)} файлов:\n"]
            for f in files:
                units = ", ".join(f.unit_numbers) if f.unit_numbers else "—"
                lines.append(f"• {f.file_name} | юниты: {units}")
            await send_message(user_id, "\n".join(lines))
        return {"ok": True}

    if text.startswith("/toniannounce "):
        msg_text = text[len("/toniannounce "):].strip()
        groups = db.query(ToniGroup).filter(ToniGroup.active == True).all()
        for g in groups:
            await toni_bot._send(g.chat_id, msg_text)
        await send_message(user_id, f"✅ Отправлено в {len(groups)} групп(ы).")
        return {"ok": True}

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
