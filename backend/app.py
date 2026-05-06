"""
FastAPI application — Toni bot for real estate agent groups.
Routes Telegram updates: groups/channel → Toni, private admin chat → AdminAgent.
Admin panel at /admin for uploading Excel project files.
"""

import base64
import json
import logging
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime

import anthropic
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, HTTPException, Request
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
    parse_csv,
    parse_excel,
)
from models import ToniFile, ToniGroup, ToniProject
import toni_bot
from telegram_bot import (
    answer_callback_query,
    get_file_bytes,
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
.sheet-list { font-size: .8rem; color: #718096; }
.empty { text-align: center; padding: 28px; color: #a0aec0; font-size: .9rem; }
"""


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


def _admin_html(projects: list) -> str:
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Тони — Projects</title>
  <style>{_ADMIN_CSS}</style>
</head>
<body>
  <div class="header">
    <div>🤖</div>
    <div>
      <h1>Тони — База проектов</h1>
      <span>Только просмотр • загрузка через Telegram</span>
    </div>
  </div>
  <div class="container">
    <div class="card">
      <h2>📲 Как загрузить проект</h2>
      <div class="hint">
        Отправьте Excel-файл (<strong>.xlsx</strong> или <strong>.xls</strong>) боту в личный чат в Telegram.<br>
        • Бот прочитает все листы и запомнит данные.<br>
        • Если проект уже был загружен — бот покажет что изменилось.<br>
        • Подпись к файлу станет названием проекта (необязательно).
      </div>
    </div>
    <div class="card">
      <h2>📋 Активные проекты в памяти бота ({len(projects)})</h2>
      {_projects_table(projects)}
    </div>
  </div>
</body>
</html>"""


# ─── Admin panel route (read-only view) ──────────────────────────────────────

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


# ─── Admin helper utilities ──────────────────────────────────────────────────

async def _transcribe_voice(audio_bytes: bytes) -> str | None:
    """Transcribe a voice OGG file via Groq Whisper."""
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
    """Analyze an image with Claude vision and return a text description."""
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
    """Ask Claude to identify the project name from spreadsheet content."""
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


# ─── Telegram spreadsheet upload handler ─────────────────────────────────────

async def _process_excel_upload(
    user_id: str,
    file_id: str,
    file_name: str,
    project_name_override: str,
    db: Session,
):
    """Download Excel from Telegram, parse, save/update ToniProject, reply to admin."""
    await send_typing(user_id)
    await send_message(user_id, f"📊 Читаю файл *{file_name}*...")

    file_bytes = await get_file_bytes(file_id)
    if not file_bytes:
        await send_message(user_id, "❌ Не удалось скачать файл. Попробуй ещё раз.")
        return

    try:
        if file_name.lower().endswith(".csv"):
            sheets_data = parse_csv(file_bytes)
        else:
            sheets_data = parse_excel(file_bytes)
    except Exception as e:
        logger.exception("Spreadsheet parse error")
        await send_message(user_id, f"❌ Ошибка чтения файла: {e}")
        return

    if not sheets_data:
        await send_message(user_id, "❌ Файл пустой или не содержит данных с заголовками.")
        return

    if project_name_override.strip():
        name = project_name_override.strip()
    else:
        name = await _detect_project_name_ai(sheets_data, file_name)
    unit_index = build_unit_index(sheets_data)
    sheet_names = list(sheets_data.keys())

    existing = (
        db.query(ToniProject)
        .filter(ToniProject.project_name == name, ToniProject.is_active == True)
        .first()
    )

    if existing:
        # Update existing project: diff + replace
        diff = diff_unit_indexes(existing.unit_index or {}, unit_index)
        report = format_diff_report(diff, name)
        new_version = existing.version + 1

        existing.is_active = False
        db.flush()

        db.add(ToniProject(
            project_name=name,
            version=new_version,
            sheet_count=len(sheets_data),
            unit_count=len(unit_index),
            sheets_data=sheets_data,
            unit_index=unit_index,
            is_active=True,
            uploaded_at=datetime.now(),
            uploaded_by=user_id,
        ))
        db.commit()

        await send_message(
            user_id,
            f"✅ Проект *{name}* обновлён → v{new_version}\n"
            f"Листов: {len(sheet_names)} ({', '.join(sheet_names[:3])}{'...' if len(sheet_names) > 3 else ''})\n"
            f"Юнитов в памяти: {len(unit_index)}\n\n"
            f"{report}"
        )
    else:
        # New project
        db.add(ToniProject(
            project_name=name,
            version=1,
            sheet_count=len(sheets_data),
            unit_count=len(unit_index),
            sheets_data=sheets_data,
            unit_index=unit_index,
            is_active=True,
            uploaded_at=datetime.now(),
            uploaded_by=user_id,
        ))
        db.commit()

        # Announce to all agent groups
        groups = db.query(ToniGroup).filter(ToniGroup.active == True).all()
        announce = (
            f"📁 Новый проект добавлен: *{name}*\n"
            f"Юнитов: {len(unit_index)}, листов: {len(sheet_names)}\n"
            f"Спрашивайте по номеру юнита!"
        )
        for g in groups:
            await toni_bot._send(g.chat_id, announce)

        await send_message(
            user_id,
            f"✅ Проект *{name}* сохранён в память бота!\n"
            f"Листов: {len(sheet_names)} ({', '.join(sheet_names[:3])}{'...' if len(sheet_names) > 3 else ''})\n"
            f"Юнитов: {len(unit_index)}\n"
            f"Объявление разослано в {len(groups)} групп(ы)."
        )


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

    if not is_admin(user_id):
        return {"ok": True}

    # ── 1. Voice / audio message → transcribe → treat as text ────────────────
    voice = message.get("voice") or message.get("audio")
    if voice and not text:
        await send_typing(user_id)
        file_bytes = await get_file_bytes(voice["file_id"])
        if file_bytes:
            transcript = await _transcribe_voice(file_bytes)
            if transcript:
                await send_message(user_id, f"🎤 _{transcript}_")
                text = transcript  # fall through to admin_agent below
            else:
                await send_message(
                    user_id,
                    "❌ Не могу распознать голосовое — добавь GROQ\\_API\\_KEY в переменные окружения."
                )
                return {"ok": True}
        else:
            return {"ok": True}

    # ── 2. Photo → Claude vision analysis ────────────────────────────────────
    photos = message.get("photo")
    if photos:
        await send_typing(user_id)
        photo = photos[-1]
        file_bytes = await get_file_bytes(photo["file_id"])
        if file_bytes:
            caption = (message.get("caption") or "").strip()
            result = await _analyze_image(file_bytes, caption, "image/jpeg")
            await send_message(user_id, result)
        return {"ok": True}

    # ── 3. Document ───────────────────────────────────────────────────────────
    doc = message.get("document")
    if doc:
        fname = doc.get("file_name", "")
        caption = (message.get("caption") or "").strip()
        fname_lower = fname.lower()

        # Spreadsheet → project memory
        if fname_lower.endswith((".xlsx", ".xls", ".csv")):
            await _process_excel_upload(user_id, doc["file_id"], fname, caption, db)
            return {"ok": True}

        # Image document → Claude vision
        if fname_lower.endswith((".png", ".jpg", ".jpeg", ".webp")):
            await send_typing(user_id)
            file_bytes = await get_file_bytes(doc["file_id"])
            if file_bytes:
                media_type = "image/png" if fname_lower.endswith(".png") else "image/jpeg"
                result = await _analyze_image(file_bytes, caption, media_type)
                await send_message(user_id, result)
            return {"ok": True}

        # Any other file → pass description to admin_agent
        await send_typing(user_id)
        file_desc = f"[Получил файл: {fname}. Подпись: {caption or 'нет'}]"
        try:
            reply = await admin_agent.process(user_id=user_id, message=file_desc, db=db)
            await send_message(user_id, reply)
        except Exception as e:
            logger.exception(f"Admin agent error on file: {e}")
        return {"ok": True}

    if not text:
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
