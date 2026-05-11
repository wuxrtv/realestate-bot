"""
Persistent group registry backed by groups.json.

Stored in the data/ directory — mount that path as a Railway persistent volume
so the file survives all code updates and redeployments.

Flow:
  startup      → sync_to_db()   : restore groups from file into WhatsAppGroup table
  new message  → register()     : save group to file + DB simultaneously
  admin cmd    → remove() / list: manage groups
"""

import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
_GROUPS_FILE = os.path.join(_DATA_DIR, "groups.json")

_cache: dict | None = None


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _load() -> dict:
    global _cache
    if _cache is not None:
        return _cache
    os.makedirs(_DATA_DIR, exist_ok=True)
    if os.path.exists(_GROUPS_FILE):
        try:
            with open(_GROUPS_FILE, encoding="utf-8") as f:
                _cache = json.load(f)
                return _cache
        except Exception:
            logger.exception("group_registry: failed to load groups.json")
    _cache = {"groups": []}
    return _cache


def _save(data: dict):
    global _cache
    _cache = data
    os.makedirs(_DATA_DIR, exist_ok=True)
    tmp = _GROUPS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, _GROUPS_FILE)
    logger.debug(f"group_registry: saved {len(data.get('groups', []))} groups")


# ─── Public API ───────────────────────────────────────────────────────────────

def get_groups(agency_id: int) -> list:
    """Return all active group dicts for this agency."""
    data = _load()
    return [g for g in data.get("groups", [])
            if g.get("agency_id") == agency_id and g.get("active", True)]


def register(chat_id: str, title: str, agency_id: int) -> bool:
    """Register a group permanently.
    Returns True = newly added (or re-activated), False = already known and active.
    """
    data = _load()
    groups = data.setdefault("groups", [])
    for g in groups:
        if g["id"] == chat_id and g.get("agency_id") == agency_id:
            if not g.get("active", True):
                g["active"] = True
                g["name"] = title or g["name"]
                _save(data)
                logger.info(f"group_registry: re-activated {chat_id} ({title})")
                return True
            # Already active — just update title if changed
            if g.get("name") != title and title:
                g["name"] = title
                _save(data)
            return False
    groups.append({
        "id": chat_id,
        "name": title or chat_id,
        "agency_id": agency_id,
        "added_date": datetime.now().strftime("%Y-%m-%d"),
        "active": True,
    })
    _save(data)
    logger.info(f"group_registry: NEW group saved — {chat_id} ({title}) agency={agency_id}")
    return True


def remove(chat_id: str, agency_id: int) -> bool:
    """Deactivate a group. Returns True if found."""
    data = _load()
    found = False
    for g in data.get("groups", []):
        if g["id"] == chat_id and g.get("agency_id") == agency_id:
            g["active"] = False
            found = True
    if found:
        _save(data)
        logger.info(f"group_registry: removed {chat_id} agency={agency_id}")
    return found


def list_groups(agency_id: int) -> str:
    """Return a formatted string listing active groups for admin."""
    groups = get_groups(agency_id)
    if not groups:
        return "Habibi no groups saved yet 😅\nJust write something in any group — I'll save it automatically 🔥"
    lines = [f"📱 Active groups ({len(groups)}):\n"]
    for g in groups:
        lines.append(f"• *{g['name']}*\n  ID: `{g['id']}`\n  Added: {g.get('added_date', '?')}")
    return "\n\n".join(lines)


# ─── Sync helpers ─────────────────────────────────────────────────────────────

def sync_to_db(db) -> int:
    """On startup: push groups.json → WhatsAppGroup table.
    Returns count of groups restored into DB.
    """
    from models import WhatsAppGroup
    data = _load()
    restored = 0
    for g in data.get("groups", []):
        if not g.get("active", True):
            continue
        chat_id = g["id"]
        agency_id = g.get("agency_id")
        if not agency_id:
            continue
        existing = db.query(WhatsAppGroup).filter(
            WhatsAppGroup.chat_id == chat_id,
            WhatsAppGroup.agency_id == agency_id,
        ).first()
        if not existing:
            db.add(WhatsAppGroup(
                chat_id=chat_id,
                title=g.get("name", chat_id),
                active=True,
                agency_id=agency_id,
            ))
            restored += 1
        elif not existing.active:
            existing.active = True
            restored += 1
    db.commit()
    if restored:
        logger.info(f"group_registry: restored {restored} groups from groups.json into DB")
    return restored


def sync_from_db(db) -> int:
    """One-time migration: pull existing WhatsAppGroup rows → groups.json."""
    from models import WhatsAppGroup
    groups = db.query(WhatsAppGroup).filter(WhatsAppGroup.active == True).all()
    added = 0
    for g in groups:
        if g.agency_id and register(g.chat_id, g.title or g.chat_id, g.agency_id):
            added += 1
    return added
