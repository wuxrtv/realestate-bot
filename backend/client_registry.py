"""
Loads all client config files from the clients/ directory.
Files starting with _ are ignored (templates/examples).
"""

import importlib
import logging
from pathlib import Path

from clients._base import ClientConfig

logger = logging.getLogger(__name__)

_by_slug: dict[str, ClientConfig] = {}
_by_phone: dict[str, str] = {}  # phone → slug


def load_all():
    """Load all client config files. Call once at startup."""
    global _by_slug, _by_phone
    _by_slug = {}
    _by_phone = {}

    clients_dir = Path(__file__).parent / "clients"
    for f in sorted(clients_dir.glob("*.py")):
        if f.stem.startswith("_"):
            continue
        try:
            mod = importlib.import_module(f"clients.{f.stem}")
            cfg: ClientConfig = mod.config
            _by_slug[cfg.slug] = cfg
            for phone in cfg.admin_phones:
                clean = phone.lstrip("+").strip()
                _by_phone[clean] = cfg.slug
            logger.info(f"ClientRegistry: loaded '{cfg.name}' (slug={cfg.slug}, phones={cfg.admin_phones})")
        except Exception:
            logger.exception(f"ClientRegistry: failed to load {f.name}")

    logger.info(f"ClientRegistry: {len(_by_slug)} client(s) loaded")


def find_by_phone(phone: str) -> ClientConfig | None:
    """Find client config by admin phone number."""
    clean = phone.lstrip("+").strip()
    slug = _by_phone.get(clean)
    return _by_slug.get(slug) if slug else None


def get(slug: str) -> ClientConfig | None:
    return _by_slug.get(slug)


def all_clients() -> list[ClientConfig]:
    return list(_by_slug.values())


def sync_to_db():
    """Sync client configs to Agency records in DB. Called at startup."""
    from database import SessionLocal
    from models import Agency

    db = SessionLocal()
    try:
        for cfg in _by_slug.values():
            agency = db.query(Agency).filter(Agency.slug == cfg.slug).first()
            if agency:
                # Update existing record
                agency.name = cfg.name
                agency.wa_admin_numbers = cfg.admin_phones
                agency.drive_root_id = cfg.drive_root_id
                agency.umar_contact = cfg.umar_contact
                agency.admin_password = cfg.admin_password
                agency.bot_character = cfg.bot_character
                agency.is_active = True
            else:
                # Create new record
                db.add(Agency(
                    name=cfg.name,
                    slug=cfg.slug,
                    bot_token="",
                    admin_ids=[],
                    admin_password=cfg.admin_password,
                    umar_contact=cfg.umar_contact,
                    wa_instance_id="",
                    wa_token="",
                    wa_admin_numbers=cfg.admin_phones,
                    drive_root_id=cfg.drive_root_id,
                    bot_character=cfg.bot_character,
                    is_active=True,
                ))
            db.commit()
            logger.info(f"ClientRegistry: synced '{cfg.slug}' to DB")
    finally:
        db.close()
