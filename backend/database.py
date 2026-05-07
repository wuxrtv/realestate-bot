from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
import os

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/realestate.db")

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def init_db():
    from models import Agency, AdminConversation, GroupConversation, ToniFile, ToniGroup, ToniProject, WhatsAppGroup  # noqa: F401
    Base.metadata.create_all(bind=engine)
    _migrate()
    _seed_default_agency()
    _sync_env_to_default_agency()


def _migrate():
    """Add new columns to existing tables without dropping data."""
    additions = [
        ("toni_projects",  "agency_id",        "INTEGER"),
        ("toni_groups",    "agency_id",         "INTEGER"),
        ("toni_files",     "agency_id",         "INTEGER"),
        ("agencies",       "wa_instance_id",    "TEXT"),
        ("agencies",       "wa_token",          "TEXT"),
        ("agencies",       "wa_admin_numbers",  "TEXT"),
        ("toni_files",     "project_name",      "TEXT"),
    ]
    with engine.connect() as conn:
        for table, col, typedef in additions:
            try:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}"))
                conn.commit()
            except Exception:
                pass  # column already exists

        # Drop old unique constraint on toni_groups.chat_id if it exists (SQLite ignores this)
        try:
            conn.execute(text("DROP INDEX IF EXISTS ix_toni_groups_chat_id"))
            conn.commit()
        except Exception:
            pass


def _seed_default_agency():
    """Create a default agency from env vars if no agencies exist yet."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        return

    db = SessionLocal()
    try:
        from models import Agency
        if db.query(Agency).count() > 0:
            return
        raw_ids = [i.strip() for i in os.getenv("ADMIN_IDS", "").split(",") if i.strip()]
        db.add(Agency(
            name="Default Agency",
            slug="default",
            bot_token=token,
            admin_ids=raw_ids,
            admin_password=os.getenv("ADMIN_PASSWORD", "toni2024"),
            bot_username=os.getenv("TONI_BOT_USERNAME", ""),
            umar_contact=os.getenv("TONI_UMAR_CONTACT", "@support"),
            db_channel_id=os.getenv("TONI_DB_CHANNEL", ""),
        ))
        db.commit()
    finally:
        db.close()


def _sync_env_to_default_agency():
    """Sync WA env vars to the default agency on every startup."""
    wa_instance_id = os.getenv("WA_INSTANCE_ID", "")
    wa_token = os.getenv("WA_TOKEN", "")
    wa_admin_numbers_raw = os.getenv("WA_ADMIN_NUMBERS", "")

    if not wa_instance_id and not wa_token:
        return

    db = SessionLocal()
    try:
        from models import Agency
        agency = db.query(Agency).filter(Agency.slug == "default").first()
        if not agency:
            return
        if wa_instance_id:
            agency.wa_instance_id = wa_instance_id
        if wa_token:
            agency.wa_token = wa_token
        if wa_admin_numbers_raw:
            agency.wa_admin_numbers = [
                n.strip().lstrip("+") for n in wa_admin_numbers_raw.split(",") if n.strip()
            ]
        db.commit()
    finally:
        db.close()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
