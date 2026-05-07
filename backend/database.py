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
    from models import Agency, AdminConversation, GroupConversation, ToniFile, ToniGroup, ToniProject  # noqa: F401
    Base.metadata.create_all(bind=engine)
    _migrate()
    _seed_default_agency()


def _migrate():
    """Add new columns to existing tables without dropping data."""
    additions = [
        ("toni_projects", "agency_id", "INTEGER"),
        ("toni_groups",   "agency_id", "INTEGER"),
        ("toni_files",    "agency_id", "INTEGER"),
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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
