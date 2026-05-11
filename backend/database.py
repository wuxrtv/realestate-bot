from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
import os

_data_dir = os.getenv("DATA_DIR", "./data")
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{_data_dir}/realestate.db")

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def init_db():
    from models import Agency, AdminConversation, GroupConversation, ToniProject, WhatsAppGroup  # noqa: F401
    Base.metadata.create_all(bind=engine)
    _migrate()

    import client_registry
    client_registry.load_all()
    client_registry.sync_to_db()


def _migrate():
    """Add new columns to existing tables without dropping data."""
    additions = [
        ("toni_projects",       "agency_id",          "INTEGER"),
        ("agencies",            "wa_admin_numbers",    "TEXT"),
        ("agencies",            "drive_root_id",       "TEXT DEFAULT ''"),
        ("agencies",            "bot_character",       "TEXT DEFAULT ''"),
        ("admin_conversations", "conversation_date",   "TEXT"),
        ("group_conversations", "conversation_date",   "TEXT"),
        ("whatsapp_groups",     "agency_id",           "INTEGER"),
    ]
    with engine.connect() as conn:
        for table, col, typedef in additions:
            try:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}"))
                conn.commit()
            except Exception:
                pass  # column already exists

        # Auto-assign agency_id to groups/projects with NULL (created before migration).
        for tbl in ("whatsapp_groups", "toni_projects"):
            try:
                conn.execute(text(f"""
                    UPDATE {tbl}
                    SET agency_id = (SELECT id FROM agencies ORDER BY id LIMIT 1)
                    WHERE agency_id IS NULL
                """))
                conn.commit()
            except Exception:
                pass

        # Ensure active=true for all groups where active is NULL (column added without default)
        try:
            conn.execute(text("UPDATE whatsapp_groups SET active = true WHERE active IS NULL"))
            conn.commit()
        except Exception:
            pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
