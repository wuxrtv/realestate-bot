from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import JSON
from sqlalchemy.sql import func
from database import Base
import os

# Use JSONB for Postgres, JSON for SQLite
_json_type = JSONB if "postgresql" in os.getenv("DATABASE_URL", "") else JSON


class ToniFile(Base):
    """File indexed from the private database channel for Toni bot."""
    __tablename__ = "toni_files"

    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(String)
    file_unique_id = Column(String, unique=True, index=True)
    file_name = Column(String)
    caption = Column(Text)
    file_type = Column(String)            # document | photo | video
    unit_numbers = Column(_json_type, default=list)   # extracted unit numbers
    message_id = Column(Integer)          # original message_id in the channel
    channel_chat_id = Column(String)      # channel chat ID for copyMessage
    created_at = Column(DateTime, default=func.now())


class ToniGroup(Base):
    """Agent group where Toni is present."""
    __tablename__ = "toni_groups"

    id = Column(Integer, primary_key=True, index=True)
    chat_id = Column(String, unique=True, index=True)
    title = Column(String)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())


class ToniProject(Base):
    """Excel project file uploaded via admin panel."""
    __tablename__ = "toni_projects"

    id = Column(Integer, primary_key=True, index=True)
    project_name = Column(String, index=True)   # human-readable name (from filename or override)
    version = Column(Integer, default=1)
    sheet_count = Column(Integer, default=0)
    unit_count = Column(Integer, default=0)
    sheets_data = Column(_json_type)            # {sheet_name: [row_dicts]}
    unit_index = Column(_json_type)             # {unit_num: {_sheet, col: val, ...}}
    is_active = Column(Boolean, default=True)
    uploaded_at = Column(DateTime, default=func.now())
    uploaded_by = Column(String, default="web")
