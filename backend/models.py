from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import JSON
from sqlalchemy.sql import func
from database import Base
import os

_json_type = JSONB if "postgresql" in os.getenv("DATABASE_URL", "") else JSON


class Agency(Base):
    """One record per client. Populated from clients/ config files at startup."""
    __tablename__ = "agencies"

    id           = Column(Integer, primary_key=True, index=True)
    name         = Column(String, nullable=False)
    slug         = Column(String, unique=True, index=True)
    contact      = Column("umar_contact", String, default="@support")
    wa_admin_numbers = Column(_json_type, default=list)  # admin phone numbers
    drive_root_id    = Column(String, default="")        # Google Drive root folder ID
    bot_character    = Column(Text, default="")          # custom Tony personality
    is_active    = Column(Boolean, default=True)
    created_at   = Column(DateTime, default=func.now())


class AdminConversation(Base):
    """Conversation history for admin ↔ Tony private chat. Resets daily (Dubai time)."""
    __tablename__ = "admin_conversations"

    id                = Column(Integer, primary_key=True, index=True)
    agency_id         = Column(Integer, index=True)
    user_id           = Column(String, index=True)
    history           = Column(_json_type, default=list)
    conversation_date = Column(String, nullable=True)   # "YYYY-MM-DD" Dubai time
    updated_at        = Column(DateTime, default=func.now())


class GroupConversation(Base):
    """Conversation history per WhatsApp group. Resets daily (Dubai time)."""
    __tablename__ = "group_conversations"

    id                = Column(Integer, primary_key=True, index=True)
    agency_id         = Column(Integer, index=True)
    chat_id           = Column(String, index=True)
    history           = Column(_json_type, default=list)
    conversation_date = Column(String, nullable=True)   # "YYYY-MM-DD" Dubai time
    updated_at        = Column(DateTime, default=func.now())


class WhatsAppGroup(Base):
    """WhatsApp group where Tony is active."""
    __tablename__ = "whatsapp_groups"
    __table_args__ = (UniqueConstraint("agency_id", "chat_id", name="uq_agency_wa_group"),)

    id         = Column(Integer, primary_key=True, index=True)
    agency_id  = Column(Integer, nullable=True, index=True)
    chat_id    = Column(String, index=True)
    title      = Column(String)
    active     = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())


class ToniProject(Base):
    """Excel inventory uploaded for a client."""
    __tablename__ = "toni_projects"

    id           = Column(Integer, primary_key=True, index=True)
    agency_id    = Column(Integer, nullable=True, index=True)
    project_name = Column(String, index=True)
    version      = Column(Integer, default=1)
    sheet_count  = Column(Integer, default=0)
    unit_count   = Column(Integer, default=0)
    sheets_data  = Column(_json_type)
    unit_index   = Column(_json_type)
    is_active    = Column(Boolean, default=True)
    uploaded_at  = Column(DateTime, default=func.now())
    uploaded_by  = Column(String, default="")
