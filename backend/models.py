from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import JSON
from sqlalchemy.sql import func
from database import Base
import os

_json_type = JSONB if "postgresql" in os.getenv("DATABASE_URL", "") else JSON


class Agency(Base):
    """One record per client agency that buys the SaaS product."""
    __tablename__ = "agencies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    slug = Column(String, unique=True, index=True)       # URL key: /telegram/webhook/{slug}
    bot_token = Column(String, unique=True, nullable=False)
    admin_ids = Column(_json_type, default=list)          # ["7567850330", ...]
    admin_password = Column(String, default="toni2024")   # for /admin/{slug}
    bot_username = Column(String, default="")             # for @mention detection in groups
    umar_contact = Column(String, default="@support")     # shown when unit not found
    db_channel_id = Column(String, default="")            # private file/brochure channel
    wa_instance_id = Column(String, default="")    # Green API instance ID
    wa_token = Column(String, default="")           # Green API token
    wa_admin_numbers = Column(_json_type, default=list)  # ["79001234567", ...]
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())


class ToniFile(Base):
    """File indexed from the private database channel or sent directly by admin."""
    __tablename__ = "toni_files"

    id = Column(Integer, primary_key=True, index=True)
    agency_id = Column(Integer, nullable=True, index=True)
    file_id = Column(String)
    file_unique_id = Column(String, unique=True, index=True)
    file_name = Column(String)
    caption = Column(Text)
    file_type = Column(String)
    unit_numbers = Column(_json_type, default=list)
    project_name = Column(String, default="", nullable=True)   # linked project
    message_id = Column(Integer)
    channel_chat_id = Column(String)
    created_at = Column(DateTime, default=func.now())


class ToniGroup(Base):
    """Agent group where the bot is present."""
    __tablename__ = "toni_groups"
    __table_args__ = (UniqueConstraint("agency_id", "chat_id", name="uq_agency_group"),)

    id = Column(Integer, primary_key=True, index=True)
    agency_id = Column(Integer, nullable=True, index=True)
    chat_id = Column(String, index=True)
    title = Column(String)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())


class AdminConversation(Base):
    """Persistent conversation history for admin ↔ AdminAgent chat."""
    __tablename__ = "admin_conversations"

    id = Column(Integer, primary_key=True, index=True)
    agency_id = Column(Integer, index=True)
    user_id = Column(String, index=True)
    history = Column(_json_type, default=list)   # list of {role, content} dicts
    updated_at = Column(DateTime, default=func.now())


class GroupConversation(Base):
    """Conversation history per group chat, shared across all agents in the group."""
    __tablename__ = "group_conversations"

    id = Column(Integer, primary_key=True, index=True)
    agency_id = Column(Integer, index=True)
    chat_id = Column(String, index=True)
    history = Column(_json_type, default=list)   # [{role, content}] last N exchanges
    updated_at = Column(DateTime, default=func.now())


class WhatsAppGroup(Base):
    """WhatsApp group where Tony is active."""
    __tablename__ = "whatsapp_groups"
    __table_args__ = (UniqueConstraint("agency_id", "chat_id", name="uq_agency_wa_group"),)

    id = Column(Integer, primary_key=True, index=True)
    agency_id = Column(Integer, nullable=True, index=True)
    chat_id = Column(String, index=True)   # e.g. "120363000000000000@g.us"
    title = Column(String)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())


class ToniProject(Base):
    """Excel project file uploaded via admin."""
    __tablename__ = "toni_projects"

    id = Column(Integer, primary_key=True, index=True)
    agency_id = Column(Integer, nullable=True, index=True)
    project_name = Column(String, index=True)
    version = Column(Integer, default=1)
    sheet_count = Column(Integer, default=0)
    unit_count = Column(Integer, default=0)
    sheets_data = Column(_json_type)
    unit_index = Column(_json_type)
    is_active = Column(Boolean, default=True)
    uploaded_at = Column(DateTime, default=func.now())
    uploaded_by = Column(String, default="web")
