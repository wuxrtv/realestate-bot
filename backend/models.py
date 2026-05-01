from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import JSON
from sqlalchemy.sql import func
from database import Base
import os

# Use JSONB for Postgres, JSON for SQLite
_json_type = JSONB if "postgresql" in os.getenv("DATABASE_URL", "") else JSON


class Lead(Base):
    __tablename__ = "leads"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, unique=True, index=True)
    platform = Column(String, default="telegram")  # telegram | whatsapp
    name = Column(String)
    phone = Column(String)
    email = Column(String)
    goal = Column(String)        # buy | rent | sell | lease_out
    budget_min = Column(Float)
    budget_max = Column(Float)
    area = Column(String)
    property_type = Column(String)  # apartment | house | commercial
    rooms = Column(Integer)
    urgency = Column(String)     # asap | 1month | 3months | just_looking
    status = Column(String, default="new")  # new | qualified | viewing_scheduled | converted | lost
    assigned_agent = Column(String)
    notes = Column(Text)
    last_contact = Column(DateTime, default=func.now(), onupdate=func.now())
    follow_up_sent = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())


class Property(Base):
    __tablename__ = "properties"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True)
    title = Column(String)
    listing_type = Column(String)   # sale | rent
    property_type = Column(String)  # apartment | house | commercial
    price = Column(Float)           # цена продажи
    rent_price = Column(Float)      # цена аренды/мес
    area = Column(String)           # район
    address = Column(String)
    rooms = Column(Integer)
    square_meters = Column(Float)
    floor = Column(Integer)
    total_floors = Column(Integer)
    description = Column(Text)
    features = Column(_json_type)   # список особенностей
    photos = Column(_json_type)     # список URL фото
    video_url = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    agent_name = Column(String)
    agent_phone = Column(String)
    status = Column(String, default="active")  # active | sold | rented
    previous_price = Column(Float)             # для отслеживания снижения цены
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class Appointment(Base):
    __tablename__ = "appointments"

    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, index=True)
    property_id = Column(Integer, index=True)
    user_id = Column(String, index=True)
    client_name = Column(String)
    client_phone = Column(String)
    scheduled_at = Column(DateTime)
    status = Column(String, default="scheduled")  # scheduled | confirmed | completed | cancelled
    reminder_day_sent = Column(Boolean, default=False)
    reminder_hour_sent = Column(Boolean, default=False)
    agent_notified = Column(Boolean, default=False)
    notes = Column(Text)
    created_at = Column(DateTime, default=func.now())


class Conversation(Base):
    __tablename__ = "conversations"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, unique=True, index=True)
    platform = Column(String, default="telegram")
    history = Column(_json_type, default=list)   # список сообщений для Claude
    lead_data = Column(_json_type, default=dict)  # собранные данные квалификации
    state = Column(String, default="greeting")    # текущий этап разговора
    last_message_at = Column(DateTime, default=func.now(), onupdate=func.now())
    follow_up_sent = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
