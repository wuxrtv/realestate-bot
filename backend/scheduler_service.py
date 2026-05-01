"""
Scheduler service — called by n8n cron workflows via HTTP endpoints.
Returns lists of users/appointments that need automated messages.
"""

from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from models import Lead, Appointment, Property, Conversation


def get_followup_leads(db: Session) -> list[dict]:
    """
    Return leads that haven't responded in 24 hours and haven't received a follow-up yet.
    Called by n8n every hour.
    """
    cutoff = datetime.utcnow() - timedelta(hours=24)
    leads = (
        db.query(Lead)
        .filter(
            Lead.follow_up_sent == False,  # noqa: E712
            Lead.last_contact <= cutoff,
            Lead.status.in_(["new", "qualified"]),
        )
        .all()
    )

    results = []
    for lead in leads:
        conv = db.query(Conversation).filter(Conversation.user_id == lead.user_id).first()
        if not conv:
            continue
        results.append({
            "user_id": lead.user_id,
            "platform": lead.platform,
            "name": lead.name or "Клиент",
            "goal": lead.goal,
        })
        lead.follow_up_sent = True

    db.commit()
    return results


def get_appointment_reminders(db: Session) -> list[dict]:
    """
    Return appointments that need day-before or hour-before reminders.
    Called by n8n every 15 minutes.
    """
    now = datetime.utcnow()
    reminders = []

    # Day-before reminder: appointment is 22–26 hours away
    day_start = now + timedelta(hours=22)
    day_end = now + timedelta(hours=26)
    day_appts = (
        db.query(Appointment)
        .filter(
            Appointment.status == "scheduled",
            Appointment.reminder_day_sent == False,  # noqa: E712
            Appointment.scheduled_at.between(day_start, day_end),
        )
        .all()
    )
    for appt in day_appts:
        prop = db.query(Property).filter(Property.id == appt.property_id).first()
        reminders.append({
            "type": "day_reminder",
            "user_id": appt.user_id,
            "appointment_id": appt.id,
            "client_name": appt.client_name,
            "scheduled_at": appt.scheduled_at.isoformat() if appt.scheduled_at else None,
            "property_title": prop.title if prop else "объект",
            "property_address": prop.address if prop else "",
        })
        appt.reminder_day_sent = True

    # Hour-before reminder: appointment is 50–70 minutes away
    hour_start = now + timedelta(minutes=50)
    hour_end = now + timedelta(minutes=70)
    hour_appts = (
        db.query(Appointment)
        .filter(
            Appointment.status == "scheduled",
            Appointment.reminder_hour_sent == False,  # noqa: E712
            Appointment.scheduled_at.between(hour_start, hour_end),
        )
        .all()
    )
    for appt in hour_appts:
        prop = db.query(Property).filter(Property.id == appt.property_id).first()
        reminders.append({
            "type": "hour_reminder",
            "user_id": appt.user_id,
            "appointment_id": appt.id,
            "client_name": appt.client_name,
            "scheduled_at": appt.scheduled_at.isoformat() if appt.scheduled_at else None,
            "property_title": prop.title if prop else "объект",
            "property_address": prop.address if prop else "",
        })
        appt.reminder_hour_sent = True

    db.commit()
    return reminders


def get_price_drop_notifications(db: Session) -> list[dict]:
    """
    Find leads who searched for properties in areas where prices dropped.
    Called by n8n once a day.
    """
    notifications = []

    dropped_props = (
        db.query(Property)
        .filter(
            Property.status == "active",
            Property.previous_price.isnot(None),
            Property.price < Property.previous_price,
        )
        .all()
    )

    if not dropped_props:
        return []

    for prop in dropped_props:
        # Find leads interested in this area/type
        leads = (
            db.query(Lead)
            .filter(
                Lead.goal.in_(["buy", "rent"]),
                Lead.status.in_(["new", "qualified"]),
            )
            .all()
        )
        for lead in leads:
            area_match = not lead.area or (lead.area.lower() in (prop.area or "").lower())
            type_match = not lead.property_type or lead.property_type == prop.property_type
            budget_ok = not lead.budget_max or (
                (prop.price or prop.rent_price or 0) <= lead.budget_max * 1.1
            )
            if area_match and type_match and budget_ok:
                drop_pct = round((1 - prop.price / prop.previous_price) * 100)
                notifications.append({
                    "user_id": lead.user_id,
                    "platform": lead.platform,
                    "client_name": lead.name or "Клиент",
                    "property_title": prop.title,
                    "property_address": prop.address,
                    "old_price": prop.previous_price,
                    "new_price": prop.price,
                    "drop_pct": drop_pct,
                    "property_id": prop.external_id,
                })

    return notifications
