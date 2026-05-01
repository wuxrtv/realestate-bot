"""
Property search and database seeding service.
"""

import json
import os
import logging
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_

from models import Property

logger = logging.getLogger(__name__)

PROPERTY_TYPE_MAP = {
    "apartment": "квартира",
    "house": "дом",
    "commercial": "коммерция",
}

LISTING_TYPE_MAP = {
    "buy": "sale",
    "rent": "rent",
}


class PropertyService:
    def __init__(self, db: Session | None = None):
        self.db = db

    def search(
        self,
        goal: str,
        budget_min: Optional[float] = None,
        budget_max: Optional[float] = None,
        area: Optional[str] = None,
        property_type: Optional[str] = None,
        rooms: Optional[int] = None,
        limit: int = 3,
    ) -> list[dict]:
        if not self.db:
            return []

        listing_type = LISTING_TYPE_MAP.get(goal, "sale")

        q = self.db.query(Property).filter(
            Property.status == "active",
            Property.listing_type == listing_type,
        )

        if property_type:
            q = q.filter(Property.property_type == property_type)

        if rooms is not None:
            # Allow ±1 room flexibility
            q = q.filter(Property.rooms.between(max(0, rooms - 1), rooms + 1))

        if area:
            q = q.filter(
                or_(
                    Property.area.ilike(f"%{area}%"),
                    Property.address.ilike(f"%{area}%"),
                )
            )

        if listing_type == "sale":
            if budget_max:
                q = q.filter(Property.price <= budget_max)
            if budget_min:
                q = q.filter(Property.price >= budget_min)
        else:
            if budget_max:
                q = q.filter(Property.rent_price <= budget_max)
            if budget_min:
                q = q.filter(Property.rent_price >= budget_min)

        props = q.order_by(
            Property.updated_at.desc()
        ).limit(limit).all()

        # If nothing found, relax area/rooms filter
        if not props and (area or rooms is not None):
            props = self._relaxed_search(listing_type, budget_min, budget_max, property_type, limit)

        return [self._to_dict(p) for p in props]

    def _relaxed_search(
        self,
        listing_type: str,
        budget_min,
        budget_max,
        property_type,
        limit: int,
    ) -> list[Property]:
        q = self.db.query(Property).filter(
            Property.status == "active",
            Property.listing_type == listing_type,
        )
        if property_type:
            q = q.filter(Property.property_type == property_type)
        if listing_type == "sale" and budget_max:
            q = q.filter(Property.price <= budget_max * 1.15)  # +15% tolerance
        elif budget_max:
            q = q.filter(Property.rent_price <= budget_max * 1.15)
        return q.order_by(Property.updated_at.desc()).limit(limit).all()

    def get_properties_with_price_drop(self) -> list[dict]:
        """Return properties where price was recently reduced."""
        props = self.db.query(Property).filter(
            Property.status == "active",
            Property.previous_price.isnot(None),
            Property.price < Property.previous_price,
        ).all()
        return [self._to_dict(p) for p in props]

    def load_sample_data(self):
        """Seed database with sample properties if empty."""
        if not self.db:
            return
        count = self.db.query(Property).count()
        if count > 0:
            return

        data_path = os.path.join(os.path.dirname(__file__), "..", "data", "sample_properties.json")
        if not os.path.exists(data_path):
            logger.warning("sample_properties.json not found, skipping seed")
            return

        with open(data_path, encoding="utf-8") as f:
            properties = json.load(f)

        for p in properties:
            prop = Property(
                external_id=p["external_id"],
                title=p["title"],
                listing_type=p["listing_type"],
                property_type=p["property_type"],
                price=p.get("price"),
                rent_price=p.get("rent_price"),
                area=p["area"],
                address=p["address"],
                rooms=p.get("rooms"),
                square_meters=p.get("square_meters"),
                floor=p.get("floor"),
                total_floors=p.get("total_floors"),
                description=p.get("description", ""),
                features=p.get("features", []),
                photos=p.get("photos", []),
                video_url=p.get("video_url"),
                latitude=p.get("latitude"),
                longitude=p.get("longitude"),
                agent_name=p.get("agent_name"),
                agent_phone=p.get("agent_phone"),
                status="active",
            )
            self.db.add(prop)

        self.db.commit()
        logger.info(f"Seeded {len(properties)} properties")

    @staticmethod
    def _to_dict(p: Property) -> dict:
        return {
            "external_id": p.external_id,
            "title": p.title,
            "listing_type": p.listing_type,
            "property_type": p.property_type,
            "price": p.price,
            "rent_price": p.rent_price,
            "area": p.area,
            "address": p.address,
            "rooms": p.rooms,
            "square_meters": p.square_meters,
            "floor": p.floor,
            "total_floors": p.total_floors,
            "description": p.description,
            "features": p.features or [],
            "photos": p.photos or [],
            "video_url": p.video_url,
            "latitude": p.latitude,
            "longitude": p.longitude,
            "agent_name": p.agent_name,
            "agent_phone": p.agent_phone,
        }
