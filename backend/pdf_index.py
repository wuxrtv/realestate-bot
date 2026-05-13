"""
Pre-index system for sales offer PDFs.

On startup (and at 07:00 daily): scan all Drive PDFs, extract price/size/view,
save to data/index_{agency_id}.json.

Search is instant (JSON read). Only selected PDFs are downloaded from Drive.
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def _ensure_data_dir():
    os.makedirs(_DATA_DIR, exist_ok=True)


def _index_path(agency_id: int) -> str:
    return os.path.join(_DATA_DIR, f"index_{agency_id}.json")


# ─── Persistence ──────────────────────────────────────────────────────────────

def load_index(agency_id: int) -> dict:
    """Load index from disk. Returns {unit_key: data} or {}."""
    path = _index_path(agency_id)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            saved = json.load(f)
        return saved.get("units", {})
    except Exception:
        logger.exception(f"Index load failed for agency {agency_id}")
        return {}


def index_info(agency_id: int) -> dict:
    path = _index_path(agency_id)
    if not os.path.exists(path):
        return {"exists": False, "count": 0, "built_at": ""}
    try:
        with open(path, encoding="utf-8") as f:
            saved = json.load(f)
        return {
            "exists": True,
            "count": saved.get("count", 0),
            "built_at": saved.get("built_at", ""),
        }
    except Exception:
        return {"exists": False, "count": 0, "built_at": ""}


def _save_index(agency_id: int, units: dict):
    _ensure_data_dir()
    path = _index_path(agency_id)
    payload = {
        "agency_id": agency_id,
        "built_at": datetime.utcnow().isoformat(),
        "count": len(units),
        "units": units,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    logger.info(f"Index saved: {len(units)} units for agency {agency_id}")


# ─── Build ────────────────────────────────────────────────────────────────────

async def build_index(agency) -> int:
    """Full rebuild: scan Drive PDFs + extract price/size/view, save to disk.
    Returns count of indexed units.
    """
    import drive_service as _drive

    agency_id = agency.id
    root_id = getattr(agency, "drive_root_id", "") or ""

    svc = _drive.get_service()
    if not svc:
        logger.error(f"Index build skipped: Drive not available for agency {agency_id}")
        return 0

    logger.info(f"Index build started for agency {agency_id}")

    # Always clear Drive cache before rebuilding so new/replaced files are picked up
    _drive.clear_cache()

    # Step 1: scan filenames (fast — no PDF reads)
    offers = await asyncio.to_thread(_drive.scan_sales_offers, svc, root_id)
    logger.info(f"Index: found {len(offers)} offer files for agency {agency_id}")

    # Save basic index immediately (unit keys + filenames, no prices yet)
    # This allows searches to work during the slow enrichment phase
    if offers:
        _save_index(agency_id, offers)
        logger.info(f"Index: basic snapshot saved ({len(offers)} units) — enriching prices...")

    # Step 2: extract price/size/view from each PDF (slow — reads bytes)
    units: dict = {}
    for unit_key, offer_data in offers.items():
        try:
            enriched = await asyncio.to_thread(_drive.enrich_offer_from_pdf, svc, offer_data)
            units[unit_key] = enriched
        except Exception:
            units[unit_key] = offer_data

    logger.info(f"Index build done: {len(units)} units for agency {agency_id}")
    _save_index(agency_id, units)
    return len(units)


# ─── Search ───────────────────────────────────────────────────────────────────

def _normalize_type(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"(\d)\s*br(s)?(\b|$)", r"\1b", s)
    s = re.sub(r"(\d)\s*bed(room)?s?(\b|$)", r"\1b", s)
    return s


def search_units(
    agency_id: int,
    query: str = "",
    floor: Optional[int] = None,
    floor_min: Optional[int] = None,
    floor_max: Optional[int] = None,
    unit_type: str = "",
    building: str = "",
    payment_plan: str = "",
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    view: str = "",
    sort_by: str = "",
) -> list:
    """Filter index. Returns list of (unit_key, unit_data, project_name).
    sort_by: 'cheapest' | 'most_expensive' | 'highest_floor' | 'lowest_floor'
    Sorting happens here so callers always get correctly ordered results.
    """
    units = load_index(agency_id)
    results = []
    for unit_key, data in units.items():
        proj_name = data.get("project_name", "Unknown")

        if query:
            q = query.upper()
            if q not in unit_key.upper() and q not in proj_name.upper():
                continue

        f = data.get("floor")
        if floor is not None and f != floor:
            continue
        if floor_min is not None and (f is None or f < floor_min):
            continue
        if floor_max is not None and (f is None or f > floor_max):
            continue

        if unit_type:
            ut = _normalize_type(unit_type)
            dt = _normalize_type(
                data.get("unit_type", "") + " " + data.get("unit_type_code", "")
            )
            if ut not in dt:
                continue

        if building and building.upper() not in data.get("building", "").upper():
            continue

        if payment_plan:
            pp = payment_plan.replace("/", "").replace(".", "").replace(" ", "")
            dp = data.get("payment_plan", "").replace("/", "").replace(".", "").replace(" ", "")
            if pp not in dp:
                continue

        pr = data.get("price_raw")
        if price_min is not None and (pr is None or pr < price_min):
            continue
        if price_max is not None and (pr is None or pr > price_max):
            continue

        if view and view.lower() not in data.get("view", "").lower():
            continue

        results.append((unit_key, data, proj_name))

    # Sort before returning — units without price_raw go to the end for price sorts
    if sort_by in ("cheapest", "lowest_price", "cheap"):
        priced = [r for r in results if r[1].get("price_raw")]
        unpriced = [r for r in results if not r[1].get("price_raw")]
        priced.sort(key=lambda x: x[1]["price_raw"])
        results = priced + unpriced
    elif sort_by in ("most_expensive", "expensive", "highest_price"):
        priced = [r for r in results if r[1].get("price_raw")]
        unpriced = [r for r in results if not r[1].get("price_raw")]
        priced.sort(key=lambda x: x[1]["price_raw"], reverse=True)
        results = priced + unpriced
    elif sort_by == "highest_floor":
        results.sort(key=lambda x: x[1].get("floor") or 0, reverse=True)
    elif sort_by == "lowest_floor":
        results.sort(key=lambda x: x[1].get("floor") or 9999)

    return results


def as_unit_list(agency_id: int, project_name: str = "") -> list:
    """Return all indexed units as (unit_key, data, proj_name) list, optionally filtered by project."""
    units = load_index(agency_id)
    result = []
    for unit_key, data in units.items():
        proj = data.get("project_name", "Unknown")
        if project_name and project_name.lower() not in proj.lower():
            continue
        result.append((unit_key, data, proj))
    return result
