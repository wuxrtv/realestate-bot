"""Google Drive integration — reads files on-demand when agents request them."""

import io
import json
import logging
import os
import re
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Sales Offer File Name Parser ─────────────────────────────────────────────
# Format: SH_A311_40.60_1B.pdf
# Floor calculation: last 2 digits of unit number = unit within floor, rest = floor number
# 311 → floor 3, unit 11 | 2701 → floor 27, unit 01 | 1311 → floor 13, unit 11

PROJECT_CODES: dict[str, str] = {
    "SH": "SAAS Hills",
}

UNIT_TYPE_CODES: dict[str, str] = {
    "ST": "Studio",
    "1B": "1 Bedroom",
    "2B": "2 Bedroom",
    "3B": "3 Bedroom",
    "4B": "4 Bedroom",
}

_OFFER_PATTERN = re.compile(
    r"^([A-Z]{1,5})_([A-Z]+)(\d{2,5})_([\d]+\.[\d]+)_(\w+)\.pdf$",
    re.IGNORECASE,
)

_INVENTORY_KEYWORDS = frozenset({
    "inventory", "availability", "available", "инвентарь", "доступно",
    "units_list", "price_list", "прайс", "all_units",
})


def get_floor_from_unit(unit_num: str) -> Optional[int]:
    """Extract floor number. Last 2 digits = unit within floor, rest = floor."""
    if not unit_num or len(unit_num) < 3:
        return None
    try:
        return int(unit_num[:-2])
    except ValueError:
        return None


def is_inventory_filename(name: str) -> bool:
    """Return True if the filename looks like an inventory/availability file."""
    name_l = name.lower()
    return any(kw in name_l for kw in _INVENTORY_KEYWORDS)


def parse_offer_filename(filename: str) -> Optional[dict]:
    """Parse SH_A311_40.60_1B.pdf → structured unit data dict.
    Returns None if filename doesn't match the offer naming format.
    """
    m = _OFFER_PATTERN.match(filename)
    if not m:
        return None
    code, building, unit_digits, payment_raw, type_code = m.groups()
    code = code.upper()
    building = building.upper()
    type_code = type_code.upper()
    floor = get_floor_from_unit(unit_digits)
    payment_plan = payment_raw.replace(".", "/")
    return {
        "project_code": code,
        "project_name": PROJECT_CODES.get(code, code),
        "building": building,
        "unit_number": unit_digits,
        "floor": floor,
        "payment_plan": payment_plan,
        "unit_type": UNIT_TYPE_CODES.get(type_code, type_code),
        "unit_type_code": type_code,
        "_sheet": "Sales Offers",
    }

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

_svc = None

# ─── Cache ────────────────────────────────────────────────────────────────────
_CACHE_TTL = 1800  # 30 minutes

_folder_cache: dict[str, tuple[list, float]] = {}         # folder_id → (items, ts)
_project_cache: dict[str, tuple[Optional[str], float]] = {}  # "root|name" → (folder_id, ts)
_inventory_cache: dict[str, tuple[dict, float]] = {}         # "root|project" → (unit_index, ts)
_offers_cache: dict[str, tuple[dict, float]] = {}            # "root" → (offers_index, ts)
_pdf_data_cache: dict[str, tuple[dict, float]] = {}          # file_id → (extracted_data, ts)


def clear_cache():
    """Call after uploading new files to Drive so next search is fresh."""
    _folder_cache.clear()
    _project_cache.clear()
    _inventory_cache.clear()
    _offers_cache.clear()
    _pdf_data_cache.clear()
    logger.info("Drive: cache cleared")


def get_service():
    global _svc
    if _svc:
        return _svc
    creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not creds_json:
        logger.error("Drive: GOOGLE_SERVICE_ACCOUNT_JSON env var is NOT SET — Drive disabled")
        return None
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        info = json.loads(creds_json)
        sa_email = info.get("client_email", "unknown")
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        _svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        logger.info(f"Drive: service initialized OK | sa={sa_email}")
        logger.info(f"Drive: IMPORTANT — share root folder with: {sa_email}")
        return _svc
    except json.JSONDecodeError as e:
        logger.error(f"Drive: GOOGLE_SERVICE_ACCOUNT_JSON is invalid JSON: {e}")
        return None
    except Exception as e:
        logger.error(f"Drive: failed to init — {e}")
        return None


def _root_id(agency_root_id: str = "") -> str:
    """Return the effective root folder ID: agency-specific or global env var."""
    return agency_root_id or os.getenv("GOOGLE_DRIVE_ROOT_ID", "")


def _list_folder(svc, folder_id: str) -> list:
    now = time.time()
    if folder_id in _folder_cache:
        items, ts = _folder_cache[folder_id]
        if now - ts < _CACHE_TTL:
            return items
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        items = []
        page_token = None
        while True:
            kwargs = {"q": q, "fields": "nextPageToken,files(id,name,mimeType)", "pageSize": 200}
            if page_token:
                kwargs["pageToken"] = page_token
            r = svc.files().list(**kwargs).execute()
            items.extend(r.get("files", []))
            page_token = r.get("nextPageToken")
            if not page_token:
                break
        _folder_cache[folder_id] = (items, now)
        return items
    except Exception:
        logger.exception("Drive: list_folder failed")
        return []


def _name_score(folder_name: str, query: str) -> int:
    """Return match score between folder name and query (higher = better)."""
    fn = folder_name.lower().replace(" ", "").replace("_", "").replace("-", "")
    q = query.lower().replace(" ", "").replace("_", "").replace("-", "")
    if fn == q:
        return 3
    if fn.startswith(q) or q.startswith(fn):
        return 2
    if q in fn or fn in q:
        return 1
    return 0


def _find_project_folder(svc, project_name: str, agency_root_id: str = "") -> Optional[str]:
    """
    Search for a project folder by name.
    Looks in ROOT directly, then one level deeper (client folders inside ROOT).
    Results cached for 30 minutes.
    """
    cache_key = f"{agency_root_id}|{project_name.lower()}"
    now = time.time()
    if cache_key in _project_cache:
        folder_id, ts = _project_cache[cache_key]
        if now - ts < _CACHE_TTL:
            logger.info(f"Drive: cache hit for '{project_name}'")
            return folder_id

    effective_root = _root_id(agency_root_id)
    root_items = _list_folder(svc, effective_root)
    folders = [i for i in root_items if i["mimeType"] == "application/vnd.google-apps.folder"]
    if not folders:
        logger.error(
            f"Drive: ROOT folder ({effective_root}) returned 0 folders! "
            "Most likely the folder is NOT shared with the service account."
        )
    else:
        logger.info(f"Drive ROOT folders ({len(folders)}): {[f['name'] for f in folders]}")

    best, best_score = None, 0
    for item in folders:
        score = _name_score(item["name"], project_name)
        if score > best_score:
            best, best_score = item["id"], score

    if best_score >= 1:
        logger.info(f"Drive: found '{project_name}' directly in root → score={best_score}")
        _project_cache[cache_key] = (best, now)
        return best

    for client_folder in folders:
        sub_items = _list_folder(svc, client_folder["id"])
        sub_folders = [i for i in sub_items if i["mimeType"] == "application/vnd.google-apps.folder"]
        for item in sub_folders:
            score = _name_score(item["name"], project_name)
            if score > best_score:
                best, best_score = item["id"], score

    result = best if best_score >= 1 else None
    if result:
        logger.info(f"Drive: found '{project_name}' in subfolder → score={best_score}")
    else:
        logger.warning(f"Drive: '{project_name}' NOT FOUND. Available: {[f['name'] for f in folders]}")

    _project_cache[cache_key] = (result, now)
    return result


# File type categories
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".gif"}
_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
_EXCEL_EXTS = {".xlsx", ".xls", ".csv"}
_PDF_EXT = ".pdf"
_BROCHURE_KEYWORDS = {"brochure", "брошюр", "presentation", "презентац", "catalog", "каталог", "флайер", "flyer"}

# Subfolder name hints for the new Drive structure:
#   ProjectName/media/        ← photos, videos, brochures
#   ProjectName/sales office/ ← unit PDFs, floor plans
_MEDIA_FOLDER_NAMES = frozenset({
    "media", "медиа", "photos", "photo", "фото", "фотографии",
    "materials", "marketing", "материалы", "renders", "рендеры",
})
_OFFICE_FOLDER_NAMES = frozenset({
    "salesoffice", "sales", "office", "офис",
    "units", "юниты", "inventory", "инвентарь", "прайс",
})


def _ext(name: str) -> str:
    return ("." + name.rsplit(".", 1)[-1]).lower() if "." in name else ""


def _is_brochure(name: str) -> bool:
    name_l = name.lower()
    return _ext(name) == _PDF_EXT or any(kw in name_l for kw in _BROCHURE_KEYWORDS)


def _is_photo(name: str) -> bool:
    return _ext(name) in _IMAGE_EXTS


def _is_video(name: str) -> bool:
    return _ext(name) in _VIDEO_EXTS


_EXPORTABLE = {
    "application/vnd.google-apps.presentation": ("application/pdf", ".pdf"),
    "application/vnd.google-apps.document": ("application/pdf", ".pdf"),
}


def _collect_files(svc, folder_id: str) -> list:
    """Recursively list all non-folder files in a folder. Google Slides/Docs marked as exportable."""
    items = _list_folder(svc, folder_id)
    files = []
    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            files.extend(_collect_files(svc, item["id"]))
        elif item["mimeType"] in _EXPORTABLE:
            # Google Slides / Docs — exportable as PDF
            _, ext = _EXPORTABLE[item["mimeType"]]
            item = dict(item)
            item["_export_mime"] = _EXPORTABLE[item["mimeType"]][0]
            if not item["name"].endswith(ext):
                item["name"] = item["name"] + ext
            files.append(item)
        elif not item["mimeType"].startswith("application/vnd.google-apps"):
            files.append(item)
    return files


def _find_named_subfolder(svc, proj_id: str, name_hints: frozenset) -> Optional[str]:
    """Return the first direct subfolder of proj_id whose name matches any hint."""
    items = _list_folder(svc, proj_id)
    for item in items:
        if item["mimeType"] != "application/vnd.google-apps.folder":
            continue
        name_l = item["name"].lower().replace(" ", "").replace("_", "").replace("-", "")
        for hint in name_hints:
            h = hint.lower().replace(" ", "").replace("_", "").replace("-", "")
            if h == name_l or h in name_l or name_l in h:
                logger.info(f"Drive: subfolder '{item['name']}' matched hint '{hint}'")
                return item["id"]
    return None


_BROCHURE_SORT_KW = frozenset({"brochure", "брошюр", "presentation", "презентац", "catalog", "каталог"})
_PAYMENT_SORT_KW  = frozenset({"payment", "plan", "план", "оплат", "рассрочк", "installment"})


def _media_sort_key(file_name: str) -> int:
    """Send order: brochure PDF=0, payment plan PDF=1, other PDF=2, photo=3, video=4, other=5."""
    name_l = file_name.lower()
    ext = _ext(file_name)
    if ext == _PDF_EXT:
        if any(kw in name_l for kw in _BROCHURE_SORT_KW):
            return 0
        if any(kw in name_l for kw in _PAYMENT_SORT_KW):
            return 1
        return 2
    if ext in _IMAGE_EXTS:
        return 3
    if ext in _VIDEO_EXTS:
        return 4
    return 5


def find_all_media(svc, project_name: str, limit: int = 15, agency_root_id: str = "") -> list:
    """Return ALL media files from the project's 'media' subfolder, sorted by send order.
    Order: brochure PDF → payment plan PDF → other PDFs → photos → videos.
    Falls back to the whole project folder if no 'media' subfolder is found.
    Returns list of (file_id, name, export_mime).
    """
    try:
        proj_id = _find_project_folder(svc, project_name, agency_root_id)
        if not proj_id:
            return []
        media_id = _find_named_subfolder(svc, proj_id, _MEDIA_FOLDER_NAMES)
        search_id = media_id or proj_id
        files = _collect_files(svc, search_id)
        result = [
            (f["id"], f["name"], f.get("_export_mime", ""))
            for f in files
            if _is_photo(f["name"]) or _is_video(f["name"]) or _is_brochure(f["name"])
        ]
        result.sort(key=lambda f: _media_sort_key(f[1]))
        logger.info(
            f"Drive: find_all_media '{project_name}' → {len(result)} files "
            f"({'media subfolder' if media_id else 'project root fallback'})"
        )
        return result[:limit]
    except Exception:
        logger.exception(f"Drive: find_all_media failed {project_name}")
    return []



_location_text_cache: dict[str, tuple[str, float]] = {}  # project_name → (text, timestamp)

_GOOGLE_DOC_MIME  = "application/vnd.google-apps.document"
_GOOGLE_SLIDE_MIME = "application/vnd.google-apps.presentation"


def _is_location_file(name: str, mime: str) -> bool:
    """Match any file that looks like a location/info text document."""
    nl = name.lower()
    return (
        "location" in nl
        or "локац" in nl
        or "text_location" in nl
        or nl == "location.txt"
    ) and (
        mime in (_GOOGLE_DOC_MIME, _GOOGLE_SLIDE_MIME)
        or nl.endswith(".txt")
        or nl.endswith(".doc")
        or nl.endswith(".docx")
    )


def _collect_all_files_flat(svc, folder_id: str, depth: int = 3) -> list:
    """Recursively list ALL files up to `depth` levels deep (no filtering)."""
    if depth <= 0:
        return []
    items = _list_folder(svc, folder_id)
    result = []
    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            result.extend(_collect_all_files_flat(svc, item["id"], depth - 1))
        else:
            result.append(item)
    return result


def _download_as_text(svc, file_id: str, mime: str) -> str:
    """Download a Drive file as plain text. Exports Google Docs as text/plain."""
    try:
        from googleapiclient.http import MediaIoBaseDownload
        buf = io.BytesIO()
        if mime in (_GOOGLE_DOC_MIME, _GOOGLE_SLIDE_MIME):
            req = svc.files().export_media(fileId=file_id, mimeType="text/plain")
        else:
            req = svc.files().get_media(fileId=file_id)
        dl = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        return buf.getvalue().decode("utf-8", errors="replace").strip()
    except Exception:
        logger.exception(f"Drive: _download_as_text failed for {file_id}")
        return ""


def get_location_text(svc, project_name: str, root_id: str = "") -> str:
    """Find and read location/info text from Drive project folder.

    Searches recursively — matches any file with 'location' or 'локац' in name.
    Accepts .txt files and Google Docs (exported as plain text).
    Cache: 24 hours.
    """
    now = time.time()
    cached = _location_text_cache.get(project_name)
    if cached and now - cached[1] < 86400:
        return cached[0]

    try:
        proj_id = _find_project_folder(svc, project_name, root_id)
        if not proj_id:
            _location_text_cache[project_name] = ("", now)
            return ""

        all_files = _collect_all_files_flat(svc, proj_id, depth=3)
        for item in all_files:
            if _is_location_file(item["name"], item["mimeType"]):
                text = _download_as_text(svc, item["id"], item["mimeType"])
                _location_text_cache[project_name] = (text, now)
                logger.info(
                    f"Drive: location text loaded — '{item['name']}' "
                    f"for '{project_name}' ({len(text)} chars)"
                )
                return text
    except Exception:
        logger.exception(f"Drive: get_location_text failed for '{project_name}'")

    _location_text_cache[project_name] = ("", now)
    logger.info(f"Drive: no location file found for '{project_name}'")
    return ""


def find_unit_file(svc, project_name: str, unit_number: str, agency_root_id: str = "") -> Optional[tuple]:
    """Find any file for a unit (e.g. '1507.pdf').
    Looks in 'sales office' subfolder first, then falls back to whole project.
    Returns (file_id, name) or None.
    """
    try:
        proj_id = _find_project_folder(svc, project_name, agency_root_id)
        if not proj_id:
            return None
        office_id = _find_named_subfolder(svc, proj_id, _OFFICE_FOLDER_NAMES)
        search_id = office_id or proj_id
        files = _collect_files(svc, search_id)
        unit_lower = unit_number.lower()
        for f in files:
            if unit_lower in f["name"].lower():
                return f["id"], f["name"]
    except Exception:
        logger.exception(f"Drive: find_unit_file failed {unit_number}")
    return None


def download_file(svc, file_id: str, export_mime: str = "") -> Optional[bytes]:
    try:
        from googleapiclient.http import MediaIoBaseDownload
        buf = io.BytesIO()
        if export_mime:
            # Google Slides/Docs — export as PDF
            req = svc.files().export_media(fileId=file_id, mimeType=export_mime)
            logger.info(f"Drive: exporting {file_id} as {export_mime}")
        else:
            req = svc.files().get_media(fileId=file_id)
        dl = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        data = buf.getvalue()
        logger.info(f"Drive: downloaded {file_id} — {len(data)} bytes")
        return data
    except Exception:
        logger.exception(f"Drive: download failed {file_id}")
        return None


def get_project_inventory(svc, project_name: str, agency_root_id: str = "") -> dict:
    """Download + parse inventory Excel/CSV from Drive, return unit_index dict (cached 30 min).
    Looks in 'sales office' subfolder first, then project root.
    Returns {} if no inventory file found.
    """
    cache_key = f"{agency_root_id}|inv|{project_name.lower()}"
    now = time.time()
    if cache_key in _inventory_cache:
        idx, ts = _inventory_cache[cache_key]
        if now - ts < _CACHE_TTL:
            logger.info(f"Drive: inventory cache hit for '{project_name}' ({len(idx)} units)")
            return idx

    try:
        proj_id = _find_project_folder(svc, project_name, agency_root_id)
        if not proj_id:
            _inventory_cache[cache_key] = ({}, now)
            return {}
        office_id = _find_named_subfolder(svc, proj_id, _OFFICE_FOLDER_NAMES)
        search_id = office_id or proj_id
        files = _collect_files(svc, search_id)
        for f in files:
            if _ext(f["name"]) in _EXCEL_EXTS:
                data = download_file(svc, f["id"])
                if data:
                    from excel_parser import build_unit_index, parse_csv, parse_excel
                    if f["name"].lower().endswith(".csv"):
                        sheets = parse_csv(data)
                    else:
                        sheets = parse_excel(data)
                    idx = build_unit_index(sheets)
                    _inventory_cache[cache_key] = (idx, now)
                    logger.info(f"Drive: loaded inventory '{f['name']}' for '{project_name}' — {len(idx)} units")
                    return idx
    except Exception:
        logger.exception(f"Drive: get_project_inventory failed {project_name}")

    _inventory_cache[cache_key] = ({}, now)
    return {}


_PDF_PRICE_RE = re.compile(
    # kept for _PDF_SIZE_RE / _PDF_VIEW_RE neighbours — price logic below no longer uses this
    r"(?:final\s+price|full\s+price|total\s+price|selling\s+price|unit\s+price|"
    r"price|total|amount|cost|value|стоимость|цена)"
    r"[^\d]{0,25}([\d][\d,\s\.]{4,14})"
    r"|"
    # Number before AED: "1,234,567 AED"
    r"([\d][\d,\s\.]{4,14})\s*(?:aed|AED|درهم)",
    re.IGNORECASE,
)
_PDF_SIZE_RE = re.compile(
    r"([\d,\.]+)\s*(?:sq\.?\s*ft|sqft|sq\.?\s*m|sqm|м²|кв\.?\s*м)",
    re.IGNORECASE,
)
_PDF_VIEW_RE = re.compile(
    r"(?:view|вид)[^\w]{0,10}([\w\s]+?)(?:\n|,|\.|$)",
    re.IGNORECASE,
)


def extract_offer_data_from_pdf(pdf_bytes: bytes) -> dict:
    """Extract price, size, and view from a sales offer PDF using pdfplumber text extraction.
    Returns dict with keys: price (formatted string), price_raw (int), size, view.
    """
    result: dict = {"price": "", "price_raw": None, "size": "", "view": ""}
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages[:3])

        tl = text.lower()
        logger.info(
            f"extract_offer_data_from_pdf: text_len={len(text)} "
            f"has_AED={'aed' in tl} "
            f"has_price_after_discount={'price after discount' in tl} "
            f"first500={text[:500]!r}"
        )

        def _to_int(s: str) -> int | None:
            try:
                n = int(float(s.replace(",", "").replace(" ", "")))
                return n if n >= 100_000 else None
            except (ValueError, OverflowError):
                return None

        price_num: int | None = None

        # Step 1 — SAAS Hills / "Price After Discount AED 1,234,567"
        m1 = re.search(r"Price\s+After\s+Discount\s+AED\s*([\d,\.]+)", text, re.IGNORECASE)
        if m1:
            price_num = _to_int(m1.group(1))
            if price_num:
                logger.info(f"extract_offer_data_from_pdf: step1 hit → {price_num}")

        # Step 2 — "AED 1,234,567" anywhere; take the LARGEST (full price, not installment)
        if not price_num:
            aed_matches = re.findall(r"AED\s*([\d,\.]+)", text, re.IGNORECASE)
            candidates = [n for raw in aed_matches for n in [_to_int(raw)] if n]
            if candidates:
                price_num = max(candidates)
                logger.info(f"extract_offer_data_from_pdf: step2 hit → {price_num} (from {candidates})")

        # Step 3 — bare numbers >= 100,000; take the largest
        if not price_num:
            bare = re.findall(r"\b(\d[\d,]{4,})\b", text)
            candidates = [n for raw in bare for n in [_to_int(raw)] if n]
            if candidates:
                price_num = max(candidates)
                logger.info(f"extract_offer_data_from_pdf: step3 hit → {price_num}")

        if price_num:
            result["price_raw"] = price_num
            result["price"] = f"AED {price_num:,}"

        # Step 1: "Total Area: X sqft" label on same line (generic format)
        m_total = re.search(
            r"Total\s+Area\s*[\(\[]?SQ\.?\s*FT[\)\]]?\s*[\|:]?\s*([\d,\.]+)",
            text, re.IGNORECASE,
        )
        if m_total:
            result["size"] = m_total.group(1).strip() + " sq.ft"
        else:
            # Step 2: SAAS Hills table format: "808.48 Sqft  262.10 Sqft  1,070.58 Sqft"
            # Suite Area, Balcony Area, Total Area — take the LAST (3rd) value
            m_three = re.search(
                r"([\d,\.]+)\s*Sqft\s+([\d,\.]+)\s*Sqft\s+([\d,\.]+)\s*Sqft",
                text, re.IGNORECASE,
            )
            if m_three:
                result["size"] = m_three.group(3).strip() + " sq.ft"
            else:
                # Step 3: fallback — first sqft mention
                m = _PDF_SIZE_RE.search(text)
                if m:
                    result["size"] = m.group(0).strip()

        m = _PDF_VIEW_RE.search(text)
        if m:
            result["view"] = m.group(1).strip().title()

        # Extract unit metadata from PDF content (for non-standard filenames)
        # Pattern: "Project Name  Unit Number  ...\nSAAS HILLS  A-411  ..."
        m_row = re.search(
            r"Project\s+Name\s+Unit\s+Number[^\n]*\n([A-Z][A-Z\s]+?)\s{2,}([A-Z]-?\d+)",
            text, re.IGNORECASE,
        )
        if m_row:
            result["_project_from_pdf"] = m_row.group(1).strip().title()
            result["_unit_id_from_pdf"] = m_row.group(2).strip()  # e.g. "A-411"
        else:
            # Fallback: "Reference No: A-411"
            m_ref = re.search(r"Reference\s+No[:\s]+([A-Z]-?\d+)", text, re.IGNORECASE)
            if m_ref:
                result["_unit_id_from_pdf"] = m_ref.group(1).strip()

        # Unit type: "Apartment Studio" / "Apartment 1" / "Townhouse 4"
        m_type = re.search(
            r"(?:Apartment|Townhouse|Villa|Penthouse)\s+(Studio|\d+)",
            text, re.IGNORECASE,
        )
        if m_type:
            t = m_type.group(1)
            if t.lower() == "studio":
                result["_unit_type_from_pdf"] = "Studio"
                result["_unit_type_code_from_pdf"] = "ST"
            else:
                n = int(t)
                _names = {1: "1 Bedroom", 2: "2 Bedroom", 3: "3 Bedroom", 4: "4 Bedroom", 5: "5 Bedroom"}
                _codes = {1: "1B", 2: "2B", 3: "3B", 4: "4B", 5: "5B"}
                result["_unit_type_from_pdf"] = _names.get(n, f"{n} Bedroom")
                result["_unit_type_code_from_pdf"] = _codes.get(n, f"{n}B")

    except Exception:
        logger.debug("extract_offer_data_from_pdf: pdfplumber not available or parse error")

    return result


def enrich_offer_from_pdf(svc, offer_data: dict) -> dict:
    """Download offer PDF and extract price/size/view. Returns enriched copy (cached per file_id)."""
    file_id = offer_data.get("file_id", "")
    if not file_id:
        return offer_data
    now = time.time()
    if file_id in _pdf_data_cache:
        extra, ts = _pdf_data_cache[file_id]
        if now - ts < _CACHE_TTL:
            return {**offer_data, **extra}
    extra: dict = {}
    try:
        pdf_bytes = download_file(svc, file_id)
        if pdf_bytes:
            inner = extract_offer_data_from_pdf(pdf_bytes)
            if inner.get("price"):
                extra["Price"] = inner["price"]
            if inner.get("price_raw") is not None:
                extra["price_raw"] = inner["price_raw"]   # exact integer for sorting
            if inner.get("size"):
                extra["Size"] = inner["size"]
            if inner.get("view"):
                extra["View"] = inner["view"]
    except Exception:
        pass
    _pdf_data_cache[file_id] = (extra, now)
    return {**offer_data, **extra}


def scan_sales_offers(svc, agency_root_id: str = "") -> dict:
    """Scan all project folders for PDFs matching the offer naming pattern.

    Returns {unit_key: offer_dict} where unit_key = "BUILDING+UNIT" (e.g. "A311").
    Each offer_dict has: project_name, building, unit_number, floor, payment_plan,
    unit_type, unit_type_code, file_id, filename, _sheet.
    Results cached for 30 minutes.
    """
    cache_key = f"{agency_root_id}|offers"
    now = time.time()
    if cache_key in _offers_cache:
        idx, ts = _offers_cache[cache_key]
        if now - ts < _CACHE_TTL:
            return idx

    idx: dict[str, dict] = {}
    try:
        effective_root = _root_id(agency_root_id)
        root_items = _list_folder(svc, effective_root)
        folders = [i for i in root_items if i["mimeType"] == "application/vnd.google-apps.folder"]

        # Search at root level and one level deep (client folders)
        project_folders: list[dict] = []
        for folder in folders:
            sub = _list_folder(svc, folder["id"])
            sub_folders = [s for s in sub if s["mimeType"] == "application/vnd.google-apps.folder"]
            if sub_folders:
                project_folders.extend(sub_folders)
            else:
                project_folders.append(folder)

        seen_sid: set[str] = set()
        for proj_folder in project_folders:
            office_id = _find_named_subfolder(svc, proj_folder["id"], _OFFICE_FOLDER_NAMES)
            search_ids = [office_id, proj_folder["id"]] if office_id else [proj_folder["id"]]
            for sid in search_ids:
                if sid in seen_sid:
                    continue
                seen_sid.add(sid)
                files = _list_folder(svc, sid)
                for f in files:
                    if not f["name"].lower().endswith(".pdf"):
                        continue
                    if is_inventory_filename(f["name"]):
                        continue
                    parsed = parse_offer_filename(f["name"])
                    if parsed:
                        unit_key = f"{parsed['building']}{parsed['unit_number']}"
                        idx[unit_key] = {**parsed, "file_id": f["id"], "filename": f["name"]}
                        logger.info(f"Drive: scan_sales_offers — indexed: {f['name']!r} → {unit_key}")
                    else:
                        # Non-standard filename — include anyway; metadata extracted from PDF during enrichment
                        unit_key = f"_raw_{f['id'][:12]}"
                        idx[unit_key] = {
                            "project_name": "",
                            "building": "",
                            "unit_number": f["name"][:-4],
                            "floor": None,
                            "payment_plan": "",
                            "unit_type": "",
                            "unit_type_code": "",
                            "_sheet": "Sales Offers",
                            "file_id": f["id"],
                            "filename": f["name"],
                        }
                        logger.info(f"Drive: scan_sales_offers — non-standard PDF included: {f['name']!r}")

        logger.info(f"Drive: scan_sales_offers → {len(idx)} offers found")
    except Exception:
        logger.exception("Drive: scan_sales_offers failed")

    _offers_cache[cache_key] = (idx, now)
    return idx


def list_project_names(svc, agency_root_id: str = "") -> list:
    """List all project folder names (searches root and one level deep)."""
    try:
        root_items = _list_folder(svc, _root_id(agency_root_id))
        names = []
        for item in root_items:
            if item["mimeType"] == "application/vnd.google-apps.folder":
                # Could be a client folder — list its subfolders too
                sub = _list_folder(svc, item["id"])
                sub_folders = [s["name"] for s in sub if s["mimeType"] == "application/vnd.google-apps.folder"]
                if sub_folders:
                    names.extend(sub_folders)  # project folders inside client folder
                else:
                    names.append(item["name"])  # treat as project folder itself
        return names
    except Exception:
        return []
