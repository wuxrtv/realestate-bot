"""Google Drive integration — reads files on-demand when agents request them."""

import io
import json
import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

_svc = None

# ─── Cache ────────────────────────────────────────────────────────────────────
_CACHE_TTL = 1800  # 30 minutes

_folder_cache: dict[str, tuple[list, float]] = {}         # folder_id → (items, ts)
_project_cache: dict[str, tuple[Optional[str], float]] = {}  # "root|name" → (folder_id, ts)


def clear_cache():
    """Call after uploading new files to Drive so next search is fresh."""
    _folder_cache.clear()
    _project_cache.clear()
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
        logger.info(f"Drive: service initialized OK | sa={sa_email} | root={root_id}")
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
        r = svc.files().list(q=q, fields="files(id,name,mimeType)", pageSize=200).execute()
        items = r.get("files", [])
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
_PDF_EXT = ".pdf"
_BROCHURE_KEYWORDS = {"brochure", "брошюр", "presentation", "презентац", "catalog", "каталог", "флайер", "flyer"}


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


def find_brochure(svc, project_name: str, agency_root_id: str = "") -> Optional[tuple]:
    """Find a brochure (PDF) for a project. Returns (file_id, name, export_mime) or None."""
    try:
        proj_id = _find_project_folder(svc, project_name, agency_root_id)
        if not proj_id:
            return None
        files = _collect_files(svc, proj_id)
        all_names = [f["name"] for f in files]
        logger.info(f"Drive: all files in '{project_name}': {all_names}")
        # Prefer files explicitly named as brochures, then any PDF, then Google Slides
        for f in files:
            if _is_brochure(f["name"]) and any(kw in f["name"].lower() for kw in _BROCHURE_KEYWORDS):
                logger.info(f"Drive: selected brochure '{f['name']}' (keyword match)")
                return f["id"], f["name"], f.get("_export_mime", "")
        for f in files:
            if _ext(f["name"]) == _PDF_EXT:
                logger.info(f"Drive: selected brochure '{f['name']}' (any PDF fallback)")
                return f["id"], f["name"], f.get("_export_mime", "")
        # Last resort: Google Slides presentation
        for f in files:
            if f.get("_export_mime"):
                logger.info(f"Drive: selected brochure '{f['name']}' (Google Slides export)")
                return f["id"], f["name"], f["_export_mime"]
        logger.warning(f"Drive: no PDF/Slides found in '{project_name}', files: {all_names}")
    except Exception:
        logger.exception(f"Drive: find_brochure failed {project_name}")
    return None


def find_photos(svc, project_name: str, limit: int = 5, agency_root_id: str = "") -> list:
    """Find photo files for a project. Returns list of (file_id, name)."""
    try:
        proj_id = _find_project_folder(svc, project_name, agency_root_id)
        if not proj_id:
            return []
        files = _collect_files(svc, proj_id)
        photos = [(f["id"], f["name"]) for f in files if _is_photo(f["name"])]
        return photos[:limit]
    except Exception:
        logger.exception(f"Drive: find_photos failed {project_name}")
    return []


def find_video(svc, project_name: str, agency_root_id: str = "") -> Optional[tuple]:
    """Find first video file for a project. Returns (file_id, name, export_mime) or None."""
    try:
        proj_id = _find_project_folder(svc, project_name, agency_root_id)
        if not proj_id:
            return None
        files = _collect_files(svc, proj_id)
        for f in files:
            if _is_video(f["name"]):
                return f["id"], f["name"], f.get("_export_mime", "")
    except Exception:
        logger.exception(f"Drive: find_video failed {project_name}")
    return None


def find_unit_file(svc, project_name: str, unit_number: str, agency_root_id: str = "") -> Optional[tuple]:
    """Find any file for a unit (e.g. '1507.pdf'). Returns (file_id, name) or None."""
    try:
        proj_id = _find_project_folder(svc, project_name, agency_root_id)
        if not proj_id:
            return None
        files = _collect_files(svc, proj_id)
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
