"""Google Drive integration — reads files on-demand when agents request them."""

import io
import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

_svc = None


def get_service():
    global _svc
    if _svc:
        return _svc
    creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    root_id = os.getenv("GOOGLE_DRIVE_ROOT_ID", "")
    if not creds_json or not root_id:
        return None
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        _svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        logger.info("Google Drive service initialized")
        return _svc
    except Exception:
        logger.exception("Drive: failed to init")
        return None


def _root_id() -> str:
    return os.getenv("GOOGLE_DRIVE_ROOT_ID", "")


def _list_folder(svc, folder_id: str) -> list:
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        r = svc.files().list(q=q, fields="files(id,name,mimeType)", pageSize=200).execute()
        return r.get("files", [])
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


def _find_project_folder(svc, project_name: str) -> Optional[str]:
    """
    Search for a project folder by name.
    Looks in ROOT directly, then one level deeper (client folders inside ROOT).
    """
    root_items = _list_folder(svc, _root_id())
    folders = [i for i in root_items if i["mimeType"] == "application/vnd.google-apps.folder"]

    # Try direct match in root
    best, best_score = None, 0
    for item in folders:
        score = _name_score(item["name"], project_name)
        if score > best_score:
            best, best_score = item["id"], score

    if best_score >= 1:
        return best

    # Try one level deeper: inside each subfolder (client folders)
    for client_folder in folders:
        sub_items = _list_folder(svc, client_folder["id"])
        for item in sub_items:
            if item["mimeType"] != "application/vnd.google-apps.folder":
                continue
            score = _name_score(item["name"], project_name)
            if score > best_score:
                best, best_score = item["id"], score

    return best if best_score >= 1 else None


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


def _collect_files(svc, folder_id: str) -> list:
    """Recursively list all non-folder files in a folder."""
    items = _list_folder(svc, folder_id)
    files = []
    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            files.extend(_collect_files(svc, item["id"]))
        elif not item["mimeType"].startswith("application/vnd.google-apps"):
            files.append(item)
    return files


def find_brochure(svc, project_name: str) -> Optional[tuple]:
    """Find a brochure (PDF) for a project. Returns (file_id, name) or None."""
    try:
        proj_id = _find_project_folder(svc, project_name)
        if not proj_id:
            return None
        files = _collect_files(svc, proj_id)
        # Prefer files explicitly named as brochures, then any PDF
        for f in files:
            if _is_brochure(f["name"]) and any(kw in f["name"].lower() for kw in _BROCHURE_KEYWORDS):
                return f["id"], f["name"]
        for f in files:
            if _ext(f["name"]) == _PDF_EXT:
                return f["id"], f["name"]
    except Exception:
        logger.exception(f"Drive: find_brochure failed {project_name}")
    return None


def find_photos(svc, project_name: str, limit: int = 5) -> list:
    """Find photo files for a project. Returns list of (file_id, name)."""
    try:
        proj_id = _find_project_folder(svc, project_name)
        if not proj_id:
            return []
        files = _collect_files(svc, proj_id)
        photos = [(f["id"], f["name"]) for f in files if _is_photo(f["name"])]
        return photos[:limit]
    except Exception:
        logger.exception(f"Drive: find_photos failed {project_name}")
    return []


def find_video(svc, project_name: str) -> Optional[tuple]:
    """Find first video file for a project. Returns (file_id, name) or None."""
    try:
        proj_id = _find_project_folder(svc, project_name)
        if not proj_id:
            return None
        files = _collect_files(svc, proj_id)
        for f in files:
            if _is_video(f["name"]):
                return f["id"], f["name"]
    except Exception:
        logger.exception(f"Drive: find_video failed {project_name}")
    return None


def find_unit_file(svc, project_name: str, unit_number: str) -> Optional[tuple]:
    """Find any file for a unit (e.g. '1507.pdf'). Returns (file_id, name) or None."""
    try:
        proj_id = _find_project_folder(svc, project_name)
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


def download_file(svc, file_id: str) -> Optional[bytes]:
    try:
        from googleapiclient.http import MediaIoBaseDownload
        buf = io.BytesIO()
        req = svc.files().get_media(fileId=file_id)
        dl = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        return buf.getvalue()
    except Exception:
        logger.exception(f"Drive: download failed {file_id}")
        return None


def list_project_names(svc) -> list:
    """List all project folder names (searches root and one level deep)."""
    try:
        root_items = _list_folder(svc, _root_id())
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
