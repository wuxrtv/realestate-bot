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


def _find_subfolder(svc, parent_id: str, name: str) -> Optional[str]:
    items = _list_folder(svc, parent_id)
    name_lower = name.lower()
    for item in items:
        if (item["mimeType"] == "application/vnd.google-apps.folder"
                and item["name"].lower() == name_lower):
            return item["id"]
    return None


def _find_project_folder(svc, project_name: str) -> Optional[str]:
    """Fuzzy-match project folder name in root."""
    items = _list_folder(svc, _root_id())
    proj_clean = project_name.lower().replace(" ", "").replace("_", "").replace("-", "")
    for item in items:
        if item["mimeType"] != "application/vnd.google-apps.folder":
            continue
        item_clean = item["name"].lower().replace(" ", "").replace("_", "").replace("-", "")
        if proj_clean in item_clean or item_clean in proj_clean:
            return item["id"]
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


def find_unit_file(svc, project_name: str, unit_number: str) -> Optional[tuple]:
    """Find any file for a unit in sales_offers/. Returns (file_id, name) or None."""
    try:
        proj_id = _find_project_folder(svc, project_name)
        if not proj_id:
            return None
        offers_id = _find_subfolder(svc, proj_id, "sales_offers")
        if not offers_id:
            return None
        unit_lower = unit_number.lower()
        for f in _list_folder(svc, offers_id):
            if unit_lower in f["name"].lower():
                return f["id"], f["name"]
    except Exception:
        logger.exception(f"Drive: find_unit_file failed {unit_number}")
    return None


def find_brochure(svc, project_name: str) -> Optional[tuple]:
    """Find a brochure file for a project. Returns (file_id, name) or None."""
    try:
        proj_id = _find_project_folder(svc, project_name)
        if not proj_id:
            return None
        brochures_id = _find_subfolder(svc, proj_id, "brochures")
        if not brochures_id:
            return None
        files = [
            f for f in _list_folder(svc, brochures_id)
            if not f["mimeType"].startswith("application/vnd.google-apps")
        ]
        return (files[0]["id"], files[0]["name"]) if files else None
    except Exception:
        logger.exception(f"Drive: find_brochure failed {project_name}")
    return None


def list_project_names(svc) -> list:
    """List all project folder names in root."""
    try:
        items = _list_folder(svc, _root_id())
        return [i["name"] for i in items if i["mimeType"] == "application/vnd.google-apps.folder"]
    except Exception:
        return []
