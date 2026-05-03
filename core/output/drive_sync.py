"""Google Drive file upload and folder maintenance for feed outputs."""
import logging
import re
import time
from pathlib import Path
from typing import List, Optional, Set

from core.config import get_config

logger = logging.getLogger(__name__)


def _import_google_drive():
    """Import Drive API modules; returns (build, MediaFileUpload, service_account) or three None values."""
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        from google.oauth2 import service_account
        return build, MediaFileUpload, service_account
    except ImportError as e:
        logger.debug("Google Drive client not installed (%s); Drive upload will be skipped.", e)
        return None, None, None


class SyncManager:
    """Uploads feed files to Google Drive with optional folder orphan handling."""

    def __init__(self):
        self.service = None
        self._MediaFileUpload = None
        self.supports_shared_drives = True
        cfg = get_config()
        if cfg.google_drive_folder_id and cfg.google_service_account_file:
            self._authenticate()

    def _authenticate(self):
        build_mod, MediaFileUpload_mod, service_account_mod = _import_google_drive()
        if build_mod is None:
            self.service = None
            return
        self._MediaFileUpload = MediaFileUpload_mod
        cfg = get_config()
        try:
            credentials = service_account_mod.Credentials.from_service_account_file(
                cfg.google_service_account_file,
                scopes=["https://www.googleapis.com/auth/drive"],
            )
            self.service = build_mod("drive", "v3", credentials=credentials)
            logger.debug("Authenticated with Google Drive")
            self._verify_folder_access()
        except Exception as e:
            logger.warning(f"Failed to authenticate with Google Drive: {e}")
            self.service = None

    def _verify_folder_access(self):
        if not self.service:
            return
        cfg = get_config()
        try:
            self.service.files().get(
                fileId=cfg.google_drive_folder_id,
                fields="id,name,mimeType",
                supportsAllDrives=True,
            ).execute()
            logger.debug(f"Verified access to folder {cfg.google_drive_folder_id}")
        except Exception as e:
            try:
                self.service.files().get(
                    fileId=cfg.google_drive_folder_id,
                    fields="id,name,mimeType",
                ).execute()
                self.supports_shared_drives = False
                logger.debug("Drive API: shared-drives flags disabled for this folder")
            except Exception as e2:
                logger.warning(f"Folder verification failed: {e2}")

    def upload_files_with_cleanup(
        self,
        file_paths: List[Path],
        current_countries: Set[str],
        sync_type: Optional[str] = None,
    ) -> int:
        if not self.service:
            logger.warning("Google Drive not configured - skipping upload")
            return 0

        logger.debug(f"Starting upload with cleanup for {len(file_paths)} files")
        if sync_type != "INCREMENTAL":
            self._cleanup_orphaned_drive_files(current_countries)

        successful_uploads = 0
        for file_path in file_paths:
            try:
                self._upload_file(file_path)
                successful_uploads += 1
            except Exception as e:
                logger.error(f"Failed to upload {file_path}: {e}")

        logger.info(f"Upload complete: {successful_uploads}/{len(file_paths)} files processed")
        return successful_uploads

    def _upload_file(self, file_path: Path) -> str:
        file_name = file_path.name
        existing_file_id = self._find_existing_file(file_name)
        cfg = get_config()

        for attempt in range(cfg.max_retries + 1):
            try:
                if existing_file_id:
                    return self._update_file(existing_file_id, file_path)
                return self._create_file(file_path)
            except Exception as e:
                if attempt == cfg.max_retries:
                    raise Exception(f"Upload failed after {cfg.max_retries + 1} attempts: {e}")
                delay = min(cfg.base_retry_delay * (2 ** attempt), cfg.max_retry_delay)
                logger.debug(f"Upload attempt {attempt + 1} failed: {e}, retrying in {delay:.1f}s")
                time.sleep(delay)

    def _mime_type(self, file_path: Path) -> str:
        suf = file_path.suffix.lower()
        if suf == ".tsv":
            return "text/tab-separated-values"
        if suf == ".csv":
            return "text/csv"
        return "application/octet-stream"

    def _update_file(self, file_id: str, file_path: Path) -> str:
        logger.debug(f"Updating existing file: {file_path.name}")
        media = self._MediaFileUpload(str(file_path), mimetype=self._mime_type(file_path))
        if self.supports_shared_drives:
            file_info = self.service.files().update(
                fileId=file_id, media_body=media, supportsAllDrives=True
            ).execute()
        else:
            file_info = self.service.files().update(fileId=file_id, media_body=media).execute()
        return file_info.get("id")

    def _create_file(self, file_path: Path) -> str:
        cfg = get_config()
        logger.debug(f"Creating new file: {file_path.name}")
        file_metadata = {"name": file_path.name, "parents": [cfg.google_drive_folder_id]}
        media = self._MediaFileUpload(str(file_path), mimetype=self._mime_type(file_path))
        if self.supports_shared_drives:
            file_info = self.service.files().create(
                body=file_metadata, media_body=media, supportsAllDrives=True
            ).execute()
        else:
            file_info = self.service.files().create(
                body=file_metadata, media_body=media
            ).execute()
        return file_info.get("id")

    def _find_existing_file(self, file_name: str) -> Optional[str]:
        cfg = get_config()
        try:
            query = f"name='{file_name}' and '{cfg.google_drive_folder_id}' in parents and trashed=false"
            if self.supports_shared_drives:
                results = self.service.files().list(
                    q=query,
                    fields="files(id,name)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                ).execute()
            else:
                results = self.service.files().list(q=query, fields="files(id,name)").execute()
            files = results.get("files", [])
            if files:
                return files[0]["id"]
        except Exception as e:
            logger.debug(f"Error searching for existing file {file_name}: {e}")
        return None

    def _cleanup_orphaned_drive_files(self, current_countries: Set[str]):
        if not current_countries:
            return
        cfg = get_config()
        ext = cfg.feed_extension.lstrip(".")
        logger.debug(f"Cleaning up orphaned Drive files (keeping {len(current_countries)} countries)")
        try:
            query = (
                f"'{cfg.google_drive_folder_id}' in parents and trashed=false "
                f"and (name contains '.tsv' or name contains '.csv')"
            )
            if self.supports_shared_drives:
                results = self.service.files().list(
                    q=query,
                    fields="files(id,name)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                ).execute()
            else:
                results = self.service.files().list(q=query, fields="files(id,name)").execute()

            files = results.get("files", [])
            pattern = re.compile(r"country_feed_([A-Z]{2})\.(tsv|csv)$")
            orphaned = []
            for file_info in files:
                match = pattern.search(file_info["name"])
                if match and match.group(1) not in current_countries:
                    orphaned.append((file_info["id"], file_info["name"], match.group(1)))

            for file_id, file_name, country_code in orphaned:
                try:
                    if self.supports_shared_drives:
                        self.service.files().update(
                            fileId=file_id, body={"trashed": True}, supportsAllDrives=True
                        ).execute()
                    else:
                        self.service.files().update(
                            fileId=file_id, body={"trashed": True}
                        ).execute()
                    logger.debug(f"Trashed orphaned file: {file_name}")
                except Exception as e:
                    logger.error(f"Failed to trash {file_name}: {e}")

            if orphaned:
                logger.info(f"Drive cleanup: {len(orphaned)} orphaned files trashed")
        except Exception as e:
            logger.error(f"Drive cleanup failed: {e}")
