import logging
from pathlib import Path
from typing import List, Set, Optional
import re
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from config import settings

logger = logging.getLogger(__name__)

class SyncManager:

    def __init__(self):
        self.service = None
        self.supports_shared_drives = True
        if settings.GOOGLE_DRIVE_FOLDER_ID and settings.GOOGLE_SERVICE_ACCOUNT_FILE:
            self._authenticate()

    def _authenticate(self):
        """Authenticate using Google APIs Client Library with service account."""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                settings.GOOGLE_SERVICE_ACCOUNT_FILE,
                scopes=['https://www.googleapis.com/auth/drive']
            )

            self.service = build('drive', 'v3', credentials=credentials)
            logger.debug("Authenticated with Google Drive using official client library")
            self._verify_folder_access()

        except Exception as e:
            logger.warning(f"Failed to authenticate with Google Drive: {e}")
            self.service = None

    def _verify_folder_access(self):
        """Verify folder access using Google APIs client with compatibility handling."""
        if not self.service:
            return

        try:
            # Try with shared drives support first
            folder_info = self.service.files().get(
                fileId=settings.GOOGLE_DRIVE_FOLDER_ID,
                fields='id,name,mimeType',
                supportsAllDrives=True
            ).execute()
            
            logger.debug(f"Verified access to folder: '{folder_info['name']}' ({settings.GOOGLE_DRIVE_FOLDER_ID})")

        except Exception as e:
            # If it fails, try without shared drives parameters
            try:
                folder_info = self.service.files().get(
                    fileId=settings.GOOGLE_DRIVE_FOLDER_ID,
                    fields='id,name,mimeType'
                ).execute()
                
                logger.debug(f"Verified access to folder: '{folder_info['name']}' ({settings.GOOGLE_DRIVE_FOLDER_ID})")
                self.supports_shared_drives = False
                logger.debug("Using legacy API mode (no shared drives support)")
                
            except Exception as e2:
                logger.warning(f"Folder verification failed: {e2}, but uploads will continue")
                self.supports_shared_drives = False

    def upload_files_with_cleanup(self, file_paths: List[Path], current_countries: Set[str], sync_type: Optional[str] = None) -> int:
        """Upload files with cleanup - synchronous approach."""
        if not self.service:
            logger.warning("Google Drive not configured - skipping upload")
            return 0

        logger.debug(f"Starting upload with cleanup for {len(file_paths)} files")

        # Only clean up on full sync; skip on incremental
        if sync_type != "INCREMENTAL":
            self._cleanup_orphaned_drive_files(current_countries)

        # Upload files
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
        """Upload a single file using Google APIs client library."""
        file_name = file_path.name
        existing_file_id = self._find_existing_file(file_name)

        # Retry logic
        for attempt in range(settings.MAX_RETRIES + 1):
            try:
                if existing_file_id:
                    return self._update_file(existing_file_id, file_path)
                else:
                    return self._create_file(file_path)

            except Exception as e:
                if attempt == settings.MAX_RETRIES:
                    raise Exception(f"Upload failed after {settings.MAX_RETRIES + 1} attempts: {e}")

                import time
                delay = min(settings.BASE_RETRY_DELAY * (2 ** attempt), 60.0)
                logger.debug(f"Upload attempt {attempt + 1} failed: {e}, retrying in {delay:.1f}s")
                time.sleep(delay)

    def _update_file(self, file_id: str, file_path: Path) -> str:
        """Update existing file using Google APIs client."""
        logger.debug(f"Updating existing file: {file_path.name}")
        
        mime_type = 'text/tab-separated-values' if file_path.suffix == '.tsv' else 'text/xml'
        media = MediaFileUpload(str(file_path), mimetype=mime_type)
        
        if self.supports_shared_drives:
            file_info = self.service.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True
            ).execute()
        else:
            file_info = self.service.files().update(
                fileId=file_id,
                media_body=media
            ).execute()

        logger.debug(f"Updated file: {file_info.get('name')} (ID: {file_info.get('id')})")
        return file_info.get('id')

    def _create_file(self, file_path: Path) -> str:
        """Create new file using Google APIs client with compatibility handling."""
        logger.debug(f"Creating new file: {file_path.name}")

        file_metadata = {
            'name': file_path.name,
            'parents': [settings.GOOGLE_DRIVE_FOLDER_ID]
        }

        mime_type = 'text/tab-separated-values' if file_path.suffix == '.tsv' else 'text/xml'
        media = MediaFileUpload(str(file_path), mimetype=mime_type)
        
        if self.supports_shared_drives:
            file_info = self.service.files().create(
                body=file_metadata,
                media_body=media,
                supportsAllDrives=True
            ).execute()
        else:
            file_info = self.service.files().create(
                body=file_metadata,
                media_body=media
            ).execute()

        logger.debug(f"Created file: {file_info.get('name')} (ID: {file_info.get('id')})")
        return file_info.get('id')

    def _find_existing_file(self, file_name: str) -> Optional[str]:
        """Find existing file by name using Google APIs client."""
        try:
            query = f"name='{file_name}' and '{settings.GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed=false"
            
            if self.supports_shared_drives:
                results = self.service.files().list(
                    q=query,
                    fields='files(id,name)',
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
            else:
                results = self.service.files().list(
                    q=query,
                    fields='files(id,name)'
                ).execute()

            files = results.get('files', [])
            if files:
                return files[0]['id']

        except Exception as e:
            logger.debug(f"Error searching for existing file {file_name}: {e}")

        return None

    def _cleanup_orphaned_drive_files(self, current_countries: Set[str]):
        """Clean up orphaned files from Google Drive."""
        if not current_countries:
            logger.debug("No current countries provided - skipping Drive cleanup")
            return

        logger.debug(f"Cleaning up orphaned Drive files (keeping {len(current_countries)} countries)")

        try:
            # List all feed files in the folder
            query = f"'{settings.GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed=false and (name contains '.tsv' or name contains '.xml')"
            
            if self.supports_shared_drives:
                results = self.service.files().list(
                    q=query,
                    fields='files(id,name)',
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
            else:
                results = self.service.files().list(
                    q=query,
                    fields='files(id,name)'
                ).execute()

            files = results.get('files', [])
            orphaned_files = []

            for file_info in files:
                file_name = file_info['name']
                file_id = file_info['id']

                # Extract country code from filename
                match = re.search(r'country_feed_([A-Z]{2})\.(tsv|xml)$', file_name)
                if match:
                    country_code = match.group(1)
                    if country_code not in current_countries:
                        orphaned_files.append((file_id, file_name, country_code))

            if not orphaned_files:
                logger.debug("No orphaned files found in Google Drive")
                return

            # Trash orphaned files
            deleted_count = 0
            for file_id, file_name, country_code in orphaned_files:
                try:
                    if self.supports_shared_drives:
                        self.service.files().update(
                            fileId=file_id,
                            body={'trashed': True},
                            supportsAllDrives=True
                        ).execute()
                    else:
                        self.service.files().update(
                            fileId=file_id,
                            body={'trashed': True}
                        ).execute()
                    deleted_count += 1
                    logger.debug(f"Deleted orphaned Drive file: {file_name} (country: {country_code})")
                except Exception as e:
                    logger.error(f"Failed to delete {file_name}: {e}")

            if deleted_count > 0:
                logger.info(f"Drive cleanup complete: {deleted_count} orphaned files processed")

        except Exception as e:
            logger.error(f"Drive cleanup failed: {e}")
