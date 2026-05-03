"""SyncManager tests for Google Drive upload and folder maintenance behavior."""
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from core.output.drive_sync import SyncManager


@pytest.fixture
def mock_drive_manager():
    mgr = SyncManager.__new__(SyncManager)
    mgr.service = MagicMock()
    mgr._MediaFileUpload = MagicMock()
    mgr.supports_shared_drives = True
    mgr._cleanup_orphaned_drive_files = MagicMock()
    mgr._upload_file = MagicMock(return_value="file-id")
    return mgr


def test_upload_skipped_when_drive_not_configured(google_env):
    mgr = SyncManager()
    assert mgr.service is None
    p = Path(google_env) / "Google Merchant - country feed updates" / "country_feed_US.tsv"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("id\tavailability\n", encoding="utf-8")
    assert mgr.upload_files_with_cleanup([p], {"US"}, "FULL") == 0


def test_full_sync_calls_orphan_cleanup(mock_drive_manager):
    p = Path("/tmp/fake.tsv")
    mock_drive_manager.upload_files_with_cleanup([p], {"DE", "FR"}, "FULL")
    mock_drive_manager._cleanup_orphaned_drive_files.assert_called_once_with({"DE", "FR"})


def test_incremental_skips_orphan_cleanup(mock_drive_manager):
    p = Path("/tmp/fake.tsv")
    mock_drive_manager.upload_files_with_cleanup([p], {"US"}, "INCREMENTAL")
    mock_drive_manager._cleanup_orphaned_drive_files.assert_not_called()


def test_mime_type_by_extension():
    mgr = SyncManager.__new__(SyncManager)
    assert mgr._mime_type(Path("a.tsv")) == "text/tab-separated-values"
    assert mgr._mime_type(Path("b.csv")) == "text/csv"
    assert mgr._mime_type(Path("c.bin")) == "application/octet-stream"


def test_upload_counts_only_successful_files(mock_drive_manager):
    p1, p2 = Path("/tmp/a.tsv"), Path("/tmp/b.tsv")
    mock_drive_manager._upload_file = MagicMock(side_effect=[RuntimeError("fail"), None])
    n = mock_drive_manager.upload_files_with_cleanup([p1, p2], {"US"}, "FULL")
    assert n == 1


def _mgr_for_upload(monkeypatch):
    mgr = SyncManager.__new__(SyncManager)
    mgr.service = MagicMock()
    mgr._MediaFileUpload = MagicMock()
    mgr.supports_shared_drives = True
    monkeypatch.setattr(
        "core.output.drive_sync.get_config",
        lambda: MagicMock(
            google_drive_folder_id="folderid12345",
            max_retries=0,
            base_retry_delay=0.01,
            max_retry_delay=1.0,
        ),
    )
    return mgr


def test_upload_existing_file_uses_update(monkeypatch):
    mgr = _mgr_for_upload(monkeypatch)
    mgr._find_existing_file = MagicMock(return_value="existing-id")
    mgr._update_file = MagicMock(return_value="existing-id")
    mgr._create_file = MagicMock(return_value="new-id")
    SyncManager._upload_file(mgr, Path("/tmp/x.tsv"))
    mgr._update_file.assert_called_once()
    mgr._create_file.assert_not_called()


def test_upload_new_file_uses_create(monkeypatch):
    mgr = _mgr_for_upload(monkeypatch)
    mgr._find_existing_file = MagicMock(return_value=None)
    mgr._update_file = MagicMock()
    mgr._create_file = MagicMock(return_value="new-id")
    SyncManager._upload_file(mgr, Path("/tmp/y.tsv"))
    mgr._create_file.assert_called_once()
    mgr._update_file.assert_not_called()


def test_cleanup_orphan_trashes_non_retained_countries(monkeypatch):
    monkeypatch.setattr(
        "core.output.drive_sync.get_config",
        lambda: MagicMock(
            google_drive_folder_id="folderid12345",
            feed_extension=".tsv",
        ),
    )
    mgr = SyncManager.__new__(SyncManager)
    mgr.supports_shared_drives = True
    list_resp = MagicMock()
    list_resp.execute.return_value = {
        "files": [
            {"id": "1", "name": "country_feed_US.tsv"},
            {"id": "2", "name": "country_feed_XX.tsv"},
        ]
    }
    files_api = MagicMock()
    files_api.list.return_value = list_resp
    upd = MagicMock()
    upd.execute.return_value = {}
    files_api.update.return_value = upd
    mgr.service = MagicMock()
    mgr.service.files.return_value = files_api

    SyncManager._cleanup_orphaned_drive_files(mgr, {"US"})
    assert files_api.update.call_count == 1
    kwargs = files_api.update.call_args.kwargs
    assert kwargs["fileId"] == "2"
    assert kwargs["body"] == {"trashed": True}


def test_cleanup_skips_when_no_current_countries(monkeypatch):
    monkeypatch.setattr(
        "core.output.drive_sync.get_config",
        lambda: MagicMock(google_drive_folder_id="x", feed_extension=".tsv"),
    )
    mgr = SyncManager.__new__(SyncManager)
    mgr.service = MagicMock()
    SyncManager._cleanup_orphaned_drive_files(mgr, set())
    mgr.service.files.assert_not_called()


def test_import_google_drive_failure_leaves_service_none(google_env, monkeypatch):
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "folderid12345")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(google_env / "sa.json"))
    (google_env / "sa.json").write_text("{}", encoding="utf-8")
    from core.config import load_config

    load_config(base_dir=google_env, target="google")
    monkeypatch.setattr(
        "core.output.drive_sync._import_google_drive",
        lambda: (None, None, None),
    )
    mgr = SyncManager()
    assert mgr.service is None


def test_verify_folder_access_disables_shared_drives_on_fallback(monkeypatch):
    monkeypatch.setattr(
        "core.output.drive_sync.get_config",
        lambda: MagicMock(google_drive_folder_id="folderid12345"),
    )
    g1 = MagicMock()
    g1.execute.side_effect = Exception("shared")
    g2 = MagicMock()
    g2.execute.return_value = {"id": "1", "name": "F"}
    files_api = MagicMock()
    files_api.get.side_effect = [g1, g2]
    mgr = SyncManager.__new__(SyncManager)
    mgr.service = MagicMock()
    mgr.service.files.return_value = files_api
    mgr.supports_shared_drives = True
    SyncManager._verify_folder_access(mgr)
    assert mgr.supports_shared_drives is False
