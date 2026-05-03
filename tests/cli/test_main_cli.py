"""CLI tests for main.main(argv=...) with an isolated project root."""
from unittest.mock import MagicMock

import pytest

import main as main_mod


def test_no_command_exits_one(tmp_project, monkeypatch):
    monkeypatch.setattr(main_mod, "_project_root", lambda: tmp_project)
    with pytest.raises(SystemExit) as e:
        main_mod.main(argv=[])
    assert e.value.code == 1


def test_invalid_command_exits_nonzero(tmp_project, monkeypatch):
    monkeypatch.setattr(main_mod, "_project_root", lambda: tmp_project)
    with pytest.raises(SystemExit) as e:
        main_mod.main(argv=["prog", "not-a-command"])
    assert e.value.code != 0


def test_validation_errors_exit_one(tmp_project, monkeypatch):
    monkeypatch.setattr(main_mod, "_project_root", lambda: tmp_project)

    def boom():
        return ["SHOPIFY_TOKEN is required"]

    monkeypatch.setattr(main_mod.ConfigValidator, "validate_all", staticmethod(boom))
    mock_orch = MagicMock()
    monkeypatch.setattr(main_mod, "SyncOrchestrator", lambda **kw: mock_orch)
    with pytest.raises(SystemExit) as e:
        main_mod.main(argv=["smart"])
    assert e.value.code == 1
    mock_orch.run_smart.assert_not_called()


def test_debug_skips_validation(tmp_project, monkeypatch):
    monkeypatch.setattr(main_mod, "_project_root", lambda: tmp_project)
    called = []

    def track():
        called.append("validate")
        return []

    monkeypatch.setattr(main_mod.ConfigValidator, "validate_all", staticmethod(track))
    mock_orch = MagicMock()
    monkeypatch.setattr(main_mod, "SyncOrchestrator", lambda **kw: mock_orch)
    main_mod.main(argv=["debug"])
    assert called == []
    mock_orch.debug_state.assert_called_once()


def test_target_google_uses_tsv_exporter(tmp_project, monkeypatch):
    monkeypatch.setattr(main_mod, "_project_root", lambda: tmp_project)
    monkeypatch.setattr(main_mod.ConfigValidator, "validate_all", staticmethod(lambda: []))
    seen = []

    class TrackTSV(main_mod.TSVExporter):
        def __init__(self):
            seen.append("google")

    monkeypatch.setattr(main_mod, "TSVExporter", TrackTSV)
    mock_orch = MagicMock()
    monkeypatch.setattr(main_mod, "SyncOrchestrator", lambda **kw: mock_orch)
    main_mod.main(argv=["smart", "--target", "google"])
    assert seen == ["google"]


def test_target_meta_uses_csv_exporter(tmp_project, monkeypatch):
    monkeypatch.setattr(main_mod, "_project_root", lambda: tmp_project)
    monkeypatch.setattr(main_mod.ConfigValidator, "validate_all", staticmethod(lambda: []))
    seen = []

    class TrackCSV(main_mod.CSVExporter):
        def __init__(self):
            seen.append("meta")

    monkeypatch.setattr(main_mod, "CSVExporter", TrackCSV)
    mock_orch = MagicMock()
    monkeypatch.setattr(main_mod, "SyncOrchestrator", lambda **kw: mock_orch)
    main_mod.main(argv=["smart", "--target", "meta"])
    assert seen == ["meta"]


def test_orchestrator_exception_exits_one(tmp_project, monkeypatch):
    monkeypatch.setattr(main_mod, "_project_root", lambda: tmp_project)
    monkeypatch.setattr(main_mod.ConfigValidator, "validate_all", staticmethod(lambda: []))
    mock_orch = MagicMock()
    mock_orch.run_smart.side_effect = RuntimeError("boom")
    monkeypatch.setattr(main_mod, "SyncOrchestrator", lambda **kw: mock_orch)
    with pytest.raises(SystemExit) as e:
        main_mod.main(argv=["smart"])
    assert e.value.code == 1


def test_keyboard_interrupt_exits_130(tmp_project, monkeypatch):
    monkeypatch.setattr(main_mod, "_project_root", lambda: tmp_project)
    monkeypatch.setattr(main_mod.ConfigValidator, "validate_all", staticmethod(lambda: []))
    mock_orch = MagicMock()
    mock_orch.run_smart.side_effect = KeyboardInterrupt()
    monkeypatch.setattr(main_mod, "SyncOrchestrator", lambda **kw: mock_orch)
    with pytest.raises(SystemExit) as e:
        main_mod.main(argv=["smart"])
    assert e.value.code == 130


def test_valid_command_dispatches(tmp_project, monkeypatch):
    monkeypatch.setattr(main_mod, "_project_root", lambda: tmp_project)
    monkeypatch.setattr(main_mod.ConfigValidator, "validate_all", staticmethod(lambda: []))
    mock_orch = MagicMock()
    monkeypatch.setattr(main_mod, "SyncOrchestrator", lambda **kw: mock_orch)
    main_mod.main(argv=["refresh-mapping"])
    mock_orch.refresh_mapping_cache.assert_called_once()
