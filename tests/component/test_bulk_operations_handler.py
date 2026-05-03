"""BulkOperationsHandler tests with controlled GraphQL and HTTP boundaries."""
import json
from pathlib import Path

import pytest

from core.shopify.bulk_operations_handler import BulkOperationsHandler


@pytest.fixture
def bulk_env(tmp_project, monkeypatch):
    monkeypatch.setenv("SHOPIFY_TOKEN", "x" * 32)
    monkeypatch.setenv("STORE_ID", "teststore")
    monkeypatch.setenv("TARGET_COUNTRIES", "US")
    from core.config import load_config

    load_config(base_dir=tmp_project, target="google")


def test_execute_bulk_query_success_downloads_jsonl(bulk_env, tmp_path, monkeypatch):
    from core.config import get_config

    cfg = get_config()
    mutation_response = {
        "data": {
            "bulkOperationRunQuery": {
                "bulkOperation": {"id": "gid://1", "status": "CREATED"},
                "userErrors": [],
            }
        }
    }
    poll_running = {
        "data": {
            "currentBulkOperation": {
                "id": "1",
                "status": "RUNNING",
                "url": None,
                "errorCode": None,
            }
        }
    }
    poll_done = {
        "data": {
            "currentBulkOperation": {
                "id": "1",
                "status": "COMPLETED",
                "url": "https://cdn.example/bulk.jsonl",
                "errorCode": None,
                "objectCount": 2,
            }
        }
    }

    exec_results = iter([json.dumps(mutation_response), json.dumps(poll_running), json.dumps(poll_done)])

    class FakeGraphQL:
        def execute(self, _query):
            return next(exec_results)

    monkeypatch.setattr("core.shopify.bulk_operations_handler.GraphQL", FakeGraphQL)
    monkeypatch.setattr("core.shopify.bulk_operations_handler.time.sleep", lambda *_a, **_k: None)

    class FakeResp:
        def raise_for_status(self):
            return None

        content = b'{"ok":true}\n'

    def fake_get(url, timeout=300):
        assert "example" in url
        return FakeResp()

    monkeypatch.setattr("core.shopify.bulk_operations_handler.requests.get", fake_get)

    handler = BulkOperationsHandler()
    handler.graphql = FakeGraphQL()
    path = handler.execute_bulk_query("{ products { id } }")
    assert path is not None
    assert Path(path).exists()
    assert Path(path).suffix == ".jsonl"
    assert Path(path).parent == cfg.temp_dir


def test_execute_bulk_query_graphql_errors_raise(bulk_env, monkeypatch):
    class FakeGraphQL:
        def execute(self, _q):
            return json.dumps({"errors": [{"message": "bad"}]})

    monkeypatch.setattr("core.shopify.bulk_operations_handler.GraphQL", FakeGraphQL)
    monkeypatch.setattr("core.shopify.bulk_operations_handler.time.sleep", lambda *_a, **_k: None)
    handler = BulkOperationsHandler()
    handler.graphql = FakeGraphQL()
    with pytest.raises(Exception, match="GraphQL errors"):
        handler.execute_bulk_query("query")


def test_poll_bulk_operation_failed_status_raises(bulk_env, monkeypatch):
    mutation_response = {
        "data": {
            "bulkOperationRunQuery": {
                "bulkOperation": {"id": "1", "status": "CREATED"},
                "userErrors": [],
            }
        }
    }
    failed = {
        "data": {
            "currentBulkOperation": {
                "id": "1",
                "status": "FAILED",
                "url": None,
                "errorCode": "TIMEOUT",
            }
        }
    }
    seq = iter([json.dumps(mutation_response), json.dumps(failed)])

    class FakeGraphQL:
        def execute(self, _q):
            return next(seq)

    monkeypatch.setattr("core.shopify.bulk_operations_handler.GraphQL", FakeGraphQL)
    monkeypatch.setattr("core.shopify.bulk_operations_handler.time.sleep", lambda *_a, **_k: None)
    handler = BulkOperationsHandler()
    handler.graphql = FakeGraphQL()
    with pytest.raises(Exception, match="Bulk operation failed"):
        handler.execute_bulk_query("query")
