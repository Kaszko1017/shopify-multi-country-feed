"""Polls Shopify bulk operations and downloads JSONL results."""
import time
import requests
import json
from pathlib import Path
import logging
from typing import Optional, Dict

from shopify import GraphQL
from core.config import get_config

logger = logging.getLogger(__name__)


class BulkOperationsHandler:
    """Runs Shopify Admin GraphQL bulk operations and downloads results to disk."""

    def __init__(self):
        self.graphql = GraphQL()

    def execute_bulk_query(self, query_string: str) -> Optional[str]:
        """Start a bulk operation, poll to completion, return local path to the JSONL result or None."""
        logger.info("Starting bulk operation...")
        cfg = get_config()

        mutation = f"""
        mutation {{
            bulkOperationRunQuery(
                query: \"\"\"
                {query_string}
                \"\"\"
            ) {{
                bulkOperation {{
                    id
                    status
                }}
                userErrors {{
                    field
                    message
                }}
            }}
        }}
        """

        try:
            result_str = GraphQL().execute(mutation)
            result = json.loads(result_str)

            if "errors" in result:
                raise Exception(f"GraphQL errors: {result['errors']}")

            user_errors = result.get("data", {}).get("bulkOperationRunQuery", {}).get("userErrors", [])
            if user_errors:
                raise Exception(f"Bulk operation errors: {user_errors}")

            logger.info("Bulk operation started")
            bulk_operation = self._poll_bulk_operation()

            if bulk_operation.get("url"):
                filename = self._download_bulk_result(bulk_operation["url"])
                logger.info(f"Bulk operation completed: {filename}")
                return filename
            else:
                logger.info("Bulk operation completed with no results")
                return None

        except Exception as e:
            logger.error(f"Bulk operation failed: {e}")
            raise

    def _poll_bulk_operation(self) -> Dict:
        query = """
        query {
            currentBulkOperation {
                id
                status
                errorCode
                createdAt
                completedAt
                objectCount
                fileSize
                url
                partialDataUrl
            }
        }
        """

        logger.info("Polling bulk operation status...")
        time.sleep(10)

        while True:
            try:
                result_str = GraphQL().execute(query)
                result = json.loads(result_str)

                if "errors" in result:
                    raise Exception(f"Polling errors: {result['errors']}")

                bulk_op = result.get("data", {}).get("currentBulkOperation")
                if not bulk_op:
                    raise Exception("No current bulk operation found")

                status = bulk_op["status"].lower()
                logger.info(f"Bulk operation status: {status}")

                if status not in {"running", "created"}:
                    if status == "completed":
                        logger.info(f"Bulk operation completed: {bulk_op.get('objectCount', 0)} objects")
                        return bulk_op
                    else:
                        error_code = bulk_op.get("errorCode", "Unknown")
                        raise Exception(f"Bulk operation failed: {status}, error: {error_code}")

                time.sleep(30)

            except Exception as e:
                logger.error(f"Error during polling: {e}")
                raise

    def _download_bulk_result(self, url: str) -> str:
        cfg = get_config()
        response = requests.get(url, timeout=300)
        response.raise_for_status()

        timestamp = int(time.time())
        filename = cfg.temp_path(f"bulk_result_{timestamp}.jsonl")

        with open(filename, "wb") as f:
            f.write(response.content)

        file_size = filename.stat().st_size
        logger.info(f"Downloaded bulk result: {filename} ({file_size // 1024} KB)")
        return str(filename)
