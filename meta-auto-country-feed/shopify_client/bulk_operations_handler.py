import time
import requests
import json
import tempfile
from pathlib import Path
import logging
from typing import Optional, Dict
from config import settings
from shopify import GraphQL


logger = logging.getLogger(__name__)

class BulkOperationsHandler:
    """Handle Shopify bulk operations using Shopify GraphQL module."""
    def __init__(self):
        self.graphql = GraphQL()
    
    def execute_bulk_query(self, query_string: str) -> Optional[str]:
        """Execute bulk query and return path to downloaded JSONL file."""
        logger.info("Starting bulk operation query...")
        
        # 1. Start bulk operation
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
            
            # 2. Poll for completion
            bulk_operation = self._poll_bulk_operation()
            
            # 3. Download JSONL file if available
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
        """Poll bulk operation until completion."""
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
                        raise Exception(f"Bulk operation failed with status: {status}, error: {error_code}")
                
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"Error during polling: {e}")
                raise
    
    def _download_bulk_result(self, url: str) -> str:
        """Download JSONL file from bulk operation result."""
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            
            timestamp = int(time.time())
            filename = settings.TEMP_DIR / f"bulk_result_{timestamp}.jsonl"
            
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            file_size = filename.stat().st_size
            logger.info(f"Downloaded bulk result: {filename} ({file_size // 1024} KB)")
            
            return str(filename)
            
        except Exception as e:
            logger.error(f"Failed to download bulk result: {e}")
            raise
