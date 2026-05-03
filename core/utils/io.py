"""Filesystem helpers for durable JSON and bulk temp cleanup."""
import json
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    """Write JSON using a temporary file and atomic replace."""
    fd, tmp = tempfile.mkstemp(suffix=".json", dir=path.parent, prefix=".tmp_")
    try:
        with open(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        Path(tmp).replace(path)
    except Exception:
        Path(tmp).unlink(missing_ok=True)
        raise


def unlink_bulk_jsonl(path: Optional[str]) -> None:
    """Delete a bulk JSONL path if present; ignores missing files and logs I/O errors at debug."""
    if not path:
        return
    try:
        p = Path(path)
        if p.is_file():
            p.unlink()
            logger.debug("Removed bulk temp file: %s", path)
    except OSError as e:
        logger.debug("Could not remove bulk temp file %s: %s", path, e)
