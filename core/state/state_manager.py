"""JSON persistence for sync and variant inventory state."""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import logging

from core.config import get_config
from core.utils.io import atomic_write_json

logger = logging.getLogger(__name__)


class StateManager:
    """Persists last sync time and per-variant stock state as JSON with atomic writes."""

    def __init__(self):
        cfg = get_config()
        self.sync_state_file = cfg.state_json_path
        self.variant_states_file = cfg.variant_state_json_path

    def save_sync_state(self, timestamp: datetime):
        try:
            state_data = {
                "last_successful_run": timestamp.isoformat(),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            atomic_write_json(self.sync_state_file, state_data)
            logger.info(f"Sync state saved: {timestamp.isoformat()}")
        except Exception as e:
            logger.error(f"Failed to save sync state: {e}")

    def load_sync_state(self) -> Optional[datetime]:
        try:
            if not self.sync_state_file.exists():
                return None
            with open(self.sync_state_file, "r", encoding="utf-8") as f:
                state_data = json.load(f)
            timestamp_str = state_data.get("last_successful_run")
            if timestamp_str:
                return datetime.fromisoformat(timestamp_str)
        except Exception as e:
            logger.warning(f"Failed to load sync state: {e}")
        return None

    def detect_stock_changes(self, current_variants: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        if not current_variants:
            return [], []
        new_variants = []
        changed_variants = []
        existing_states = self._load_variant_states()
        variant_lookup = {}
        for variant in current_variants:
            variant_id = variant.get("id")
            if variant_id:
                inventory_qty = self._safe_int(variant.get("inventory_quantity", 0))
                current_status = "in stock" if inventory_qty > 0 else "out of stock"
                variant_lookup[variant_id] = (variant, current_status)

        for variant_id, (variant, current_status) in variant_lookup.items():
            if variant_id not in existing_states:
                new_variants.append(variant)
            elif existing_states[variant_id]["stock_status"] != current_status:
                changed_variants.append(variant)

        logger.info(f"Changes: {len(new_variants)} new, {len(changed_variants)} changed")
        return new_variants, changed_variants

    def update_variant_states(self, variants: List[Dict]):
        if not variants:
            return
        timestamp = datetime.now(timezone.utc).isoformat()
        existing_states = self._load_variant_states()
        for variant in variants:
            variant_id = variant.get("id")
            if variant_id:
                inventory_qty = self._safe_int(variant.get("inventory_quantity", 0))
                current_status = "in stock" if inventory_qty > 0 else "out of stock"
                existing_states[variant_id] = {
                    "stock_status": current_status,
                    "last_updated": timestamp,
                }
        try:
            atomic_write_json(self.variant_states_file, existing_states)
            logger.info(f"Updated {len(variants)} variant states")
        except Exception as e:
            logger.error(f"Failed to update variant states: {e}")

    def reset_variant_states(self):
        try:
            if self.variant_states_file.exists():
                self.variant_states_file.unlink()
            logger.info("Variant states reset")
        except Exception as e:
            logger.error(f"Failed to reset variant states: {e}")

    def get_stats(self) -> Dict:
        try:
            variant_states = self._load_variant_states()
            in_stock_count = sum(
                1 for state in variant_states.values() if state.get("stock_status") == "in stock"
            )
            sync_state = self.load_sync_state()
            last_sync = sync_state.isoformat() if sync_state else None
            sync_size = self.sync_state_file.stat().st_size if self.sync_state_file.exists() else 0
            variant_size = (
                self.variant_states_file.stat().st_size if self.variant_states_file.exists() else 0
            )
            return {
                "variant_count": len(variant_states),
                "in_stock_count": in_stock_count,
                "out_of_stock_count": len(variant_states) - in_stock_count,
                "last_sync": last_sync,
                "db_size_kb": (sync_size + variant_size) // 1024,
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {
                "variant_count": 0,
                "in_stock_count": 0,
                "out_of_stock_count": 0,
                "last_sync": None,
                "db_size_kb": 0,
            }

    def _load_variant_states(self) -> Dict:
        try:
            if not self.variant_states_file.exists():
                return {}
            with open(self.variant_states_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load variant states: {e}")
            return {}

    def _safe_int(self, value) -> int:
        try:
            if value is None:
                return 0
            return int(float(str(value)))
        except (ValueError, TypeError):
            return 0
