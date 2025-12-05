import json
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import logging
from config import settings

logger = logging.getLogger(__name__)

class StateManager:

    def __init__(self):
        self.sync_state_file = settings.STATE_JSON_PATH
        self.variant_states_file = settings.VARIANT_STATE_JSON_PATH

    def save_sync_state(self, timestamp: datetime):
        """Save sync timestamp to JSON file."""
        try:
            state_data = {
                'last_successful_run': timestamp.isoformat(),
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            with open(self.sync_state_file, 'w') as f:
                json.dump(state_data, f, indent=2)
            logger.info(f"Sync state saved: {timestamp.isoformat()}")
        except Exception as e:
            logger.error(f"Failed to save sync state: {e}")

    def load_sync_state(self) -> Optional[datetime]:
        """Load last sync timestamp from JSON file."""
        try:
            if not self.sync_state_file.exists():
                return None

            with open(self.sync_state_file, 'r') as f:
                state_data = json.load(f)

            timestamp_str = state_data.get('last_successful_run')
            if timestamp_str:
                return datetime.fromisoformat(timestamp_str)

        except Exception as e:
            logger.warning(f"Failed to load sync state: {e}")

        return None

    def detect_stock_changes(self, current_variants: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Detect new and changed variants using JSON comparison."""
        if not current_variants:
            return [], []

        new_variants = []
        changed_variants = []

        # Load existing variant states
        existing_states = self._load_variant_states()

        # Build variant lookup for efficient comparison
        variant_lookup = {}
        for variant in current_variants:
            variant_id = variant.get('id')
            if variant_id:
                inventory_qty = self._safe_int(variant.get('inventory_quantity', 0))
                current_status = "in stock" if inventory_qty > 0 else "out of stock"
                variant_lookup[variant_id] = (variant, current_status)

        # Compare states to detect changes
        for variant_id, (variant, current_status) in variant_lookup.items():
            if variant_id not in existing_states:
                new_variants.append(variant)
            elif existing_states[variant_id]['stock_status'] != current_status:
                changed_variants.append(variant)

        logger.info(f"State comparison: {len(new_variants)} new, {len(changed_variants)} changed variants")
        return new_variants, changed_variants

    def update_variant_states(self, variants: List[Dict]):
        """Update variant states in JSON file."""
        if not variants:
            return

        timestamp = datetime.now(timezone.utc).isoformat()

        # Load existing states
        existing_states = self._load_variant_states()

        # Update with new variant data
        for variant in variants:
            variant_id = variant.get('id')
            if variant_id:
                inventory_qty = self._safe_int(variant.get('inventory_quantity', 0))
                current_status = "in stock" if inventory_qty > 0 else "out of stock"

                existing_states[variant_id] = {
                    'stock_status': current_status,
                    'last_updated': timestamp
                }

        # Save updated states
        try:
            with open(self.variant_states_file, 'w') as f:
                json.dump(existing_states, f, indent=2)
            logger.info(f"Updated {len(variants)} variant states in JSON")
        except Exception as e:
            logger.error(f"Failed to update variant states: {e}")

    def reset_variant_states(self):
        """Reset all variant states (for full sync)."""
        try:
            if self.variant_states_file.exists():
                self.variant_states_file.unlink()
            logger.info("Variant states reset for full sync")
        except Exception as e:
            logger.error(f"Failed to reset variant states: {e}")

    def get_stats(self) -> Dict:
        """Get statistics about current state."""
        try:
            variant_states = self._load_variant_states()
            in_stock_count = sum(1 for state in variant_states.values()
                                 if state.get('stock_status') == 'in stock')

            sync_state = self.load_sync_state()
            last_sync = sync_state.isoformat() if sync_state else None

            # Calculate file sizes
            sync_size = self.sync_state_file.stat().st_size if self.sync_state_file.exists() else 0
            variant_size = self.variant_states_file.stat().st_size if self.variant_states_file.exists() else 0

            return {
                'variant_count': len(variant_states),
                'in_stock_count': in_stock_count,
                'out_of_stock_count': len(variant_states) - in_stock_count,
                'last_sync': last_sync,
                'db_size_kb': (sync_size + variant_size) // 1024
            }

        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {
                'variant_count': 0,
                'in_stock_count': 0,
                'out_of_stock_count': 0,
                'last_sync': None,
                'db_size_kb': 0
            }

    def _load_variant_states(self) -> Dict:
        """Load variant states from JSON file."""
        try:
            if not self.variant_states_file.exists():
                return {}

            with open(self.variant_states_file, 'r') as f:
                return json.load(f)

        except Exception as e:
            logger.warning(f"Failed to load variant states: {e}")
            return {}

    def _safe_int(self, value):
        """Safely convert value to integer."""
        try:
            if value is None:
                return 0
            return int(float(str(value)))
        except (ValueError, TypeError):
            return 0
