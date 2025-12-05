import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import logging
from typing import Dict, Optional, Tuple

from config import settings

logger = logging.getLogger(__name__)

class CountryLocationMapper:

    def __init__(self, shopify_sync=None):
        self.shopify_sync = shopify_sync

    def get_mapping_with_change_detection(self) -> Tuple[Dict, bool, str]:
        """Get mapping data and detect changes."""
        if not self.shopify_sync:
            raise ValueError("ShopifySync instance required")

        if not settings.SMART_MAPPING_ENABLED:
            return self._get_cached_mapping(), False, "Smart mapping disabled"

        logger.debug("Fetching fresh mapping data...")
        fresh_mapping = self._build_fresh_mapping()

        previous_hash = self._load_previous_mapping_hash()
        current_hash = fresh_mapping.get('mapping_hash')

        if not previous_hash:
            reason = "No previous mapping found - first run"
            has_changed = True
        elif previous_hash != current_hash:
            reason = "Mapping structure changed - hash mismatch"
            has_changed = True
        else:
            reason = "Mapping unchanged - hash match"
            has_changed = False

        self._save_mapping_hash(current_hash)
        self._log_mapping_decision(has_changed, reason, previous_hash, current_hash)

        return fresh_mapping, has_changed, reason

    def _build_fresh_mapping(self) -> Dict:
        """Build country mapping using bulk operations."""
        logger.debug("Building country mapping...")

        try:
            markets_data = self.shopify_sync.bulk_query.get_markets_and_countries()
            active_countries = self._parse_active_countries(markets_data["data"])

            locations_jsonl_path = self.shopify_sync.bulk_query.get_locations()
            locations_map = self._parse_locations_from_jsonl(locations_jsonl_path)

            country_location_map = self._create_country_mapping(locations_map, active_countries)
            mapping_hash = self._generate_mapping_hash(active_countries, country_location_map)

            return {
                "active_countries": active_countries,
                "location_country_map": country_location_map,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "mapping_hash": mapping_hash,
                "method": "bulk_operations"
            }

        except Exception as e:
            logger.error(f"Failed to build mapping: {e}")
            raise

    def _parse_active_countries(self, markets_data: Dict) -> Dict:
        active_countries = {}
        markets = markets_data.get("markets", {}).get("edges", [])
        
        for market_edge in markets:
            market_node = market_edge.get("node", {})
            market_id = market_node.get("id")
            regions = market_node.get("regions", {}).get("edges", [])
            
            for region_edge in regions:
                region_node = region_edge.get("node", {})
                country_code = region_node.get("code")
                country_name = region_node.get("name")
                
                # Filter to target countries only
                if country_code and country_name and country_code in settings.TARGET_COUNTRIES:
                    active_countries[country_code] = {
                        "name": country_name,
                        "market_id": market_id
                    }
        
        logger.info(f"Filtered to {len(active_countries)} target countries: {list(active_countries.keys())}")
        return active_countries

    def _create_country_mapping(self, locations: Dict, active_countries: Dict) -> Dict:
        """Create location mapping using delivery profiles."""
        location_country_map = {}

        try:
            delivery_relationships = self.shopify_sync.bulk_query.get_location_country_relationships()
            logger.debug(f"Retrieved delivery relationships for {len(delivery_relationships)} locations")
        except Exception as e:
            logger.warning(f"Could not get delivery relationships: {e}")
            delivery_relationships = {}

        for location_id, location in locations.items():
            is_active = location.get('is_active', True)

            if is_active:
                if location_id in delivery_relationships:
                    served_countries = [
                        country for country in delivery_relationships[location_id] 
                        if country in active_countries
                    ]
                    
                    if served_countries:
                        location_country_map[location_id] = {
                            'name': location.get('name'),
                            'countries': served_countries
                        }
                        continue

                location_country = location.get('country_code')
                if location_country in active_countries:
                    location_country_map[location_id] = {
                        'name': location.get('name'),
                        'countries': [location_country]
                    }

        return location_country_map

    def _parse_locations_from_jsonl(self, jsonl_path: str) -> Dict:
        """Parse locations from JSONL file."""
        locations = {}
        
        with open(jsonl_path, 'r') as f:
            for line in f:
                record = json.loads(line)
                
                if record.get('id', '').startswith('gid://shopify/Location/'):
                    location_gid = record['id']
                    location_id = location_gid.split('/')[-1]

                    locations[location_id] = {
                        'id': location_id,
                        'gid': location_gid,
                        'name': record.get('name'),
                        'country_code': record.get('address', {}).get('countryCode'),
                        'city': record.get('address', {}).get('city'),
                        'province': record.get('address', {}).get('province'),
                        'is_active': record.get('isActive', True)
                    }

        return locations

    def _generate_mapping_hash(self, active_countries: Dict, location_country_map: Dict) -> str:
        """Generate hash for mapping comparison."""
        hash_structure = {
            "countries": sorted(active_countries.keys()),
            "locations": sorted(location_country_map.keys()),
            "target_countries": sorted(list(settings.TARGET_COUNTRIES))
        }
        
        hash_string = json.dumps(hash_structure, sort_keys=True, separators=(',', ':'))
        return hashlib.md5(hash_string.encode()).hexdigest()

    def _load_previous_mapping_hash(self) -> Optional[str]:
        """Load previous mapping hash."""
        try:
            if settings.MAPPING_COMPARISON_FILE.exists():
                with open(settings.MAPPING_COMPARISON_FILE, 'r') as f:
                    data = json.load(f)
                    return data.get('hash')
        except Exception as e:
            logger.debug(f"Could not load previous mapping hash: {e}")
        return None

    def _save_mapping_hash(self, mapping_hash: str):
        """Save current mapping hash."""
        try:
            hash_data = {'hash': mapping_hash}
            with open(settings.MAPPING_COMPARISON_FILE, 'w') as f:
                json.dump(hash_data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save mapping hash: {e}")

    def _log_mapping_decision(self, has_changed: bool, reason: str,
                             previous_hash: Optional[str], current_hash: str):
        logger.info("=== MAPPING CHANGE DETECTION ===")
        logger.info(f"Result: {'CHANGED' if has_changed else 'UNCHANGED'}")
        logger.info(f"Reason: {reason}")
        if previous_hash:
            logger.info(f"Previous Hash: {previous_hash[:12]}...")
        else:
            logger.info("Previous Hash: None")
        logger.info(f"Current Hash: {current_hash[:12]}...")
        logger.info("==============================")

    def clear_mapping_hash(self):
        """Clear stored mapping hash."""
        try:
            if settings.MAPPING_COMPARISON_FILE.exists():
                settings.MAPPING_COMPARISON_FILE.unlink()
                logger.info("Mapping hash cleared")
        except Exception as e:
            logger.warning(f"Failed to clear mapping hash: {e}")

    def get_mapping_stats(self) -> Dict:
        """Get mapping statistics."""
        try:
            if settings.MAPPING_COMPARISON_FILE.exists():
                with open(settings.MAPPING_COMPARISON_FILE, 'r') as f:
                    hash_data = json.load(f)
                    return {
                        'has_previous_hash': True,
                        'last_hash': hash_data.get('hash', '')[:12] + '...',
                        'last_updated': 'Available'
                    }
        except Exception:
            pass

        return {
            'has_previous_hash': False,
            'last_hash': 'None',
            'last_updated': 'Never'
        }

    def _get_cached_mapping(self) -> Dict:
        """Fallback method for cached mapping."""
        mapping_file = settings.CACHE_DIR / f"country-mapping.json"
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cached mapping: {e}")
        
        return self._build_fresh_mapping()
