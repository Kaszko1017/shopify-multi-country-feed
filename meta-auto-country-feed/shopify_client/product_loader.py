import json
import re
import logging
from html.parser import HTMLParser
from typing import Dict, List, Generator
from config import settings
from pathlib import Path

logger = logging.getLogger(__name__)

class HTMLTextExtractor(HTMLParser):
    """Extract text content from HTML."""

    def __init__(self):
        super().__init__()
        self.text_parts = []

    def handle_data(self, data):
        self.text_parts.append(data.strip())

    def get_text(self):
        return " ".join(part for part in self.text_parts if part)

def clean_description(raw_description):
    """Clean HTML from product descriptions and remove unwanted sections."""
    if not raw_description:
        return "Product description not available"

    parser = HTMLTextExtractor()
    parser.feed(raw_description)
    full_description = parser.get_text()

    delimiters = ["Colour description", "Material &amp; Care", "Size Guide", "Reviews"]
    for delimiter in delimiters:
        if delimiter in full_description:
            return full_description.split(delimiter)[0].strip()

    return full_description.strip() or "Product description not available"

def extract_size_from_sku(sku, original_sku=None):
    """Extract size information from SKU format."""
    sku_to_parse = original_sku or sku

    if not sku_to_parse or "-" not in sku_to_parse:
        return "Unknown"

    parts = sku_to_parse.split("-")
    return parts[1] if len(parts) >= 2 else "Unknown"

class ProductLoader:
    
    def load_products_from_bulk_jsonl(self, jsonl_file_path: str, 
                                    active_countries: Dict, 
                                    location_country_map: Dict) -> List[Dict]:
        """Load products from bulk operation JSONL file."""
        logger.info(f"Processing bulk operation JSONL: {jsonl_file_path}")
        
        if not Path(jsonl_file_path).exists():
            logger.error(f"JSONL file not found: {jsonl_file_path}")
            return []
        
        # Data structures for reconstruction
        products = {}
        variants = {}
        inventory_levels = {}
        
        # Parse JSONL file
        try:
            line_count = 0
            with open(jsonl_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line_count += 1
                    if not line.strip():
                        continue
                        
                    try:
                        record = json.loads(line.strip())
                        self._process_bulk_record(record, products, variants, inventory_levels)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON at line {line_count}: {e}")
                        continue
            
            logger.info(f"Processed {line_count} lines from JSONL")
            logger.info(f"Found: {len(products)} products, {len(variants)} variants, {len(inventory_levels)} inventory records")
            
        except Exception as e:
            logger.error(f"Error reading JSONL file: {e}")
            return []
        
        # Build country-specific variants
        return self._build_country_variants_from_bulk(
            products, variants, inventory_levels, active_countries, location_country_map
        )
    
    def _process_bulk_record(self, record: Dict, products: Dict, variants: Dict, inventory_levels: Dict):
        """Process a single record from bulk JSONL - handles separate inventory records."""
        record_id = record.get('id', '')
        parent_id = record.get('__parentId', '')
        
        # Top-level records (have ID, no parent)
        if record_id and not parent_id:
            if 'ProductVariant' in record_id:
                # This is a variant record
                variants[record_id] = record
                
                # Extract embedded product data
                if 'product' in record and record['product']:
                    product_data = record['product']
                    if 'id' in product_data:
                        products[product_data['id']] = product_data
                        
            elif 'Product' in record_id:
                # Standalone product record
                products[record_id] = record
                
        # Child records (have parent, may not have own ID)
        elif parent_id:
            # Inventory level records - these are the separate records we're missing!
            if 'location' in record and 'quantities' in record:
                # This is an inventory level record
                if parent_id not in inventory_levels:
                    inventory_levels[parent_id] = []
                inventory_levels[parent_id].append(record)
            # Handle other child record types if needed
            elif record_id and 'ProductVariant' in record_id:
                # Nested variant (shouldn't happen in our query but handle it)
                variants[record_id] = record

    
    def _build_country_variants_from_bulk(self, products: Dict, variants: Dict, 
                                        inventory_levels: Dict, active_countries: Dict, 
                                        location_country_map: Dict) -> List[Dict]:
        """Build country-specific variants from bulk data."""
        logger.info("Building country-specific variants from bulk data...")
        
        # Calculate country inventory from separate inventory level records
        country_inventory = {}
        warned_locations = set()
        
        # Process separate inventory level records
        for variant_id, levels in inventory_levels.items():
            for level in levels:
                location_gid = level.get('location', {}).get('id', '')
                if not location_gid:
                    continue
                
                location_id = location_gid.split('/')[-1]
                
                # Get available quantity
                quantities = level.get('quantities', [])
                available_qty = 0
                for qty in quantities:
                    if qty.get('name') == 'available':
                        available_qty = qty.get('quantity', 0)
                        break
                
                # Map to countries
                if available_qty > 0 and location_id in location_country_map:
                    countries = location_country_map[location_id]['countries']
                    for country_code in countries:
                        if country_code in active_countries:
                            key = f"{variant_id}-{country_code}"
                            if key not in country_inventory:
                                country_inventory[key] = 0
                            country_inventory[key] += available_qty
                elif location_id not in location_country_map and location_id not in warned_locations:
                    logger.warning(f"Location {location_id} not found in mapping")
                    warned_locations.add(location_id)
        
        # Determine target countries (only those with inventory)
        countries_with_inventory = set()
        for key, qty in country_inventory.items():
            if qty > 0:
                country_code = key.split('-')[-1]
                countries_with_inventory.add(country_code)
        
        target_countries = list(countries_with_inventory) if countries_with_inventory else []
        logger.info(f"Target countries with inventory: {sorted(target_countries)}")
        
        # Build final variants
        final_variants = []
        skipped_count = 0
        
        for variant_id, variant_data in variants.items():
            # Get product data
            product_data = variant_data.get('product', {})
            
            for country_code in target_countries:
                if country_code not in active_countries:
                    continue
                
                variant = self._create_country_variant_from_bulk(
                    variant_data, product_data, country_code, variant_id, country_inventory, active_countries
                )
                
                # Validate variant has required data
                inventory_item = variant_data.get('inventoryItem', {})
                sku = inventory_item.get('sku') if inventory_item else None
                
                if not sku:
                    skipped_count += 1
                    continue
                
                final_variants.append(variant)
        
        if skipped_count:
            logger.warning(f"Skipped {skipped_count} variants with missing SKU")
        
        variants_with_inventory = sum(1 for v in final_variants if v.get("inventory_quantity", 0) > 0)
        logger.info(f"Created {len(final_variants)} country-based variants ({variants_with_inventory} in stock)")
        
        return final_variants
   
    def _create_country_variant_from_bulk(self, variant_data: Dict, product_data: Dict, 
                                        country_code: str, variant_gid: str, 
                                        country_inventory: Dict, active_countries: Dict) -> Dict:
        """Create a country-specific variant from bulk data."""
        numeric_variant_id = variant_gid.split('/')[-1]
        composite_id = f"{numeric_variant_id}-{country_code}"
        
        # Get inventory quantity
        inventory_qty = country_inventory.get(f"{variant_gid}-{country_code}", 0)
        
        # Extract inventory item data
        inventory_item = variant_data.get('inventoryItem', {})
        sku = inventory_item.get('sku', '')
        
        # Build country variant
        country_variant = {
            "id": composite_id,
            "shopify_variant_id": numeric_variant_id,
            "country_code": country_code,
            "country_name": active_countries[country_code]["name"],
            "inventory_quantity": inventory_qty,
            "sku": sku,
            "price": variant_data.get("price", 0),
            "size": extract_size_from_sku(sku),
            "updated_at": variant_data.get("updatedAt", ""),
            
            # Product information
            "product_id": product_data.get("id", "").split('/')[-1] if product_data.get("id") else "",
            "product_title": product_data.get("title", ""),
            "product_handle": product_data.get("handle", ""),
            "product_description": clean_description(product_data.get("description", "")),
            "featured_image": (product_data.get("featuredImage") or {}).get("url", ""),
        }
        
        return country_variant
