"""Domain types for configuration and pipeline data."""
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set


@dataclass
class SyncConfig:
    """Runtime configuration loaded from environment after process start."""

    target: str
    base_dir: Path
    cache_dir: Path
    output_dir: Path
    temp_dir: Path
    state_json_path: Path
    variant_state_json_path: Path
    mapping_comparison_file: Path
    shopify_token: str
    store_id: str
    shopify_api_version: str
    shopify_session_config: Dict[str, str]
    google_drive_folder_id: str
    google_service_account_file: str
    target_countries: Set[str]
    bulk_chunk_size: int
    max_retries: int
    base_retry_delay: float
    csv_buffer_size: int
    smart_mapping_enabled: bool
    enable_data_validation: bool
    restrict_markets_to_target_countries: bool = True
    feed_prefix: str = "country_feed_"
    feed_extension: str = ".tsv"
    feed_id_prefix: str = "shopify_US_"
    max_retry_delay: float = 60.0
    max_inventory_threshold: int = 100_000

    def cache_path(self, *parts: str) -> Path:
        """Build a path inside this target's cache directory."""
        return self.cache_dir.joinpath(*parts)

    def temp_path(self, *parts: str) -> Path:
        """Build a path inside this target's temp directory."""
        return self.temp_dir.joinpath(*parts)


@dataclass
class CountryMapping:
    """Resolved country and location mapping from Shopify."""

    active_countries: Dict
    location_country_map: Dict
    created_at: str
    mapping_hash: str
    method: str = "bulk_operations"


@dataclass
class VariantSnapshot:
    """Per-variant fields used for state tracking and export."""

    id: str
    country_code: str
    product_id: str
    inventory_quantity: int
    shopify_variant_id: Optional[str] = None
    sku: Optional[str] = None
    price: Optional[str] = None
    extra: Dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Dict) -> "VariantSnapshot":
        return cls(
            id=d.get("id", ""),
            country_code=d.get("country_code", ""),
            product_id=d.get("product_id", ""),
            inventory_quantity=int(d.get("inventory_quantity") or 0),
            shopify_variant_id=d.get("shopify_variant_id"),
            sku=d.get("sku"),
            price=d.get("price"),
            extra={k: v for k, v in d.items() if k not in {
                "id", "country_code", "product_id", "inventory_quantity",
                "shopify_variant_id", "sku", "price"
            }},
        )

    def to_dict(self) -> Dict:
        out = {
            "id": self.id,
            "country_code": self.country_code,
            "product_id": self.product_id,
            "inventory_quantity": self.inventory_quantity,
        }
        if self.shopify_variant_id is not None:
            out["shopify_variant_id"] = self.shopify_variant_id
        if self.sku is not None:
            out["sku"] = self.sku
        if self.price is not None:
            out["price"] = self.price
        out.update(self.extra)
        return out


@dataclass
class FeedRecord:
    """One row in a country feed file."""

    id: str
    availability: str
    override: Optional[str] = None


@dataclass
class SyncResult:
    """Outcome of a completed sync run."""

    sync_type: str
    change_reason: str
    variants_count: int
    files_created: List[Path]
    total_time_seconds: float
    stats: Dict = field(default_factory=dict)
