"""Shopify Admin API session setup."""
import logging
import shopify

from core.config import get_config
from core.shopify.shopify_bulk_query import ShopifyBulkQuery

logger = logging.getLogger(__name__)


class ShopifySync:
    """Activates a Shopify API session and exposes bulk GraphQL helpers."""

    def __init__(self):
        cfg = get_config()
        shopify.Session.setup(api_key="dummy", secret="dummy")
        self.session = shopify.Session(
            cfg.shopify_session_config["shop_url"],
            cfg.shopify_session_config["api_version"],
            cfg.shopify_session_config["access_token"],
        )
        shopify.ShopifyResource.activate_session(self.session)
        self.bulk_query = ShopifyBulkQuery(self)
        logger.info(f"Shopify SDK initialized for {cfg.store_id}")
