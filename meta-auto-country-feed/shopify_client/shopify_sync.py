import shopify
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from config import settings
from shopify_client.shopify_bulk_query import ShopifyBulkQuery

logger = logging.getLogger(__name__)

class ShopifySync:
    """Shopify SDK-based data fetching with centralized GraphQL query handling."""

    def __init__(self):
        # Initialize Shopify session
        shopify.Session.setup(api_key="dummy", secret="dummy")
        self.session = shopify.Session(
            settings.SHOPIFY_SESSION_CONFIG['shop_url'],
            settings.SHOPIFY_SESSION_CONFIG['api_version'],
            settings.SHOPIFY_SESSION_CONFIG['access_token']
        )
        shopify.ShopifyResource.activate_session(self.session)
        
        # Initialize bulk query handler
        self.bulk_query = ShopifyBulkQuery(self)
        
        logger.info(f"Shopify SDK initialized for {settings.STORE_ID}")

