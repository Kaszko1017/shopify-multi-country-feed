import json
import logging
from typing import Dict, Optional
from config import settings
import shopify
from shopify_client.bulk_operations_handler import BulkOperationsHandler

logger = logging.getLogger(__name__)

class ShopifyBulkQuery:
    """GraphQL bulk query handler for Shopify data."""

    def __init__(self, shopify_sync=None):
        self.shopify_sync = shopify_sync
        self.bulk_handler = BulkOperationsHandler()
        self._ensure_session()

    def _ensure_session(self):
        """Ensure Shopify session is active."""
        if not self.shopify_sync:
            shopify.Session.setup(api_key="dummy", secret="dummy")
            session = shopify.Session(
                settings.SHOPIFY_SESSION_CONFIG['shop_url'],
                settings.SHOPIFY_SESSION_CONFIG['api_version'],
                settings.SHOPIFY_SESSION_CONFIG['access_token']
            )
            shopify.ShopifyResource.activate_session(session)

    def execute_query(self, query: str) -> Dict:
        """Execute GraphQL query."""
        try:
            result = shopify.GraphQL().execute(query)
            return json.loads(result)
        except Exception as e:
            logger.error(f"GraphQL query failed: {e}")
            raise

    def get_products_variants_inventory_bulk(self, since_timestamp: Optional[str] = None) -> str:
        """Get products, variants, and inventory via bulk operation."""
        logger.info("Fetching products and inventory via bulk operation...")
        
        time_filter = ""
        if since_timestamp:
            time_filter = f'updated_at:>{since_timestamp}'

        query = f"""
        {{
            productVariants(query: "product_status:active {time_filter}") {{
                edges {{
                    node {{
                        id
                        sku
                        price
                        updatedAt
                        product {{
                            id
                            title
                            handle
                            description
                            featuredImage {{ url }}
                        }}
                        inventoryItem {{
                            id
                            sku
                            inventoryLevels {{
                                edges {{
                                    node {{
                                        location {{
                                            id
                                        }}
                                        quantities(names: ["available"]) {{
                                            name
                                            quantity
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
        """

        logger.info(f"Bulk query filter: {time_filter if time_filter else 'all active variants'}")
        return self.bulk_handler.execute_bulk_query(query)

    def get_markets_and_countries(self) -> Dict:
        """Get markets and countries."""
        query = """
        query getMarketsComplete {
            markets(first: 250) {
                edges {
                    node {
                        id
                        regions(first: 250) {
                            edges {
                                node {
                                    ... on MarketRegionCountry {
                                        code
                                        name
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        logger.info("Fetching markets and countries")
        return self.execute_query(query)

    def get_locations(self) -> str:
        """Get locations via bulk operation."""
        query = """
        {
            locations {
                edges {
                    node {
                        id
                        address {
                            countryCode
                        }
                        isActive
                    }
                }
            }
        }
        """
        return self.bulk_handler.execute_bulk_query(query)

    def get_location_country_relationships(self) -> Dict:
        """Get location-country relationships from delivery profiles."""
        query = """
        {
            deliveryProfiles(first: 100) {
                edges {
                    node {
                        id
                        profileLocationGroups {
                            locationGroup {
                                locations(first: 200) {
                                    edges {
                                        node {
                                            id
                                        }
                                    }
                                }
                            }
                            locationGroupZones(first: 250) {
                                edges {
                                    node {
                                        zone {
                                            countries {
                                                code {
                                                    countryCode
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        try:
            result = self.execute_query(query)

            if "errors" in result:
                logger.error(f"GraphQL errors: {result['errors']}")
                return {}

            location_countries = {}

            for profile_edge in result["data"]["deliveryProfiles"]["edges"]:
                profile = profile_edge["node"]

                for location_group_data in profile.get('profileLocationGroups', []):
                    location_edges = location_group_data.get('locationGroup', {}).get('locations', {}).get('edges', [])
                    location_ids = [edge['node']['id'].split('/')[-1] for edge in location_edges]

                    zone_edges = location_group_data.get('locationGroupZones', {}).get('edges', [])
                    countries = []
                    for zone_edge in zone_edges:
                        zone_countries = zone_edge.get('node', {}).get('zone', {}).get('countries', [])
                        for country_data in zone_countries:
                            country_code = country_data.get('code', {}).get('countryCode')
                            if country_code:
                                countries.append(country_code)

                    for location_id in location_ids:
                        if location_id not in location_countries:
                            location_countries[location_id] = set()
                        location_countries[location_id].update(countries)

            for location_id in location_countries:
                location_countries[location_id] = list(location_countries[location_id])

            return location_countries

        except Exception as e:
            logger.error(f"Error fetching delivery relationships: {e}")
            return {}
