import json
import logging
from typing import Dict, Optional

from config import settings
import shopify
from shopify_client.bulk_operations_handler import BulkOperationsHandler

logger = logging.getLogger(__name__)

class ShopifyBulkQuery:
    """GraphQL query handler with bulk operations support."""

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

    def execute_query(self, query: str, variables: Optional[Dict] = None, operation_name: Optional[str] = None) -> Dict:
        """Execute GraphQL query using shopify.GraphQL().execute()."""
        try:
            if variables or operation_name:
                result = shopify.GraphQL().execute(
                    query=query,
                    variables=variables or {},
                    operation_name=operation_name
                )
            else:
                result = shopify.GraphQL().execute(query)
            return json.loads(result)
        except Exception as e:
            logger.error(f"GraphQL query execution failed: {e}")
            raise

    def get_products_variants_inventory_bulk(self, since_timestamp: Optional[str] = None) -> str:
        """Get all products, variants, and inventory in one bulk operation."""
        logger.info("Fetching products, variants, and inventory via bulk operation...")
        
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

        logger.info(f"Executing bulk query with filter: {time_filter if time_filter else 'all active variants'}")
        return self.bulk_handler.execute_bulk_query(query)

    def get_markets_and_countries(self) -> Dict:
        # NOTE: first:250 and regular query is fine since there are only 237 regions and one region can only be added to one market i.e. there will always be less than 250 regions
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

        logger.info("Executing markets query")
        return self.execute_query(query)

    def get_locations(self) -> str:
        """Get all locations via bulk operation."""
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
        """Get location-to-country relationships from delivery profiles."""
        # NOTE: locationGroup locations cannot exceed 200 due to Shopify plan limits
        # https://help.shopify.com/en/manual/fulfillment/setup/locations-management
        # Refer to Location quantity limit section
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
                                pageInfo {
                                    hasNextPage
                                    endCursor
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

            # Paginate only the required locationGroupZones using node(id)
            paginated_targets = []
            for profile in result["data"]["deliveryProfiles"]["edges"]:
                profile_id = profile["node"]["id"]
                for group_index, group in enumerate(profile["node"].get("profileLocationGroups", [])):
                    page_info = group.get("locationGroupZones", {}).get("pageInfo", {})
                    if page_info.get("hasNextPage"):
                        paginated_targets.append({
                            "profile_id": profile_id,
                            "group_index": group_index,
                            "cursor": page_info.get("endCursor")
                        })

            for target in paginated_targets:
                profile_id = target["profile_id"]
                group_index = target["group_index"]
                cursor = target["cursor"]

                while cursor:
                    paginated_query = f"""
                    {{
                        node(id: "{profile_id}") {{
                            ... on DeliveryProfile {{
                                profileLocationGroups {{
                                    locationGroupZones(first: 250, after: "{cursor}") {{
                                        pageInfo {{
                                            hasNextPage
                                            endCursor
                                        }}
                                        edges {{
                                            node {{
                                                zone {{
                                                    countries {{
                                                        code {{
                                                            countryCode
                                                        }}
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
                    next_result = self.execute_query(paginated_query)
                    group = next_result["data"]["node"]["profileLocationGroups"][group_index]
                    extra_edges = group["locationGroupZones"]["edges"]
                    cursor = group["locationGroupZones"]["pageInfo"]["endCursor"] if group["locationGroupZones"]["pageInfo"]["hasNextPage"] else None

                    for profile in result["data"]["deliveryProfiles"]["edges"]:
                        if profile["node"]["id"] == profile_id:
                            profile["node"]["profileLocationGroups"][group_index]["locationGroupZones"]["edges"].extend(extra_edges)
                            break

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
