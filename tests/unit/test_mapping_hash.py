"""Tests for mapping hash / structural change detection (CountryLocationMapper._generate_mapping_hash)."""
from core.mapping.country_location_mapper import CountryLocationMapper


def _hash(mapper: CountryLocationMapper, countries, loc_map):
    return mapper._generate_mapping_hash(countries, loc_map)


def test_identical_mapping_different_key_order_same_hash():
    mapper = CountryLocationMapper(shopify_sync=None)
    ac1 = {"US": {"name": "U"}, "CA": {"name": "C"}}
    ac2 = {"CA": {"name": "C"}, "US": {"name": "U"}}
    lcm = {
        "1": {"name": "L1", "countries": ["US"]},
        "2": {"name": "L2", "countries": ["CA", "US"]},
    }
    h1 = _hash(mapper, ac1, lcm)
    h2 = _hash(mapper, ac2, lcm)
    assert h1 == h2


def test_location_country_list_order_normalized():
    mapper = CountryLocationMapper(shopify_sync=None)
    ac = {"US": {"n": "x"}}
    a = {"10": {"name": "x", "countries": ["US", "CA"]}}
    b = {"10": {"name": "x", "countries": ["CA", "US"]}}
    assert _hash(mapper, {**ac, "CA": {"n": "y"}}, a) == _hash(
        mapper, {**ac, "CA": {"n": "y"}}, b
    )


def test_changing_location_country_relationship_changes_hash():
    mapper = CountryLocationMapper(shopify_sync=None)
    ac = {"US": {"n": "1"}}
    before = {"5": {"name": "w", "countries": ["US"]}}
    after = {"5": {"name": "w", "countries": ["CA"]}}
    assert _hash(mapper, {**ac, "CA": {"n": "2"}}, before) != _hash(
        mapper, {**ac, "CA": {"n": "2"}}, after
    )


def test_adding_location_changes_hash():
    mapper = CountryLocationMapper(shopify_sync=None)
    ac = {"US": {"n": "1"}}
    one = {"1": {"name": "a", "countries": ["US"]}}
    two = {
        "1": {"name": "a", "countries": ["US"]},
        "2": {"name": "b", "countries": ["US"]},
    }
    assert _hash(mapper, ac, one) != _hash(mapper, ac, two)


def test_removing_location_changes_hash():
    mapper = CountryLocationMapper(shopify_sync=None)
    ac = {"US": {"n": "1"}}
    one = {"1": {"name": "a", "countries": ["US"]}}
    two = {
        "1": {"name": "a", "countries": ["US"]},
        "2": {"name": "b", "countries": ["US"]},
    }
    assert _hash(mapper, ac, two) != _hash(mapper, ac, one)


def test_adding_active_country_changes_hash():
    mapper = CountryLocationMapper(shopify_sync=None)
    ac1 = {"US": {"n": "1"}}
    ac2 = {"US": {"n": "1"}, "CA": {"n": "2"}}
    lcm = {"1": {"name": "a", "countries": ["US"]}}
    assert _hash(mapper, ac1, lcm) != _hash(mapper, ac2, lcm)


def test_hash_depends_on_nested_structure_not_only_top_keys():
    mapper = CountryLocationMapper(shopify_sync=None)
    ac = {"US": {"n": "1"}, "CA": {"n": "2"}}
    lcm_a = {"1": {"name": "a", "countries": ["US"]}}
    lcm_b = {"1": {"name": "a", "countries": ["US", "CA"]}}
    assert _hash(mapper, ac, lcm_a) != _hash(mapper, ac, lcm_b)
