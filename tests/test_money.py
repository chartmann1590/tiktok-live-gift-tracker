"""Unit tests for diamond resolution and diamond → USD conversion."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from money import (  # noqa: E402
    diamonds_to_usd,
    index_gift_catalog,
    normalize_gift_name,
    resolve_diamonds_per_unit,
)


class TestDiamondsToUsd(unittest.TestCase):
    def test_zero(self):
        self.assertEqual(diamonds_to_usd(0), 0.0)

    def test_single_diamond(self):
        self.assertEqual(diamonds_to_usd(1), 0.01)

    def test_rounds_half_up(self):
        # 3 diamonds × 0.005 = 0.015 → 0.02 (half up)
        self.assertEqual(diamonds_to_usd(3), 0.02)
        self.assertEqual(diamonds_to_usd(2), 0.01)

    def test_large_count_stable(self):
        self.assertEqual(diamonds_to_usd(10_000), 50.0)

    def test_negative_rejected(self):
        with self.assertRaises(ValueError):
            diamonds_to_usd(-1)


class TestGiftCatalog(unittest.TestCase):
    def test_index_empty(self):
        self.assertEqual(index_gift_catalog(None), {})
        self.assertEqual(index_gift_catalog({}), {})
        self.assertEqual(index_gift_catalog({"gifts": "bad"}), {})

    def test_index_parses_gifts_list(self):
        data = {
            "gifts": [
                {"id": 7, "diamond_count": 1},
                {"gift_id": 8, "diamondCount": 5},
                {"id": 9, "diamond_count": 0},
            ]
        }
        self.assertEqual(index_gift_catalog(data), {7: 1, 8: 5})

    def test_index_nested_data(self):
        data = {"data": {"gifts": [{"id": 1, "diamond_count": 99}]}}
        self.assertEqual(index_gift_catalog(data), {1: 99})


class TestResolveDiamondsPerUnit(unittest.TestCase):
    def test_prefers_catalog_over_payload(self):
        cat = {100: 10}
        self.assertEqual(
            resolve_diamonds_per_unit(100, "Anything", 1, cat),
            10,
        )

    def test_payload_when_no_catalog_match(self):
        self.assertEqual(
            resolve_diamonds_per_unit(999_999, "Unknown", 42, {}),
            42,
        )

    def test_json_name_fallback(self):
        # gift_diamond_rates.json includes "rose" → 1
        self.assertEqual(
            resolve_diamonds_per_unit(0, "Rose", 0, {}),
            1,
        )

    def test_zero_when_unresolved(self):
        self.assertEqual(
            resolve_diamonds_per_unit(0, "Totally Unknown Gift Xyz", 0, {}),
            0,
        )


class TestNormalizeGiftName(unittest.TestCase):
    def test_collapse_space(self):
        self.assertEqual(normalize_gift_name("  Finger   Heart "), "finger heart")


if __name__ == "__main__":
    unittest.main()
