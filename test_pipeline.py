"""
Automated Unit and Integration Tests for Python Pipeline
"""
import unittest
from automate import standardize_date, clean_currency, normalize_category

class TestDataFlowPipeline(unittest.TestCase):
    def test_standardize_date(self):
        self.assertEqual(standardize_date("2026-11-01"), "2026-11-01")
        self.assertEqual(standardize_date("01/11/2026"), "2026-11-01")
        self.assertEqual(standardize_date("2026/11/02"), "2026-11-02")
        self.assertIsNone(standardize_date(""))

    def test_clean_currency(self):
        self.assertEqual(clean_currency("$1,250.50"), 1250.50)
        self.assertEqual(clean_currency("  $45.00 "), 45.00)
        self.assertEqual(clean_currency("99.99"), 99.99)

    def test_normalize_category(self):
        self.assertEqual(normalize_category("elec"), "Electronics")
        self.assertEqual(normalize_category("Clothing"), "Apparel")
        self.assertEqual(normalize_category("  Home & Kitchen  "), "Home & Kitchen")

if __name__ == "__main__":
    unittest.main()

