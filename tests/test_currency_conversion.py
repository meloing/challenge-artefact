"""
Unit Tests: Currency Conversion and Monetary Zone Validation.

Verifies:
  - UEMOA XOF fixed peg to EUR (655.957 XOF = 1 EUR)
  - Ghana GHS conversion rate
  - Rejection of invalid currencies by country

Author: Armel Kouadio
"""

import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lakehouse.utils.currency import convert_to_eur, is_valid_currency_for_country


class TestCurrencyConversion(unittest.TestCase):

    def test_xof_to_eur_fixed_peg(self):
        """Verifies exact WAEMU peg conversion (655.957 XOF = 1.0 EUR)."""
        xof_amount = 655_957.0
        eur = convert_to_eur(xof_amount, "XOF")
        self.assertAlmostEqual(eur, 1000.0, places=1)

    def test_ghs_to_eur_conversion(self):
        """Verifies Ghanaian Cedi conversion."""
        ghs_amount = 10_000.0
        eur = convert_to_eur(ghs_amount, "GHS")
        self.assertEqual(eur, 650.0)

    def test_invalid_currency_rejection(self):
        """Verifies that an unsupported currency raises ValueError."""
        with self.assertRaises(ValueError):
            convert_to_eur(100.0, "XYZ")

    def test_country_currency_validity(self):
        """Verifies correct pairing of countries and legal tender."""
        self.assertTrue(is_valid_currency_for_country("CI", "XOF"))
        self.assertTrue(is_valid_currency_for_country("SN", "XOF"))
        self.assertTrue(is_valid_currency_for_country("GH", "GHS"))
        self.assertFalse(is_valid_currency_for_country("CI", "GHS"))
        self.assertFalse(is_valid_currency_for_country("GH", "XOF"))


if __name__ == "__main__":
    unittest.main()
