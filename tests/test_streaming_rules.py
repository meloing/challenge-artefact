"""
Unit Tests: Real-Time Fraud & AML Streaming Detection Rules and PII Masking.

Verifies:
  - BCEAO AML threshold trigger (1,000,000 XOF)
  - Ghana FIC AML threshold trigger (5,000 GHS)
  - Masking of sensitive PII (account numbers, customer IDs)
  - Raw transaction validation

Author: Armel Kouadio
"""

import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lakehouse.config import BCEAO_AML_THRESHOLD_XOF, GHANA_AML_THRESHOLD_GHS
from lakehouse.utils.validation import (
    mask_account_id,
    mask_customer_id,
    pseudonymize_identifier,
    validate_bank_transaction_record
)


class TestStreamingRulesAndSecurity(unittest.TestCase):

    def test_pii_account_masking(self):
        """Verifies account number masking preserves prefix/suffix while obscuring middle."""
        raw_account = "WABA-CI-A-0012345"
        masked = mask_account_id(raw_account)
        self.assertEqual(masked, "WABA-CI-A-***2345")

    def test_pii_customer_masking(self):
        """Verifies customer identifier masking."""
        raw_cust = "WABA-SN-C-000987"
        masked = mask_customer_id(raw_cust)
        self.assertEqual(masked, "WABA-SN-C-***987")

    def test_pseudonymization_determinism(self):
        """Verifies cryptographic pseudonymization produces stable 16-char hashes."""
        h1 = pseudonymize_identifier("WABA-CI-A-0001")
        h2 = pseudonymize_identifier("WABA-CI-A-0001")
        h3 = pseudonymize_identifier("WABA-CI-A-0002")

        self.assertEqual(h1, h2)
        self.assertNotEqual(h1, h3)
        self.assertEqual(len(h1), 16)

    def test_bceao_aml_threshold_trigger(self):
        """Verifies AML alert triggers for transfers >= 1,000,000 XOF in WAEMU."""
        sub_threshold_amount = 750_000.0
        over_threshold_amount = 1_500_000.0

        is_sub_alert = sub_threshold_amount >= BCEAO_AML_THRESHOLD_XOF
        is_over_alert = over_threshold_amount >= BCEAO_AML_THRESHOLD_XOF

        self.assertFalse(is_sub_alert)
        self.assertTrue(is_over_alert)

    def test_ghana_aml_threshold_trigger(self):
        """Verifies AML alert triggers for transfers >= 5,000 GHS in Ghana."""
        ghs_amount = 6_500.0
        is_alert = ghs_amount >= GHANA_AML_THRESHOLD_GHS
        self.assertTrue(is_alert)

    def test_bank_transaction_validation_success(self):
        """Verifies valid transaction passes data quality filters."""
        valid_row = {
            "transaction_id": "tx-1234",
            "timestamp": "2026-10-05T12:00:00",
            "account_id": "WABA-CI-A-0001234",
            "country_code": "CI",
            "amount": 25000.0,
            "currency": "XOF",
            "transaction_status": "SUCCESS"
        }
        is_valid, reason = validate_bank_transaction_record(valid_row)
        self.assertTrue(is_valid)
        self.assertIsNone(reason)

    def test_bank_transaction_validation_currency_mismatch(self):
        """Verifies transaction is rejected if country currency does not match."""
        invalid_row = {
            "transaction_id": "tx-9999",
            "timestamp": "2026-10-05T12:00:00",
            "account_id": "WABA-CI-A-0001234",
            "country_code": "CI",
            "amount": 25000.0,
            "currency": "GHS",  # Invalid for Côte d'Ivoire
            "transaction_status": "SUCCESS"
        }
        is_valid, reason = validate_bank_transaction_record(invalid_row)
        self.assertFalse(is_valid)
        self.assertIn("Invalid currency", reason)


if __name__ == "__main__":
    unittest.main()
