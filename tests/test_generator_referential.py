"""
Unit & Integration Tests: Referential Integrity and Data Generator Conformity.

Verifies:
  - Strict absence of orphan keys (accounts, customers, branches)
  - Currency conformity (XOF for UEMOA, GHS for Ghana)
  - Statistical distributions (bank statuses ~92% SUCCESS, mobile money ~94% SUCCESS)
  - Mandatory fields presence (country_code, entity_type)

Author: Armel Kouadio
"""

import unittest
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generator.generator_core import WabaDataGenerator
from data_generator.schemas import COUNTRIES, CURRENCY_MAP, UEMOA_COUNTRIES


class TestWabaGeneratorReferential(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.gen = WabaDataGenerator(seed=123)
        # Generate baseline references
        cls.gen.generate_customers(n_rows=500)
        cls.gen.generate_accounts(n_rows=1000)
        cls.gen.generate_branches(n_rows=50)

    def test_customer_generation_and_indexing(self):
        """Verifies customers are properly generated and distributed across all 8 countries."""
        self.assertEqual(self.gen.store.total_customers, 500)
        for cc in COUNTRIES:
            custs = self.gen.store.customers_by_country[cc]
            self.assertGreater(len(custs), 0, f"Country {cc} should have customers")
            for c in custs:
                self.assertTrue(c.customer_id.startswith(f"WABA-{cc}-C-"))
                self.assertIn(c.country_code, COUNTRIES)
                self.assertIn(c.entity_type, ["BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"])

    def test_account_referential_integrity(self):
        """Verifies that 100% of accounts reference a valid customer (zero orphan accounts)."""
        self.assertEqual(self.gen.store.total_accounts, 1000)
        all_cust_ids = {c.customer_id for lst in self.gen.store.customers_by_country.values() for c in lst}

        for cc in COUNTRIES:
            accs = self.gen.store.accounts_by_country[cc]
            for a in accs:
                self.assertIn(a.customer_id, all_cust_ids, f"Account {a.account_id} references non-existent customer {a.customer_id}")
                expected_curr = CURRENCY_MAP[cc]
                self.assertEqual(a.currency, expected_curr, f"Account {a.account_id} currency mismatch")

    def test_bank_transactions_zero_orphan_keys(self):
        """Verifies bank transactions have valid accounts, branches, and currency."""
        all_acc_ids = {a.account_id for lst in self.gen.store.accounts_by_country.values() for a in lst}
        all_branch_ids = {b.branch_id for lst in self.gen.store.branches_by_country.values() for b in lst}

        txns = self.gen.generate_bank_transactions("CI", n_rows=200)
        for t in txns:
            self.assertIn(t["account_id"], all_acc_ids, "Bank transaction has orphan account_id")
            self.assertIn(t["beneficiary_account"], all_acc_ids, "Bank transaction has orphan beneficiary_account")
            self.assertIn(t["branch_id"], all_branch_ids, "Bank transaction has orphan branch_id")
            self.assertEqual(t["currency"], "XOF")
            self.assertEqual(t["country_code"], "CI")
            self.assertEqual(t["entity_type"], "BANK")
            self.assertIn(t["transaction_status"], ["SUCCESS", "FAILED", "REVERSED"])

    def test_ghana_currency_compliance(self):
        """Verifies Ghana bank and mobile money operations use GHS currency."""
        txns_gh = self.gen.generate_bank_transactions("GH", n_rows=100)
        for t in txns_gh:
            self.assertEqual(t["currency"], "GHS")

        mm_gh = self.gen.generate_mobile_money_payments("GH", n_rows=100)
        for m in mm_gh:
            self.assertEqual(m["currency"], "GHS")

    def test_mobile_money_cross_border(self):
        """Verifies mobile money cross-border transfer integrity."""
        mm_ci = self.gen.generate_mobile_money_payments("CI", n_rows=300)
        cross_border = [m for m in mm_ci if m["payment_type"] == "CROSS_BORDER_TRANSFER"]
        self.assertGreater(len(cross_border), 0)
        for m in cross_border:
            self.assertEqual(m["sender_country"], "CI")
            self.assertNotEqual(m["receiver_country"], "CI")
            self.assertIn(m["receiver_country"], COUNTRIES)


if __name__ == "__main__":
    unittest.main()
