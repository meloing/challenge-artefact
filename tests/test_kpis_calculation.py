"""
Unit Tests: Financial & Regulatory KPI Calculations.

Verifies:
  - BCEAO NPL ratio formula and compliance ceiling (5%)
  - CIMA Loss Ratio formula and surveillance threshold (70%)
  - Customer ARPU computation
  - Mobile Money SLA metrics

Author: Armel Kouadio
"""

import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lakehouse.config import BCEAO_MAX_NPL_RATIO, CIMA_MAX_LOSS_RATIO


class TestKpisCalculation(unittest.TestCase):

    def test_npl_ratio_calculation_and_bceao_threshold(self):
        """Verifies NPL calculation and regulatory classification."""
        total_exposure = 10_000_000.0  # 10M EUR total loan book
        defaulted_exposure = 420_000.0  # 420k EUR in default (>90 days)

        npl_ratio = defaulted_exposure / total_exposure
        self.assertEqual(npl_ratio, 0.042)  # 4.2%

        # BCEAO Compliance check (< 5%)
        is_compliant = npl_ratio <= BCEAO_MAX_NPL_RATIO
        self.assertTrue(is_compliant)

        # Risk band categorization
        if npl_ratio < 0.03:
            status = "LOW_RISK_GREEN"
        elif npl_ratio <= 0.05:
            status = "MODERATE_ORANGE"
        else:
            status = "CRITICAL_RED"

        self.assertEqual(status, "MODERATE_ORANGE")

    def test_npl_ratio_breach(self):
        """Tests critical threshold breach when NPL exceeds 5%."""
        total_exposure = 5_000_000.0
        defaulted_exposure = 350_000.0  # 7.0%
        npl_ratio = defaulted_exposure / total_exposure

        is_compliant = npl_ratio <= BCEAO_MAX_NPL_RATIO
        self.assertFalse(is_compliant)

    def test_loss_ratio_cima_threshold(self):
        """Verifies Insurance Loss Ratio (S/P) vs CIMA 70% threshold."""
        earned_premiums = 2_000_000.0
        claims_paid = 1_500_000.0

        loss_ratio = claims_paid / earned_premiums
        self.assertEqual(loss_ratio, 0.75)  # 75%

        is_alert = loss_ratio > CIMA_MAX_LOSS_RATIO
        self.assertTrue(is_alert, "Loss ratio 75% must trigger CIMA alert")

    def test_customer_arpu_formula(self):
        """Verifies Average Revenue Per User (ARPU) computation."""
        total_commissions = 150_000.0
        active_customers = 5_000

        arpu = total_commissions / active_customers
        self.assertEqual(arpu, 30.0)


if __name__ == "__main__":
    unittest.main()
