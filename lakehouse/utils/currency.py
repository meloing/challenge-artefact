"""
Currency and Exchange Rate Utilities for West African Operations.

Covers:
  - Fixed peg conversion for UEMOA XOF (1 EUR = 655.957 XOF)
  - Floating conversion for Ghanaian Cedi (GHS)
  - Currency validation by country ISO code

Author: Armel Kouadio
"""

from typing import Optional
from lakehouse.config import FX_RATES

# Currency validity map per country
VALID_CURRENCIES = {
    "CI": "XOF",
    "SN": "XOF",
    "ML": "XOF",
    "BF": "XOF",
    "GN": "XOF",
    "TG": "XOF",
    "BJ": "XOF",
    "GH": "GHS"
}


def convert_to_eur(amount: Optional[float], currency: str) -> Optional[float]:
    """
    Converts a local currency monetary amount to Euro (EUR)
    for unified group-level financial consolidation.
    """
    if amount is None:
        return None
    rate = FX_RATES.get(currency.upper())
    if rate is None:
        raise ValueError(f"Unsupported currency code: {currency}")
    return round(float(amount) * rate, 2)


def is_valid_currency_for_country(country_code: str, currency: str) -> bool:
    """
    Validates whether the transaction currency strictly matches
    the official monetary zone of the country (Annex A.4 - A.7).
    """
    expected = VALID_CURRENCIES.get(country_code.upper())
    return expected is not None and expected == currency.upper()
