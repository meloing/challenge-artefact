"""
Data Validation, Data Quality Checks, and PII Masking Utilities.

Implements:
  - Strict PII anonymization / pseudonymization (account numbers, client ids)
  - Business rules validation (country codes, currency conformity)
  - Malformed row detection for Bronze ingestion rejection

Author: Armel Kouadio
"""

import hashlib
import re
from typing import Any, Dict, Optional, Tuple

VALID_COUNTRIES = {"CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"}
VALID_ENTITIES = {"BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"}


def mask_account_id(account_id: Optional[str]) -> str:
    """
    Masks internal bank account identifier for non-privileged viewers.
    Example: 'WABA-CI-A-0001234' -> 'WABA-CI-A-***1234'
    """
    if not account_id or not isinstance(account_id, str):
        return "UNKNOWN"
    if len(account_id) <= 6:
        return "***"
    prefix = account_id[:10]  # keeps 'WABA-CI-A-'
    suffix = account_id[-4:]  # keeps last 4 digits
    return f"{prefix}***{suffix}"


def mask_customer_id(customer_id: Optional[str]) -> str:
    """
    Masks customer ID while preserving country routing information.
    Example: 'WABA-SN-C-000567' -> 'WABA-SN-C-***567'
    """
    if not customer_id or not isinstance(customer_id, str):
        return "UNKNOWN"
    if len(customer_id) <= 6:
        return "***"
    prefix = customer_id[:10]
    suffix = customer_id[-3:]
    return f"{prefix}***{suffix}"


def pseudonymize_identifier(identifier: str, salt: str = "WABA_SECURITY_SALT_2026") -> str:
    """
    One-way cryptographic pseudonymization (SHA-256) for audit trails.
    """
    if not identifier:
        return ""
    salted = f"{salt}_{identifier}".encode("utf-8")
    return hashlib.sha256(salted).hexdigest()[:16]


def validate_bank_transaction_record(row: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validates a raw banking transaction row. Returns (is_valid, error_reason).
    """
    required = ["transaction_id", "timestamp", "account_id", "country_code", "amount", "currency", "transaction_status"]
    for f in required:
        if f not in row or row[f] is None or str(row[f]).strip() == "":
            return False, f"Missing required field: {f}"

    if row["country_code"] not in VALID_COUNTRIES:
        return False, f"Invalid country_code: {row['country_code']}"

    try:
        amt = float(row["amount"])
        if amt <= 0:
            return False, f"Amount must be strictly positive: {amt}"
    except (ValueError, TypeError):
        return False, f"Non-numeric amount: {row['amount']}"

    # Currency zone check
    expected_curr = "GHS" if row["country_code"] == "GH" else "XOF"
    if str(row["currency"]).upper() != expected_curr:
        return False, f"Invalid currency {row['currency']} for country {row['country_code']}, expected {expected_curr}"

    return True, None
