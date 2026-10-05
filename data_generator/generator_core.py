"""
Moteur de génération de données de simulation WABA Group.

Ce script produit les référentiels (clients, comptes, agences, produits) puis les
transactions en garantissant une cohérence relationnelle stricte (zéro clé orpheline).
Gère les particularités monétaires (XOF zone UEMOA, GHS Ghana) et les distributions métiers.

Auteur : Armel Kouadio
"""

import csv
import io
import logging
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, Generator, List, Optional, Tuple

import numpy as np

from data_generator.schemas import (
    ACCOUNT_STATUS_PROBS,
    ACCOUNT_STATUSES,
    ACCOUNT_TYPES,
    BRANCH_TYPE_PROBS,
    BRANCH_TYPES,
    CHANNEL_PROBS,
    CHANNELS,
    CLAIM_STATUS_PROBS,
    CLAIM_STATUSES,
    COUNTRIES,
    CURRENCY_MAP,
    DEFAULT_PRODUCTS,
    ENTITY_COUNTRY_SCOPE,
    ENTITY_TYPES,
    INSURANCE_OPERATION_PROBS,
    INSURANCE_OPERATION_TYPES,
    KYC_LEVELS,
    KYC_PROBS,
    LOAN_TYPE_PROBS,
    LOAN_TYPES,
    MM_OPERATOR_PROBS,
    MM_OPERATORS,
    MM_STATUS_PROBS,
    MM_STATUSES,
    MOBILE_MONEY_TYPE_PROBS,
    MOBILE_MONEY_TYPES,
    PRODUCT_LINES,
    REGIONS,
    REPAYMENT_STATUS_PROBS,
    REPAYMENT_STATUSES,
    SEGMENT_PROBS,
    CUSTOMER_SEGMENTS,
    TXN_STATUS_PROBS,
    TXN_STATUSES,
    TXN_TYPE_PROBS,
    TXN_TYPES,
    UEMOA_COUNTRIES,
)

logger = logging.getLogger("waba.generator")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


@dataclass
class CustomerRef:
    customer_id: str
    country_code: str
    entity_type: str
    segment: str
    kyc_level: str
    onboarding_date: date
    region: str
    is_active: bool


@dataclass
class AccountRef:
    account_id: str
    customer_id: str
    country_code: str
    account_type: str
    currency: str
    balance: float
    credit_limit: float
    opened_date: date
    status: str


@dataclass
class BranchRef:
    branch_id: str
    country_code: str
    entity_type: str
    city: str
    region: str
    branch_type: str
    is_active: bool


class WabaDataStore:
    """
    In-memory reference registry guaranteeing 100% referential integrity
    across all synthetic datasets and simulation passes.
    """

    def __init__(self):
        self.customers_by_country: Dict[str, List[CustomerRef]] = {c: [] for c in COUNTRIES}
        self.accounts_by_country: Dict[str, List[AccountRef]] = {c: [] for c in COUNTRIES}
        self.loan_accounts_by_country: Dict[str, List[AccountRef]] = {c: [] for c in COUNTRIES}
        self.branches_by_country: Dict[str, List[BranchRef]] = {c: [] for c in COUNTRIES}
        self.products: List[dict] = list(DEFAULT_PRODUCTS)

    @property
    def total_customers(self) -> int:
        return sum(len(lst) for lst in self.customers_by_country.values())

    @property
    def total_accounts(self) -> int:
        return sum(len(lst) for lst in self.accounts_by_country.values())

    @property
    def total_branches(self) -> int:
        return sum(len(lst) for lst in self.branches_by_country.values())


class WabaDataGenerator:
    """
    Orchestrates the generation of realistic West African financial datasets.
    Implements Annexes A.1 -> A.7 with true-to-life local constraints.
    """

    def __init__(self, seed: Optional[int] = 42):
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
        self.store = WabaDataStore()
        logger.info("Initialized WABA Data Generator engine.")

    # -------------------------------------------------------------------------
    # Date & Number Utilities
    # -------------------------------------------------------------------------
    @staticmethod
    def _random_timestamp(start_dt: datetime, end_dt: datetime) -> datetime:
        delta = end_dt - start_dt
        int_delta = int(delta.total_seconds())
        if int_delta <= 0:
            return start_dt
        random_second = random.randint(0, int_delta)
        return start_dt + timedelta(seconds=random_second)

    @staticmethod
    def _get_quarter_date_range() -> Tuple[datetime, datetime]:
        """Returns the default simulation period (last completed quarter)."""
        now = datetime.now()
        # Default to a recent 90-day window
        end_dt = now.replace(hour=23, minute=59, second=59, microsecond=0)
        start_dt = end_dt - timedelta(days=90)
        return start_dt, end_dt

    # -------------------------------------------------------------------------
    # 1. Reference Data Generation (Customers, Accounts, Branches, Products)
    # -------------------------------------------------------------------------
    def generate_customers(self, n_rows: int = 500_000) -> List[CustomerRef]:
        """
        Generates client master data partitioned across 8 countries.
        Annex A.1 schema.
        """
        logger.info(f"Generating {n_rows:,} customer reference records...")
        country_choices = np.random.choice(COUNTRIES, size=n_rows)
        entity_choices = np.random.choice(ENTITY_TYPES, size=n_rows, p=[0.55, 0.20, 0.18, 0.07])
        segment_choices = np.random.choice(CUSTOMER_SEGMENTS, size=n_rows, p=SEGMENT_PROBS)
        kyc_choices = np.random.choice(KYC_LEVELS, size=n_rows, p=KYC_PROBS)

        base_start = datetime(2015, 1, 1)
        base_end = datetime(2025, 12, 31)

        customers: List[CustomerRef] = []
        for i in range(n_rows):
            cc = country_choices[i]
            # Ensure entity is valid for country
            valid_entities = [e for e in ENTITY_TYPES if cc in ENTITY_COUNTRY_SCOPE[e]]
            ent = entity_choices[i] if entity_choices[i] in valid_entities else random.choice(valid_entities)

            cid = f"WABA-{cc}-C-{i + 1:06d}"
            region = random.choice(REGIONS[cc])
            onboard_dt = self._random_timestamp(base_start, base_end).date()
            is_active = random.random() > 0.04  # ~96% active clients

            ref = CustomerRef(
                customer_id=cid,
                country_code=cc,
                entity_type=ent,
                segment=segment_choices[i],
                kyc_level=kyc_choices[i],
                onboarding_date=onboard_dt,
                region=region,
                is_active=is_active
            )
            customers.append(ref)
            self.store.customers_by_country[cc].append(ref)

        logger.info(f"Successfully generated and indexed {len(customers):,} customers.")
        return customers

    def generate_accounts(self, n_rows: int = 800_000) -> List[AccountRef]:
        """
        Generates account master data linked to existing customers.
        Annex A.2 schema. Guarantees no orphan customers.
        """
        if self.store.total_customers == 0:
            raise ValueError("Cannot generate accounts before customers! Call generate_customers() first.")

        logger.info(f"Generating {n_rows:,} account reference records...")
        accounts: List[AccountRef] = []

        type_choices = np.random.choice(ACCOUNT_TYPES, size=n_rows, p=[0.40, 0.25, 0.15, 0.12, 0.08])
        status_choices = np.random.choice(ACCOUNT_STATUSES, size=n_rows, p=ACCOUNT_STATUS_PROBS)

        base_start = datetime(2015, 1, 1)
        base_end = datetime(2025, 12, 31)

        for i in range(n_rows):
            cc = random.choice(COUNTRIES)
            cust_pool = self.store.customers_by_country[cc]
            if not cust_pool:
                # Fallback to any country with customers
                available = [c for c in COUNTRIES if self.store.customers_by_country[c]]
                cc = random.choice(available)
                cust_pool = self.store.customers_by_country[cc]

            cust = random.choice(cust_pool)
            aid = f"WABA-{cc}-A-{i + 1:07d}"
            curr = CURRENCY_MAP[cc]
            acc_type = type_choices[i]

            # Realistic balance distribution in local currency
            if curr == 'XOF':
                # Typical WAEMU bank balance 50,000 to 10,000,000 XOF
                balance = float(round(np.random.exponential(scale=350_000) + 15_000, 2))
                credit_limit = float(round(random.choice([0, 0, 500_000, 1_500_000, 5_000_000]), 2)) if acc_type in ['CURRENT', 'LOAN'] else 0.0
            else:
                # Ghana GHS balance ~ 500 to 100,000 GHS
                balance = float(round(np.random.exponential(scale=12_000) + 250, 2))
                credit_limit = float(round(random.choice([0, 0, 15_000, 50_000]), 2)) if acc_type in ['CURRENT', 'LOAN'] else 0.0

            opened_date = max(cust.onboarding_date, self._random_timestamp(base_start, base_end).date())

            ref = AccountRef(
                account_id=aid,
                customer_id=cust.customer_id,
                country_code=cc,
                account_type=acc_type,
                currency=curr,
                balance=balance,
                credit_limit=credit_limit,
                opened_date=opened_date,
                status=status_choices[i]
            )
            accounts.append(ref)
            self.store.accounts_by_country[cc].append(ref)
            if acc_type == 'LOAN':
                self.store.loan_accounts_by_country[cc].append(ref)

        logger.info(f"Successfully generated and indexed {len(accounts):,} accounts.")
        return accounts

    def generate_branches(self, n_rows: int = 200) -> List[BranchRef]:
        """
        Generates branch / agency network master data.
        Annex A.3 schema.
        """
        logger.info(f"Generating {n_rows} agency/branch records...")
        branches: List[BranchRef] = []
        b_counter: Dict[str, int] = {c: 0 for c in COUNTRIES}

        per_country = max(1, n_rows // len(COUNTRIES))
        for cc in COUNTRIES:
            cities = REGIONS[cc]
            count = per_country if cc != COUNTRIES[-1] else (n_rows - len(branches))
            for _ in range(count):
                b_counter[cc] += 1
                bid = f"WABA-{cc}-B-{b_counter[cc]:03d}"
                city = random.choice(cities)
                region = city  # In our config, regions align with regional hubs
                b_type = np.random.choice(BRANCH_TYPES, p=BRANCH_TYPE_PROBS)
                is_active = random.random() > 0.03

                ref = BranchRef(
                    branch_id=bid,
                    country_code=cc,
                    entity_type='BANK' if cc in ['CI', 'SN', 'GH'] else random.choice(['BANK', 'MICROFINANCE']),
                    city=city,
                    region=region,
                    branch_type=b_type,
                    is_active=is_active
                )
                branches.append(ref)
                self.store.branches_by_country[cc].append(ref)

        logger.info(f"Successfully generated {len(branches)} branches across 8 countries.")
        return branches

    def ensure_reference_data(self, scale_factor: float = 1.0):
        """
        Convenience bootstrap that creates reference data if not yet present.
        scale_factor allows fast local tests (e.g. 0.01 for 5k customers).
        """
        if self.store.total_customers == 0:
            n_cust = max(500, int(500_000 * scale_factor))
            self.generate_customers(n_cust)
        if self.store.total_accounts == 0:
            n_acc = max(1000, int(800_000 * scale_factor))
            self.generate_accounts(n_acc)
        if self.store.total_branches == 0:
            n_br = max(40, int(200 * scale_factor))
            self.generate_branches(n_br)

    # -------------------------------------------------------------------------
    # 2. Bank Transactions Generation (Annex A.4)
    # -------------------------------------------------------------------------
    def generate_bank_transactions(
        self,
        country_code: str,
        n_rows: int = 10_000,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[dict]:
        """
        Generates bank transactions for a specific country.
        Guarantees:
          - account_id, beneficiary_account, branch_id are valid
          - currency is strictly XOF or GHS
          - transaction_status distribution: ~92% SUCCESS, ~5% FAILED, ~3% REVERSED
        """
        self.ensure_reference_data(scale_factor=0.01 if self.store.total_customers == 0 else 1.0)
        curr = CURRENCY_MAP[country_code]
        acc_pool = self.store.accounts_by_country[country_code]
        branch_pool = self.store.branches_by_country[country_code]

        if not acc_pool or not branch_pool:
            raise ValueError(f"No accounts or branches registered for country {country_code}")

        if start_date is None or end_date is None:
            start_date, end_date = self._get_quarter_date_range()

        logger.info(f"Generating {n_rows:,} bank transactions for [{country_code}] ({curr})...")

        status_choices = np.random.choice(TXN_STATUSES, size=n_rows, p=TXN_STATUS_PROBS)
        txn_type_choices = np.random.choice(TXN_TYPES, size=n_rows, p=TXN_TYPE_PROBS)
        channel_choices = np.random.choice(CHANNELS, size=n_rows, p=CHANNEL_PROBS)

        # Lognormal distribution calibrated to West African banking
        # median ~ 40,000 XOF (~60 EUR) / 600 GHS
        mu = 10.6 if curr == 'XOF' else 6.4
        sigma = 1.2
        raw_amounts = np.random.lognormal(mean=mu, sigma=sigma, size=n_rows)

        rows = []
        for i in range(n_rows):
            status = status_choices[i]
            t_type = txn_type_choices[i]
            acc = random.choice(acc_pool)
            # Beneficiary can be another account or same for cash deposit/withdrawal
            beneficiary = random.choice(acc_pool) if t_type in ['TRANSFER', 'INTERNATIONAL_WIRE', 'PAYMENT'] else acc
            branch = random.choice(branch_pool)

            amount = float(max(500.0 if curr == 'XOF' else 10.0, round(raw_amounts[i], 2)))

            # Fee logic: ~0.1% to 1% if SUCCESS, 0 if FAILED
            if status == 'SUCCESS':
                fee = round(amount * random.uniform(0.001, 0.008), 2)
            else:
                fee = 0.0

            ts = self._random_timestamp(start_date, end_date)

            rows.append({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": ts.isoformat(),
                "account_id": acc.account_id,
                "beneficiary_account": beneficiary.account_id,
                "branch_id": branch.branch_id,
                "country_code": country_code,
                "transaction_type": t_type,
                "amount": amount,
                "currency": curr,
                "channel": channel_choices[i],
                "transaction_status": status,
                "fee_amount": fee,
                "entity_type": "BANK"
            })

        return rows

    # -------------------------------------------------------------------------
    # 3. Insurance Operations Generation (Annex A.5)
    # -------------------------------------------------------------------------
    def generate_insurance_operations(
        self,
        country_code: str,
        n_rows: int = 5_000,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[dict]:
        """
        Generates insurance policies, premium payments, and claims.
        Maintains realistic loss ratio (50% - 85%) for CIMA compliance.
        """
        self.ensure_reference_data(scale_factor=0.01 if self.store.total_customers == 0 else 1.0)
        curr = CURRENCY_MAP[country_code]
        cust_pool = self.store.customers_by_country[country_code]
        acc_pool = self.store.accounts_by_country[country_code]

        if start_date is None or end_date is None:
            start_date, end_date = self._get_quarter_date_range()

        logger.info(f"Generating {n_rows:,} insurance operations for [{country_code}]...")

        op_choices = np.random.choice(INSURANCE_OPERATION_TYPES, size=n_rows, p=INSURANCE_OPERATION_PROBS)
        line_choices = np.random.choice(PRODUCT_LINES, size=n_rows)
        claim_status_choices = np.random.choice(CLAIM_STATUSES, size=n_rows, p=CLAIM_STATUS_PROBS)

        rows = []
        for i in range(n_rows):
            op_type = op_choices[i]
            cust = random.choice(cust_pool)
            acc = random.choice(acc_pool)
            line = line_choices[i]

            is_claim = op_type in ['CLAIM_SUBMISSION', 'CLAIM_PAYMENT']

            if is_claim:
                claim_status = claim_status_choices[i] if op_type == 'CLAIM_SUBMISSION' else 'PAID'
                # Processing days in business days
                processing_days = int(random.randint(3, 45))
                # Claim amount typically higher than premium
                base_claim = np.random.lognormal(mean=12.5 if curr == 'XOF' else 8.5, sigma=0.9)
                amount = float(round(base_claim, 2))
            else:
                claim_status = ""  # NULL representation in CSV
                processing_days = ""
                # Premium payment
                base_prem = np.random.lognormal(mean=10.5 if curr == 'XOF' else 6.5, sigma=0.8)
                amount = float(round(base_prem, 2))

            ts = self._random_timestamp(start_date, end_date)

            rows.append({
                "operation_id": str(uuid.uuid4()),
                "timestamp": ts.isoformat(),
                "customer_id": cust.customer_id,
                "account_id": acc.account_id,
                "country_code": country_code,
                "operation_type": op_type,
                "product_line": line,
                "amount": amount,
                "currency": curr,
                "claim_status": claim_status,
                "processing_days": processing_days,
                "entity_type": "INSURANCE"
            })

        return rows

    # -------------------------------------------------------------------------
    # 4. Mobile Money Payments Generation (Annex A.6)
    # -------------------------------------------------------------------------
    def generate_mobile_money_payments(
        self,
        country_code: str,
        n_rows: int = 20_000,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[dict]:
        """
        Generates mobile money transactions (P2P, merchant, cross-border, etc.).
        Annex A.6 schema: SUCCESS ~94%, FAILED ~4%, PENDING ~2%.
        """
        self.ensure_reference_data(scale_factor=0.01 if self.store.total_customers == 0 else 1.0)
        curr = CURRENCY_MAP[country_code]
        sender_pool = self.store.customers_by_country[country_code]

        if start_date is None or end_date is None:
            start_date, end_date = self._get_quarter_date_range()

        logger.info(f"Generating {n_rows:,} mobile money payments for [{country_code}]...")

        type_choices = np.random.choice(MOBILE_MONEY_TYPES, size=n_rows, p=MOBILE_MONEY_TYPE_PROBS)
        status_choices = np.random.choice(MM_STATUSES, size=n_rows, p=MM_STATUS_PROBS)
        operator_choices = np.random.choice(MM_OPERATORS, size=n_rows, p=MM_OPERATOR_PROBS)

        # Realistic mobile money transaction values (500 XOF - 250,000 XOF / 10 - 5,000 GHS)
        scale = 15_000 if curr == 'XOF' else 350
        raw_amounts = np.random.exponential(scale=scale, size=n_rows)

        rows = []
        for i in range(n_rows):
            p_type = type_choices[i]
            sender = random.choice(sender_pool)
            status = status_choices[i]
            operator = operator_choices[i]

            if p_type == 'CROSS_BORDER_TRANSFER':
                # Target corridor within UEMOA/Ghana
                possible_receivers = [c for c in COUNTRIES if c != country_code]
                receiver_country = random.choice(possible_receivers)
                rec_pool = self.store.customers_by_country[receiver_country]
                receiver = random.choice(rec_pool) if rec_pool else sender
            else:
                receiver_country = country_code
                receiver = random.choice(sender_pool)

            amount = float(max(200.0 if curr == 'XOF' else 5.0, round(raw_amounts[i] + 500, 2)))
            # Standard Telco fee ~ 1% capped
            fee = round(min(amount * 0.015, 2500.0 if curr == 'XOF' else 50.0), 2)

            ts = self._random_timestamp(start_date, end_date)

            rows.append({
                "payment_id": str(uuid.uuid4()),
                "timestamp": ts.isoformat(),
                "sender_id": sender.customer_id,
                "receiver_id": receiver.customer_id,
                "sender_country": country_code,
                "receiver_country": receiver_country,
                "amount": amount,
                "currency": curr,
                "payment_type": p_type,
                "operator": operator,
                "status": status,
                "fee_amount": fee,
                "entity_type": "MOBILE_MONEY"
            })

        return rows

    # -------------------------------------------------------------------------
    # 5. Loan Repayments Generation (Annex A.7)
    # -------------------------------------------------------------------------
    def generate_loan_repayments(
        self,
        country_code: str,
        n_rows: int = 5_000,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[dict]:
        """
        Generates loan repayment schedules and status tracking.
        Annex A.7: ON_TIME ~82%, LATE ~12%, DEFAULT ~6% (calibrated to BCEAO NPL benchmark 3%-8%).
        """
        self.ensure_reference_data(scale_factor=0.01 if self.store.total_customers == 0 else 1.0)
        curr = CURRENCY_MAP[country_code]
        loan_pool = self.store.loan_accounts_by_country[country_code]
        cust_pool = self.store.customers_by_country[country_code]

        if not loan_pool:
            # Fallback to any account if loan pool empty
            loan_pool = self.store.accounts_by_country[country_code]

        if start_date is None or end_date is None:
            start_date, end_date = self._get_quarter_date_range()

        logger.info(f"Generating {n_rows:,} loan repayments for [{country_code}]...")

        status_choices = np.random.choice(REPAYMENT_STATUSES, size=n_rows, p=REPAYMENT_STATUS_PROBS)
        loan_type_choices = np.random.choice(LOAN_TYPES, size=n_rows, p=LOAN_TYPE_PROBS)

        rows = []
        for i in range(n_rows):
            status = status_choices[i]
            loan_type = loan_type_choices[i]
            acc = random.choice(loan_pool)
            cust = next((c for c in cust_pool if c.customer_id == acc.customer_id), random.choice(cust_pool))

            base_due = np.random.lognormal(mean=11.2 if curr == 'XOF' else 7.1, sigma=0.8)
            amount_due = float(round(base_due, 2))

            due_dt = self._random_timestamp(start_date, end_date).date()

            if status == 'ON_TIME':
                amount_paid = amount_due
                payment_dt = due_dt
                days_overdue = 0
            elif status == 'LATE':
                amount_paid = round(amount_due * random.uniform(0.3, 0.95), 2)
                days_overdue = random.randint(15, 60)
                payment_dt = due_dt + timedelta(days=days_overdue)
            else:  # DEFAULT (NPL)
                amount_paid = 0.0
                days_overdue = random.randint(91, 240)  # > 90 days default regulatory trigger
                payment_dt = ""  # NULL in CSV

            ts = datetime.combine(due_dt, datetime.min.time()) + timedelta(hours=random.randint(8, 17))

            rows.append({
                "repayment_id": str(uuid.uuid4()),
                "timestamp": ts.isoformat(),
                "loan_account_id": acc.account_id,
                "customer_id": cust.customer_id,
                "country_code": country_code,
                "amount_due": amount_due,
                "amount_paid": amount_paid,
                "currency": curr,
                "due_date": due_dt.isoformat(),
                "payment_date": payment_dt.isoformat() if isinstance(payment_dt, date) else "",
                "days_overdue": days_overdue,
                "loan_type": loan_type,
                "repayment_status": status,
                "entity_type": "BANK" if country_code in ['CI', 'SN', 'GH'] else "MICROFINANCE"
            })

        return rows

    # -------------------------------------------------------------------------
    # CSV Formatting Helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def to_csv_string(rows: List[dict], fieldnames: List[str]) -> str:
        """Converts in-memory row dictionaries to RFC-4180 CSV string."""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        return buf.getvalue()

    def get_reference_csvs(self) -> Dict[str, str]:
        """
        Serializes reference tables to CSV format:
          - customers.csv
          - accounts.csv
          - branches.csv
          - products.csv
        """
        self.ensure_reference_data(scale_factor=0.02 if self.store.total_customers == 0 else 1.0)

        # Customers
        cust_fields = ["customer_id", "country_code", "entity_type", "segment", "kyc_level", "onboarding_date", "region", "is_active"]
        all_custs = [vars(c) for lst in self.store.customers_by_country.values() for c in lst]
        cust_csv = self.to_csv_string(all_custs, cust_fields)

        # Accounts
        acc_fields = ["account_id", "customer_id", "country_code", "account_type", "currency", "balance", "credit_limit", "opened_date", "status"]
        all_accs = [vars(a) for lst in self.store.accounts_by_country.values() for a in lst]
        acc_csv = self.to_csv_string(all_accs, acc_fields)

        # Branches
        branch_fields = ["branch_id", "country_code", "entity_type", "city", "region", "branch_type", "is_active"]
        all_branches = [vars(b) for lst in self.store.branches_by_country.values() for b in lst]
        branch_csv = self.to_csv_string(all_branches, branch_fields)

        # Products
        prod_fields = ["product_id", "name", "entity_type", "category"]
        prod_csv = self.to_csv_string(self.store.products, prod_fields)

        return {
            "customers.csv": cust_csv,
            "accounts.csv": acc_csv,
            "branches.csv": branch_csv,
            "products.csv": prod_csv
        }
