"""
Schemas and Constants for WABA Group Data Generation.
Conforms strictly to Technical Challenge Specifications (Annexes A.1 -> A.7).

Author: Armel Kouadio (WABA Data Lakehouse Project)
"""

from typing import Dict, List

# --- Countries & Currencies ---
# 8 West African countries
COUNTRIES: List[str] = ['CI', 'SN', 'ML', 'BF', 'GN', 'TG', 'BJ', 'GH']

# UEMOA Zone (WAEMU) - currency is XOF (Guinée included in XOF zone for challenge spec)
UEMOA_COUNTRIES: List[str] = ['CI', 'SN', 'ML', 'BF', 'GN', 'TG', 'BJ']

# Currency Mapping: XOF for UEMOA, GHS for Ghana
CURRENCY_MAP: Dict[str, str] = {c: 'XOF' for c in UEMOA_COUNTRIES}
CURRENCY_MAP['GH'] = 'GHS'

# Approximate exchange rates to EUR for financial consolidation
EUR_EXCHANGE_RATES: Dict[str, float] = {
    'XOF': 1.0 / 655.957,  # Pegged at 655.957 XOF = 1 EUR
    'GHS': 0.065,          # Realistic floating rate ~ 15 GHS = 1 EUR
    'EUR': 1.0
}

# --- Entities & Lines of Business ---
ENTITY_TYPES: List[str] = ['BANK', 'INSURANCE', 'MOBILE_MONEY', 'MICROFINANCE']

# Mapping of countries where entities operate according to challenge specs
ENTITY_COUNTRY_SCOPE: Dict[str, List[str]] = {
    'BANK': ['CI', 'SN', 'ML', 'BF', 'GN', 'TG', 'BJ', 'GH'],       # Retail (7 UEMOA) + Corporate (UEMOA + GH)
    'INSURANCE': ['CI', 'SN', 'ML', 'BF', 'GN', 'TG', 'BJ', 'GH'],  # Vie (7 UEMOA) + IARD (UEMOA + GH)
    'MOBILE_MONEY': ['CI', 'SN', 'BF', 'GH'],                        # WABA Mobile Money
    'MICROFINANCE': ['ML', 'GN', 'BF']                               # WABA Microfinance
}

# --- Customer Segments & KYC ---
CUSTOMER_SEGMENTS: List[str] = ['RETAIL', 'SME', 'CORPORATE', 'PREMIUM']
SEGMENT_PROBS: List[float] = [0.65, 0.20, 0.10, 0.05]

KYC_LEVELS: List[str] = ['BASIC', 'STANDARD', 'ENHANCED']
KYC_PROBS: List[float] = [0.30, 0.50, 0.20]

# --- Regions per Country (from challenge Annex A.8) ---
REGIONS: Dict[str, List[str]] = {
    'CI': ['Abidjan', 'Bouake', 'Yamoussoukro', 'San Pedro', 'Korhogo'],
    'SN': ['Dakar', 'Thies', 'Ziguinchor', 'Saint-Louis', 'Kaolack'],
    'ML': ['Bamako', 'Sikasso', 'Segou', 'Mopti', 'Tombouctou'],
    'BF': ['Ouagadougou', 'Bobo-Dioulasso', 'Koudougou', 'Banfora'],
    'GN': ['Conakry', 'Nzerekore', 'Kindia', 'Kankan'],
    'TG': ['Lome', 'Sokode', 'Kara', 'Atakpame'],
    'BJ': ['Cotonou', 'Porto-Novo', 'Parakou', 'Abomey-Calavi'],
    'GH': ['Accra', 'Kumasi', 'Tamale', 'Cape Coast', 'Sunyani']
}

# --- Account Types & Statuses ---
ACCOUNT_TYPES: List[str] = ['CURRENT', 'SAVINGS', 'LOAN', 'MOBILE_WALLET', 'INSURANCE_POLICY']
ACCOUNT_STATUSES: List[str] = ['ACTIVE', 'FROZEN', 'CLOSED', 'DORMANT']
ACCOUNT_STATUS_PROBS: List[float] = [0.85, 0.05, 0.03, 0.07]

# --- Branch Types ---
BRANCH_TYPES: List[str] = ['FULL_SERVICE', 'DIGITAL_ONLY', 'AGENCY_BANKING', 'ATM_POINT']
BRANCH_TYPE_PROBS: List[float] = [0.35, 0.15, 0.35, 0.15]

# --- Banking Transactions Specs (Annex A.4) ---
TXN_TYPES: List[str] = ['TRANSFER', 'PAYMENT', 'WITHDRAWAL', 'DEPOSIT', 'INTERNATIONAL_WIRE']
TXN_TYPE_PROBS: List[float] = [0.35, 0.30, 0.15, 0.15, 0.05]

CHANNELS: List[str] = ['BRANCH', 'ATM', 'MOBILE_APP', 'INTERNET_BANKING', 'USSD']
CHANNEL_PROBS: List[float] = [0.20, 0.15, 0.35, 0.20, 0.10]

# Annex A.4: Distribution: SUCCESS ~92%, FAILED ~5%, REVERSED ~3%
TXN_STATUSES: List[str] = ['SUCCESS', 'FAILED', 'REVERSED']
TXN_STATUS_PROBS: List[float] = [0.92, 0.05, 0.03]

# --- Insurance Operations Specs (Annex A.5) ---
INSURANCE_OPERATION_TYPES: List[str] = [
    'PREMIUM_PAYMENT',
    'CLAIM_SUBMISSION',
    'CLAIM_PAYMENT',
    'POLICY_RENEWAL',
    'POLICY_CANCELLATION'
]
INSURANCE_OPERATION_PROBS: List[float] = [0.55, 0.15, 0.12, 0.13, 0.05]

PRODUCT_LINES: List[str] = [
    'VIE',
    'IARD_AUTO',
    'IARD_HABITATION',
    'IARD_SANTE',
    'PREVOYANCE'
]

CLAIM_STATUSES: List[str] = ['PENDING', 'APPROVED', 'REJECTED', 'PAID']
CLAIM_STATUS_PROBS: List[float] = [0.25, 0.35, 0.15, 0.25]

# --- Mobile Money Specs (Annex A.6) ---
MOBILE_MONEY_TYPES: List[str] = [
    'P2P',
    'MERCHANT_PAYMENT',
    'BILL_PAYMENT',
    'AIRTIME',
    'CROSS_BORDER_TRANSFER'
]
MOBILE_MONEY_TYPE_PROBS: List[float] = [0.45, 0.20, 0.15, 0.10, 0.10]

MM_OPERATORS: List[str] = ['WABA_PAY', 'ORANGE_MONEY_PARTNER', 'MTN_PARTNER']
MM_OPERATOR_PROBS: List[float] = [0.50, 0.30, 0.20]

# Annex A.6: SUCCESS ~94%, FAILED ~4%, PENDING ~2%
MM_STATUSES: List[str] = ['SUCCESS', 'FAILED', 'PENDING']
MM_STATUS_PROBS: List[float] = [0.94, 0.04, 0.02]

# --- Loan Repayments Specs (Annex A.7) ---
LOAN_TYPES: List[str] = ['CONSUMER', 'MORTGAGE', 'SME', 'AGRICULTURAL', 'MICROCREDIT']
LOAN_TYPE_PROBS: List[float] = [0.35, 0.15, 0.25, 0.15, 0.10]

# Annex A.7: ON_TIME ~82%, LATE ~12%, DEFAULT ~6% (Satisfies BCEAO NPL realistic 3%-8%)
REPAYMENT_STATUSES: List[str] = ['ON_TIME', 'LATE', 'DEFAULT']
REPAYMENT_STATUS_PROBS: List[float] = [0.82, 0.12, 0.06]

# --- Product Catalog (Annex A.1 / Products) ---
DEFAULT_PRODUCTS = [
    {"product_id": "PRD-BNK-001", "name": "Compte Courant Particulier", "entity_type": "BANK", "category": "RETAIL"},
    {"product_id": "PRD-BNK-002", "name": "Livret Epargne WABA", "entity_type": "BANK", "category": "SAVINGS"},
    {"product_id": "PRD-BNK-003", "name": "Credit Consommation Express", "entity_type": "BANK", "category": "LOAN"},
    {"product_id": "PRD-BNK-004", "name": "Credit Immobilier Habitat", "entity_type": "BANK", "category": "LOAN"},
    {"product_id": "PRD-BNK-005", "name": "Compte Entreprise PME", "entity_type": "BANK", "category": "CORPORATE"},
    {"product_id": "PRD-BNK-006", "name": "Trade Finance & L/C", "entity_type": "BANK", "category": "CORPORATE"},
    {"product_id": "PRD-INS-001", "name": "WABA Assur Auto Tiers & Tous Risques", "entity_type": "INSURANCE", "category": "IARD_AUTO"},
    {"product_id": "PRD-INS-002", "name": "WABA Habitation Multirisque", "entity_type": "INSURANCE", "category": "IARD_HABITATION"},
    {"product_id": "PRD-INS-003", "name": "WABA Sante Famille & Entreprise", "entity_type": "INSURANCE", "category": "IARD_SANTE"},
    {"product_id": "PRD-INS-004", "name": "WABA Vie & Epargne Retraite", "entity_type": "INSURANCE", "category": "VIE"},
    {"product_id": "PRD-INS-005", "name": "WABA Prevoyance Deces/Accident", "entity_type": "INSURANCE", "category": "PREVOYANCE"},
    {"product_id": "PRD-MM-001",  "name": "Portefeuille WABA Pay", "entity_type": "MOBILE_MONEY", "category": "WALLET"},
    {"product_id": "PRD-MM-002",  "name": "Transfert Transfrontalier UEMOA", "entity_type": "MOBILE_MONEY", "category": "TRANSFER"},
    {"product_id": "PRD-MCF-001", "name": "Microcredit Agricole Campagne", "entity_type": "MICROFINANCE", "category": "MICROCREDIT"},
    {"product_id": "PRD-MCF-002", "name": "Tontine Digitale Communautaire", "entity_type": "MICROFINANCE", "category": "SAVINGS"}
]
