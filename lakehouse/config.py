"""
Centralized Configuration for PySpark, Apache Iceberg, and MinIO S3 Lakehouse.

Supports running both inside Docker/Kubernetes clusters with S3A/Iceberg REST Catalog
and locally with standard Spark or local parquet fallback.

Author: Armel Kouadio
"""

import os
from typing import Dict

# MinIO & S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))

# Buckets
BUCKET_RAW = os.getenv("BUCKET_RAW", "raw-landing")
BUCKET_LAKEHOUSE = os.getenv("BUCKET_LAKEHOUSE", "lakehouse")
BUCKET_ARCHIVE = os.getenv("BUCKET_ARCHIVE", "archive")

# Iceberg Catalog Settings
ICEBERG_CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "waba_catalog")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", f"s3a://{BUCKET_LAKEHOUSE}/")

# Exchange Rates for Consolidation to EUR
FX_RATES: Dict[str, float] = {
    "XOF": 1.0 / 655.957,  # WAEMU Fixed Peg: 1 EUR = 655.957 XOF
    "GHS": 0.065,          # Ghana Cedi floating rate ~ 15.38 GHS = 1 EUR
    "EUR": 1.0
}

# Regulatory Thresholds
BCEAO_MAX_NPL_RATIO = 0.05       # 5% maximum Non-Performing Loan ratio
CIMA_MAX_LOSS_RATIO = 0.70       # 70% maximum Loss Ratio (Claims / Premiums)
BCEAO_AML_THRESHOLD_XOF = 1_000_000.0  # 1,000,000 XOF regulatory AML reporting trigger
GHANA_AML_THRESHOLD_GHS = 5_000.0      # 5,000 GHS regulatory AML reporting trigger
