"""
Kafka Topics Provisioning and Lifecycle Management for WABA Group.

Initializes all 10 topics across the Medallion layers:
  - Raw Ingestion Topics (published by NiFi)
  - Silver Topics (published by Spark Streaming Job 1)
  - Gold Alerts & Compliance Topics (published by Spark Streaming Job 2)
  - Dead Letter Queue (DLQ) for malformed event auditing

Author: Armel Kouadio
"""

import logging
import os
import sys
from typing import Dict, List

logger = logging.getLogger("waba.kafka.admin")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

TOPIC_CONFIGS: Dict[str, dict] = {
    # 1. Raw Ingestion Layer (NiFi -> Kafka)
    "raw-bank-transactions": {"partitions": 8, "replication": 1, "retention_ms": 86400000},       # 24h retention
    "raw-insurance-operations": {"partitions": 8, "replication": 1, "retention_ms": 86400000},
    "raw-mobile-money-payments": {"partitions": 8, "replication": 1, "retention_ms": 86400000},
    "raw-loan-repayments": {"partitions": 8, "replication": 1, "retention_ms": 86400000},

    # 2. Silver Stream Layer (Job 1 Cleaned & Validated Stream)
    "silver-bank-transactions": {"partitions": 8, "replication": 1, "retention_ms": 172800000},    # 48h retention
    "silver-insurance-operations": {"partitions": 8, "replication": 1, "retention_ms": 172800000},
    "silver-mobile-money": {"partitions": 8, "replication": 1, "retention_ms": 172800000},

    # 3. Gold Real-Time Alerts Layer (Job 2 Compliance & Fraud Stream)
    "gold-fraud-alerts": {"partitions": 4, "replication": 1, "retention_ms": 604800000},          # 7 days retention
    "gold-aml-events": {"partitions": 4, "replication": 1, "retention_ms": 2592000000},           # 30 days compliance retention
    "gold-liquidity-alerts": {"partitions": 4, "replication": 1, "retention_ms": 604800000},

    # 4. Dead Letter Queue
    "dlq-financial-events": {"partitions": 4, "replication": 1, "retention_ms": 1209600000}       # 14 days audit retention
}


def initialize_kafka_topics(bootstrap_servers: str = None):
    """
    Connects to Kafka cluster and idempotently creates missing topics.
    """
    bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Connecting to Kafka Admin Client at: {bootstrap_servers}...")

    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="waba_kafka_admin"
        )

        existing_topics = set(admin.list_topics())
        new_topics: List[NewTopic] = []

        for name, cfg in TOPIC_CONFIGS.items():
            if name not in existing_topics:
                new_topics.append(
                    NewTopic(
                        name=name,
                        num_partitions=cfg["partitions"],
                        replication_factor=cfg["replication"],
                        topic_configs={"retention.ms": str(cfg["retention_ms"])}
                    )
                )

        if new_topics:
            admin.create_topics(new_topics=new_topics, validate_only=False)
            logger.info(f"Successfully created {len(new_topics)} new Kafka topics.")
        else:
            logger.info("All WABA Kafka topics are already provisioned.")

        for name in TOPIC_CONFIGS.keys():
            logger.info(f"  ✓ Topic ready: {name}")

        admin.close()

    except ImportError:
        logger.warning("kafka-python not installed locally. Generating bash CLI provisioning fallback script...")
        with open("streaming/create_topics.sh", "w") as f:
            f.write("#!/bin/bash\n# Kafka CLI provisioning script\n")
            f.write(f"BOOTSTRAP='{bootstrap_servers}'\n\n")
            for name, cfg in TOPIC_CONFIGS.items():
                f.write(
                    f"kafka-topics.sh --create --if-not-exists --bootstrap-server $BOOTSTRAP "
                    f"--topic {name} --partitions {cfg['partitions']} --replication-factor {cfg['replication']} "
                    f"--config retention.ms={cfg['retention_ms']}\n"
                )
        os.chmod("streaming/create_topics.sh", 0o755)
        logger.info("Created executable script: streaming/create_topics.sh")
    except Exception as e:
        logger.warning(f"Kafka cluster connection error: {e}. Fallback script available.")


if __name__ == "__main__":
    servers = sys.argv[1] if len(sys.argv) > 1 else None
    initialize_kafka_topics(bootstrap_servers=servers)
