"""
MinIO Object Storage Connector for WABA Group.

Handles:
  - Automated bucket initialization ('raw-landing', 'lakehouse', 'archive')
  - Strict hierarchical partitioning: raw-landing/{country_code}/{entity_type}/...
  - Seamless upload of generated CSV buffers
  - Fallback local disk staging mode when MinIO cluster is offline

Author: Armel Kouadio
"""

import io
import logging
import os
from typing import Optional, Union

logger = logging.getLogger("waba.minio")

try:
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    logger.warning("boto3 is not yet installed. Running in local simulation fallback mode.")


class MinioUploader:
    """
    MinIO S3 Client specialized for the WABA Lakehouse landing zone.
    """

    DEFAULT_BUCKETS = ["raw-landing", "lakehouse", "archive"]

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: bool = False,
        local_fallback_dir: str = "./storage/minio"
    ):
        self.endpoint_url = endpoint_url or os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"))
        self.secure = secure
        self.local_fallback_dir = local_fallback_dir
        self.s3_client = None
        self.is_connected = False

        self._init_client()

    def _init_client(self):
        """Attempts to establish connection with MinIO server."""
        if not BOTO3_AVAILABLE:
            logger.info("Using local directory fallback mode for MinIO storage.")
            self._ensure_local_dirs()
            return

        try:
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(signature_version="s3v4"),
                region_name="us-east-1"
            )
            # Ping MinIO
            self.s3_client.list_buckets()
            self.is_connected = True
            logger.info(f"Successfully connected to MinIO at {self.endpoint_url}")
            self.ensure_buckets()
        except Exception as e:
            logger.warning(f"Could not connect to MinIO ({self.endpoint_url}): {e}. Enabling local disk fallback.")
            self.is_connected = False
            self._ensure_local_dirs()

    def _ensure_local_dirs(self):
        """Ensures local storage directories exist when running locally without Docker."""
        for b in self.DEFAULT_BUCKETS:
            p = os.path.join(self.local_fallback_dir, b)
            os.makedirs(p, exist_ok=True)

    def ensure_buckets(self):
        """Creates target buckets if they do not already exist."""
        if not self.is_connected or not self.s3_client:
            self._ensure_local_dirs()
            return

        existing = [b["Name"] for b in self.s3_client.list_buckets().get("Buckets", [])]
        for bucket in self.DEFAULT_BUCKETS:
            if bucket not in existing:
                try:
                    self.s3_client.create_bucket(Bucket=bucket)
                    logger.info(f"Created MinIO bucket: '{bucket}'")
                except Exception as ex:
                    logger.error(f"Error creating bucket {bucket}: {ex}")

    def upload_data(
        self,
        bucket_name: str,
        object_key: str,
        data: Union[str, bytes],
        content_type: str = "text/csv"
    ) -> bool:
        """
        Uploads in-memory data (CSV string or bytes) to MinIO S3 or local fallback.
        """
        if isinstance(data, str):
            payload = data.encode("utf-8")
        else:
            payload = data

        if self.is_connected and self.s3_client:
            try:
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=payload,
                    ContentType=content_type
                )
                logger.info(f"Uploaded s3://{bucket_name}/{object_key} ({len(payload):,} bytes)")
                return True
            except Exception as e:
                logger.error(f"Failed to upload s3://{bucket_name}/{object_key}: {e}")

        # Local fallback execution
        local_path = os.path.join(self.local_fallback_dir, bucket_name, object_key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(payload)
        logger.info(f"Saved to local fallback: {local_path} ({len(payload):,} bytes)")
        return True

    def upload_raw_file(
        self,
        country_code: str,
        entity_type: str,
        filename: str,
        data: str
    ) -> str:
        """
        Saves transaction file to raw-landing bucket organized by country and entity.
        Target key: {country_code}/{entity_type}/{filename}
        """
        key = f"{country_code}/{entity_type}/{filename}"
        self.upload_data(bucket_name="raw-landing", object_key=key, data=data)
        return key

    def upload_reference_file(self, filename: str, data: str) -> str:
        """
        Saves master reference files into raw-landing/references/{filename}.
        """
        key = f"references/{filename}"
        self.upload_data(bucket_name="raw-landing", object_key=key, data=data)
        return key
