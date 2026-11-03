from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import PathLike, atomic_write_chunks, validate_object_key, validate_object_prefix


class S3StorageBackend:
    """Amazon S3 adapter using boto3's standard credential provider chain.

    The bucket must already exist. Credentials are resolved by boto3 (for
    example, environment, shared profile, web identity, or workload role).
    """

    def __init__(
        self,
        bucket_name: str,
        *,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        max_concurrency: int = 4,
        multipart_threshold_mb: int = 64,
        client: Any | None = None,
        transfer_config: Any | None = None,
    ) -> None:
        if not bucket_name or "/" in bucket_name:
            raise ValueError("bucket_name must be a non-empty bucket name")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if multipart_threshold_mb < 5:
            raise ValueError("multipart_threshold_mb must be >= 5")

        self.bucket_name = bucket_name
        self.max_concurrency = max_concurrency
        self.client = client
        self.transfer_config = transfer_config

        if self.client is None:
            try:
                import boto3
                from boto3.s3.transfer import TransferConfig
                from botocore.config import Config
            except ImportError as exc:
                raise RuntimeError(
                    "S3 storage requires the optional dependency: uv sync --extra s3"
                ) from exc

            session = boto3.Session()
            self.client = session.client(
                "s3",
                region_name=region_name,
                endpoint_url=endpoint_url,
                config=Config(max_pool_connections=max_concurrency),
            )
            self.transfer_config = TransferConfig(
                multipart_threshold=multipart_threshold_mb * 1024 * 1024,
                multipart_chunksize=8 * 1024 * 1024,
                max_concurrency=max_concurrency,
                max_io_queue=max(2, max_concurrency * 2),
                io_chunksize=256 * 1024,
                use_threads=True,
            )

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        response = getattr(error, "response", None)
        if not isinstance(response, dict):
            return False
        error_code = response.get("Error", {}).get("Code")
        status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return error_code in {"404", "NoSuchKey", "NotFound"} or status_code == 404

    def upload_file(self, source_path: PathLike, destination_key: str) -> str:
        source = Path(source_path)
        if not source.is_file():
            raise FileNotFoundError(source)
        key = validate_object_key(destination_key)
        with source.open("rb") as stream:
            self.client.upload_fileobj(
                stream,
                Bucket=self.bucket_name,
                Key=key,
                Config=self.transfer_config,
            )
        return key

    def download_file(self, source_key: str, destination_path: PathLike) -> str:
        key = validate_object_key(source_key)
        response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        body = response["Body"]

        def chunks():
            while True:
                chunk = body.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk

        try:
            return atomic_write_chunks(destination_path, chunks())
        finally:
            body.close()

    def exists(self, key: str) -> bool:
        object_key = validate_object_key(key)
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=object_key)
        except Exception as exc:
            if self._is_not_found(exc):
                return False
            raise
        return True

    def list_keys(self, prefix: str = "") -> list[str]:
        validated_prefix = validate_object_prefix(prefix)
        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=validated_prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
        return sorted(keys)

    def delete(self, key: str) -> None:
        object_key = validate_object_key(key)
        # DeleteObject is idempotent for absent keys. On versioned buckets this
        # creates a delete marker; lifecycle/version retention remains bucket policy.
        self.client.delete_object(Bucket=self.bucket_name, Key=object_key)

    def close(self) -> None:
        """Close the underlying botocore client."""
        self.client.close()
