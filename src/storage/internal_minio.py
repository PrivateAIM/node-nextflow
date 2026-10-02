from boto3 import client as boto3_client
from botocore.config import Config

from src import config


class InternalMinioClient:
    def __init__(self, endpoint: str, access_key: str, secret_key: str) -> None:
        self._s3 = boto3_client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    @classmethod
    def from_config(cls) -> "InternalMinioClient":
        return cls(endpoint=config.get_internal_minio_endpoint(),
                   access_key=config.get_internal_minio_access_key(),
                   secret_key=config.get_internal_minio_secret_key())

    def put_object(self, bucket: str, key: str, body: bytes) -> str:
        self._s3.put_object(Bucket=bucket, Key=key, Body=body)
        return f"s3://{bucket}/{key}"

    def list_objects(self, bucket: str, prefix: str) -> list[dict]:
        """All objects below prefix as [{key (relative to prefix), size, etag}], sorted by key."""
        out = []
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"][len(prefix):]
                if key and not key.endswith("/"):
                    out.append({"key": key, "size": obj["Size"], "etag": obj["ETag"].strip('"')})
        return sorted(out, key=lambda o: o["key"])

    def get_object(self, bucket: str, key: str, byte_range: str | None = None) -> dict:
        """boto3 get_object response; ['Body'] is a stream. byte_range e.g. 'bytes=0-99'."""
        kwargs = {"Range": byte_range} if byte_range else {}
        return self._s3.get_object(Bucket=bucket, Key=key, **kwargs)
