from boto3 import client as boto3_client
from botocore.config import Config


class InternalMinioClient:
    def __init__(self, endpoint: str, access_key: str, secret_key: str):
        self._s3 = boto3_client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4",
                          s3={"addressing_style": "path"}),
        )

    def put_object(self, bucket: str, key: str, body: bytes) -> str:
        self._s3.put_object(Bucket=bucket, Key=key, Body=body)
        return f"s3://{bucket}/{key}"
