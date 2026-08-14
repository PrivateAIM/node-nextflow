import base64
import csv
import io
from typing import Callable
from urllib.parse import urlsplit, urlunsplit

import httpx
from boto3 import client as boto3_client
from botocore.config import Config
from kubernetes import client as k8s_client

from src.k8s.utils import get_current_namespace


_PATH_EXTENSIONS = (
    ".fastq.gz", ".fq.gz", ".fastq", ".fq",
    ".fasta", ".fa", ".fna",
    ".bam", ".cram", ".sam",
    ".vcf", ".vcf.gz",
    ".bed", ".gtf", ".gff", ".gff3",
)


def _read_k8s_secret(secret_name: str, secret_key: str, namespace: str) -> str:
    secret = k8s_client.CoreV1Api().read_namespaced_secret(name=secret_name, namespace=namespace)
    return base64.b64decode(secret.data[secret_key]).decode()


class KongMinioPresigner:
    def __init__(self, base_url: str, path_prefix: str, access_key: str, secret_key: str,
                 session_token: str | None = None,
                 kong_query_params: dict[str, str] | None = None):
        self._base_url = base_url.rstrip("/")
        # Kong matches & strips this prefix. It must be inserted AFTER signing so
        # the SigV4 canonical request matches what MinIO sees post-strip upstream.
        self._path_prefix = "/" + path_prefix.strip("/") if path_prefix else ""
        self._s3 = boto3_client(
            "s3",
            endpoint_url=self._base_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            config=Config(signature_version="s3v4",
                          s3={"addressing_style": "path"}),
        )
        self._kong_q = kong_query_params or {}

    def presign_get(self, bucket: str, key: str, ttl: int) -> str:
        signed = self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=ttl,
        )
        url = self._inject_path_prefix(signed)
        if self._kong_q:
            sep = "&" if "?" in url else "?"
            url = url + sep + "&".join(f"{k}={v}" for k, v in self._kong_q.items())
        return url

    def fetch(self, bucket: str, key: str, ttl: int = 300) -> bytes:
        url = self.presign_get(bucket, key, ttl)
        response = httpx.get(url, follow_redirects=True, timeout=60.0)
        response.raise_for_status()
        return response.content

    def _inject_path_prefix(self, url: str) -> str:
        if not self._path_prefix:
            return url
        parts = urlsplit(url)
        new_path = self._path_prefix + parts.path
        return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))

    @classmethod
    def from_k8s_project_secret(cls, base_url: str, path_prefix: str, project_id: str,
                                kong_apikey: str) -> 'KongMinioPresigner':
        namespace = get_current_namespace()
        secret_name = f"minio-project-{project_id}"
        access_key = _read_k8s_secret(secret_name, "admin_access_key_id", namespace)
        secret_key = _read_k8s_secret(secret_name, "admin_secret_access_key", namespace)
        return cls(
            base_url=base_url,
            path_prefix=path_prefix,
            access_key=access_key,
            secret_key=secret_key,
            kong_query_params={"apikey": kong_apikey},
        )


def is_path_cell(value: str) -> bool:
    if not value:
        return False
    v = value.strip()
    if not v:
        return False
    if v.startswith(("http://", "https://", "s3://", "gs://", "az://")):
        return False
    return v.lower().endswith(_PATH_EXTENSIONS)


def rewrite_samplesheet(csv_bytes: bytes, bucket: str,
                        presign: Callable[[str, str], str]) -> bytes:
    text = csv_bytes.decode("utf-8")
    reader = csv.reader(io.StringIO(text))
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    for row in reader:
        new_row = [
            presign(bucket, cell.strip().lstrip("/")) if is_path_cell(cell) else cell
            for cell in row
        ]
        writer.writerow(new_row)
    return out.getvalue().encode("utf-8")
