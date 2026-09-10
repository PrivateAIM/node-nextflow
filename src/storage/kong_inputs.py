import csv
import io
import os
from typing import Callable
from urllib.parse import quote

import httpx


_DEFAULT_PATH_EXTENSIONS = (
    ".fastq.gz", ".fq.gz", ".fastq", ".fq",
    ".fasta", ".fa", ".fna",
    ".bam", ".cram", ".sam",
    ".vcf", ".vcf.gz",
    ".bed", ".gtf", ".gff", ".gff3",
)


def _path_extensions() -> tuple[str, ...]:
    override = os.getenv("NF_INPUT_PATH_EXTENSIONS", "").strip()
    if not override:
        return _DEFAULT_PATH_EXTENSIONS
    return tuple(ext.strip().lower() for ext in override.split(",") if ext.strip())


def _object_url(kong_base_url: str, datastore: str, key: str) -> str:
    # The hub adapter provisions one route per project/datastore link at
    # /{service_name}/{datastore_type} (kong.py _create_link:
    # paths=[f"/{svc.name}/{ds_type}"]), with strip_path on. The bucket is NOT
    # part of the key: the minio-gateway plugin inserts conf.bucket_name into
    # the upstream path itself, so including it here would double it.
    return (f"{kong_base_url.rstrip('/')}/{datastore.strip('/')}/s3/"
            f"{quote(key.lstrip('/'), safe='/')}")


def build_object_url(kong_base_url: str, datastore: str, key: str, apikey: str) -> str:
    """URL a worker pod can GET directly. Kong's key-auth validates the apikey,
    acl checks the project group, and minio-gateway signs upstream."""
    return f"{_object_url(kong_base_url, datastore, key)}?apikey={quote(apikey, safe='')}"


def fetch_object(kong_base_url: str, datastore: str, key: str, apikey: str) -> bytes:
    """Read a (small) object through Kong from the launcher itself.

    The apikey goes in a header rather than the query, so this works whether or
    not the route's key-auth has key_in_query enabled.
    """
    response = httpx.get(_object_url(kong_base_url, datastore, key),
                         headers={"apikey": apikey},
                         follow_redirects=True,
                         timeout=60.0)
    response.raise_for_status()
    return response.content


def is_path_cell(value: str) -> bool:
    if not value:
        return False
    v = value.strip()
    if not v:
        return False
    if v.startswith(("http://", "https://", "s3://", "gs://", "az://")):
        return False
    return v.lower().endswith(_path_extensions())


def rewrite_samplesheet(csv_bytes: bytes, make_url: Callable[[str], str]) -> bytes:
    text = csv_bytes.decode("utf-8")
    reader = csv.reader(io.StringIO(text))
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    for row in reader:
        new_row = [
            make_url(cell.strip().lstrip("/")) if is_path_cell(cell) else cell
            for cell in row
        ]
        writer.writerow(new_row)
    return out.getvalue().encode("utf-8")
