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
    """URL a worker pod can GET directly. Kong's key-auth validates the apikey
    (needs key_in_query on the route), acl checks the project group, and
    minio-gateway signs upstream.

    The trailing `file` parameter repeats the file name so the URL still ends in
    the file's extension: nf-core samplesheet schemas check cells against
    patterns like `^\\S+\\.f(ast)?q\\.gz$`. Nextflow names the staged file after
    the path only, so the query does not change it.
    """
    filename = key.rstrip("/").rsplit("/", 1)[-1]
    return (f"{_object_url(kong_base_url, datastore, key)}"
            f"?apikey={quote(apikey, safe='')}&file={quote(filename, safe='')}")


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
    """A samplesheet cell holding a relative path into the datastore (as opposed to a URL or a plain value)."""
    v = value.strip().lower()
    return v.endswith(_path_extensions()) and not v.startswith(("http://", "https://", "s3://", "gs://", "az://"))


def rewrite_samplesheet(csv_bytes: bytes, make_url: Callable[[str], str]) -> bytes:
    """Replace every path cell with the URL make_url builds for it."""
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    for row in csv.reader(io.StringIO(csv_bytes.decode("utf-8"))):
        writer.writerow([make_url(cell.strip().lstrip("/")) if is_path_cell(cell) else cell for cell in row])
    return out.getvalue().encode("utf-8")
