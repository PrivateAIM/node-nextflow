import os


def _require(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Required environment variable '{name}' is not set")
    return value


def get_kong_base_url() -> str:
    # Public Kong proxy the launcher and the worker pods can both reach.
    return _require("KONG_BASE_URL").rstrip("/")


def get_internal_minio_endpoint() -> str:
    return _require("INTERNAL_MINIO_ENDPOINT")


def get_internal_minio_bucket() -> str:
    return os.getenv("NF_MINIO_BUCKET", "flame")


def get_internal_minio_prefix() -> str:
    return os.getenv("NF_MINIO_PREFIX", "Nextflow")


def get_internal_minio_access_key() -> str:
    return _require("AWS_ACCESS_KEY_ID")


def get_internal_minio_secret_key() -> str:
    return _require("AWS_SECRET_ACCESS_KEY")
