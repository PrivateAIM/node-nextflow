import os


def _require(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Required environment variable '{name}' is not set")
    return value


def get_kong_minio_base_url() -> str:
    return _require("KONG_MINIO_BASE_URL")


def get_kong_minio_path_prefix() -> str:
    return os.getenv("KONG_MINIO_PATH_PREFIX", "")


def get_kong_presign_ttl() -> int:
    return int(os.getenv("KONG_PRESIGN_TTL_SECONDS", "86400"))


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
