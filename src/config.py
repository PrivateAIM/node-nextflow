import os

from src.k8s.utils import find_service_names, get_current_namespace


def _require(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Required environment variable '{name}' is not set")
    return value


def get_kong_base_url() -> str:
    # Public Kong proxy the launcher and the worker pods can both reach.
    return _require("KONG_BASE_URL").rstrip("/")


# ---- internal object store (MinIO / SeaweedFS S3) --------------------------------------------------

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


def run_prefix(run_id: str) -> str:
    """Key prefix of everything a run stores (work/, inputs/, results/) inside the internal bucket."""
    return f"{get_internal_minio_prefix()}/{run_id}"


def run_location(run_id: str) -> str:
    return f"s3://{get_internal_minio_bucket()}/{run_prefix(run_id)}"


# ---- forwarding of results to another node (docs/result-handling-plan.md, section 3) ----------------

def forward_allowed() -> bool:
    return os.getenv("NF_ALLOW_FORWARD", "false").lower() == "true"


def get_forward_part_size() -> int:
    return int(os.getenv("NF_FORWARD_PART_SIZE", str(256 * 2 ** 20)))


def get_forward_max_object_size() -> int:
    # node-storage-service rejects objects above 1 GiB today; raise together with it
    return int(os.getenv("NF_FORWARD_MAX_OBJECT_SIZE", str(2 ** 30)))


def get_forward_max_total_size() -> int:
    return int(os.getenv("NF_FORWARD_MAX_TOTAL_SIZE", str(50 * 2 ** 30)))


def get_forward_attempts() -> int:
    return int(os.getenv("NF_FORWARD_ATTEMPTS", "3"))


def get_forward_timeout() -> float:
    return float(os.getenv("NF_FORWARD_TIMEOUT_S", "600"))


def get_storage_service_url() -> str:
    """In-cluster base URL of node-storage-service (paths /intermediate, /local, ...)."""
    url = os.getenv("NF_STORAGE_URL")
    if url:
        return url.rstrip("/")
    names = find_service_names("component=flame-storage-service", get_current_namespace())
    if len(names) != 1:
        raise RuntimeError("node-storage-service not found (label component=flame-storage-service); "
                           "set NF_STORAGE_URL")
    return f"http://{names[0]}:8080"
