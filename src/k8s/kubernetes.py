import os
import shlex

from kubernetes import client

from src import config

SERVICE_ACCOUNT = os.getenv("NF_SERVICE_ACCOUNT", "nextflow-sa")
NF_IMAGE = os.getenv("NF_IMAGE", "nextflow/nextflow:25.10.8")  # 25.04's S3 plugin misreads SeaweedFS listings
CONFIGMAP_NAME = os.getenv("NF_CONFIGMAP", "nextflow-config")
CONFIGMAP_KEY = os.getenv("NF_CONFIGMAP_KEY", "nextflow.config")
BACKOFF_LIMIT = int(os.getenv("NF_BACKOFF_LIMIT", "0"))
MINIO_SECRET_NAME = os.getenv("NF_MINIO_SECRET", "node1-seaweedfs-s3-secret")
# Optional shared work volume instead of Fusion: an RWX PVC (e.g. SeaweedFS CSI) whose root is the filer
# directory behind s3://{bucket}/{prefix}, so the same files stay reachable through S3.
# The Nextflow config must set k8s.storageClaimName / k8s.storageMountPath to the same values.
WORK_PVC = os.getenv("NF_WORK_PVC")
WORK_MOUNT = os.getenv("NF_WORK_MOUNT", "/workspace")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://nextflow-launcher:8000") + "/nextflow/conclude"
CONF_MOUNT = "/conf"

# Runs Nextflow, then reports the outcome to the launcher's /conclude webhook (body = ConcludeNextflowRun).
JOB_SCRIPT = """
set -Eeuo pipefail

notify() {{
  body=$(printf '{{"run_id":"%s","run_status":"%s","storage_location":"%s"}}' \\
               "$RUN_ID" "$1" "$STORAGE_LOCATION")
  for delay in 1 2 4 8 16; do
    if curl -fsS -X POST "$WEBHOOK_URL" -H "Content-Type: application/json" --data "$body"; then
      echo "Conclude webhook delivered: $1"
      return 0
    fi
    echo "Webhook attempt failed; retrying in $delay s..." >&2
    sleep "$delay"
  done
  echo "Webhook failed after retries; continuing." >&2  # don't block Job termination on notify issues
}}

echo 'Nextflow:' && nextflow -version
echo 'Using config:' && cat {config_file}

if {command}; then
  notify "succeeded"
else
  # stdout only has the summary; the exception details are in Nextflow's own log
  echo '--- exceptions from .nextflow.log ---'
  grep -E -B1 -A25 'Exception' .nextflow.log 2>/dev/null | head -150 || true
  notify "failed"
  exit 1
fi
"""


def _secret_env(name: str, key: str) -> client.V1EnvVar:
    return client.V1EnvVar(name=name, value_from=client.V1EnvVarSource(
        secret_key_ref=client.V1SecretKeySelector(name=MINIO_SECRET_NAME, key=key)))


def create_nextflow_run(run_id: str, pipeline_name: str, run_args: list[str], namespace: str) -> None:
    """Submit a Job that runs the pipeline; raises kubernetes.client.ApiException if the API refuses it."""
    config_file = f"{CONF_MOUNT}/{CONFIGMAP_KEY}"
    # With a shared work volume, Nextflow works on the mount; the files are the same objects as in S3
    work_dir = f"{WORK_MOUNT}/{run_id}/work" if WORK_PVC else f"{config.run_location(run_id)}/work"
    command = shlex.join(["nextflow", "run", pipeline_name, "-c", config_file, "-work-dir", work_dir, *run_args])

    env = [
        client.V1EnvVar(name="NXF_JVM_ARGS", value="-Xms1g -Xmx7g"),  # cap JVM heap
        client.V1EnvVar(name="NXF_HOME", value="/tmp/.nextflow"),  # ephemeral local dir
        client.V1EnvVar(name="NXF_WORK", value=work_dir),
        client.V1EnvVar(name="RUN_ID", value=run_id),
        client.V1EnvVar(name="WEBHOOK_URL", value=WEBHOOK_URL),
        client.V1EnvVar(name="STORAGE_LOCATION", value=config.run_location(run_id)),
        _secret_env("AWS_ACCESS_KEY_ID", "admin_access_key_id"),
        _secret_env("AWS_SECRET_ACCESS_KEY", "admin_secret_access_key"),
    ]
    volume_mounts = [client.V1VolumeMount(name="config", mount_path=CONF_MOUNT)]
    volumes = [client.V1Volume(name="config", config_map=client.V1ConfigMapVolumeSource(
        name=CONFIGMAP_NAME, items=[client.V1KeyToPath(key=CONFIGMAP_KEY, path=CONFIGMAP_KEY)]))]

    if WORK_PVC:
        # tasks symlink pipeline assets (e.g. multiqc_config.yml); keep the checkout on the shared volume
        env.append(client.V1EnvVar(name="NXF_ASSETS", value=f"{WORK_MOUNT}/{run_id}/assets"))
        volume_mounts.append(client.V1VolumeMount(name="work", mount_path=WORK_MOUNT))
        volumes.append(client.V1Volume(name="work", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
            claim_name=WORK_PVC)))

    container = client.V1Container(
        name="nf",
        image=NF_IMAGE,
        image_pull_policy="IfNotPresent",
        command=["/bin/bash", "-lc"],
        args=[JOB_SCRIPT.format(config_file=config_file, command=command)],
        env=env,
        volume_mounts=volume_mounts,
        resources=client.V1ResourceRequirements(requests={"cpu": "500m", "memory": "4Gi"},
                                                limits={"cpu": "1", "memory": "8Gi"}),
    )
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=run_id, namespace=namespace,
                                     labels={"app": run_id, "component": "flame-analysis-nf"}),
        spec=client.V1JobSpec(
            backoff_limit=BACKOFF_LIMIT,
            template=client.V1PodTemplateSpec(spec=client.V1PodSpec(
                service_account_name=SERVICE_ACCOUNT,
                restart_policy="Never",
                containers=[container],
                volumes=volumes,
            )),
        ),
    )
    client.BatchV1Api().create_namespaced_job(namespace=namespace, body=job)
