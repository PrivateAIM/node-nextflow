import os
from typing import Any, Optional
from kubernetes import client
from fastapi import HTTPException


# Load Nextflow Config from environment variables
SERVICE_ACCOUNT   = os.getenv("NF_SERVICE_ACCOUNT", "nextflow-sa")
NF_IMAGE          = os.getenv("NF_IMAGE", "nextflow/nextflow:25.04.3")
CONFIGMAP_NAME    = os.getenv("NF_CONFIGMAP", "nextflow-config")
CONFIGMAP_KEY     = os.getenv("NF_CONFIGMAP_KEY", "nextflow.config")
BACKOFF_LIMIT     = int(os.getenv("NF_BACKOFF_LIMIT", "0"))
MINIO_BUCKET      = os.getenv("NF_MINIO_BUCKET", "flame")
MINIO_PREFIX      = os.getenv("NF_MINIO_PREFIX", "Nextflow")
MINIO_SECRET_NAME = os.getenv("NF_MINIO_SECRET", "node1-seaweedfs-s3-secret")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://nextflow-service:8000") + "/nextflow/conclude"


def create_nextflow_run(#input_data: Any,
                        run_id: str,
                        pipeline_name: Optional[str] = None,
                        run_args: Optional[list[str]] = None,
                        namespace: str = 'default') -> None:
    batch = client.BatchV1Api()

    job_name = run_id
    conf_mount_path = "/conf"
    # Work dir lives in MinIO under the shared prefix, scoped per run
    run_work_dir = f"s3://{MINIO_BUCKET}/{MINIO_PREFIX}/{run_id}"

    # Build the nextflow command
    pieces = [
        "nextflow", "run", pipeline_name,
        "-c", f"{conf_mount_path}/{CONFIGMAP_KEY}",
        "-work-dir", f"{run_work_dir}/work",
    ]

    # Add input_data parameter if needed by the pipeline
    #if input_data:
    #    pieces.extend(["--input_data", f"'{run_work_dir}/input'"])

    if run_args:
        # Prevent shell injection by splitting params safely if you pass them as a single string
        pieces.extend(run_args)

    command = " ".join(pieces)
    notify_wrapper = f"""
    set -Eeuo pipefail

    notify() {{
      status="$1"
      # Build JSON body matching ConcludeNextflowRun
      body=$(printf '{{"run_id":"%s","run_status":"%s","storage_location":"%s"}}' \
                   "$RUN_ID" "$status" "$STORAGE_LOCATION")

      # Exponential backoff: 1,2,4,8,16s (tunable)
      for d in 1 2 4 8 16; do
        if curl -fsS -X POST "$WEBHOOK_URL" \
             -H "Content-Type: application/json" \
             --data "$body"; then
          echo "Conclude webhook delivered: $status"
          return 0
        fi
        echo "Webhook attempt failed; retrying in $d s..." >&2
        sleep "$d"
      done
      echo "Webhook failed after retries; continuing." >&2
      return 0  # don't block Job termination on notify issues
    }}

    echo 'Nextflow:' && nextflow -version
    echo 'Using config:' && cat {conf_mount_path}/{CONFIGMAP_KEY}

    # ---- run your existing command; notify on both paths ----
    if {command}; then
      notify "succeeded"
    else
      notify "failed"
      exit 1
    fi
    """

    container = client.V1Container(
        name="nf",
        image=NF_IMAGE,
        image_pull_policy="IfNotPresent",
        command=["/bin/bash", "-lc"],
        args=[notify_wrapper],
        env=[
            client.V1EnvVar(name="NXF_JVM_ARGS", value="-Xms1g -Xmx7g"),  # cap JVM heap
            client.V1EnvVar(name="NXF_HOME", value="/tmp/.nextflow"),  # ephemeral local dir
            client.V1EnvVar(name="NXF_WORK", value=f"{run_work_dir}/work"),
            client.V1EnvVar(name="RUN_ID", value=run_id),
            client.V1EnvVar(name="WEBHOOK_URL", value=WEBHOOK_URL),
            client.V1EnvVar(name="STORAGE_LOCATION", value=run_work_dir),
            # MinIO credentials injected from Kubernetes secret
            client.V1EnvVar(
                name="AWS_ACCESS_KEY_ID",
                value_from=client.V1EnvVarSource(
                    secret_key_ref=client.V1SecretKeySelector(
                        name=MINIO_SECRET_NAME, key="admin_access_key_id"
                    )
                ),
            ),
            client.V1EnvVar(
                name="AWS_SECRET_ACCESS_KEY",
                value_from=client.V1EnvVarSource(
                    secret_key_ref=client.V1SecretKeySelector(
                        name=MINIO_SECRET_NAME, key="admin_secret_access_key"
                    )
                ),
            ),
        ],
        volume_mounts=[
            client.V1VolumeMount(name="config", mount_path=conf_mount_path),
        ],
        resources=client.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "4Gi"},
            limits={"cpu": "1", "memory": "8Gi"},
        ),
    )

    pod_spec = client.V1PodSpec(
        service_account_name=SERVICE_ACCOUNT,
        restart_policy="Never",
        containers=[container],
        volumes=[
            client.V1Volume(
                name="config",
                config_map=client.V1ConfigMapVolumeSource(
                    name=CONFIGMAP_NAME,
                    items=[client.V1KeyToPath(key=CONFIGMAP_KEY, path=CONFIGMAP_KEY)],
                ),
            ),
        ],
    )

    job_spec = client.V1JobSpec(
        backoff_limit=BACKOFF_LIMIT,
        template=client.V1PodTemplateSpec(spec=pod_spec),
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name,
                                     labels={'app': job_name, 'component': "flame-analysis-nf"},
                                     namespace=namespace),
        spec=job_spec,
    )

    try:
        batch.create_namespaced_job(namespace=namespace, body=job)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
