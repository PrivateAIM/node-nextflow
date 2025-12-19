import os
from typing import Any, Optional
from kubernetes import client
from fastapi import HTTPException


# Load Nextflow Config from environment variables
SERVICE_ACCOUNT  = os.getenv("NF_SERVICE_ACCOUNT", "nextflow-sa")
PVC_NAME         = os.getenv("NF_PVC", "nextflow-pvc")
NF_IMAGE         = os.getenv("NF_IMAGE", "nextflow/nextflow:24.10.0")
CONFIGMAP_NAME   = os.getenv("NF_CONFIGMAP", "nextflow-config")
CONFIGMAP_KEY    = os.getenv("NF_CONFIGMAP_KEY", "nextflow.config")
BACKOFF_LIMIT    = int(os.getenv("NF_BACKOFF_LIMIT", "0"))

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "flame-nextflow") + "/conclude"  # <-- set me
                           # <-- kubectl apply -f secret below


def create_nextflow_run(input_data: Any,
                        run_id: str,
                        pipeline_name: Optional[str] = None,
                        run_args: Optional[list[str]] = None,
                        namespace: str = 'default') -> None:
    batch = client.BatchV1Api()

    job_name = run_id
    work_mount_path = "/workspace/" + run_id
    conf_mount_path = "/conf/" + run_id
    # Build the nextflow command
    pieces = [
        "nextflow", "run", pipeline_name,
        "-c", f"{conf_mount_path}/" + CONFIGMAP_KEY,
        "--input_data", f"'{work_mount_path}/input'",
    ]
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
            client.V1EnvVar(name="NXF_HOME", value=f"{work_mount_path}/.nextflow"),
            client.V1EnvVar(name="NXF_WORK", value=f"{work_mount_path}/work"),
            client.V1EnvVar(name="RUN_ID", value=run_id),
            client.V1EnvVar(name="WEBHOOK_URL", value=WEBHOOK_URL),
            client.V1EnvVar(name="STORAGE_LOCATION", value=work_mount_path),
        ],
        volume_mounts=[
            client.V1VolumeMount(name="work", mount_path=work_mount_path),
            client.V1VolumeMount(name="config", mount_path=conf_mount_path),
        ],
    )

    pod_spec = client.V1PodSpec(
        service_account_name=SERVICE_ACCOUNT,
        restart_policy="Never",
        containers=[container],
        volumes=[
            client.V1Volume(
                name="work",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=PVC_NAME
                ),
            ),
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
