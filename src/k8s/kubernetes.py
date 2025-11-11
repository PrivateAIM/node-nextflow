from src.entities.nextflow_run_entity import NextflowRunEntity
from kubernetes import client, config

SERVICE_ACCOUNT  = os.getenv("NF_SERVICE_ACCOUNT", "nextflow-sa")
PVC_NAME         = os.getenv("NF_PVC", "nextflow-work")
NF_IMAGE         = os.getenv("NF_IMAGE", "nextflow/nextflow:24.10.0")
CONFIGMAP_NAME   = os.getenv("NF_CONFIGMAP", "nextflow-config")
CONFIGMAP_KEY    = os.getenv("NF_CONFIGMAP_KEY", "nextflow.config")
WORK_MOUNT_PATH  = os.getenv("NF_WORK_MOUNT", "/workspace")
CONF_MOUNT_PATH  = os.getenv("NF_CONF_MOUNT", "/conf")
BACKOFF_LIMIT    = int(os.getenv("NF_BACKOFF_LIMIT", "0"))


def create_nextflow_run(run: NextflowRunEntity, namespace: str = 'default') -> None:
    batch = client.BatchV1Api()

    job_name = run.run_id

    # Build the nextflow command
    pieces = [
        "nextflow", "run", run.pipeline_name,
        "-c", f"{CONF_MOUNT_PATH}/" + CONFIGMAP_KEY,
    ]
    if run.run_args:
        # Prevent shell injection by splitting params safely if you pass them as a single string
        pieces.extend(run_args)

    command = " ".join(pieces)



    container = client.V1Container(
        name="nf",
        image=NF_IMAGE,
        image_pull_policy="IfNotPresent",
        command=["/bin/bash", "-lc"],
        args=[f"""
                set -Eeuo pipefail
                notify() {{
                    wget -qO- --header="X-Job-Name: ${JOB_NAME}" \
                    --post-data "status=$1&node=${NODE_NAME}" \
                    https://example.com/webhook || true
                }}
                trap'notify fail' ERR
                echo 'Nextflow:' && nextflow -version && \
                echo 'Using config:' && cat {CONF_MOUNT_PATH}/{configmap_key} && \
                {command}
                notify success 
            """],
        env=[
            client.V1EnvVar(name="NXF_HOME", value=f"{WORK_MOUNT_PATH}/.nextflow"),
            client.V1EnvVar(name="NXF_WORK", value=f"{WORK_MOUNT_PATH}/work"),
        ],
        volume_mounts=[
            client.V1VolumeMount(name="work", mount_path=WORK_MOUNT_PATH),
            client.V1VolumeMount(name="config", mount_path=CONF_MOUNT_PATH),
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
                    claim_name=pvc_name
                ),
            ),
            client.V1Volume(
                name="config",
                config_map=client.V1ConfigMapVolumeSource(
                    name=configmap_name,
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
        metadata=client.V1ObjectMeta(name=job_name, namespace=namespace),
        spec=job_spec,
    )

    try:
        batch.create_namespaced_job(namespace=namespace, body=job)
        return {"status": "submitted", "job": job_name, "namespace": NAMESPACE, "image": NF_IMAGE}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
