import os, uuid, shlex
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kubernetes import client, config

# Load in-cluster when running in K8s; fallback for local dev
try:
    config.load_incluster_config()
except Exception:
    config.load_kube_config()

app = FastAPI(title="Nextflow Job Launcher")

NAMESPACE        = os.getenv("NF_NAMESPACE", "nextflow")
SERVICE_ACCOUNT  = os.getenv("NF_SERVICE_ACCOUNT", "nextflow-sa")
PVC_NAME         = os.getenv("NF_PVC", "nextflow-pvc")
NF_IMAGE         = os.getenv("NF_IMAGE", "nextflow/nextflow:24.10.0")
CONFIGMAP_NAME   = os.getenv("NF_CONFIGMAP", "nextflow-config")
CONFIGMAP_KEY    = os.getenv("NF_CONFIGMAP_KEY", "nextflow.config")
WORK_MOUNT_PATH  = os.getenv("NF_WORK_MOUNT", "/workspace")
CONF_MOUNT_PATH  = os.getenv("NF_CONF_MOUNT", "/conf")
BACKOFF_LIMIT    = int(os.getenv("NF_BACKOFF_LIMIT", "0"))

class RunRequest(BaseModel):
    pipeline: str                 # e.g. "hello" or "github.com/org/pipeline"
    params: str | None = None     # e.g. "--reads 'data/*.fastq.gz' --genome GRCh38"
    run_id: str | None = None     # optional custom job suffix
    report: bool = True           # add "-with-report"
    work_dir: str | None = None   # override work dir (default from config)

@app.get("/healthz")
def health():
    return {"ok": True}

@app.post("/run")
def run(req: RunRequest):
    batch = client.BatchV1Api()

    run_id = req.run_id or uuid.uuid4().hex[:8]
    job_name = f"nf-run-{run_id}"

    # Build the nextflow command
    pieces = [
        "nextflow", "run", req.pipeline,
        "-c", f"{CONF_MOUNT_PATH}/" + CONFIGMAP_KEY,
    ]
    if req.params:
        # Prevent shell injection by splitting params safely if you pass them as a single string
        pieces.extend(shlex.split(req.params))
    if req.report:
        pieces.extend(["-with-report", "report.html"])
    if req.work_dir:
        pieces.extend(["-w", req.work_dir])

    command = " ".join(pieces)

    container = client.V1Container(
        name="nf",
        image=NF_IMAGE,
        image_pull_policy="IfNotPresent",
        command=["/bin/bash","-lc"],
        args=[f"""
            echo 'Nextflow:' && nextflow -version && \
            echo 'Using config:' && cat {CONF_MOUNT_PATH}/{CONFIGMAP_KEY} && \
            {command}
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
        metadata=client.V1ObjectMeta(name=job_name, namespace=NAMESPACE),
        spec=job_spec,
    )

    try:
        batch.create_namespaced_job(namespace=NAMESPACE, body=job)
        return {"status": "submitted", "job": job_name, "namespace": NAMESPACE, "image": NF_IMAGE}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
