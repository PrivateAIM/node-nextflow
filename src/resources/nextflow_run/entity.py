import threading
import time
import uuid
from typing import Literal, Optional

from fastapi import HTTPException
from kubernetes.client import ApiException
from pydantic import BaseModel, field_validator

from src import config
from src.k8s.kubernetes import create_nextflow_run
from src.k8s.utils import delete_job, get_current_namespace
from src.resources.clients.analysis_client import AnalysisClient
from src.resources.database.entity import Database
from src.storage import forward as fwd
from src.storage.internal_minio import InternalMinioClient
from src.storage.kong_inputs import build_object_url, fetch_object, rewrite_samplesheet

RESULTS_DIR = "results/"  # below the run's prefix; manifest keys and forward keys are relative to it


class InputRef(BaseModel):
    key: str
    param_name: str
    samplesheet: bool = False


class ForwardSpec(BaseModel):
    """What to do with the results once the run has finished (executed by the launcher in conclude())."""
    keys: list[str]                       # keys / globs relative to results/
    to: str                               # target node id, handed to node-storage-service as remote_node_id
    part_size: Optional[int] = None       # bytes; default NF_FORWARD_PART_SIZE
    compression: Literal["none", "gzip"] = "none"

    @field_validator("keys")
    @classmethod
    def _keys(cls, keys: list[str]) -> list[str]:
        if not keys:
            raise ValueError("keys must not be empty")
        return [fwd.normalize_key(k) for k in keys]

    @field_validator("to")
    @classmethod
    def _to(cls, to: str) -> str:
        if not to.strip():
            raise ValueError("to must be a node id")
        return to.strip()

    @field_validator("part_size")
    @classmethod
    def _part_size(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 1:
            raise ValueError("part_size must be positive")
        return v


class CreateNextflowRun(BaseModel):
    analysis_id: str = 'analysis_id'
    project_id: str = 'project_id'  # TODO: accepted but not stored yet
    pipeline_name: str = 'pipeline_name'
    run_args: list[str] = []
    keycloak_token: str = 'keycloak_token'
    inputs: list[InputRef] = []
    kong_apikey: Optional[str] = None
    kong_datastore: Optional[str] = None
    forward: Optional[ForwardSpec] = None


class ConcludeNextflowRun(BaseModel):
    run_id: str = 'run_id'
    run_status: str = 'run_status'
    storage_location: str = 'storage_location'


def result_payload(run_id: str, run_status: str, storage_location: str, manifest: list[dict]) -> dict:
    """Body of the callback that tells the analysis its run has concluded."""
    return {"run_id": run_id, "run_status": run_status, "storage_location": storage_location,
            "results_prefix": RESULTS_DIR, "files": manifest}


def resume_interrupted_forwards(database: Database) -> None:
    """A launcher restart interrupts running forwards; continue them where they stopped (uploaded parts are kept)."""
    for row in database.get_nf_runs_with_forward_status("running"):
        run = NextflowRunEntity.from_database(row.run_id, database)
        payload = result_payload(row.run_id, row.run_status, config.run_location(row.run_id), row.manifest)
        run.start_forward(database, payload, row.forward_state)


class NextflowRunEntity:
    def __init__(self,
                 analysis_id: str,
                 keycloak_token: str,
                 pipeline_name: Optional[str] = None,
                 run_args: Optional[list[str]] = None,
                 run_id: Optional[str] = None,
                 time_created: Optional[float] = None,
                 forward_spec: Optional[dict] = None) -> None:
        self.analysis_id = analysis_id
        self.keycloak_token = keycloak_token
        self.pipeline_name = pipeline_name
        self.run_args = run_args
        self.run_id = run_id or f"nf-run-{uuid.uuid4()}"
        self.time_created = time_created or time.time()
        self.forward_spec = forward_spec

    @classmethod
    def from_database(cls, run_id: str, database: Database) -> 'NextflowRunEntity':
        nf_run = database.get_nf_run_by_run_id(run_id)
        return cls(analysis_id=nf_run.analysis_id,
                   keycloak_token=nf_run.keycloak_token,
                   run_id=nf_run.run_id,
                   time_created=nf_run.time_created,
                   forward_spec=nf_run.forward_spec)

    def __str__(self) -> str:
        return (f"NextflowRunEntity(analysis_id={self.analysis_id}, pipeline_name={self.pipeline_name}, "
                f"run_args={self.run_args}, run_id={self.run_id})")

    @property
    def results_prefix(self) -> str:
        return f"{config.run_prefix(self.run_id)}/{RESULTS_DIR}"

    # ---- start / stop ------------------------------------------------------------------------------

    def start(self,
              database: Database,
              inputs: list[InputRef] | None = None,
              kong_apikey: str | None = None,
              kong_datastore: str | None = None) -> dict[str, str]:
        if self.forward_spec is not None and not config.forward_allowed():
            raise HTTPException(status_code=403,
                                detail="forwarding results is disabled on this node (NF_ALLOW_FORWARD)")
        if self.pipeline_name is None or self.run_args is None:
            raise HTTPException(status_code=500,
                                detail=f"Missing value for pipeline_name and/or run_args in {self}")

        # "{run_id}" lets callers point outputs at a per-run location they cannot know beforehand,
        # e.g. --outdir s3://flame/Nextflow/{run_id}/results
        run_args = [arg.replace("{run_id}", self.run_id) for arg in self.run_args]
        if inputs:
            run_args += self._input_args(inputs, kong_apikey, kong_datastore)

        database.create_nf_run(self.run_id, self.analysis_id, self.keycloak_token, self.time_created,
                               forward_spec=self.forward_spec)
        try:
            create_nextflow_run(run_id=self.run_id, pipeline_name=self.pipeline_name, run_args=run_args,
                                namespace=get_current_namespace())
        except ApiException as e:
            error_message = f"Exception during nextflow run creation with {self}: {e}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)
        return {"status": "job submitted", "run_id": self.run_id}

    def _input_args(self, inputs: list[InputRef], kong_apikey: str | None, kong_datastore: str | None) -> list[str]:
        """`--<param_name> <url>` for each input.

        The launcher signs nothing: Kong's key-auth authenticates the apikey, its acl plugin enforces project
        isolation, and the minio-gateway plugin signs the upstream request. Both values come from the caller,
        which provisioned them via the hub adapter (the datastore's Kong service name is admin-chosen, so it
        cannot be derived from project_id).
        """
        for name, value in (("kong_apikey", kong_apikey), ("kong_datastore", kong_datastore)):
            if not value:
                raise HTTPException(status_code=400,
                                    detail=f"{name} is required in the request body when inputs are provided")
        kong_base_url = config.get_kong_base_url()

        def make_url(key: str) -> str:
            return build_object_url(kong_base_url, kong_datastore, key, kong_apikey)

        args = []
        for inp in inputs:
            if inp.samplesheet:
                # Workers read the small rewritten sheet from the internal store; its path cells point at Kong
                sheet = rewrite_samplesheet(fetch_object(kong_base_url, kong_datastore, inp.key, kong_apikey),
                                            make_url=make_url)
                key = f"{config.run_prefix(self.run_id)}/inputs/{inp.key.rsplit('/', 1)[-1]}"
                url = InternalMinioClient.from_config().put_object(config.get_internal_minio_bucket(), key, sheet)
            else:
                url = make_url(inp.key)
            args += [f"--{inp.param_name}", url]
        return args

    def stop(self) -> None:
        delete_job(self.run_id, get_current_namespace())

    # ---- conclude: results, forwarding, callback ---------------------------------------------------

    def conclude(self, run_status: str, storage_location: str, database: Database) -> None:
        print(f"Concluding run {self.run_id}: status={run_status}, location={storage_location}")
        try:
            manifest = InternalMinioClient.from_config().list_objects(config.get_internal_minio_bucket(),
                                                                      self.results_prefix)
        except Exception as e:
            print(f"Warning: could not list results of {self.run_id}: {e!r}")
            manifest = []
        row = database.get_nf_run_by_run_id(self.run_id)
        state = row.forward_state if row else None
        database.update_nf_run(self.run_id, run_status=run_status, manifest=manifest)

        payload = result_payload(self.run_id, run_status, storage_location, manifest)
        forward_status = (state or {}).get("status")
        if self.forward_spec is None:
            self._inform(payload)
        elif run_status != "succeeded":
            self._inform({**payload, "forward": {"status": "skipped", "error": {"reason": "run_not_succeeded"}}})
        elif forward_status == "done":  # webhook retry
            self._inform({**payload, "forward": state})
        elif forward_status == "running":  # it informs the analysis when done
            print(f"Forward of {self.run_id} already running")
        else:
            self.start_forward(database, payload, state or {})
        # TODO: delete the Job once the analysis reports done / after a TTL; kept for now to inspect logs

    def start_forward(self, database: Database, payload: dict, state: dict) -> None:
        # can take minutes to hours: do not hold the Job's webhook
        threading.Thread(target=self.forward_and_inform, args=(database, payload, state), daemon=True).start()

    def forward_and_inform(self, database: Database, payload: dict, state: dict) -> None:
        """Run the forward block of this run, then tell the analysis (callback carries the reference or failure)."""
        row = database.get_nf_run_by_run_id(self.run_id)
        bucket = config.get_internal_minio_bucket()
        try:
            internal = InternalMinioClient.from_config()

            def open_object(key: str):
                return internal.get_object(bucket, self.results_prefix + key)["Body"].iter_chunks(fwd.CHUNK)

            state = fwd.run_forward(
                files=fwd.select_files(row.manifest or [], self.forward_spec["keys"]),
                open_object=open_object,
                spec=self.forward_spec,
                token=row.keycloak_token,
                storage_url=config.get_storage_service_url(),
                part_size=config.get_forward_part_size(),
                max_object_size=config.get_forward_max_object_size(),
                max_total_size=config.get_forward_max_total_size(),
                attempts=config.get_forward_attempts(),
                timeout_s=config.get_forward_timeout(),
                state=state,
                save_state=lambda st: database.update_nf_run(self.run_id, forward_state=dict(st)))
        except Exception as e:
            state = {**state, "status": "failed", "error": {"reason": "internal_error", "detail": repr(e)[:300]}}
            database.update_nf_run(self.run_id, forward_state=state)
        print(f"Forward of {self.run_id}: {state.get('status')} {state.get('error') or ''}")
        self._inform({**payload, "forward": state})

    def _inform(self, payload: dict) -> None:
        # Tell the analysis (POST /analysis/nextflow on its nginx sidecar, allowed from this pod only).
        # A failure must not fail the Job's webhook: it would retry and inform the analysis again.
        try:
            AnalysisClient(self.analysis_id).inform_analysis(payload)
            print(f"Informed analysis {self.analysis_id} about run {self.run_id}")
        except Exception as e:
            print(f"Warning: could not inform analysis {self.analysis_id} about run {self.run_id}: {e!r}")
