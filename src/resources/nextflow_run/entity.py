import uuid
import time
from typing import Optional

from fastapi import HTTPException
from pydantic import BaseModel

import src.config as config
from src.resources.clients.analysis_client import AnalysisClient
from src.resources.database.entity import Database
from src.k8s.kubernetes import create_nextflow_run
from src.k8s.utils import get_current_namespace, delete_k8s_resource
from src.storage.kong_inputs import build_object_url, fetch_object, rewrite_samplesheet
from src.storage.internal_minio import InternalMinioClient


class InputRef(BaseModel):
    key: str
    param_name: str
    samplesheet: bool = False


class NextflowRunEntity:
    def __init__(self,
                 analysis_id: str,
                 project_id: str, #TODO
                 keycloak_token: str,
                 pipeline_name: Optional[str] = None,
                 run_args: Optional[list[str]] = None,
                 run_id: Optional[str] = None,
                 time_created: Optional[float] = None) -> None:
        self.analysis_id = analysis_id
        self.project_id = project_id #TODO
        self.pipeline_name = pipeline_name
        self.run_args = run_args
        self.keycloak_token = keycloak_token
        self.run_id = f"nf-run-{str(uuid.uuid4())}" if run_id is None else run_id
        self.time_created: float = time.time() if time_created is None else time_created

    @classmethod
    def from_database(cls, run_id: str, database: Database) -> 'NextflowRunEntity':
        nf_run = database.get_nf_run_by_run_id(run_id)
        return cls(analysis_id=nf_run.analysis_id,
                   keycloak_token=nf_run.keycloak_token,
                   run_id=nf_run.run_id,
                   time_created=nf_run.time_created)

    def __str__(self) -> str:
        return (f"NextflowRunEntity("
                f"analysis_id={self.analysis_id}, "
                f"pipeline_name={self.pipeline_name}, "
                f"run_args={self.run_args}, "
                f"run_id={self.run_id})")

    def start(self, database: Database, inputs: list[InputRef] | None = None,
              kong_apikey: str | None = None,
              kong_datastore: str | None = None) -> dict[str, str]:
        if None not in [self.pipeline_name, self.run_args]:
            effective_run_args = list(self.run_args)

            if inputs:
                # The launcher signs nothing: Kong's key-auth authenticates the
                # apikey, its acl plugin enforces project isolation, and the
                # minio-gateway plugin signs the upstream request. Both values
                # come from the caller, which provisioned them via the hub
                # adapter (the datastore's Kong service name is admin-chosen, so
                # it cannot be derived from project_id).
                if not kong_apikey:
                    raise HTTPException(
                        status_code=400,
                        detail="kong_apikey is required in the request body when inputs are provided",
                    )
                if not kong_datastore:
                    raise HTTPException(
                        status_code=400,
                        detail="kong_datastore is required in the request body when inputs are provided",
                    )
                kong_base_url = config.get_kong_base_url()

                def make_url(key: str) -> str:
                    return build_object_url(kong_base_url, kong_datastore, key, kong_apikey)

                internal: InternalMinioClient | None = None
                for inp in inputs:
                    if inp.samplesheet:
                        if internal is None:
                            internal = InternalMinioClient(
                                endpoint=config.get_internal_minio_endpoint(),
                                access_key=config.get_internal_minio_access_key(),
                                secret_key=config.get_internal_minio_secret_key(),
                            )
                        csv_bytes = fetch_object(kong_base_url, kong_datastore,
                                                 inp.key, kong_apikey)
                        rewritten = rewrite_samplesheet(csv_bytes, make_url=make_url)
                        basename = inp.key.rsplit("/", 1)[-1]
                        internal_key = (
                            f"{config.get_internal_minio_prefix()}/{self.run_id}/inputs/{basename}"
                        )
                        uri = internal.put_object(
                            config.get_internal_minio_bucket(), internal_key, rewritten,
                        )
                        effective_run_args.extend([f"--{inp.param_name}", uri])
                    else:
                        effective_run_args.extend([f"--{inp.param_name}", make_url(inp.key)])

            database.create_nf_run(self.run_id,
                                   self.analysis_id,
                                   self.keycloak_token,
                                   self.time_created)

            try:
                create_nextflow_run(run_id=self.run_id,
                                    pipeline_name=self.pipeline_name,
                                    run_args=effective_run_args,
                                    namespace=get_current_namespace())
                return {"status": "job submitted", "run_id": self.run_id}
            except HTTPException as e:
                error_message = f"Exception during nextflow run creation with {str(self)}: {e}"
                print(error_message)
                raise HTTPException(status_code=500, detail=error_message)
        else:
            raise HTTPException(status_code=500,
                                detail=f"Exception during start() function in {str(self)}: "
                                       f"Missing value for pipeline_name and/or run_args")

    def _get_project_id(self) -> str:
        analysis_client = AnalysisClient(self.analysis_id)
        return analysis_client.get_project_id()

    def stop(self) -> None:
        delete_k8s_resource(name=self.run_id, resource_type='job', namespace=get_current_namespace())

    def conclude(self, run_status: str, storage_location: str) -> None:
        print(f"Concluding run {self.run_id}: status={run_status}, location={storage_location}")

        # TODO: Step 7 - Wrap result files in tar & move to analysis project folder in MinIO
        # TODO: Step 8 - Inform analysis via AnalysisClient
        # TODO: Step 9 - Move result to global storage or load to analysis via Kong

        # TODO: re-enable cleanup after debugging
        # For now, keep the job around so we can inspect logs
        # try:
        #     self.stop()
        # except Exception as e:
        #     print(f"Warning: cleanup failed for {self.run_id}: {e}")


class CreateNextflowRun(BaseModel):
    analysis_id: str = 'analysis_id'
    project_id: str = 'project_id'
    pipeline_name: str = 'pipeline_name'
    run_args: list[str] = []
    keycloak_token: str = 'keycloak_token'
    inputs: list[InputRef] = []
    kong_apikey: Optional[str] = None
    kong_datastore: Optional[str] = None


class ConcludeNextflowRun(BaseModel):
    run_id: str = 'analysis_id'
    run_status: str = 'run_status'
    storage_location: str = 'storage_location'
