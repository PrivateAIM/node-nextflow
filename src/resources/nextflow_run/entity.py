import uuid
import time
from typing import Optional

from fastapi import HTTPException
from pydantic import BaseModel

from src.resources.database.entity import Database
from src.k8s.kubernetes import create_nextflow_run
from src.k8s.utils import get_current_namespace, delete_k8s_resource


class NextflowRunEntity:
    def __init__(self,
                 analysis_id: str,
                 keycloak_token: str,
                 pipeline_name: Optional[str] = None,
                 run_args: Optional[list[str]] = None,
                 run_id: Optional[str] = None,
                 time_created: Optional[float] = None) -> None:
        self.analysis_id = analysis_id
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

    def start(self, database: Database, input_location: str) -> dict[str, str]:
        if None not in [self.pipeline_name, self.run_args]:
            database.create_nf_run(self.run_id,
                                   self.analysis_id,
                                   self.keycloak_token,
                                   self.time_created)

            # TODO: Retrieve input data from StorageClient [Step 2-3]
            # storage_client = StorageClient(self.keycloak_token)
            # input_data = storage_client.retrieve_data(input_location)

            # Execute Nextflow run command [Step 4]
            try:
                create_nextflow_run(run_id=self.run_id,
                                    pipeline_name=self.pipeline_name,
                                    run_args=self.run_args,
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

    def stop(self) -> None:
        # Stop Nextflow run, during cleanup [Step 10] or during manual interrupt
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
    pipeline_name: str = 'pipeline_name'
    run_args: list[str] = []
    keycloak_token: str = 'keycloak_token'
    input_location: str = 'input_location'


class ConcludeNextflowRun(BaseModel):
    run_id: str = 'analysis_id'
    run_status: str = 'run_status'
    storage_location: str = 'storage_location'
