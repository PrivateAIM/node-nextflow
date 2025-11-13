import os
import uuid
import time
from typing import Optional

from fastapi import HTTPException
from pydantic import BaseModel
from io import BytesIO

from src.resources.clients.analysis_client import AnalysisClient
from src.resources.clients.storage_client import StorageClient
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
            # Retrieve and delete data from StorageClient [Step 3]
            storage_client = StorageClient(self.keycloak_token)
            input_data = storage_client.retrieve_data(input_location)

            # Execute Nextflow run command using input- and output_location [Step 4]
            try:
                create_nextflow_run(run=self, input_data=input_data, namespace=get_current_namespace())
                return {"status": "job submitted"}
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
        storage_client = StorageClient(self.keycloak_token)
        analysis_client = AnalysisClient(self.analysis_id)

        # If successful, create result_storage with StorageClient using storage_location  [Step 7]
        storage_id = None
        if run_status != 'success':
            with open(storage_location, 'rb') as result_file:
                storage_id = storage_client.push_result(BytesIO(result_file.read()))

        # Inform analysis via AnalysisClient about conclusion (deliver result_storage id, if successful)  [Step 8]
        analysis_client.inform_analysis({"run_status": run_status, "storage_id": storage_id})
        # Cleanup Nextflow Run and shared PVC [Step 10] # TODO: Consider every run gets its own PVC with resource limits that is created/deleted per run
        self.stop()
        os.remove(storage_location)


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
