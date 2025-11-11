import uuid
import time
from pydantic import BaseModel

from src.resources.database.entity import Database
from src.resources.database.db_models import NextflowRunDB


class NextflowRunEntity:
    def __init__(self,
                 analysis_id: str,
                 pipeline_name: str,
                 run_args: list[str],
                 run_id: Optional[str],
                 database: Optional[Database]) -> None:
        if database is not None:
            self.run_id = f"nf-run-{uuid.uuid4()}"
            self.analysis_id = analysis_id
            self.pipeline_name = pipeline_name
            self.run_args = run_args
        else:
            self.run_id = run_id
            nf_run = database.get_nf_run_by_run_id(run_id)
            self.analysis_id = nf_run.analysis_id
            self.pipeline_name = nf_run.pipeline_name
            self.run_args = nf_run.run_args
        self.time_created: float = time.time()
        self.time_updated: float = time.time()

    def start(self, database: Database) -> None:
        database.create_nf_run(self.run_id, self.analysis_id)
        # TODO: Retrieve data from StorageClient
        # TODO: Execute Nextflow run command

    def stop(self) -> None:
        # TODO: Stop Nextflow run
        pass

    def conclude(self, run_status: str, storage_location: str) -> None:
        # TODO: Check run_status
        # TODO: If successful, create result_storage with StorageClient using storage_location
        # TODO: Inform analysis via AnalysisClient about conclusion (deliver result_storage id, if successful)
        self.stop()


def read_db_nf_run(nf_run: NextflowRunDB) -> NextflowRunEntity:
    return NextflowRunEntity(nf_run.analysis_id, nf_run.pipeline_name, nf_run.run_args)


class CreateNextflowRun(BaseModel):
    analysis_id: str = 'analysis_id'
    pipeline_name: str = 'pipeline_name'
    run_args: list[str] = []
    input_location: str = 'input_location'
    output_location: str = 'output_location'


class ConcludeNextflowRun(BaseModel):
    run_id: str = 'analysis_id'
    run_status: str = 'run_status'
    storage_location: str = 'storage_location'
