import uvicorn

from fastapi import APIRouter, FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.resources.database.entity import Database
from src.api.oauth import valid_access_token
from src.resources.nextflow_run.entity import NextflowRunEntity, CreateNextflowRun, ConcludeNextflowRun


class FlameNextflowAPI:
    def __init__(self, database: Database, namespace: str = 'default'):
        self.database = database

        self.namespace = namespace
        app = FastAPI(title="FLAME Nextflow Job Launcher",
                      docs_url="/api/docs",
                      redoc_url="/api/redoc",
                      openapi_url="/api/v1/openapi.json")

        origins = [
            "http://localhost:8080/",
        ]

        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        router = APIRouter()
        router.add_api_route("/run",
                             self.run_call,
                             dependencies=[Depends(valid_access_token)],
                             methods=["POST"],
                             response_class=JSONResponse)
        router.add_api_route("/stop/{analysis_id}",
                             self.interrupt_call,
                             dependencies=[Depends(valid_access_token)],
                             methods=["POST"],
                             response_class=JSONResponse)
        router.add_api_route("/conclude",
                             self.conclude_call,
                             #dependencies=[Depends(valid_access_token)], #TODO decide on auth for this endpoint
                             methods=["POST"],
                             response_class=JSONResponse)
        router.add_api_route("/healthz",
                             self.health_call,
                             methods=["GET"],
                             response_class=JSONResponse)

        app.include_router(
            router,
            prefix="/nextflow",
        )

        uvicorn.run(app, host="0.0.0.0", port=8000)

    def run_call(self, body: CreateNextflowRun):
        nf_run = NextflowRunEntity(analysis_id=body.analysis_id,
                                   pipeline_name=body.pipeline_name,
                                   run_args=body.run_args,
                                   keycloak_token=body.keycloak_token)
        return nf_run.start(self.database, body.input_location)

    def conclude_call(self, body: ConcludeNextflowRun):
        nf_run = NextflowRunEntity.from_database(run_id=body.run_id, database=self.database)
        nf_run.conclude(body.run_status, body.storage_location)
        return {'status': f"Nextflow run with id={body.run_id} concluded."}

    def interrupt_call(self, analysis_id: str):
        for nf_db in self.database.get_nf_runs_by_analysis_id(analysis_id):
            nf_run = NextflowRunEntity.from_database(run_id=nf_db.run_id, database=self.database)
            nf_run.stop()
        return {'status': f"Nextflow runs for analysis_id={analysis_id} interrupted."}

    def health_call(self):
        return {'status': "ok"}
