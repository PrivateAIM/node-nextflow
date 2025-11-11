import uvicorn

from fastapi import APIRouter, FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.resources.nextflow_run.entity import read_db_nf_run, CreateNextflowRun, ConcludeNextflowRun


class FlameNexflowAPI:
    def __init__(self, database: Database, namespace: str = 'default'):
        self.database = database

        self.node_id = get_node_id_by_robot(self.hub_core_client, robot_id) if self.hub_core_client else None
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
                             dependencies=[Depends(valid_access_token)],
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

    def run_call(self, body: CreateNextflowRun) -> JSONResponse:
        nf_run = NextflowRunEntity(analysis_id=body.analysis_id,
                                   pipeline_name=body.pipeline_name,
                                   run_args=body.run_args)
        nf_run.start(self.database)
        return {'status': f"Nextflow run started (id={nf_run.run_id})."}

    def conclude_call(self, body: ConcludeNextflowRun) -> JSONResponse:
        nf_run = NextflowRunEntity(run_id=body.run_id, database=self.database)
        nf_run.conclude(body.run_status, body.storage_location)
        return {'status': f"Nextflow run with id={run_id} concluded."}

    def interrupt_call(self, analysis_id: str) -> JSONResponse:
        for nf_db in self.database.get_nf_runs_by_analysis_id(analysis_id):
            nf_run = NextflowRunEntity(run_id=nf_db.run_id, database=self.database)
            nf_run.stop()
        return {'status': f"Nextflow runs for analysis_id={analysis_id} interrupted."}

    def health_call(self):
        main_alive = threading.main_thread().is_alive()
        if not main_alive:
            raise RuntimeError("Main thread is not alive.")
        else:
            return {'status': "ok"}
