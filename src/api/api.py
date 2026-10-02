import re
from typing import Optional

import uvicorn
from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from src import config
from src.resources.database.db_models import NextflowRunDB
from src.resources.database.entity import Database
from src.resources.nextflow_run.entity import RESULTS_DIR, ConcludeNextflowRun, CreateNextflowRun, NextflowRunEntity
from src.storage.forward import normalize_key
from src.storage.internal_minio import InternalMinioClient


def parse_range(header: str, size: int) -> Optional[tuple[int, int]]:
    """Inclusive (start, end) of a single-range `Range: bytes=...` header, None for the whole file."""
    m = re.fullmatch(r"bytes=(\d*)-(\d*)", header)
    if not m or not (m.group(1) or m.group(2)):
        return None
    first, last = m.groups()
    if first:
        start, end = int(first), min(int(last), size - 1) if last else size - 1
    else:  # suffix range: the last N bytes
        start, end = max(size - int(last), 0), size - 1
    if start > end or start >= size:
        raise HTTPException(status_code=416, detail="range not satisfiable",
                            headers={"Content-Range": f"bytes */{size}"})
    return start, end


class FlameNextflowAPI:
    def __init__(self, database: Database) -> None:
        self.database = database
        self.app = FastAPI(title="FLAME Nextflow Job Launcher",
                           docs_url="/api/docs",
                           redoc_url="/api/redoc",
                           openapi_url="/api/v1/openapi.json")
        self.app.add_middleware(CORSMiddleware,
                                allow_origins=["http://localhost:8080/"],
                                allow_credentials=True,
                                allow_methods=["*"],
                                allow_headers=["*"])

        # TODO: decide on auth; src.api.oauth.valid_access_token is ready to be added as a dependency
        router = APIRouter()
        router.add_api_route("/run", self.run_call, methods=["POST"])
        router.add_api_route("/stop/{analysis_id}", self.interrupt_call, methods=["POST"])
        router.add_api_route("/conclude", self.conclude_call, methods=["POST"])
        router.add_api_route("/results/{run_id}", self.results_call, methods=["GET"])
        router.add_api_route("/results/{run_id}/{key:path}", self.result_file_call, methods=["GET"])
        router.add_api_route("/healthz", self.health_call, methods=["GET"])
        self.app.include_router(router, prefix="/nextflow")

    def serve(self) -> None:
        uvicorn.run(self.app, host="0.0.0.0", port=8000)

    def run_call(self, body: CreateNextflowRun, x_flame_analysis_id: Optional[str] = Header(None)):
        # Calls from an analysis pass its nginx sidecar, which sets this header from the analysis config
        # and overwrites any client value, so it takes precedence over the self-reported body field.
        nf_run = NextflowRunEntity(analysis_id=x_flame_analysis_id or body.analysis_id,
                                   keycloak_token=body.keycloak_token,
                                   pipeline_name=body.pipeline_name,
                                   run_args=body.run_args,
                                   forward_spec=body.forward.model_dump() if body.forward else None)
        return nf_run.start(self.database, body.inputs, body.kong_apikey, body.kong_datastore)

    def interrupt_call(self, analysis_id: str):
        for row in self.database.get_nf_runs_by_analysis_id(analysis_id):
            NextflowRunEntity.from_database(row.run_id, self.database).stop()
        return {'status': f"Nextflow runs for analysis_id={analysis_id} interrupted."}

    def conclude_call(self, body: ConcludeNextflowRun):
        nf_run = NextflowRunEntity.from_database(body.run_id, self.database)
        nf_run.conclude(body.run_status, body.storage_location, self.database)
        return {'status': f"Nextflow run with id={body.run_id} concluded."}

    def _owned_run(self, run_id: str, x_flame_analysis_id: Optional[str]) -> NextflowRunDB:
        """Only the analysis that started a run may read it; nginx sets the header and overwrites client values."""
        row = self.database.get_nf_run_by_run_id(run_id)
        if row is None or x_flame_analysis_id is None or row.analysis_id != x_flame_analysis_id:
            raise HTTPException(status_code=404, detail="run not found")  # same answer for foreign runs
        return row

    def results_call(self, run_id: str, x_flame_analysis_id: Optional[str] = Header(None)):
        row = self._owned_run(run_id, x_flame_analysis_id)
        return {"run_id": row.run_id, "run_status": row.run_status, "results_prefix": RESULTS_DIR,
                "files": row.manifest, "forward": row.forward_state}

    def result_file_call(self, run_id: str, key: str, request: Request,
                         x_flame_analysis_id: Optional[str] = Header(None)):
        row = self._owned_run(run_id, x_flame_analysis_id)
        try:
            key = normalize_key(key)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        entry = next((f for f in row.manifest or [] if f["key"] == key), None)
        if entry is None:
            raise HTTPException(status_code=404, detail="no such result file")

        size = entry["size"]
        headers = {"Accept-Ranges": "bytes", "Content-Length": str(size)}
        status, byte_range = 200, None
        if (requested := parse_range(request.headers.get("range", ""), size)) is not None:
            start, end = requested
            status, byte_range = 206, f"bytes={start}-{end}"
            headers.update({"Content-Range": f"bytes {start}-{end}/{size}", "Content-Length": str(end - start + 1)})

        obj = InternalMinioClient.from_config().get_object(
            config.get_internal_minio_bucket(), f"{config.run_prefix(run_id)}/{RESULTS_DIR}{key}", byte_range)
        return StreamingResponse(obj["Body"].iter_chunks(1024 * 1024), status_code=status, headers=headers,
                                 media_type="application/octet-stream")

    def health_call(self):
        return {'status': "ok"}
