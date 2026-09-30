"""FLAME analysis that starts a Nextflow run through the launcher (Hub-driven E2E test).

Analyzer (node1): reads the project's S3 source (only the samplesheet CSV lives there) and
POSTs /nextflow/run on its own nginx sidecar, which routes it to the launcher.
Aggregator (node4): submits the returned run ids as the final result.

The analyzer then waits for the launcher's conclude callback (POST /analysis/nextflow through the
nginx sidecar) and returns the run status and result location, so the analysis only finishes once the
pipeline has.
"""
import json
import os
import threading
import urllib.error
import urllib.request

import uvicorn
from fastapi import Request

import flamesdk.resources.rest_api as _flame_rest_api
from flame.star import StarModel, StarAnalyzer, StarAggregator

# hub_run.py rewrites this line for `create hello`
PIPELINE_CHOICE = "demo"
PIPELINES = {
    # {run_id} is substituted by the launcher, so results land in the run's own folder of the internal store
    "demo": ("nf-core/demo",
             ["-r", "1.0.1", "-profile", "docker", "--outdir", "s3://flame/Nextflow/{run_id}/results",
              # nf-schema would otherwise try to validate s3://ngi-igenomes on the internal store
              "--validate_params", "false"]),
    # tiny pipeline without containers/inputs: exercises launch -> conclude(succeeded) quickly
    "hello": ("nextflow-io/hello", []),
}
PIPELINE, RUN_ARGS = PIPELINES[PIPELINE_CHOICE]
CONCLUDE_TIMEOUT_S = int(os.getenv("NF_CONCLUDE_TIMEOUT_S", "3600"))

# ---- receiving the launcher's conclude callback ----------------------------------------------------
# flamesdk has no /nextflow endpoint yet and builds its FastAPI app inside FlameAPI.__init__, so this test
# adds the route by wrapping uvicorn.run as seen by the SDK. Replace with proper SDK support.
_concluded: dict[str, dict] = {}
_concluded_cv = threading.Condition()


async def _nextflow_callback(request: Request) -> dict:
    body = await request.json()
    with _concluded_cv:
        _concluded[body["run_id"]] = body
        _concluded_cv.notify_all()
    return {"status": "received"}


class _UvicornWithNextflowRoute:
    def __getattr__(self, name):
        return getattr(uvicorn, name)

    @staticmethod
    def run(app, *args, **kwargs):
        app.add_api_route("/nextflow", _nextflow_callback, methods=["POST"])
        return uvicorn.run(app, *args, **kwargs)


_flame_rest_api.uvicorn = _UvicornWithNextflowRoute()


def wait_for_conclusion(run_id: str, timeout: int) -> dict | None:
    with _concluded_cv:
        _concluded_cv.wait_for(lambda: run_id in _concluded, timeout=timeout)
        return _concluded.get(run_id)


class MyAnalyzer(StarAnalyzer):
    def __init__(self, flame):
        super().__init__(flame)

    def _s3_source_name(self) -> str:
        sources = self.flame.get_data_sources() or []
        for source in sources:
            if any(str(p).rstrip("/").endswith("/s3") for p in source.get("paths", [])):
                return source["name"]
        return sources[0]["name"]

    def analysis_method(self, data, aggregator_results):
        keys = [k for source in data for k in source]
        csv_key = next((k for k in keys if k.lower().endswith(".csv")), keys[0])
        datastore = self._s3_source_name()
        self.flame.flame_log(f"samplesheet={csv_key} datastore={datastore}")

        body = {
            "analysis_id": os.getenv("ANALYSIS_ID", "unknown"),  # nginx overrides via X-Flame-Analysis-Id
            "pipeline_name": PIPELINE,
            "run_args": RUN_ARGS,
            "keycloak_token": getattr(self.flame.config, "keycloak_token", None) or "unused",
            "inputs": [{"key": csv_key, "param_name": "input", "samplesheet": True}],
            "kong_apikey": os.getenv("DATA_SOURCE_TOKEN"),
            "kong_datastore": datastore,
        }
        req = urllib.request.Request(
            f"http://nginx-{os.getenv('DEPLOYMENT_NAME')}/nextflow/run",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                result = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            result = {"error": e.code, "detail": e.read().decode()[:500]}
        self.flame.flame_log(f"launcher response: {result}")
        if "run_id" not in result:
            return json.dumps(result)

        self.flame.flame_log(f"waiting up to {CONCLUDE_TIMEOUT_S}s for {result['run_id']} to conclude")
        concluded = wait_for_conclusion(result["run_id"], CONCLUDE_TIMEOUT_S)
        if concluded is None:
            result["run_status"] = "timeout"
        else:
            result.update(run_status=concluded["run_status"], storage_location=concluded["storage_location"])
        self.flame.flame_log(f"run concluded: {result}")
        return json.dumps(result)


class MyAggregator(StarAggregator):
    def __init__(self, flame):
        super().__init__(flame)

    def aggregation_method(self, analysis_results):
        return json.dumps([json.loads(r) for r in analysis_results])

    def has_converged(self, result, last_result):
        return True


def main():
    StarModel(analyzer=MyAnalyzer,
              aggregator=MyAggregator,
              data_type="s3",
              query=None,
              simple_analysis=True,
              output_type="str")


if __name__ == "__main__":
    main()
