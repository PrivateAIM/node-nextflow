"""FLAME analysis that starts a Nextflow run through the launcher (Hub-driven E2E test).

Analyzer (node1): reads the pipeline's samplesheet from the project's S3 source and POSTs /nextflow/run
on its own nginx sidecar, which routes it to the launcher. For `demo` only the samplesheet lives in the
source (its FASTQs are https URLs); for `sarek` the FASTQs and references live there too (under sarek/)
and reach the workers through Kong.
Aggregator (node4): submits the returned run ids as the final result.

The analyzer then waits for the launcher's conclude callback (POST /analysis/nextflow through the
nginx sidecar) and returns the run status and result location, so the analysis only finishes once the
pipeline has.
"""
import gzip
import hashlib
import io
import json
import os
import tarfile
import threading
import urllib.error
import urllib.request

import uvicorn
from fastapi import Request

import flamesdk.resources.rest_api as _flame_rest_api
from flame.star import StarModel, StarAnalyzer, StarAggregator

# {run_id} is substituted by the launcher, so results land in the run's own folder of the internal store
OUTDIR = ["--outdir", "s3://flame/Nextflow/{run_id}/results"]
# Public reference data lives once per node in the internal store, not in the project store: nf-core pipelines
# open reference params with Channel.fromPath, which treats '?' as a glob and so drops a Kong URL's ?apikey=.
SAREK_REFERENCE = "s3://flame/references/sarek-test/"

# hub_run.py rewrites this line for `create hello|sarek`
PIPELINE_CHOICE = "demo"
PIPELINES = {
    "demo": {
        "pipeline": "nf-core/demo",
        # nf-schema would otherwise try to validate s3://ngi-igenomes on the internal store
        "run_args": ["-r", "1.0.1", "-profile", "docker", *OUTDIR, "--validate_params", "false"],
        "samplesheet": "samplesheet.csv",
    },
    # tiny pipeline without containers/inputs: exercises launch -> conclude(succeeded) quickly
    "hello": {"pipeline": "nextflow-io/hello", "run_args": [], "samplesheet": "samplesheet.csv"},
    # Sarek's test data set (chr22 subset, one sample, two lanes): samplesheet + FASTQs in the project store (read
    # through Kong), references from the node's internal store. Without `-profile test`, which takes both from GitHub.
    "sarek": {
        "pipeline": "nf-core/sarek",
        "run_args": ["-r", "3.5.1", "-profile", "docker", *OUTDIR, "--validate_params", "false",
                     "--igenomes_ignore", "--tools", "strelka", "--split_fastq", "0"],
        "samplesheet": "sarek/samplesheet.csv",
        "references": {
            "fasta": "genome.fasta",
            "fasta_fai": "genome.fasta.fai",
            "dict": "genome.dict",
            "intervals": "genome.interval_list",
            "dbsnp": "dbsnp_146.hg38.vcf.gz",
            "dbsnp_tbi": "dbsnp_146.hg38.vcf.gz.tbi",
            "germline_resource": "gnomAD.r2.1.1.vcf.gz",
            "germline_resource_tbi": "gnomAD.r2.1.1.vcf.gz.tbi",
            "known_indels": "mills_and_1000G.indels.vcf.gz",
            "known_indels_tbi": "mills_and_1000G.indels.vcf.gz.tbi",
            "ngscheckmate_bed": "SNP_GRCh38_hg38_wChr.bed",
        },
    },
}
PIPELINE = PIPELINES[PIPELINE_CHOICE]
# hub_run.py rewrites this line for `create demo-forward`: the launcher forwards results/ to the aggregator
FORWARD_RESULTS = False
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
        csv_key = PIPELINE["samplesheet"]
        if not any(csv_key in source for source in data):
            return {"error": f"samplesheet {csv_key} not found in the project's S3 source"}
        datastore = self._s3_source_name()
        self.flame.flame_log(f"samplesheet={csv_key} datastore={datastore}")

        run_args = list(PIPELINE["run_args"])
        for param, name in PIPELINE.get("references", {}).items():
            run_args += [f"--{param}", SAREK_REFERENCE + name]
        body = {
            "analysis_id": os.getenv("ANALYSIS_ID", "unknown"),  # nginx overrides via X-Flame-Analysis-Id
            "pipeline_name": PIPELINE["pipeline"],
            "run_args": run_args,
            "keycloak_token": getattr(self.flame.config, "keycloak_token", None) or "unused",
            "inputs": [{"key": csv_key, "param_name": "input", "samplesheet": True}],
            "kong_apikey": os.getenv("DATA_SOURCE_TOKEN"),
            "kong_datastore": datastore,
        }
        if FORWARD_RESULTS:
            # the launcher sends the result files to the aggregator itself when the run concludes
            body["forward"] = {"keys": ["**"], "to": self.flame.get_aggregator_id()}
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
            return result

        self.flame.flame_log(f"waiting up to {CONCLUDE_TIMEOUT_S}s for {result['run_id']} to conclude")
        concluded = wait_for_conclusion(result["run_id"], CONCLUDE_TIMEOUT_S)
        if concluded is None:
            result["run_status"] = "timeout"
        else:
            result.update(run_status=concluded["run_status"], storage_location=concluded["storage_location"],
                          files=concluded.get("files"))
            if concluded.get("forward"):
                result["forward"] = concluded["forward"]
        self.flame.flame_log(f"run concluded: {result}")
        return result


class MyAggregator(StarAggregator):
    def __init__(self, flame):
        super().__init__(flame)

    def fetch_forwarded(self, ref: dict) -> dict:
        """Stand-in for flame.transfer.fetch: fetch the parts in order, verify, (gunzip,) untar."""
        if ref.get("status") != "done":
            return {"fetched": False, "forward": ref}
        # the launcher uploads raw tar bytes, but get_intermediate_data() always unpickles; read the parts raw
        storage = self.flame._storage_api.storage_client.client

        def raw_part(p):
            resp = storage.get(f"/intermediate/{p['url'].split('/')[-1]}", timeout=600)
            resp.raise_for_status()
            return resp.content

        stream = b"".join(raw_part(p) for p in sorted(ref["parts"], key=lambda p: p["index"]))
        ok = len(stream) == ref["total_size"] and hashlib.sha256(stream).hexdigest() == ref["sha256"]
        if ref["compression"] == "gzip":
            stream = gzip.decompress(stream)
        with tarfile.open(fileobj=io.BytesIO(stream)) as t:
            got = sorted((m.name, m.size) for m in t.getmembers())
        ok = ok and got == sorted((f["key"], f["size"]) for f in ref["files"])
        return {"fetched": ok, "parts": len(ref["parts"]), "total_size": ref["total_size"], "files": got}

    def aggregation_method(self, analysis_results):
        out = []
        for r in (r if isinstance(r, dict) else json.loads(r) for r in analysis_results):
            if r.get("forward"):
                r["received"] = self.fetch_forwarded(r.pop("forward"))
                self.flame.flame_log(f"received forwarded results: {r['received']}")
            out.append(r)
        return json.dumps(out)

    def has_converged(self, result, last_result):
        return True


def main():
    StarModel(analyzer=MyAnalyzer,
              aggregator=MyAggregator,
              data_type="s3",
              query=[PIPELINE["samplesheet"]],  # only the sheet; the data files go to the workers via Kong
              simple_analysis=True,
              output_type="str")


if __name__ == "__main__":
    main()
