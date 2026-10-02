from httpx import Client

from src.k8s.utils import find_service_names, get_current_namespace


class AnalysisClient:
    """Talks to an analysis through its nginx sidecar."""

    def __init__(self, analysis_id: str) -> None:
        names = [name for name in find_service_names("component=flame-analysis-nginx", get_current_namespace())
                 if analysis_id in name]
        if not names:
            raise LookupError(f"No nginx service found for analysis {analysis_id}")
        # a restarted analysis gets a new service with a higher trailing counter
        latest = max(names, key=lambda name: int(name.rsplit("-", 1)[-1]))
        self.client = Client(base_url=f"http://{latest}:80/analysis", follow_redirects=True)

    def inform_analysis(self, result: dict) -> None:
        self.client.post("/nextflow", json=result).raise_for_status()
