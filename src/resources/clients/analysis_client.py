from httpx import Client, HTTPStatusError

from src.k8s.utils import find_k8s_resources, get_current_namespace


class AnalysisClient:
    def __init__(self, analysis_id: str) -> None:
        analysis_nginx_client_base_url = find_k8s_resources('service',
                                                            'label',
                                                            f"component=flame-analysis-nginx",
                                                            manual_name_selector=analysis_id,
                                                            namespace=get_current_namespace())
        if type(analysis_nginx_client_base_url) == list:
            analysis_nginx_client_base_url = self._find_latest_url(analysis_nginx_client_base_url)

        self.client = Client(base_url=f"http://{analysis_nginx_client_base_url}:80/analysis",
                             follow_redirects=True)

    def inform_analysis(self, result: dict) -> dict:
        response = self.client.post(f"/nextflow",
                                    json=result,
                                    headers={"Content-Type": "application/json"})
        try:
            response.raise_for_status()
        except HTTPStatusError as e:
            print("HTTP Error in analysis client:", repr(e))

        return response.json()

    def _find_latest_url(self, urls: list[str]) -> str:
        nginx_url = ""
        latest_count = -1
        for url in urls:
            count = int(url.rsplit('-', 1)[-1])
            if count > latest_count:
                nginx_url = url
        return nginx_url
