from httpx import Client, HTTPStatusError
import uuid
from datetime import datetime
from typing import Any
from io import BytesIO
import pickle

from src.k8s.utils import find_k8s_resources, get_current_namespace


class StorageClient:
    def __init__(self, keycloak_token: str) -> None:
        self.keycloak_token = keycloak_token
        self.result_client_base_url = find_k8s_resources('service',
                                                         'label',
                                                         'component=flame-result-service',
                                                         namespace=get_current_namespace())
        self.client = Client(base_url=f"http://{self.result_client_base_url}:8080/storage",
                             headers={"Authorization": f"Bearer {keycloak_token}"},
                             follow_redirects=True)

    def retrieve_data(self, storage_id: str) -> Any:
        response = self.client.get(f"/local/{storage_id}")
        try:
            response.raise_for_status()
            return pickle.loads(BytesIO(response.content).read())
        except HTTPStatusError as e:
            print("HTTP Error in result client during download:", repr(e))

    def push_result(self, result: BytesIO) -> str:
        request_path = "/local/"
        response = self.client.put(request_path,
                                   files={
                                       "file": (f"result_{str(uuid.uuid4())[:4]}_{datetime.now().strftime('%y%m%d%H%M%S')}",
                                                result)},
                                   headers=[('Connection', 'close')])
        try:
            response.raise_for_status()
        except HTTPStatusError as e:
            print("HTTP Error in result client during upload:", repr(e))

        return response.json()['id']
