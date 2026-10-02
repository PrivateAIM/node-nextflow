from kubernetes import client, config

NAMESPACE_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


def load_cluster_config() -> None:
    config.load_incluster_config()


def get_current_namespace() -> str:
    try:
        with open(NAMESPACE_FILE) as file:
            return file.read().strip()
    except FileNotFoundError:
        return "default"


def find_service_names(label_selector: str, namespace: str) -> list[str]:
    services = client.CoreV1Api().list_namespaced_service(namespace=namespace, label_selector=label_selector)
    return [service.metadata.name for service in services.items]


def delete_job(name: str, namespace: str) -> None:
    """Delete the Job and its pods; a Job that is already gone is fine."""
    try:
        client.BatchV1Api().delete_namespaced_job(name=name, namespace=namespace, propagation_policy="Foreground")
    except client.ApiException as e:
        if e.status != 404:
            raise
