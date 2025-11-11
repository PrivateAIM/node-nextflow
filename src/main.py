from dotenv import load_dotenv, find_dotenv

from src.k8s.kubernetes import load_cluster_config, get_current_namespace
from src.resources.database.entity import Database
from src.api.api import FlameNextflowAPI


def main():
    flame_nextflow_api()
    # init database
    database = Database()
    FlameNextflowAPI(database=database, namespace=get_current_namespace())

if __name__ == "__main__":
    main()
