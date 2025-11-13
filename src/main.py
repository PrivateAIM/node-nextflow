from dotenv import load_dotenv, find_dotenv

from src.k8s.utils import load_cluster_config, get_current_namespace
from src.resources.database.entity import Database
from src.api.api import FlameNextflowAPI


def main():
    # load env
    load_dotenv(find_dotenv())

    # load cluster config
    load_cluster_config()

    # init database
    database = Database()
    FlameNextflowAPI(database=database, namespace=get_current_namespace())


if __name__ == "__main__":
    main()
