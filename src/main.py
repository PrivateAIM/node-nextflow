from dotenv import find_dotenv, load_dotenv

from src.api.api import FlameNextflowAPI
from src.k8s.utils import load_cluster_config
from src.resources.database.entity import Database
from src.resources.nextflow_run.entity import resume_interrupted_forwards


def main() -> None:
    load_dotenv(find_dotenv())
    load_cluster_config()

    database = Database()
    resume_interrupted_forwards(database)
    FlameNextflowAPI(database).serve()


if __name__ == "__main__":
    main()
