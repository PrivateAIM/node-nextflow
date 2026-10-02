import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from .db_models import Base, NextflowRunDB


class Database:
    def __init__(self) -> None:
        conn_uri = (f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
                    f"@{os.getenv('POSTGRES_HOST')}:5432/{os.getenv('POSTGRES_DB')}")
        self.engine = create_engine(conn_uri, pool_pre_ping=True, pool_recycle=3600)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        Base.metadata.create_all(bind=self.engine)
        self._add_missing_columns()

    def _add_missing_columns(self) -> None:
        """create_all does not alter existing tables; add the result-handling columns to older databases."""
        with self.engine.begin() as conn:
            for column in ("run_status VARCHAR", "manifest JSON", "forward_spec JSON", "forward_state JSON"):
                conn.execute(text(f"ALTER TABLE nextflow_runs ADD COLUMN IF NOT EXISTS {column}"))

    def create_nf_run(self,
                      run_id: str,
                      analysis_id: str,
                      keycloak_token: str,
                      time_created: float,
                      forward_spec: dict | None = None) -> NextflowRunDB:
        nf_run = NextflowRunDB(run_id=run_id,
                               analysis_id=analysis_id,
                               keycloak_token=keycloak_token,
                               time_created=time_created,
                               forward_spec=forward_spec)
        with self.SessionLocal() as session:
            session.add(nf_run)
            session.commit()
            session.refresh(nf_run)
        return nf_run

    def get_nf_run_by_run_id(self, run_id: str) -> NextflowRunDB | None:
        with self.SessionLocal() as session:
            return session.query(NextflowRunDB).filter_by(run_id=run_id).first()

    def get_nf_runs_by_analysis_id(self, analysis_id: str) -> list[NextflowRunDB]:
        with self.SessionLocal() as session:
            return session.query(NextflowRunDB).filter_by(analysis_id=analysis_id).all()

    def get_nf_runs_with_forward_status(self, status: str) -> list[NextflowRunDB]:
        with self.SessionLocal() as session:
            rows = session.query(NextflowRunDB).filter(NextflowRunDB.forward_state.isnot(None)).all()
        return [row for row in rows if row.forward_state.get("status") == status]

    def update_nf_run(self, run_id: str, **fields) -> None:
        with self.SessionLocal() as session:
            session.query(NextflowRunDB).filter_by(run_id=run_id).update(fields)
            session.commit()
