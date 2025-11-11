import json
import os
import time
from typing import Optional

from secretstorage.dhcrypto import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .db_models import Base, NextflowRunDB


class Database:
    def __init__(self) -> None:
        host = os.getenv('POSTGRES_HOST')
        port = "5432"
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        database = os.getenv('POSTGRES_DB')
        conn_uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(conn_uri,
                                    pool_pre_ping=True,
                                    pool_recycle=3600)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        Base.metadata.create_all(bind=self.engine)

    def reset_db(self) -> None:
        Base.metadata.drop_all(bind=self.engine)
        Base.metadata.create_all(bind=self.engine)

    def create_nf_run(self, run_id: str, analysis_id: str) -> NextflowRunDB:
        nf_run = NextflowRunDB(run_id=run_id,
                               analysis_id=analysis_id,
                               time_created=time.time())
        with self.SessionLocal() as session:
            session.add(analysis)
            session.commit()
            session.refresh(analysis)
        return analysis

    def get_nf_runs(self) -> list[NextflowRunDB]:
        with self.SessionLocal as session:
            return session.query(NextflowRunDB).all()

    def get_nf_runs_by_analysis_id(self, analysis_id: str) -> list[NextflowRunDB]:
        with self.SessionLocal as session:
            return session.query(NextflowRunDB).filter_by(**{"analysis_id": analysis_id}).all()

    def get_nf_run_by_run_id(self, run_id: str) -> NextflowRunDB:
        with self.SessionLocal as session:
            return session.query(NextflowRunDB).filter_by(**{"run_id": run_id}).first()

    def delete_nf_run(self, run_id: str) -> None:
        with self.SessionLocal as session:
            run = session.query(NextflowRunDB).filter_by(**{"run_id": run_id}).one()
            session.delete(run)
            session.commit()

    def delete_all_analysis_nf_runs(self, analysis_id: str):
        with self.SessionLocal as session:
            runs = session.query(NextflowRunDB).filter_by(**{"analysis_id": analysis_id}).all()
            for run in runs:
                session.delete(run)
                session.commit()
