from sqlalchemy import JSON, Column, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class NextflowRunDB(Base):
    __tablename__ = "nextflow_runs"
    id = Column(Integer, primary_key=True, index=True)
    analysis_id = Column(String, index=True)
    keycloak_token = Column(String, nullable=True)
    run_id = Column(String, unique=True, index=True)
    time_created = Column(Float, nullable=True)
    # result handling (docs/result-handling-plan.md)
    run_status = Column(String, nullable=True)
    manifest = Column(JSON, nullable=True)       # [{key, size, etag}] below results/
    forward_spec = Column(JSON, nullable=True)   # the `forward` block of the /run call
    forward_state = Column(JSON, nullable=True)  # transfer progress / outcome, see storage/forward.py
