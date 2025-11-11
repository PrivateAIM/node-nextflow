from typing import Any
from sqlalchemy import JSON, Column, Integer, String, Float
from sqlalchemy.ext.declarative import as_declarative, declared_attr


@as_declarative()
class Base:
    id: Any
    __name__: str

    # Generate __tablename__ automatically
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


class NextflowRunDB(Base):
    __tablename__ = "nextflow_runs"
    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(String, unique=True, index=True)
    analysis_id = Column(String, unique=False, index=True)
    pipeline_name =  Column(String, nullable=True)
    run_args = Column(JSON, nullable=True)
    time_created = Column(Float, nullable=True)
    time_updated = Column(Float, nullable=True)
