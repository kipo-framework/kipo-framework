import os
from pathlib import Path
from sqlmodel import SQLModel, create_engine, Session, select
from kipo.core.models import PipelineRun, RunStatus
from datetime import datetime

# Define DB path
# We use the current working directory because we want the DB to live in the user's project
DB_DIR = Path.cwd() / ".kipo"
DB_NAME = "kipo.db"
DB_URL = f"sqlite:///{DB_DIR}/{DB_NAME}"

engine = create_engine(DB_URL)


def init_db():
    """
    Initializes the SQLite database.
    Creates the .kipo directory if it doesn't exist.
    Creates tables based on SQLModel metadata.
    """
    if not DB_DIR.exists():
        DB_DIR.mkdir(parents=True, exist_ok=True)

    SQLModel.metadata.create_all(engine)


def create_run(pipeline_name: str) -> PipelineRun:
    """
    Creates a new pipeline run record with status RUNNING.
    """
    init_db()  # Ensure DB exists

    with Session(engine) as session:
        run = PipelineRun(pipeline_name=pipeline_name)
        session.add(run)
        session.commit()
        session.refresh(run)
        return run


def update_run_status(run_id: int, status: str, error_message: str = None):
    """
    Updates the status and end time of a run.
    """
    with Session(engine) as session:
        run = session.get(PipelineRun, run_id)
        if run:
            run.status = status
            run.end_time = datetime.utcnow()
            run.duration_seconds = (
                run.end_time - run.start_time).total_seconds()
            if error_message:
                run.error_message = error_message

            session.add(run)
            session.commit()
            session.refresh(run)
            return run
