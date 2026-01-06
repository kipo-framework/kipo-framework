from pathlib import Path
from typing import List
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select, desc
from kipo.core.db import engine
from kipo.core.models import PipelineRun, RunStatus
from kipo.core.runner import run_pipeline


# Resolución robusta de rutas
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="Kipo Dashboard")


def get_available_pipelines() -> List[str]:
    """
    Lists all .py files in the user's pipelines/ directory.
    Assumes web server is run from the project root (CWD).
    """
    pipelines_dir = Path.cwd() / "pipelines"
    if not pipelines_dir.exists():
        return []
    return [f.name for f in pipelines_dir.glob("*.py")]


@app.get("/")
def dashboard(request: Request):
    """
    Renders the main dashboard with the latest runs and available pipelines.
    """
    available_pipelines = get_available_pipelines()

    with Session(engine) as session:
        runs = session.exec(select(PipelineRun).order_by(
            desc(PipelineRun.start_time)).limit(50)).all()

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "runs": runs,
            "pipelines": available_pipelines,
            "RunStatus": RunStatus
        }
    )


@app.post("/run/{pipeline_name}")
async def run_pipeline_endpoint(pipeline_name: str, background_tasks: BackgroundTasks):
    """
    Endpoint to trigger a pipeline execution in the background.
    """
    background_tasks.add_task(run_pipeline, pipeline_name)
    return {"message": f"Pipeline {pipeline_name} started"}
