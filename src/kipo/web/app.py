from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select, desc
from kipo.core.db import engine
from kipo.core.models import PipelineRun, RunStatus

# Resolución robusta de rutas
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="Kipo Dashboard")


@app.get("/")
def dashboard(request: Request):
    """
    Renders the main dashboard with the latest pipeline runs.
    """
    with Session(engine) as session:
        runs = session.exec(select(PipelineRun).order_by(
            desc(PipelineRun.start_time)).limit(50)).all()

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "runs": runs,
            "RunStatus": RunStatus
        }
    )
