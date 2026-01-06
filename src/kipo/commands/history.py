from rich.console import Console
from rich.table import Table

from rich import box
from sqlmodel import Session, select, desc
from datetime import timezone
from kipo.core.db import engine
from kipo.core.models import PipelineRun, RunStatus


console = Console()


def show_history(limit: int = 10):
    """
    Displays a formatted table of the most recent pipeline runs.
    """
    try:
        with Session(engine) as session:
            statement = select(PipelineRun).order_by(
                desc(PipelineRun.start_time)).limit(limit)
            runs = session.exec(statement).all()

        if not runs:
            console.print(
                "[italic yellow]No execution history found.[/italic yellow]")
            return

        table = Table(
            title="Pipeline Execution History",
            box=box.ROUNDED,
            header_style="bold white",
            border_style="dim white"
        )

        table.add_column("ID", style="dim", justify="right")
        table.add_column("Pipeline", style="bold white")
        table.add_column("Status", justify="center")
        table.add_column("Start Time", style="cyan")
        table.add_column("Duration", justify="right")
        table.add_column("Error Message", style="red")

        for run in runs:
            # Status styling
            status_style = "white"
            if run.status == RunStatus.SUCCESS:
                status_style = "green"
            elif run.status == RunStatus.FAILED:
                status_style = "red"
            elif run.status == RunStatus.RUNNING:
                status_style = "blue"

            # Date formatting (YYYY-MM-DD HH:MM:SS) - Converted to Local Time
            if run.start_time.tzinfo is None:
                local_time = run.start_time.replace(
                    tzinfo=timezone.utc).astimezone()
            else:
                local_time = run.start_time.astimezone()

            start_time_str = local_time.strftime("%Y-%m-%d %H:%M:%S")

            # Duration formatting
            duration_str = "-"
            if run.duration_seconds is not None:
                duration_str = f"{run.duration_seconds:.2f}s"

            # Error handling
            error_msg = run.error_message or ""

            table.add_row(
                str(run.id),
                run.pipeline_name,
                f"[{status_style}]{run.status}[/{status_style}]",
                start_time_str,
                duration_str,
                error_msg
            )

        console.print(table)

    except Exception as e:
        console.print(f"[bold red]Error fetching history:[/bold red] {e}")
