import runpy
import time
from pathlib import Path
from rich.console import Console
from kipo.core.db import create_run, update_run_status
from kipo.core.models import RunStatus


console = Console()


def run_pipeline(pipeline_name: str):
    """
    Locates and executes a pipeline script from the user's `pipelines/` directory.

    Args:
        pipeline_name: Name of the pipeline file (with or without .py extension).
    """
    # 1. Resolver ruta base (CWD del usuario)
    cwd = Path.cwd()
    pipelines_dir = cwd / "pipelines"

    # 2. Manejo inteligente de sufijo
    if not pipeline_name.endswith(".py"):
        pipeline_name += ".py"

    pipeline_path = pipelines_dir / pipeline_name

    # 3. Validación de existencia
    if not pipeline_path.exists():
        console.print(
            f"[bold red]❌ Pipeline not found:[/bold red] {pipeline_name}")
        console.print(f"Searched in: [dim]{pipeline_path}[/dim]")
        raise FileNotFoundError(f"Pipeline {pipeline_name} not found.")

    # 4. Ejecución
    # 4. Ejecución
    console.print(
        f"[bold blue]🚀 Launching Pipeline:[/bold blue] {pipeline_name}")

    # --- METADATA STORE START ---
    run_record = create_run(pipeline_name)
    # ----------------------------

    try:
        start_time = time.perf_counter()
        # runpy.run_path ejecuta el script como si se llamara con `python script.py`
        # run_name="__main__" activa el bloque `if __name__ == "__main__":`
        runpy.run_path(str(pipeline_path), run_name="__main__")

        duration = time.perf_counter() - start_time

        # --- SUCCESS UPDATE ---
        update_run_status(run_record.id, RunStatus.SUCCESS)
        console.print(
            f"\n[bold green]✅ Pipeline Execution Completed[/bold green] in {duration:.2f}s")

    except Exception as e:
        # --- FAILURE UPDATE ---
        update_run_status(run_record.id, RunStatus.FAILED,
                          error_message=str(e))
        console.print(f"\n[bold red]❌ Pipeline Crashed:[/bold red] {e}")
        # Re-raise para que Typer pueda manejar el código de salida si es necesario
        raise e
