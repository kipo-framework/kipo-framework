import typer
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from kipo import __version__
from kipo.commands.init import init_project
from kipo.core.io import read
from kipo.core.definitions import DataLayer
from kipo.core.definitions import DataLayer
from kipo.core.runner import run_pipeline
from kipo.commands.history import show_history

app = typer.Typer(
    name="kipo",
    help="Kipo ETL Framework - Convention over Configuration",
    add_completion=False,
)
console = Console()


@app.command()
def hello(name: str = "Kipo User"):
    """
    Say hello to the Kipo user.
    """
    console.print(Panel.fit(
        f"[bold green]Hello {name}! Welcome to Kipo ETL Framework![/bold green]", title="Kipo System"))


@app.command()
def version():
    """
    Show current Kipo version.
    """
    console.print(f"Kipo Version: [bold cyan]{__version__}[/bold cyan]")


@app.command()
def init(
    project_name: Optional[str] = typer.Argument(
        None, help="Name of the project directory")
):
    """
    Initialize a new Kipo project.
    """
    if not project_name:
        project_name = typer.prompt("Project name", default=".")

    init_project(project_name)


@app.command()
def show(
    layer: str = typer.Argument(..., help="Layer: bronze, silver, or gold"),
    name: str = typer.Argument(..., help="Dataset name (e.g., process_data)"),
    limit: int = typer.Option(
        10, "--limit", "-n", help="Number of rows to display")
):
    """
    Inspect a dataset directly from the CLI without writing scripts.
    Example: kipo show silver process_data
    """
    try:
        # 1. Normalizar el input del layer (string -> Enum)
        try:
            target_layer = DataLayer[layer.upper()]
        except KeyError:
            console.print(
                f"[bold red]❌ Invalid Layer:[/bold red] '{layer}'. Options are: bronze, silver, gold.")
            raise typer.Exit(code=1)

        # 2. Feedback visual
        console.print(
            f"[bold blue]🔍 Inspecting:[/bold blue] {name} [{target_layer}]")

        # 3. Lectura usando el motor de I/O
        df = read(target_layer, name)

        # 4. Mostrar datos (Polars se encarga del formato bonito)
        print(df.head(limit))

    except FileNotFoundError:
        console.print(f"[bold yellow]⚠️  Dataset not found.[/bold yellow]")
        console.print(f"Checked in: [italic]{layer}/{name}.parquet[/italic]")
        raise typer.Exit(code=1)

    except Exception as e:
        console.print(f"[bold red]❌ Unexpected Error:[/bold red] {e}")
        raise typer.Exit(code=1)


@app.command()
def run(
    pipeline_name: str = typer.Argument(
        ..., help="Name of the pipeline to run (e.g., example_pipeline)")
):
    """
    Execute a pipeline script located in the pipelines/ directory.
    Example: kipo run example_pipeline
    """
    try:
        run_pipeline(pipeline_name)
    except FileNotFoundError:
        raise typer.Exit(code=1)
    except Exception:
        raise typer.Exit(code=1)


@app.command()
def history(
    limit: int = typer.Option(
        10, "--limit", "-n", help="Number of rows to display")
):
    """
    Show pipeline execution history.
    """
    show_history(limit)


if __name__ == "__main__":

    app()
