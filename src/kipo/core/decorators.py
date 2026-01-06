import functools
from typing import Callable, Any, Optional

import polars as pl
from rich.console import Console
from kipo.core.definitions import DataLayer
from kipo.core.io import get_data_path


console = Console()


def step(layer: DataLayer, name: Optional[str] = None):
    """
    Decorator to mark a function as an ETL step.
    Wraps execution with Rich logging, error handling, 
    AND automatic data persistence based on the layer.
    """
    def decorator(func: Callable[..., Optional[pl.DataFrame]]) -> Callable[..., Optional[pl.DataFrame]]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Optional[pl.DataFrame]:
            # Lógica de nombre: Priorizamos el 'name' del decorador, si no, el de la función.
            # SANITIZACIÓN: Forzamos minúsculas y reemplazamos espacios por guiones bajos.
            raw_name = name or func.__name__
            step_name = raw_name.strip().lower().replace(" ", "_")

            # 1. Definir rutas
            output_file = get_data_path(layer, step_name)
            output_dir = output_file.parent

            console.print(
                f"[bold blue]Starting step:[/bold blue] {step_name} [{layer}]")

            try:

                # 2. Ejecutar la lógica del usuario
                result = func(*args, **kwargs)

                # 3. Lógica de Persistencia Automática
                # Solo guardamos si el resultado es un DataFrame
                if isinstance(result, pl.DataFrame):
                    # Asegurar que el directorio existe (mkdir -p)
                    output_dir.mkdir(parents=True, exist_ok=True)

                    # Guardar en Parquet (Estándar de oro)
                    result.write_parquet(output_file)

                    console.print(f"[dim]Saved to: {output_file}[/dim]")
                elif result is None:
                    console.print(
                        f"[dim]Step returned None (nothing to save)[/dim]")

                console.print(
                    f"[bold green]Step finished:[/bold green] {step_name}")
                return result

            except Exception as e:
                console.print(f"[bold red]Step failed:[/bold red] {step_name}")
                console.print(f"[red]{e}[/red]")

                raise e

        return wrapper
    return decorator
