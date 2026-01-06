from pathlib import Path
import polars as pl
from kipo.core.definitions import DataLayer
from kipo.core.config import get_base_dir

BASE_DIR = get_base_dir()


def get_data_path(layer: DataLayer, name: str) -> Path:
    """
    Calcula la ruta estandarizada para un dataset.
    Aplica sanitización: minúsculas, espacios a guiones bajos y elimina espacios extra.
    """
    clean_name = name.strip().lower().replace(" ", "_")
    # Asume que DataLayer es un Enum, extrae el nombre (ej: 'BRONZE') y lo pasa a minúsculas
    layer_name = str(layer).split('.')[-1].lower()

    return BASE_DIR / layer_name / f"{clean_name}.parquet"


def read(layer: DataLayer, name: str) -> pl.DataFrame:
    """
    Lee un dataset del framework asegurando consistencia en la ruta.
    """
    path = get_data_path(layer, name)
    if not path.exists():
        raise FileNotFoundError(f"No dataset found at {path}")

    return pl.read_parquet(path)


def read_raw(filename: str) -> pl.DataFrame:
    """
    Ingesta un archivo crudo (Excel o CSV) desde la carpeta 'raw' del Data Lake.
    Detecta automáticamente la extensión.

    Uso: df = kipo.read_raw("cosecha_semanal.xlsx")
    """
    base = get_base_dir()
    raw_path = base / "raw" / filename

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    # Detección de extensión
    suffix = raw_path.suffix.lower()

    print(f"Ingesting: {filename}...")

    if suffix in [".xlsx", ".xls"]:

        # Usamos engine='calamine' porque es ultrarrápido y ya lo tienes en dependencias
        return pl.read_excel(raw_path, engine="calamine")

    elif suffix == ".csv":
        return pl.read_csv(raw_path)

    elif suffix == ".parquet":
        return pl.read_parquet(raw_path)

    else:
        raise ValueError(f"Unsupported format: {suffix}")
