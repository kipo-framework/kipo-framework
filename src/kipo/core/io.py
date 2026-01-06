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
        raise FileNotFoundError(f"❌ No dataset found at {path}")
    
    return pl.read_parquet(path)
