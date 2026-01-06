import tomllib
from pathlib import Path
from typing import Any, Dict

# Buscamos el archivo en el directorio actual de ejecución
CONFIG_PATH = Path("kipo_config.toml")

def load_config() -> Dict[str, Any]:
    """
    Carga la configuración del proyecto.
    Si no existe el archivo, devuelve valores por defecto seguros.
    """
    if not CONFIG_PATH.exists():
        # Fallback por defecto si el usuario borró el archivo
        return {"storage": {"base_dir": "data"}}

    with open(CONFIG_PATH, "rb") as f:
        return tomllib.load(f)

def get_base_dir() -> Path:
    """Helper para obtener directamente el directorio base."""
    config = load_config()
    # Navegamos el diccionario: storage -> base_dir. Default: "data"
    dir_name = config.get("storage", {}).get("base_dir", "data")
    return Path(dir_name)