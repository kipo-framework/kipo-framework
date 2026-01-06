from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class KipoContext:
    project_root: Path = field(default_factory=lambda: Path.cwd())
    data_dir: Path = field(default_factory=lambda: Path("data"))
    
    _instance: Optional["KipoContext"] = None

    @classmethod
    def get_instance(cls) -> "KipoContext":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
