from pathlib import Path
from typing import Any
from job_plat.config.env_config import BronzeConfig, PathsConfig, EnvironmentConfig

import yaml

class ConfigLoader:
    """
    Load YAML configuration files.
    """
    
    def __init__(
        self,
        config_path: str | Path = "settings.yaml",
        env: str | None = None,
        project_root: Path | None = None,
    ):
        self.env = env
        self.project_root = (project_root if project_root is not None else self._detect_project_root())
        self.config_path = self.project_root / config_path
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        self._config = self._load()
        
    
    @staticmethod
    def _detect_project_root() -> Path:
        """
        Resolve project root if not specified.
        
        Args:
        Returns:
            (Path): path of the project root directory.
        """
        return Path(__file__).resolve().parents[3]
    
    def _load(self) -> dict[str, Any]:
        """
        Return the configuration data in a dictionary structure.
        
        Args:
        Returns:
            (Dict[str, Any]): dictionary with configuration data.
        """
        with self.config_path.open() as f:
            config = yaml.safe_load(f)
        
        return config
    
    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)
    
    def as_dict(self) -> dict[str, Any]:
        return self._config
    
    def load_env(self, env: str | None = None) -> EnvironmentConfig:
        env = env or self.env
        if not env:
            raise ValueError("Environment must be specified.")
    
        raw_env_config = self._config.get("environments", {}).get(env)
        if raw_env_config is None:
            raise KeyError(f"Environment '{env}' not found in config.")
        
        env_config = EnvironmentConfig(
            env=env,
            **raw_env_config,
        )
        
        # Handle local paths
        if env_config.storage.type == "local":
            # Normalize paths
            root_path = (self.project_root / env_config.paths.root).resolve()
            metadata_path = (self.project_root / env_config.paths.metadata).resolve()
            
            # Ensure directories exists
            root_path.mkdir(parents=True, exist_ok=True)
            metadata_path.mkdir(parents=True, exist_ok=True)
            
            # Write back normalized paths
            env_config.paths.root = str(root_path)
            env_config.paths.metadata = str(metadata_path)
        
        # Handle GCS
        elif env_config.storage.type == "gcs":
            if not str(env_config.paths.root).startswith("gs://"):
                raise ValueError("GCS root path must start with 'gs://'")
            if not str(env_config,paths.metadata).startswith("gs://"):
                raise ValueError("GCS metadata path must start with 'gs://'")
        
        return env_config
