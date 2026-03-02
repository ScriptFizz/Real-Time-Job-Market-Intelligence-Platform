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

        return EnvironmentConfig(
            env=env,
            **raw_env_config,
        )


# def load_env(self, env: str | None = None) -> EnvironmentConfig:
        # env = env or self.env
        # if not env:
            # raise ValueError("Environment must be specified.")
    
        # env_config = self._config.get("environments", {}).get(env)
        # if env_config is None:
            # raise KeyError(f"Environment '{env}' not found in config.")
    
        # required_keys = ["paths", "storage", "logging_level", "bronze"]
        # for key in required_keys:
            # if key not in env_config:
                # raise ValueError(f"Missing required config key: {key}")
    
        # required_paths = ["bronze", "silver", "gold_v1", "gold_v2"]
        # for path_key in required_paths:
            # if path_key not in env_config["paths"]:
                # raise ValueError(f"Missing paths.{path_key} in config")
                
        # required_bronze_keys = ["query", "location"]
        # for bronze_key in required_bronze_keys:
            # if bronze_key not in env_config["bronze"]:
                # raise ValueError(f"Missing bronze.{bronze_key} in config")
    
        # if "type" not in env_config["storage"]:
            # raise ValueError("Missing storage.type in config")
    
        # paths_config = PathsConfig(
            # bronze=env_config["paths"]["bronze"],
            # silver=env_config["paths"]["silver"],
            # gold_v1=env_config["paths"]["gold_v1"],
            # gold_v2=env_config["paths"]["gold_v2"],
        # )
        
        # bronze_config = BronzeConfig(
            # query = env_config["bronze"].get("query"),
            # location = env_config["bronze"].get("location"),
            # max_pages = env_config["bronze"].get("max_pages")
        # )
        # return EnvironmentConfig(
            # env=env,
            # paths=paths_config,
            # bronze=bronze_config,
            # logging_level=env_config["logging_level"],
            # storage_type=env_config["storage"]["type"],
        # )


    # def get_storage_type(self) -> str:
        # return self._config["storage"]["type"]
    
    # def get_logging_level() -> str:
        # return self._config["logging_level"]
