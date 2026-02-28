import logging
import logging.config
import sys
from pathlib import Path

def setup_logging(log_level=logging.INFO):
    """
    Define logging configurations for applications.
    """
    
    project_root = Path(__file__).resolve().parents[3]
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "minimal": {"format": "%(message)s"},
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "minimal",
                "level": log_level,
            },
            "info": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": str(logs_dir / "info.log"),
                "maxBytes": 10_000_000,
                "backupCount": 5,
                "formatter": "json",
                "level": logging.INFO,
            },
            "error": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": str(logs_dir / "error.log"),
                "maxBytes": 10_000_000,
                "backupCount": 5,
                "formatter": "json",
                "level": logging.ERROR,
            },
        },
        "root": {
            "handlers": ["console", "info", "error"],
            "level": log_level,
            "propagate": True,
        },
    }
    
    logging.config.dictConfig(logging_config)
