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
            "detailed": {
                "format": (
                    "%(levelname)s %(asctime)s "
                    "[%(name)s:%(filename)s:%(funcName)s:%(lineno)d]\n"
                    "%(message)s\n"
                )
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
                "filename": logs_dir / "info.log",
                "maxBytes": 10_000_000,
                "backupCount": 5,
                "formatter": "detailed",
                "level": logging.INFO,
            },
            "error": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": logs_dir / "error.log",
                "maxBytes": 10_000_000,
                "backupCount": 5,
                "formatter": "detailed",
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
