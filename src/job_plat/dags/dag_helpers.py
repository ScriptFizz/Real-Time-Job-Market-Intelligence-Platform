import subprocess
import sys
import time
from collections.abc import Mapping
from datetime import datetime
from typing import Any


def build_cli_command(command: str, context: Mapping[str, Any]) -> list[str]:
    logical_date = context.get("logical_date")
    if not isinstance(logical_date, datetime) or logical_date.utcoffset() is None:
        raise ValueError("Airflow logical_date must be a timezone-aware datetime")

    params = context.get("params")
    if not isinstance(params, Mapping) or not isinstance(params.get("env"), str):
        raise ValueError("Airflow context must contain a string params.env")

    return [
        "-m",
        "job_plat.cli",
        command,
        "--execution-date",
        logical_date.isoformat(),
        "--env",
        params["env"],
    ]


def run_command(cmd: list[str]) -> None:
    start = time.time()
    print(f"Running command: {cmd}")

    subprocess.run([sys.executable, *cmd], check=True)

    duration = time.time() - start
    print(f"Finished in {duration:.2f}s")
