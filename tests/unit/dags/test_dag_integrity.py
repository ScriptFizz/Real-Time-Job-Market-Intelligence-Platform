import importlib
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

os.environ.setdefault(
    "AIRFLOW_HOME",
    str(Path(tempfile.gettempdir()) / "job-plat-airflow-tests"),
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

pytest.importorskip("airflow")

from airflow.models import DagBag
from airflow.serialization.serialized_objects import SerializedDAG

DAG_FOLDER = Path(__file__).parents[3] / "src" / "job_plat" / "dags"


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_production_dags_import_and_serialize(dag_bag):
    assert dag_bag.import_errors == {}
    assert set(dag_bag.dag_ids) == {
        "ingestion_dag",
        "processing_dag",
        "ml_dag",
    }

    for dag in dag_bag.dags.values():
        assert dag.start_date.utcoffset() is not None
        assert SerializedDAG.to_dict(dag)


def test_cross_dag_sensors_use_exact_logical_date(dag_bag):
    bronze_sensor = dag_bag.dags["processing_dag"].get_task("wait_for_bronze")
    assert bronze_sensor.external_dag_id == "ingestion_dag"
    assert bronze_sensor.external_task_id == "ingest_jobs"
    assert bronze_sensor.execution_delta is None
    assert bronze_sensor.execution_date_fn is None

    gold_sensor = dag_bag.dags["ml_dag"].get_task("wait_for_gold")
    assert gold_sensor.external_dag_id == "processing_dag"
    assert gold_sensor.external_task_id == "run_gold"
    assert gold_sensor.execution_delta is None
    assert gold_sensor.execution_date_fn is None


@pytest.mark.parametrize(
    ("module_name", "task_id", "command_name"),
    [
        ("job_plat.dags.ingestion_dag", "ingest_jobs", "bronze"),
        ("job_plat.dags.processing_dag", "run_silver", "silver"),
        ("job_plat.dags.processing_dag", "run_gold", "gold"),
        ("job_plat.dags.ml_dag", "run_features", "feature"),
        ("job_plat.dags.ml_dag", "run_ml", "ml"),
    ],
)
def test_every_dag_task_passes_logical_date(
    monkeypatch,
    module_name,
    task_id,
    command_name,
):
    module = importlib.import_module(module_name)
    logical_date = datetime(2025, 3, 2, tzinfo=UTC)
    commands = []
    monkeypatch.setattr(
        module,
        "get_current_context",
        lambda: {"logical_date": logical_date, "params": {"env": "prod"}},
    )
    monkeypatch.setattr(module, "run_command", commands.append)

    module.dag.get_task(task_id).python_callable()

    assert commands == [
        [
            "-m",
            "job_plat.cli",
            command_name,
            "--execution-date",
            logical_date.isoformat(),
            "--env",
            "prod",
        ]
    ]
