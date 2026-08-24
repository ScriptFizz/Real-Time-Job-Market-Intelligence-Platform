from datetime import UTC, datetime
from typing import Any, Literal

from job_plat.storage.paths import join_storage_path
from job_plat.storage.storages import Storage

RunStatus = Literal["started", "committed", "failed", "no_op"]


class RunManifestWriter:
    """Persist the latest state of one stage attempt as a JSON document."""

    def __init__(self, *, storage: Storage, metadata_path: str):
        self.storage = storage
        self.metadata_path = metadata_path

    def write(
        self,
        *,
        run_id: str,
        stage: str,
        status: RunStatus,
        started_at: datetime,
        batch_id: str | None,
        partitions: list[str],
        inputs: dict[str, Any] | None = None,
        outputs: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        model_identity: dict[str, Any] | None = None,
        failure: dict[str, str] | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        payload = {
            "run_id": run_id,
            "stage": stage,
            "status": status,
            "started_at": started_at.astimezone(UTC).isoformat(),
            "finished_at": (
                finished_at.astimezone(UTC).isoformat() if finished_at else None
            ),
            "batch_id": batch_id,
            "partitions": partitions,
            "inputs": inputs or {},
            "outputs": outputs or {},
            "metrics": metrics or {},
            "model_identity": model_identity or {},
            "failure": failure,
        }
        path = join_storage_path(
            self.metadata_path,
            "run_manifests",
            stage,
            f"{run_id}.json",
        )
        self.storage.write_json(payload, path)
