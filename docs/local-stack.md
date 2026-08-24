# Reproducible local stack

## Host installation

Requirements: Python 3.11, Java 17, and Poetry 2.4.x.

```bash
poetry install --with dev
poetry run job-plat --help
```

Install optional runtimes only when needed:

```bash
poetry install --with dev,airflow  # DAG import and local Airflow development
poetry install --with cloud        # GCS Python client
```

## Docker Compose

Build the CPU-only Spark/Delta CLI image:

```bash
docker compose build
docker compose run --rm job-platform --help
```

Run a command by replacing the default arguments:

```bash
docker compose run --rm job-platform silver \
  --env dev \
  --execution-date 2026-08-24T00:00:00Z
```

The Compose service mounts `data/`, `metadata/`, `artifacts/`, and `logs/`, so
results survive container removal. Copy `.env.example` to `.env` only when using
live connectors. Do not commit `.env`.

The image intentionally contains the pipeline CLI, not an embedded Airflow or
YARN cluster. That keeps local reproduction small and makes the boundary between
application code and deployment infrastructure explicit.

Delta's JVM artifacts are resolved through Spark. The initial container run may
need network access to Maven; production images should pre-populate the Ivy cache
or provide the artifacts through the cluster image.
