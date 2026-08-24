# Getting started

## Host runtime

Install Python 3.11, Java 17, and Poetry 2.4.x, then run:

```bash
poetry install --with dev
poetry run job-plat --help
```

Validate the repository:

```bash
poetry run ruff format --check src tests
poetry run ruff check src tests
poetry run mypy src --no-incremental
PYTHONDONTWRITEBYTECODE=1 SPARK_LOCAL_IP=127.0.0.1 \
  poetry run pytest -p no:cacheprovider -q
```

## Container runtime

```bash
docker compose build
docker compose run --rm job-platform --help
```

See the [local stack guide](../local-stack.md) for mounted state and command
examples.

## Live ingestion

Live ingestion is optional. Copy `.env.example` to `.env`, configure credentials,
and enable the desired connector in `settings.yaml`. Silver and later stages can
also be exercised with synthetic or previously captured Bronze fixtures; a
credential-free demo generator is a documented follow-up.
