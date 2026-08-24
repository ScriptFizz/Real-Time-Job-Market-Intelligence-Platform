FROM python:3.11-slim-bookworm

ARG POETRY_VERSION=2.4.1

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=/opt/spark-ivy pyspark-shell"

RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir "poetry==${POETRY_VERSION}" \
    && mkdir -p /opt/spark-ivy /workspace/data /workspace/metadata /workspace/artifacts /workspace/logs

WORKDIR /workspace

COPY pyproject.toml poetry.lock README.md ./
COPY src ./src
COPY settings.yaml .env.example ./

RUN poetry install --only main --no-interaction --no-ansi \
    && useradd --create-home --uid 10001 jobplat \
    && chown -R jobplat:jobplat /workspace /opt/spark-ivy

USER jobplat

ENTRYPOINT ["job-plat"]
CMD ["--help"]
