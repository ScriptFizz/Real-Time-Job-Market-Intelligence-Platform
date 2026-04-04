# ---- Builder stage ----
FROM python:3.11-slim-bookworm as builder

RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml poetry.lock ./

RUN pip install poetry && poetry self add poetry-plugin-export

ENV PIP_EXTRA_INDEX_URL=https://download.pytorch.org/whl/cpu
RUN poetry export \
  --without dev \ 
  --without viz \ 
  -f requirements.txt \ 
  --output requirements.txt
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt


# ---- Runtime stage ----
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/app/src

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-cache /wheels/*

COPY . .

CMD ["python", "-m", "job_plat.cli"]

