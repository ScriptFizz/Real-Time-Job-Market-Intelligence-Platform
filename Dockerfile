# ---- Base ----
FROM python:3.11-slim

# ---- System deps ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# ---- Environment ----
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin:$PATH"

# env tweaks
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TOKENIZERS_PARALLELISM=false \
    TRANSFORMERS_NO_ADVISORY_WARNINGS=true \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

 ---- Install Spark ----
WORKDIR /opt

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    | tar -xz && \
    mv spark-3.5.0-bin-hadoop3 $SPARK_HOME

# ---- Create user ----
RUN useradd -u 1000 -m sparkuser

# ---- App setup ----
WORKDIR /opt/jobplat

COPY requirements.spark.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.spark.txt

COPY src /opt/jobplat/src
COPY settings.yaml /opt/jobplat/settings.yaml

# ---- Permissions ----
RUN chown -R sparkuser:sparkuser /opt/jobplat
RUN chown -R sparkuser:sparkuser $SPARK_HOME

RUN mkdir -p $SPARK_HOME/work && chown -R sparkuser:sparkuser $SPARK_HOME/work
RUN mkdir -p /tmp/spark-events && chmod -R 777 /tmp/spark-events

USER sparkuser
WORKDIR /opt/jobplat

###############  09-04-2026 #######################

# ---- Base ----
#FROM apache/spark:3.5.0-python3

#USER root

#ENV PIP_NO_CACHE_DIR=1 \
#    PYTHONDONTWRITEBYTECODE=1 \
#    PYTHONUNBUFFERED=1 

# ---- Python deps ----
#RUN apt-get update && apt-get install -y --no-install-recommends \
#    curl \
#    build-essential \
#    && rm -rf /var/lib/apt/lists/*


# ------ Set working dir -----
#WORKDIR /opt/spark/jobs

# ------ Copy dependency files ---------
#COPY pyproject.toml poetry.lock ./

# ---- Install poetry + deps ----
#RUN pip install --no-cache-dir \
#    poetry==1.7.1 \
#    poetry-plugin-export==1.6.0

# Debug poetry plugin line
#RUN poetry self show plugins

# Export requirements
#RUN poetry export \
#        --without dev \
#        --without viz \
#        -f requirements.txt \
#        -o requirements.txt 

# Remove CUDA dependencies pulled by sentence-transformers
#RUN sed -i '/^torch/d' requirements.txt \
# && sed -i '/^nvidia-/d' requirements.txt

# Install CPU-only torch
#RUN pip install --no-cache-dir \
#    torch==2.2.2 \
#    --index-url https://download.pytorch.org/whl/cpu

# Install the rest of dependencies
#RUN pip install --no-cache-dir -r requirements.txt

# ---- Copy your project ----
#COPY src/job_plat /opt/spark/jobs/job_plat
#COPY settings.yaml /opt/spark/jobs/settings.yaml

# ---- Permissions (important for Spark) ----
#RUN chmod -R 755 /opt/spark/jobs

#USER 185

###########################################
# ---- Base ----
#FROM python:3.11-slim-bookworm AS base

#ENV PYTHONDONTWRITEBYTECODE=1 \
#    PYTHONUNBUFFERED=1 \
#     PIP_NO_CACHE_DIR=1 \
#    POETRY_CACHE_DIR=/tmp/poetry-cache

#WORKDIR /app

# Install system dependencies for building Python packages
#RUN apt-get update && apt-get install -y --no-install-recommends \
#    build-essential gcc \
#    && rm -rf /var/lib/apt/lists/*

# Install Poetry
#RUN pip install poetry && poetry self add poetry-plugin-export

# Copy dependency files only (for caching)
#COPY pyproject.toml poetry.lock ./

# Export requirements
#RUN poetry export \
#    --without dev \
#    --without viz \
#    -f requirements.txt \
#    -o requirements.txt \
#    && rm -rf /tmp/poetry-cache

# Remove CUDA dependencies pulled by sentence-transformers
#RUN sed -i '/^torch/d' requirements.txt \
# && sed -i '/^nvidia-/d' requirements.txt

# Install CPU-only torch
#RUN pip install --no-cache-dir \
#    torch==2.2.2 \
#    --index-url https://download.pytorch.org/whl/cpu#

# Install the rest of dependencies
#RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
#COPY . .

# Default command for testing/debugging
#CMD ["python", "-m", "job_plat.cli"]
#############################################
# ---- Builder stage ----
#FROM python:3.11-slim-bookworm as builder

#RUN apt-get update && apt-get install -y \
#    gcc \
#    build-essential \
#    && rm -rf /var/lib/apt/lists/*

#WORKDIR /app
#COPY pyproject.toml poetry.lock ./

#RUN pip install poetry && poetry self add poetry-plugin-export

#ENV PIP_EXTRA_INDEX_URL=https://download.pytorch.org/whl/cpu
#RUN poetry export \
#  --without dev \ 
#  --without viz \ 
#  -f requirements.txt \ 
#  --output requirements.txt
#RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt


# ---- Runtime stage ----
#FROM python:3.11-slim-bookworm

#RUN apt-get update && apt-get install -y \
#    openjdk-17-jdk \
#    curl \
#    && rm -rf /var/lib/apt/lists/*

#ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
#ENV PYTHONPATH=/app/src

#WORKDIR /app

#COPY --from=builder /wheels /wheels
#RUN pip install --no-cache /wheels/*

#COPY . .

#CMD ["python", "-m", "job_plat.cli"]
