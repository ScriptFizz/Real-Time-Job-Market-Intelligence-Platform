# ---- Base ----
FROM python:3.11-slim-bookworm

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
ENV SPARK_MODE=k8s

# env tweaks
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TOKENIZERS_PARALLELISM=false \
    TRANSFORMERS_NO_ADVISORY_WARNINGS=true \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# ---- Install Spark ----
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
#RUN pip install --no-cache-dir kubernetes

COPY src /opt/jobplat/src
COPY settings.yaml /opt/jobplat/settings.yaml

# ---- Permissions ----
RUN chown -R sparkuser:sparkuser /opt/jobplat
RUN chown -R sparkuser:sparkuser $SPARK_HOME

RUN mkdir -p $SPARK_HOME/work && chown -R sparkuser:sparkuser $SPARK_HOME/work
RUN mkdir -p /tmp/spark-events && chmod -R 777 /tmp/spark-events

USER sparkuser
WORKDIR /opt/jobplat

