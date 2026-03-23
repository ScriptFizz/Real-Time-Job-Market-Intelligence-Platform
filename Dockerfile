# ---- Base image ----
FROM python:3.11-slim

# ---- System dependencies ----
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ---- Java config (for Spark) ----
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ---- Python settings ----
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# ---- Working directory ----
WORKDIR /app

# ---- Install Python deps ----
COPY pyproject.toml ./
COPY poetry.lock ./

RUN pip install --no-cached-dir .

# ---- Copy project code ---
COPY . .

# ---- Default command (your CLI entrypoint)
CMD ["python", "-m", "job_plat.cli"]

RUN python -c "import job_plat; print('Package works')"
