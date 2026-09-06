# ==============================================================================
# STAGE 1: Builder (Compile and package dependencies)
# ==============================================================================
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# ==============================================================================
# STAGE 2: Production Runtime (Minimal final image)
# ==============================================================================
FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR="/app/datasets" \
    OUTPUT_DIR="/app/outputs"

COPY datasets/ /app/datasets/
COPY solutions/ /app/solutions/
COPY starter_files/ /app/starter_files/

RUN mkdir -p /app/outputs && \
    useradd -u 1000 -m appuser && \
    chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "solutions/etl_starter.py"]
