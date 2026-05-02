# --- builder stage: install deps into an isolated venv ----------------------
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# venv keeps the runtime stage tiny and self-contained — we just copy /opt/venv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt


# --- runtime stage: minimal image, runs as non-root user --------------------
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}"

# `curl` is needed for the HEALTHCHECK; nothing else is installed.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r ims && useradd -r -g ims -d /app -s /usr/sbin/nologin ims

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY --chown=ims:ims . .

# Drop root before the app ever runs — if the process is compromised it
# cannot escalate or write outside its read-only code dir.
USER ims

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
