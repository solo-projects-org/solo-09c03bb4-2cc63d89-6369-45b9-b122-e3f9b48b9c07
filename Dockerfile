FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    GSB_DATA_DIR=/data \
    GSB_RULES_FILE=/app/config/rules.yaml

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY config ./config

RUN mkdir -p /data && \
    useradd --system --uid 10001 --create-home --home-dir /home/gsb gsb && \
    chown -R gsb:gsb /data /app

USER gsb

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3)"

# One worker on purpose: DuckDB metadata writes are serialized in-process.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
