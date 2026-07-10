FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    CLOUD_DEPLOYMENT=1 \
    CHROME_BIN=/usr/bin/chromium \
    MOPS_DOWNLOAD_DIR=/tmp/mops_csv

RUN apt-get update \
    && apt-get install -y --no-install-recommends chromium chromium-driver curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /tmp/mops_csv \
    && chown -R appuser:appuser /app /tmp/mops_csv

USER appuser

EXPOSE 10000

CMD ["sh", "-c", "gunicorn server:app --bind 0.0.0.0:${PORT:-10000} --workers 2 --threads 4 --timeout 600"]
