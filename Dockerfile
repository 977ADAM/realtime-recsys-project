FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --create-home --home-dir /app app

COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip check

COPY app ./app
COPY src ./src
COPY sql ./sql

RUN chown -R app:app /app

USER app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
  CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2); sys.exit(0)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
