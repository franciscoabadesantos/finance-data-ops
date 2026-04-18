FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY flows /app/flows
COPY services/finance-jobs-worker/requirements.txt /app/requirements-worker.txt
COPY services/finance-jobs-worker/app /app/app

RUN pip install --no-cache-dir -e /app \
    && pip install --no-cache-dir -r /app/requirements-worker.txt

ENV PORT=8080
ENV FINANCE_DATA_OPS_ROOT=/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
