# FROM python:3.10-slim
FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/serviceAccountKeyVERTEXIA.json
ENV STORAGE_CREDENTIALS_PATH=/app/serviceAccountKeySTORAGE.json
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y gcc libffi-dev musl-dev && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install-deps chromium
COPY . .
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}