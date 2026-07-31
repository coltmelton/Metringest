FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker/app ./app
COPY scripts/dlq_replay.py /tools/dlq_replay.py

CMD ["python", "-m", "app.main"]
