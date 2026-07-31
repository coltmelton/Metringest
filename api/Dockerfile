FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/app ./app
COPY schemas /schemas

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
