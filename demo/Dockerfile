FROM python:3.13-slim

WORKDIR /app
COPY producer /app/producer
RUN pip install --no-cache-dir /app/producer fastapi==0.115.12 uvicorn==0.34.2
COPY demo /app/demo

CMD ["uvicorn", "demo.app:app", "--host", "0.0.0.0", "--port", "8000"]
