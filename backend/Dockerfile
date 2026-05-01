FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data

EXPOSE $PORT

# Railway injects $PORT at runtime; default to 8000 for local dev
CMD uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}
