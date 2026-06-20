# Producer image. Build from the REPO ROOT so the Avro schema can be baked in:
#   docker build -t ghcr.io/shaked98/market-producer:latest -f producer/Dockerfile .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SCHEMA_PATH=/app/schemas/quote.avsc

WORKDIR /app

# Dependencies first (better layer caching). confluent-kafka ships librdkafka in the
# manylinux wheel (with zstd), so no apt build deps are needed.
COPY producer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Application + the bundled message contract.
COPY producer/market_producer ./market_producer
COPY schemas ./schemas

# Run unprivileged.
RUN useradd --uid 10001 --create-home appuser
USER 10001

CMD ["python", "-m", "market_producer"]
