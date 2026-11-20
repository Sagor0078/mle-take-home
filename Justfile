set shell := ["bash", "-euo", "pipefail", "-c"]
set dotenv-load := true

default: setup

# Install the producer's dependencies.
setup:
    uv sync --locked

# Start Postgres (with pgvector) and RabbitMQ, and wait until both are healthy.
up:
    docker compose up -d --wait

# Stop everything and delete the volumes.
down:
    docker compose down -v

# Replay the registry feed onto the company-events queue.
publish:
    uv run python -m producer.publish

# Apply database migrations.
migrate:
    uv run python -m app.migrate

# Run the queue consumer.
consume:
    uv run python -m app.consumer

# Serve the similar-companies API.
serve:
    uv run uvicorn app.api:app --port ${API_PORT:-8000}

# Run the test suite.
test:
    uv run pytest

# Run the recommendation-quality evaluation.
eval:
    uv run python -m eval.evaluate
