"""Runtime settings, read from environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    rabbitmq_url: str
    embed_model: str
    prefetch: int

    @classmethod
    def from_env(cls) -> "Settings":
        """Build settings from the environment, falling back to the docker-compose defaults."""
        pg_port = os.environ.get("POSTGRES_PORT") or "5432"
        mq_port = os.environ.get("RABBITMQ_PORT") or "5672"
        return cls(
            database_url=os.environ.get(
                "DATABASE_URL", f"postgresql://postgres:postgres@localhost:{pg_port}/companies"
            ),
            rabbitmq_url=os.environ.get("RABBITMQ_URL", f"amqp://guest:guest@localhost:{mq_port}/"),
            embed_model=os.environ.get("EMBED_MODEL", "BAAI/bge-small-en-v1.5"),
            prefetch=int(os.environ.get("PREFETCH", "32")),
        )
