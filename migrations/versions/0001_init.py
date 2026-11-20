"""Initial schema: per-field last-writer-wins companies, event log, rejections, embedding cache.

Revision ID: 0001_init
Revises:
"""

from alembic import op

revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # Single place that answers "have we seen this message_id?" (accepted and rejected alike).
    op.execute("""
        CREATE TABLE processed_messages (
          message_id text PRIMARY KEY,
          processed_at timestamptz NOT NULL DEFAULT now()
        )
    """)

    # Every valid event, for audit and replay.
    op.execute("""
        CREATE TABLE company_events (
          id bigserial PRIMARY KEY,
          message_id text UNIQUE NOT NULL REFERENCES processed_messages (message_id),
          reg_no text NOT NULL,
          type text NOT NULL,
          event_at timestamptz,
          payload jsonb NOT NULL,
          received_at timestamptz NOT NULL DEFAULT now()
        )
    """)

    op.execute("""
        CREATE TABLE rejected_messages (
          message_id text PRIMARY KEY,
          body bytea NOT NULL,
          reason text NOT NULL,
          received_at timestamptz NOT NULL DEFAULT now()
        )
    """)

    # Keyed by model so a model swap never mixes vectors.
    op.execute("""
        CREATE TABLE embedding_cache (
          model text NOT NULL,
          text_hash text NOT NULL,
          embedding vector(384) NOT NULL,
          PRIMARY KEY (model, text_hash)
        )
    """)

    # Each field carries the timestamp and message_id of the event that last set it (last-writer-wins).
    op.execute("""
        CREATE TABLE companies (
          reg_no text PRIMARY KEY,
          name text, name_updated_at timestamptz, name_message_id text,
          nature text, nature_updated_at timestamptz, nature_message_id text,
          industry text, industry_updated_at timestamptz, industry_message_id text,
          state text, state_updated_at timestamptz, state_message_id text,
          last_upsert_at timestamptz,
          struck_off_at timestamptz,
          embedding vector(384),
          embedding_source text,          -- 'nature' | 'fallback'
          embedding_model text,
          embedding_text_hash text,
          -- A company is active unless struck off and not upserted again since.
          active boolean GENERATED ALWAYS AS (
            struck_off_at IS NULL OR COALESCE(last_upsert_at, '-infinity') > struck_off_at
          ) STORED
        )
    """)

    op.execute("CREATE INDEX ON companies (active) WHERE embedding IS NOT NULL")


def downgrade() -> None:
    # Children before parents (company_events references processed_messages).
    for table in ("companies", "embedding_cache", "rejected_messages", "company_events", "processed_messages"):
        op.execute(f"DROP TABLE {table}")
    # The vector extension is left installed: other objects in the database may use it.
