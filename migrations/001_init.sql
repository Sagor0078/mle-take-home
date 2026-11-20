CREATE EXTENSION IF NOT EXISTS vector;

-- Single place that answers "have we seen this message_id?" (accepted and rejected alike).
CREATE TABLE processed_messages (
  message_id text PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);

-- Every valid event, for audit and replay.
CREATE TABLE company_events (
  id bigserial PRIMARY KEY,
  message_id text UNIQUE NOT NULL REFERENCES processed_messages (message_id),
  reg_no text NOT NULL,
  type text NOT NULL,
  event_at timestamptz,
  payload jsonb NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE rejected_messages (
  message_id text PRIMARY KEY,
  body bytea NOT NULL,
  reason text NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now()
);

-- Keyed by model so a model swap never mixes vectors.
CREATE TABLE embedding_cache (
  model text NOT NULL,
  text_hash text NOT NULL,
  embedding vector(384) NOT NULL,
  PRIMARY KEY (model, text_hash)
);

-- Each field carries the timestamp and message_id of the event that last set it (last-writer-wins).
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
);

CREATE INDEX ON companies (active) WHERE embedding IS NOT NULL;
