CREATE TABLE IF NOT EXISTS event_outbox (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id          TEXT NOT NULL,
  event_type        TEXT NOT NULL CHECK (event_type IN ('impression', 'watch')),
  kafka_topic       TEXT NOT NULL,
  kafka_key         TEXT NOT NULL,
  payload           JSONB NOT NULL,
  status            TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'publishing', 'published', 'dead')),
  attempt_count     INT NOT NULL DEFAULT 0,
  max_attempts      INT NOT NULL DEFAULT 25 CHECK (max_attempts > 0),
  next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_expires_at  TIMESTAMPTZ,
  last_error        TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at      TIMESTAMPTZ,
  UNIQUE (event_id, event_type)
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_dispatch
  ON event_outbox (status, next_attempt_at, id)
  WHERE published_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_event_outbox_created_at
  ON event_outbox (created_at);

CREATE INDEX IF NOT EXISTS idx_event_outbox_published_at
  ON event_outbox (published_at);

CREATE TABLE IF NOT EXISTS feature_event_consumed (
  stream_event_id    TEXT PRIMARY KEY,
  source_topic       TEXT NOT NULL,
  source_partition   INT NOT NULL,
  source_offset      BIGINT NOT NULL,
  consumed_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_feature_event_consumed_topic_partition_offset
  ON feature_event_consumed (source_topic, source_partition, source_offset DESC);
