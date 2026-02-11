CREATE TABLE IF NOT EXISTS feature_consumer_dlq (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_topic      TEXT NOT NULL,
  source_partition  INT NOT NULL,
  source_offset     BIGINT NOT NULL,
  kafka_key         TEXT NOT NULL DEFAULT '',
  payload           JSONB NOT NULL,
  error_text        TEXT NOT NULL,
  retry_count       INT NOT NULL CHECK (retry_count > 0),
  replay_count      INT NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
  last_replay_error TEXT,
  status            TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'replayed')),
  first_failed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_failed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  replayed_at       TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX IF NOT EXISTS idx_feature_consumer_dlq_status_id
  ON feature_consumer_dlq (status, id);

CREATE INDEX IF NOT EXISTS idx_feature_consumer_dlq_topic_status
  ON feature_consumer_dlq (source_topic, status, id);
