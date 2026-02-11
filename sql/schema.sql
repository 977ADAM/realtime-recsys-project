-- Raw events for debugging and analytics
CREATE TABLE IF NOT EXISTS events (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id     TEXT NOT NULL,
  user_id      TEXT NOT NULL,
  item_id      TEXT NOT NULL,
  event_type   TEXT NOT NULL CHECK (event_type IN ('view', 'click', 'purchase')),
  ts           TIMESTAMPTZ NOT NULL
);

ALTER TABLE events ADD COLUMN IF NOT EXISTS event_id TEXT;
UPDATE events
SET event_id = CONCAT('legacy-', id::text)
WHERE event_id IS NULL;
ALTER TABLE events ALTER COLUMN event_id SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_user_ts ON events(user_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_item_ts ON events(item_id, ts DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id);

-- Popularity score per item
CREATE TABLE IF NOT EXISTS item_popularity (
  item_id      TEXT PRIMARY KEY,
  score        BIGINT NOT NULL DEFAULT 0,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_item_popularity_score ON item_popularity(score DESC);

-- Last N user interactions
CREATE TABLE IF NOT EXISTS user_history (
  user_id      TEXT NOT NULL,
  pos          BIGINT NOT NULL,
  item_id      TEXT NOT NULL,
  ts           TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (user_id, pos)
);

CREATE INDEX IF NOT EXISTS idx_user_history_user_pos ON user_history(user_id, pos DESC);

-- Co-visitation transitions prev -> next
CREATE TABLE IF NOT EXISTS co_visitation (
  prev_item_id TEXT NOT NULL,
  next_item_id TEXT NOT NULL,
  cnt          BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (prev_item_id, next_item_id)
);

CREATE INDEX IF NOT EXISTS idx_co_prev_cnt ON co_visitation(prev_item_id, cnt DESC);

-- Impression log for training-set joins and replay/debug
CREATE TABLE IF NOT EXISTS impressions (
  event_id      TEXT NOT NULL,
  user_id       TEXT NOT NULL,
  session_id    TEXT NOT NULL,
  request_id    TEXT NOT NULL,
  ts_ms         BIGINT NOT NULL,
  server_received_ts_ms BIGINT,
  item_id       TEXT NOT NULL,
  position      INT NOT NULL,
  feed_id       TEXT,
  slot          TEXT,
  context       JSONB,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE impressions ADD COLUMN IF NOT EXISTS server_received_ts_ms BIGINT;
DO $$
DECLARE
  pk_cols TEXT[];
BEGIN
  SELECT ARRAY_AGG(att.attname ORDER BY key.ord)
  INTO pk_cols
  FROM pg_constraint con
  JOIN UNNEST(con.conkey) WITH ORDINALITY AS key(attnum, ord) ON TRUE
  JOIN pg_attribute att
    ON att.attrelid = con.conrelid
   AND att.attnum = key.attnum
  WHERE con.conname = 'impressions_pkey'
    AND con.conrelid = 'impressions'::regclass
    AND con.contype = 'p';

  IF pk_cols IS DISTINCT FROM ARRAY['event_id', 'position'] THEN
    IF pk_cols IS NOT NULL THEN
      ALTER TABLE impressions DROP CONSTRAINT impressions_pkey;
    END IF;
    ALTER TABLE impressions ADD CONSTRAINT impressions_pkey PRIMARY KEY (event_id, position);
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_impressions_user_ts ON impressions(user_id, ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_impressions_request ON impressions(request_id);

-- Watch log for supervised labels and quality analytics
CREATE TABLE IF NOT EXISTS watches (
  event_id         TEXT PRIMARY KEY,
  user_id          TEXT NOT NULL,
  session_id       TEXT NOT NULL,
  request_id       TEXT NOT NULL,
  item_id          TEXT NOT NULL,
  ts_ms            BIGINT NOT NULL,
  server_received_ts_ms BIGINT,
  watch_time_sec   DOUBLE PRECISION NOT NULL,
  percent_watched  DOUBLE PRECISION,
  ended            BOOLEAN,
  playback_speed   DOUBLE PRECISION,
  rebuffer_count   INT,
  context          JSONB,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE watches ADD COLUMN IF NOT EXISTS server_received_ts_ms BIGINT;
CREATE INDEX IF NOT EXISTS idx_watches_user_ts ON watches(user_id, ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_watches_request ON watches(request_id);

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
