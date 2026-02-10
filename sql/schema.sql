-- Raw events for debugging and analytics
CREATE TABLE IF NOT EXISTS events (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id      TEXT NOT NULL,
  item_id      TEXT NOT NULL,
  event_type   TEXT NOT NULL CHECK (event_type IN ('view', 'click', 'purchase')),
  ts           TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_user_ts ON events(user_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_item_ts ON events(item_id, ts DESC);

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
