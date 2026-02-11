from typing import Callable, Optional
import uuid

if __package__:
    from .repositories import FeatureRepository
    from .runtime_utils import normalize_unix_ts_seconds
else:  # pragma: no cover - fallback for direct script execution
    from repositories import FeatureRepository
    from runtime_utils import normalize_unix_ts_seconds


WEIGHTS = {
    "view": 1,
    "click": 3,
    "purchase": 10,
}


def _resolve_db_context_factories():
    if __package__:
        from .db import get_conn, transaction
    else:  # pragma: no cover - fallback for direct script execution
        from db import get_conn, transaction
    return get_conn, transaction


class FeatureStore:
    def __init__(
        self,
        history_size: int = 20,
        repository: Optional[FeatureRepository] = None,
        *,
        get_conn_factory: Optional[Callable] = None,
        transaction_factory: Optional[Callable] = None,
    ):
        self.history_size = history_size
        self.repository = repository or FeatureRepository()

        if get_conn_factory is None or transaction_factory is None:
            default_get_conn, default_transaction = _resolve_db_context_factories()
            if get_conn_factory is None:
                get_conn_factory = default_get_conn
            if transaction_factory is None:
                transaction_factory = default_transaction
        self._get_conn = get_conn_factory
        self._transaction = transaction_factory

    def add_event(
        self,
        user_id: str,
        item_id: str,
        event_type: str,
        ts: Optional[int] = None,
        event_id: Optional[str] = None,
    ) -> bool:
        event_id = event_id or uuid.uuid4().hex
        weight = WEIGHTS[event_type]
        ts_seconds = normalize_unix_ts_seconds(ts)

        with self._transaction() as conn:
            with conn.cursor() as cur:
                return self.repository.apply_feedback_event(
                    cur,
                    event_id=event_id,
                    user_id=user_id,
                    item_id=item_id,
                    event_type=event_type,
                    ts_seconds=ts_seconds,
                    weight=weight,
                    history_size=self.history_size,
                    require_inserted_event=True,
                    skip_same_item_transition=False,
                )

    def get_user_history(self, user_id: str):
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                return self.repository.get_user_history(cur, user_id)

    def get_related_items(self, item_id: str, limit: Optional[int] = None):
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                return self.repository.get_related_items(cur, item_id, limit)

    def get_related_items_for_targets(self, item_id: str, target_item_ids):
        if not target_item_ids:
            return {}
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                return self.repository.get_related_items_for_targets(cur, item_id, target_item_ids)

    def get_popularity_scores(self, limit: Optional[int] = None):
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                return self.repository.get_popularity_scores(cur, limit)

    def get_popularity_scores_for_items(self, item_ids):
        if not item_ids:
            return {}
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                return self.repository.get_popularity_scores_for_items(cur, item_ids)

    def retrieve_candidates(self, user_id: str, limit: int = 200):
        history = self.get_user_history(user_id)
        seen = set(history)
        scores = {}

        # Use a few recent interactions with decay to stabilize near-real-time retrieval.
        for depth, src_item in enumerate(reversed(history[-5:])):
            decay = 1.0 / (depth + 1)
            related = self.get_related_items(src_item, limit=200)
            for item_id, cnt in related.items():
                if item_id in seen:
                    continue
                scores[item_id] = scores.get(item_id, 0.0) + (cnt * decay)

        # Popularity fallback keeps recall robust for cold users / sparse history.
        popularity = self.get_popularity_scores(limit=max(limit * 5, 500))
        for item_id, score in popularity.items():
            if item_id in seen:
                continue
            scores[item_id] = scores.get(item_id, 0.0) + (0.1 * score)

        ranked = sorted(scores.items(), key=lambda pair: pair[1], reverse=True)
        return [item_id for item_id, _ in ranked[:limit]]

    def get_features_for_ranking(self, user_id: str, item_ids):
        history = self.get_user_history(user_id)
        history_set = set(history)
        last_item = history[-1] if history else None

        popularity = self.get_popularity_scores_for_items(item_ids)
        co_vis_last = (
            self.get_related_items_for_targets(last_item, item_ids)
            if last_item
            else {}
        )

        features = {}
        for item_id in item_ids:
            features[item_id] = {
                "seen_recent": 1.0 if item_id in history_set else 0.0,
                "popularity_score": float(popularity.get(item_id, 0.0)),
                "co_vis_last": float(co_vis_last.get(item_id, 0.0)),
            }
        return features
