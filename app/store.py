from typing import Callable, Iterable, Optional
import uuid

if __package__:
    from .online_cache import OnlineFeatureCache
    from .repositories import FeatureRepository
    from .runtime_utils import normalize_unix_ts_seconds, positive_int_env
else:  # pragma: no cover - fallback for direct script execution
    from online_cache import OnlineFeatureCache
    from repositories import FeatureRepository
    from runtime_utils import normalize_unix_ts_seconds, positive_int_env


WEIGHTS = {
    "view": 1,
    "click": 3,
    "purchase": 10,
}

_HISTORY_CACHE_PREFIX = "reco:user_history:"
_RELATED_CACHE_PREFIX = "reco:related:"
_POPULARITY_CACHE_PREFIX = "reco:popularity:"


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
        cache: Optional[OnlineFeatureCache] = None,
        *,
        get_conn_factory: Optional[Callable] = None,
        transaction_factory: Optional[Callable] = None,
    ):
        self.history_size = history_size
        self.repository = repository or FeatureRepository()
        self.cache = cache or OnlineFeatureCache()

        self.history_cache_ttl_sec = positive_int_env("ONLINE_CACHE_HISTORY_TTL_SEC", 20)
        self.related_cache_ttl_sec = positive_int_env("ONLINE_CACHE_RELATED_TTL_SEC", 30)
        self.popularity_cache_ttl_sec = positive_int_env("ONLINE_CACHE_POPULARITY_TTL_SEC", 5)

        if get_conn_factory is None or transaction_factory is None:
            default_get_conn, default_transaction = _resolve_db_context_factories()
            if get_conn_factory is None:
                get_conn_factory = default_get_conn
            if transaction_factory is None:
                transaction_factory = default_transaction
        self._get_conn = get_conn_factory
        self._transaction = transaction_factory

    def start_cache(self) -> bool:
        return self.cache.start()

    def stop_cache(self) -> None:
        self.cache.close()

    def cache_snapshot(self) -> dict:
        return self.cache.snapshot()

    def cache_ready(self) -> bool:
        return self.cache.is_ready

    def _history_cache_key(self, user_id: str) -> str:
        return f"{_HISTORY_CACHE_PREFIX}{user_id}"

    def _related_cache_key(self, item_id: str, limit: int) -> str:
        return f"{_RELATED_CACHE_PREFIX}{item_id}:{limit}"

    def _popularity_cache_key(self, limit: int) -> str:
        return f"{_POPULARITY_CACHE_PREFIX}{limit}"

    @staticmethod
    def _normalize_score_map(payload: dict) -> dict:
        return {str(item_id): float(score) for item_id, score in payload.items()}

    def invalidate_user_history_cache(self, user_id: str) -> None:
        self.cache.delete(self._history_cache_key(user_id))

    def invalidate_popularity_cache(self) -> None:
        self.cache.delete_prefix(_POPULARITY_CACHE_PREFIX)

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
                applied = self.repository.apply_feedback_event(
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

        if applied:
            self.invalidate_user_history_cache(user_id)
            self.invalidate_popularity_cache()
        return applied

    def get_user_history(self, user_id: str):
        cache_key = self._history_cache_key(user_id)
        cached = self.cache.get(cache_key)
        if isinstance(cached, list):
            return [str(item_id) for item_id in cached]

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                history = self.repository.get_user_history(cur, user_id)

        self.cache.set(cache_key, history, ttl_sec=self.history_cache_ttl_sec)
        return history

    def get_related_items(self, item_id: str, limit: Optional[int] = None):
        normalized_limit = max(int(limit or 200), 1)
        cache_key = self._related_cache_key(item_id, normalized_limit)
        cached = self.cache.get(cache_key)
        if isinstance(cached, dict):
            return self._normalize_score_map(cached)

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                related = self.repository.get_related_items(cur, item_id, normalized_limit)

        normalized = self._normalize_score_map(related)
        self.cache.set(cache_key, normalized, ttl_sec=self.related_cache_ttl_sec)
        return normalized

    def get_related_items_for_sources(self, source_item_ids: Iterable[str], limit_per_source: int = 200) -> dict:
        source_ids = list(dict.fromkeys(source_item_ids))
        if not source_ids:
            return {}

        normalized_limit = max(int(limit_per_source), 1)
        result: dict[str, dict[str, float]] = {}
        missing: list[str] = []

        for source_id in source_ids:
            cache_key = self._related_cache_key(source_id, normalized_limit)
            cached = self.cache.get(cache_key)
            if isinstance(cached, dict):
                result[source_id] = self._normalize_score_map(cached)
            else:
                missing.append(source_id)

        if missing:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    fetched = self.repository.get_related_items_for_sources(
                        cur,
                        source_item_ids=missing,
                        limit_per_source=normalized_limit,
                    )

            for source_id in missing:
                source_related = self._normalize_score_map(fetched.get(source_id, {}))
                result[source_id] = source_related
                cache_key = self._related_cache_key(source_id, normalized_limit)
                self.cache.set(cache_key, source_related, ttl_sec=self.related_cache_ttl_sec)

        return result

    def get_related_items_for_targets(self, item_id: str, target_item_ids):
        if not target_item_ids:
            return {}
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                related = self.repository.get_related_items_for_targets(cur, item_id, target_item_ids)
        return self._normalize_score_map(related)

    def get_popularity_scores(self, limit: Optional[int] = None):
        normalized_limit = max(int(limit or 500), 1)
        cache_key = self._popularity_cache_key(normalized_limit)
        cached = self.cache.get(cache_key)
        if isinstance(cached, dict):
            return self._normalize_score_map(cached)

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                popularity = self.repository.get_popularity_scores(cur, normalized_limit)

        normalized = self._normalize_score_map(popularity)
        self.cache.set(cache_key, normalized, ttl_sec=self.popularity_cache_ttl_sec)
        return normalized

    def get_popularity_scores_for_items(self, item_ids):
        if not item_ids:
            return {}
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                popularity = self.repository.get_popularity_scores_for_items(cur, item_ids)
        return self._normalize_score_map(popularity)

    def _retrieve_candidates_from_history(self, history: list[str], limit: int) -> list[str]:
        seen = set(history)
        scores = {}

        # Query co-visitation for all recent source items in one DB round-trip.
        source_items = list(reversed(history[-5:]))
        related_by_source = self.get_related_items_for_sources(source_items, limit_per_source=200)

        for depth, src_item in enumerate(source_items):
            decay = 1.0 / (depth + 1)
            related = related_by_source.get(src_item, {})
            for item_id, cnt in related.items():
                if item_id in seen:
                    continue
                scores[item_id] = scores.get(item_id, 0.0) + (float(cnt) * decay)

        # Popularity fallback keeps recall robust for cold users / sparse history.
        popularity = self.get_popularity_scores(limit=max(limit * 5, 500))
        for item_id, score in popularity.items():
            if item_id in seen:
                continue
            scores[item_id] = scores.get(item_id, 0.0) + (0.1 * float(score))

        ranked = sorted(scores.items(), key=lambda pair: pair[1], reverse=True)
        return [item_id for item_id, _ in ranked[:limit]]

    def retrieve_candidates(self, user_id: str, limit: int = 200):
        history = self.get_user_history(user_id)
        return self._retrieve_candidates_from_history(history, limit)

    def retrieve_candidates_and_history(self, user_id: str, limit: int = 200) -> tuple[list[str], list[str]]:
        history = self.get_user_history(user_id)
        candidates = self._retrieve_candidates_from_history(history, limit)
        return candidates, history

    def get_features_for_ranking(self, user_id: str, item_ids, user_history: Optional[list[str]] = None):
        history = user_history if user_history is not None else self.get_user_history(user_id)
        history_set = set(history)
        last_item = history[-1] if history else None

        popularity = self.get_popularity_scores_for_items(item_ids)
        co_vis_last = {}
        if last_item:
            related_last = self.get_related_items(last_item, limit=max(len(item_ids), 200))
            co_vis_last = {item_id: float(related_last.get(item_id, 0.0)) for item_id in item_ids}

        features = {}
        for item_id in item_ids:
            features[item_id] = {
                "seen_recent": 1.0 if item_id in history_set else 0.0,
                "popularity_score": float(popularity.get(item_id, 0.0)),
                "co_vis_last": float(co_vis_last.get(item_id, 0.0)),
            }
        return features
