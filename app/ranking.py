from typing import Mapping, Optional


DEFAULT_RANKING_WEIGHTS = {
    "co_vis_last": 1.5,
    "popularity_score": 1.0,
    "seen_recent": 1.5,
}


def rank_items_by_features(
    candidates: list[str],
    features: Mapping[str, Mapping[str, float]],
    k: int,
    weights: Optional[Mapping[str, float]] = None,
) -> list[str]:
    if not candidates or k <= 0:
        return []

    active_weights = weights or DEFAULT_RANKING_WEIGHTS

    def _score(item_id: str) -> tuple[float, float, float]:
        item_features = features.get(item_id, {})
        score = (
            active_weights["co_vis_last"] * float(item_features.get("co_vis_last", 0.0))
            + active_weights["popularity_score"] * float(item_features.get("popularity_score", 0.0))
            - active_weights["seen_recent"] * float(item_features.get("seen_recent", 0.0))
        )
        # Tie-breaking prefers higher popularity and stronger short-term co-visitation.
        return (
            score,
            float(item_features.get("popularity_score", 0.0)),
            float(item_features.get("co_vis_last", 0.0)),
        )

    ranked = sorted(candidates, key=_score, reverse=True)
    return ranked[:k]
