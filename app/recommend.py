from typing import List


def recommend(user_id: str, store, k: int = 10) -> List[str]:
    if hasattr(store, "get_user_history"):
        history = store.get_user_history(user_id)
    else:
        history = list(store.user_history[user_id])

    seen = set(history)
    candidates = []
    added = set()

    # 70% co-visitation
    if history:
        last = history[-1]

        if hasattr(store, "get_related_items"):
            related = store.get_related_items(last)
        else:
            related = store.co_visitation[last]

        ranked = sorted(related.items(), key=lambda x: -x[1])

        for item, _ in ranked:
            if item not in seen and item not in added:
                candidates.append(item)
                added.add(item)

    # 30% popularity fallback
    if hasattr(store, "get_popularity_scores"):
        popularity = store.get_popularity_scores()
    else:
        popularity = store.popularity

    popular = sorted(popularity.items(), key=lambda x: -x[1])
    for item, _ in popular:
        if item not in seen and item not in added:
            candidates.append(item)
            added.add(item)

    return candidates[:k]
