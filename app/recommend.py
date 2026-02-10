from typing import List


def recommend(user_id: str, store, k: int = 10) -> List[str]:
    history = store.user_history[user_id]
    seen = set(history)

    candidates = []

    # 70% co-visitation
    if history:
        last = history[-1]
        related = store.co_visitation[last]
        ranked = sorted(related.items(), key=lambda x: -x[1])

        for item, _ in ranked:
            if item not in seen:
                candidates.append(item)

    # 30% popularity fallback
    popular = sorted(store.popularity.items(), key=lambda x: -x[1])
    for item, _ in popular:
        if item not in seen:
            candidates.append(item)

    return candidates[:k]
