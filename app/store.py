from collections import defaultdict, deque


WEIGHTS = {
    "view": 1,
    "click": 3,
    "purchase": 10
}


class FeatureStore:
    def __init__(self, history_size: int = 20):
        self.popularity = defaultdict(int)
        self.user_history = defaultdict(lambda: deque(maxlen=history_size))
        self.co_visitation = defaultdict(lambda: defaultdict(int))

    def add_event(self, user_id: str, item_id: str, event_type: str):
        # popularity
        self.popularity[item_id] += WEIGHTS[event_type]

        # co-visitation
        history = self.user_history[user_id]
        if history:
            prev_item = history[-1]
            self.co_visitation[prev_item][item_id] += 1

        history.append(item_id)
