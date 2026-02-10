import unittest

from app.ranking import rank_items_by_features


class RankingTests(unittest.TestCase):
    def test_rank_items_is_deterministic(self):
        candidates = ["item-3", "item-1", "item-2"]
        features = {
            "item-1": {"co_vis_last": 2.0, "popularity_score": 1.0, "seen_recent": 0.0},
            "item-2": {"co_vis_last": 1.0, "popularity_score": 3.0, "seen_recent": 0.0},
            "item-3": {"co_vis_last": 0.0, "popularity_score": 2.0, "seen_recent": 1.0},
        }

        first = rank_items_by_features(candidates=candidates, features=features, k=3)
        second = rank_items_by_features(candidates=candidates, features=features, k=3)

        self.assertEqual(first, ["item-1", "item-2", "item-3"])
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
