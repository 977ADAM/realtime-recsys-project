from app.ranking import rank_items_by_features


def test_rank_items_by_features_orders_by_weighted_score():
    candidates = ["item_a", "item_b", "item_c"]
    features = {
        "item_a": {"co_vis_last": 4.0, "popularity_score": 1.0, "seen_recent": 0.0},
        "item_b": {"co_vis_last": 3.0, "popularity_score": 4.0, "seen_recent": 0.0},
        "item_c": {"co_vis_last": 2.0, "popularity_score": 6.0, "seen_recent": 1.0},
    }

    ranked = rank_items_by_features(candidates=candidates, features=features, k=3)

    assert ranked == ["item_b", "item_c", "item_a"]


def test_rank_items_by_features_respects_k_and_custom_weights():
    candidates = ["x", "y", "z"]
    features = {
        "x": {"co_vis_last": 2.0, "popularity_score": 1.0, "seen_recent": 1.0},
        "y": {"co_vis_last": 1.0, "popularity_score": 2.0, "seen_recent": 0.0},
        "z": {"co_vis_last": 0.0, "popularity_score": 3.0, "seen_recent": 0.0},
    }
    weights = {"co_vis_last": 1.0, "popularity_score": 1.0, "seen_recent": 100.0}

    ranked = rank_items_by_features(candidates=candidates, features=features, k=2, weights=weights)

    assert ranked == ["z", "y"]
