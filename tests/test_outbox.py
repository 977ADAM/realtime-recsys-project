from app.db import compute_outbox_retry_delay_sec


def test_compute_outbox_retry_delay_sec_exponential_growth():
    assert compute_outbox_retry_delay_sec(1, base_retry_sec=2, max_retry_sec=60) == 2
    assert compute_outbox_retry_delay_sec(2, base_retry_sec=2, max_retry_sec=60) == 4
    assert compute_outbox_retry_delay_sec(3, base_retry_sec=2, max_retry_sec=60) == 8


def test_compute_outbox_retry_delay_sec_respects_cap():
    assert compute_outbox_retry_delay_sec(10, base_retry_sec=2, max_retry_sec=30) == 30
