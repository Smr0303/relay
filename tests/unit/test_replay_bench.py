"""E6 gate: replaying a 40-event journal must take <= 5 ms (median)."""

from bench.replay_micro import TARGET_MS, measure


def test_replay_of_40_events_is_under_5ms():
    r = measure(iterations=300)
    assert r["events"] == 40
    assert r["p50_ms"] <= TARGET_MS, r
