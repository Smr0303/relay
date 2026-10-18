"""E7 gate (slow; `make chaos`): 1,000 payments + 200 LLM pipelines under random kills."""

import pytest

from tests.chaos.chaos import run


@pytest.mark.chaos
def test_exactly_once_effects_under_chaos(pg):
    report = run(orders=1000, lectures=200, workers=4, servers=2, timeout_s=600)
    assert report["verdict"] == "PASS", report["first_problems"]
    assert report["effects"] == {"charge": 1000, "llm": 600, "email": 1000, "refund": 500}
    assert report["workers_killed"] > 0 and report["servers_killed"] > 0
