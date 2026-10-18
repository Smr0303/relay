"""Phase 3 gate: timers, signals, retries with backoff, heartbeats."""

import asyncio
import concurrent.futures as cf
import random
import time
from datetime import datetime

import asyncpg
import httpx

from relay.sdk.client import Client
from tests.integration.cluster import DSN

APPS = "tests.integration.apps"


def _at(e) -> float:
    return datetime.fromisoformat(e["at"]).timestamp()


def test_signal_vs_timeout_race_never_ships_and_refunds(cluster):
    """1,000 orders; "shipped" lands right around the 3-"day" timeout (1 day = 1 s here).
    Every order must end exactly one way, with effects to match."""
    s1, s2 = cluster.start_server(), cluster.start_server()
    cluster.start_effects()
    for _ in range(4):
        cluster.start_worker("examples.order", slots=200)
    c = Client(s1)
    n = 1000
    with cf.ThreadPoolExecutor(32) as pool:
        starts = list(
            pool.map(
                lambda i: (
                    Client([s1, s2][i % 2]).start("order", {"order_id": i, "amount": 5000}),
                    time.monotonic(),
                ),
                range(n),
            )
        )

    def ship(item):
        i, (run_id, _) = item
        rnd = random.Random(i)
        cl = Client([s1, s2][i % 2])
        # waiting on "shipped" now (or already timed out and refunded: nothing to send)
        if (
            cl.wait(run_id, timeout=60, statuses={"suspended", "completed"})["status"]
            != "suspended"
        ):
            return
        armed = next(_at(e) for e in cl.events(run_id) if e["type"] == "TimerCreated")
        time.sleep(max(0.0, armed + 3.0 + rnd.uniform(-0.25, 0.25) - time.time()))
        r = httpx.post(
            f"{[s1, s2][i % 2]}/v1/runs/{run_id}/signals",
            json={"name": "shipped", "data": {"tracking": f"T{i}"}},
        )
        assert r.status_code in (200, 409), r.text  # 409: already refunded and closed

    with cf.ThreadPoolExecutor(200) as pool:
        list(pool.map(ship, enumerate(starts)))

    results = {run_id: c.wait(run_id, timeout=90)["result"]["status"] for run_id, _ in starts}
    shipped = sum(1 for s in results.values() if s == "done")
    refunded = sum(1 for s in results.values() if s == "refunded")
    assert shipped + refunded == n
    assert shipped > 50 and refunded > 50, (shipped, refunded)  # the race really happened

    async def effects():
        conn = await asyncpg.connect(DSN)
        try:
            return {
                (r["key"], r["kind"]) for r in await conn.fetch("SELECT key, kind FROM fx_objects")
            }
        finally:
            await conn.close()

    fx = asyncio.run(effects())
    for run_id, status in results.items():
        charged = (f"{run_id}:2", "charge") in fx
        refund = (f"{run_id}:4", "refund") in fx
        receipt = (f"{run_id}:4", "email") in fx
        assert charged
        assert refund != receipt, (run_id, status, refund, receipt)  # never both, never neither
        assert (status == "refunded") == refund
    assert sum(1 for _, kind in fx if kind == "charge") == n


def test_retry_backoff_follows_the_policy(cluster):
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    run_id = c.start("retrying", {"succeed_on": 4})  # attempts 1-3 fail; 1 s, 2 s, 4 s apart
    run = c.wait(run_id, timeout=30)
    assert run["status"] == "completed" and run["result"] == {"attempt": 4}
    ev = c.events(run_id)
    fails = [e for e in ev if e["type"] == "StepFailed"]
    done = next(e for e in ev if e["type"] == "StepCompleted")
    assert [f["payload"]["attempt"] for f in fails] == [1, 2, 3]
    assert not any(f["payload"]["final"] for f in fails)
    times = [_at(f) for f in fails] + [_at(done)]
    gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
    for gap, want in zip(gaps, [1.0, 2.0, 4.0], strict=True):
        assert abs(gap - want) <= max(0.1 * want, 0.15), (gaps, [1.0, 2.0, 4.0])


def test_retries_exhausted_fail_the_run(cluster):
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    run = c.wait(c.start("gives_up", None), timeout=30)
    assert run["status"] == "failed" and run["error"]["type"] == "StepError"
    fails = [e for e in c.events(run["run_id"]) if e["type"] == "StepFailed"]
    assert [f["payload"]["attempt"] for f in fails] == [1, 2, 3, 4, 5]
    assert fails[-1]["payload"]["final"] is True


def test_non_retryable_error_fails_at_once(cluster):
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    run = c.wait(c.start("non_retryable", None), timeout=15)
    assert run["status"] == "failed"
    fails = [e for e in c.events(run["run_id"]) if e["type"] == "StepFailed"]
    assert len(fails) == 1 and fails[0]["payload"]["final"] is True
    assert fails[0]["payload"]["error"]["type"] == "InvalidCard"


def test_heartbeats_keep_a_long_step_alive(cluster):
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    run = c.wait(c.start("long_running", {"seconds": 6}), timeout=20)
    assert run["status"] == "completed"
    assert run["result"] == {"slept": 6, "attempt": 1}  # never re-run despite the 2 s lease
    assert not [e for e in c.events(run["run_id"]) if e["type"] == "StepFailed"]


def test_durable_sleep_survives_a_server_restart(cluster):
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    run_id = c.start("sleeper", {"sleep_s": 4})
    c.wait(run_id, timeout=10, statuses={"suspended"})
    for p in cluster.procs:  # every server process dies; the timer is just a row
        p.kill()
        p.wait()
    cluster.procs.clear()
    cluster.server_urls.clear()
    new = cluster.start_server()
    cluster.start_worker(APPS, server=new)
    run = Client(new).wait(run_id, timeout=15)
    assert run["status"] == "completed" and run["result"] == "woke"
