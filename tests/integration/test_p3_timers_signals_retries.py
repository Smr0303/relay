"""Phase 3 gate: timers, signals, retries with backoff, heartbeats."""

import asyncio
import concurrent.futures as cf
import heapq
import random
import threading
import time
from datetime import datetime

import asyncpg

from relay.sdk.client import Client
from tests.integration.cluster import DSN

APPS = "tests.integration.apps"


def _at(e) -> float:
    return datetime.fromisoformat(e["at"]).timestamp()


def test_signal_vs_timeout_race_never_ships_and_refunds(cluster):
    """1,000 orders; "shipped" lands within +/-500 ms of each order's own timeout. Every order
    must end exactly one way (refund XOR receipt), with effects to match."""
    s1, s2 = cluster.start_server(), cluster.start_server()
    cluster.start_effects()
    for _ in range(4):
        cluster.start_worker(APPS, slots=200)
    n, timeout_s = 1000, 15.0  # generous, so a slow CI runner can still aim the signals
    servers = [s1, s2]
    local = threading.local()

    def client(i: int) -> Client:
        if not hasattr(local, "c"):
            local.c = {s: Client(s) for s in servers}
        return local.c[servers[i % 2]]

    # Pace starts at 50/s so deadlines (and so signals) spread over ~20 s: the harness then
    # sends ~50 signals/s, which even a 2-core CI runner delivers within the +/-500 ms window.
    ids: list = [None] * n
    schedule: list = []  # heap of (send_at, i), filled as each run arms its timeout
    cond = threading.Condition()
    t0 = time.time()

    def start_and_arm(i: int) -> None:
        time.sleep(max(0.0, t0 + i / 50 - time.time()))
        cl = client(i)
        ids[i] = cl.start("race_order", {"order_id": i, "timeout_s": timeout_s})
        cl.wait(ids[i], timeout=60, statuses={"suspended"})
        armed = next(_at(e) for e in cl.events(ids[i]) if e["type"] == "TimerCreated")
        jitter = random.Random(i).uniform(-0.5, 0.5)
        with cond:
            heapq.heappush(schedule, (armed + timeout_s + jitter, i))
            cond.notify()

    def ship(i: int) -> None:
        r = client(i)._http.post(
            f"/v1/runs/{ids[i]}/signals", json={"name": "shipped", "data": {"tracking": f"T{i}"}}
        )
        assert r.status_code in (200, 409), r.text  # 409: already refunded and closed

    lates: list = []
    with cf.ThreadPoolExecutor(64) as senders, cf.ThreadPoolExecutor(64) as starters:
        starting = [starters.submit(start_and_arm, i) for i in range(n)]
        sent = []
        while len(sent) < n:
            with cond:
                if not schedule:
                    cond.wait(0.05)
                    if any(f.done() and f.exception() for f in starting):
                        break
                    continue
                at, i = schedule[0]
                delay = at - time.time()
                if delay > 0:
                    cond.wait(min(delay, 0.05))
                    continue
                heapq.heappop(schedule)
            lates.append(-delay)
            sent.append(senders.submit(ship, i))
        for f in starting + sent:
            f.result()
    lates.sort()
    harness_p90 = lates[int(0.9 * n)]  # reported if the outcome split below fails

    c = Client(s1)
    results = {rid: c.wait(rid, timeout=90)["result"]["status"] for rid in ids}
    shipped = sum(1 for s in results.values() if s == "done")
    refunded = sum(1 for s in results.values() if s == "refunded")
    assert shipped + refunded == n
    assert shipped > 50 and refunded > 50, (
        shipped,
        refunded,
        f"harness p90 late {harness_p90:.2f} s",
    )

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
        assert (f"{run_id}:1", "charge") in fx
        refund = (f"{run_id}:3", "refund") in fx
        receipt = (f"{run_id}:3", "email") in fx
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
