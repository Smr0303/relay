"""Phase 2 gate: Postgres + server + worker, with real processes and SIGKILL."""

import concurrent.futures as cf
import time

import httpx

from relay.sdk.client import Client

APPS = "tests.integration.apps"


def _wait_for(pred, timeout=15.0, every=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(every)
    raise TimeoutError("condition not met")


def test_kill_9_mid_step_completes_on_another_worker_with_one_charge(cluster):
    server = cluster.start_server()
    cluster.start_effects()
    w1 = cluster.start_worker(APPS)
    c = Client(server)
    run_id = c.start("slow_order", {"amount": 5000})

    _wait_for(lambda: cluster.stats()["requests"] >= 1)  # Stripe was charged...
    cluster.kill_worker(w1)  # ...and the worker dies before it can ack
    cluster.start_worker(APPS)

    run = c.wait(run_id, timeout=20)
    assert run["status"] == "completed"
    stats = cluster.stats()
    assert stats["objects"] == {"charge": 1}  # exactly one charge
    assert stats["keys_seen_twice"] == 1  # the retry reused the key and was deduplicated
    types = [(e["type"], e["payload"].get("attempt")) for e in c.events(run_id)]
    assert ("StepFailed", 1) in types  # attempt 1: lease expired (the kill), visible in history
    assert ("StepCompleted", 2) in types


def test_repeated_request_id_returns_the_same_run(cluster):
    server = cluster.start_server()
    c = Client(server)
    ids = {c.start("counter", {"n": 1}, request_id="ord-123") for _ in range(5)}
    assert len(ids) == 1
    with cf.ThreadPoolExecutor(10) as pool:  # and under concurrency
        ids = set(
            pool.map(
                lambda _: Client(server).start("counter", {"n": 1}, request_id="ord-456"), range(20)
            )
        )
    assert len(ids) == 1


def _poll(server, worker_id, handles=(("counter", 1),)):
    r = httpx.post(
        f"{server}/v1/tasks/poll",
        json={"worker_id": worker_id, "handles": list(handles), "wait_s": 5},
        timeout=10,
    )
    assert r.status_code == 200, r.status_code
    return r.json()[0]


def test_zombie_ack_after_lease_expiry_is_refused(cluster):
    server = cluster.start_server()
    c = Client(server)
    run_id = c.start("counter", {"n": 1})
    zombie = _poll(server, "zombie")  # ADVANCE leased for 3 s (RELAY_ADVANCE_TIMEOUT_S)
    time.sleep(4.5)  # lease expires; the reaper takes the task back
    fresh = _poll(server, "fresh")
    assert fresh["task_id"] == zombie["task_id"] and fresh["attempt"] == 2

    ack = {"lease_token": zombie["lease_token"], "journal_len": 1, "commands": []}
    r = httpx.post(f"{server}/v1/tasks/{zombie['task_id']}/ack/advance", json=ack)
    assert r.status_code == 409 and "lease lost" in r.text
    ack["lease_token"] = fresh["lease_token"]
    r = httpx.post(f"{server}/v1/tasks/{fresh['task_id']}/ack/advance", json=ack)
    assert r.status_code == 200
    assert c.get(run_id)["status"] != "parked"


def test_racing_advances_one_wins_and_the_journal_has_no_gaps(cluster):
    server = cluster.start_server()
    c = Client(server)
    run_id = c.start("counter", {"n": 1})
    a = _poll(server, "A")  # A reads a 1-event journal
    c.signal(run_id, "tick", {"v": 1})  # a new event lands: seq 2 + a fresh ADVANCE
    b = _poll(server, "B")  # B reads the 2-event journal
    assert len(a["journal"]) == 1 and len(b["journal"]) == 2

    create = {"kind": "create_timer", "call_seq": 1, "fire_in_s": None, "signal": "tick"}
    r = httpx.post(
        f"{server}/v1/tasks/{a['task_id']}/ack/advance",
        json={"lease_token": a["lease_token"], "journal_len": 1, "commands": [create]},
    )
    assert r.status_code == 409 and "stale" in r.text
    done = {"kind": "complete_run", "result": 1}
    r = httpx.post(
        f"{server}/v1/tasks/{b['task_id']}/ack/advance",
        json={"lease_token": b["lease_token"], "journal_len": 2, "commands": [create, done]},
    )
    assert r.status_code == 200
    seqs = [e["seq"] for e in c.events(run_id)]
    assert seqs == list(range(1, len(seqs) + 1))
    assert c.get(run_id)["status"] == "completed"


def test_signal_storm_on_many_runs_loses_nothing(cluster):
    """50 runs x 20 concurrent signals, two servers, three workers: every signal counted once,
    every journal gapless."""
    s1, s2 = cluster.start_server(), cluster.start_server()
    for _ in range(3):
        cluster.start_worker(APPS)
    c = Client(s1)
    runs = [c.start("counter", {"n": 20}) for _ in range(50)]

    def send(i):
        Client([s1, s2][i % 2]).signal(runs[i % 50], "tick", {"v": 1})

    with cf.ThreadPoolExecutor(32) as pool:
        list(pool.map(send, range(50 * 20)))
    for run_id in runs:
        run = c.wait(run_id, timeout=60)
        assert run["status"] == "completed" and run["result"] == 20, run
        seqs = [e["seq"] for e in c.events(run_id)]
        assert seqs == list(range(1, len(seqs) + 1))
