"""Phase 5 gate: versioning, patch gates, non-determinism parking, cancel, continue_as_new."""

import concurrent.futures as cf
import time

import httpx

from relay.sdk.client import Client

APPS = "tests.integration.apps"


def _worker(cluster, app, **env):
    old = dict(cluster.env)
    cluster.env.update(env)
    try:
        return cluster.start_worker(app)
    finally:
        cluster.env.clear()
        cluster.env.update(old)


def test_deploy_v2_while_500_v1_runs_sleep(cluster):
    server = cluster.start_server()
    _worker(cluster, "tests.integration.apps_versions", APP_VERSION="1")
    c = Client(server)
    time.sleep(1.5)  # the v1 worker has polled: v1 is the live version
    with cf.ThreadPoolExecutor(16) as pool:
        old = list(
            pool.map(
                lambda _: Client(server).start("pricing", {"base": 10, "sleep_s": 6}), range(500)
            )
        )
    _worker(cluster, "tests.integration.apps_versions", APP_VERSION="2")  # deploy v2
    time.sleep(1.5)
    new = [c.start("pricing", {"base": 10}) for _ in range(50)]  # no version given: newest
    for rid in old:
        assert c.wait(rid, timeout=60)["result"] == {"code_version": 1, "price": 10}
    for rid in new:
        run = c.wait(rid, timeout=30)
        assert run["version"] == 2 and run["result"] == {"code_version": 2, "price": 20}


def test_patch_gate_rollout_mid_flight(cluster):
    server = cluster.start_server()
    w_old = _worker(cluster, "tests.integration.apps_flow", FLOW_CODE="old")
    c = Client(server)
    old_run = c.start("flow", {"sleep_s": 3})
    c.wait(old_run, timeout=10, statuses={"suspended"})
    cluster.kill_worker(w_old)
    _worker(cluster, "tests.integration.apps_flow", FLOW_CODE="patched")  # same version
    new_run = c.start("flow", {"sleep_s": 1})
    assert c.wait(old_run, timeout=20)["result"] == ["a", "b"]
    assert c.wait(new_run, timeout=20)["result"] == ["a", "a2", "b"]


def test_unsafe_change_parks_the_run_and_resume_finishes_it(cluster):
    server = cluster.start_server()
    w_old = _worker(cluster, "tests.integration.apps_flow", FLOW_CODE="old")
    c = Client(server)
    run_id = c.start("flow", {"sleep_s": 2})
    c.wait(run_id, timeout=10, statuses={"suspended"})
    cluster.kill_worker(w_old)
    w_bad = _worker(cluster, "tests.integration.apps_flow", FLOW_CODE="unpatched")
    run = c.wait(run_id, timeout=20, statuses={"parked"})
    assert run["error"]["type"] == "NonDeterminismError"
    assert "relay_nondeterminism_total 1.0" in httpx.get(server + "/metrics").text
    cluster.kill_worker(w_bad)  # roll back the bad deploy...
    _worker(cluster, "tests.integration.apps_flow", FLOW_CODE="old")
    c.resume(run_id)  # ...and resume: nothing was lost
    assert c.wait(run_id, timeout=20)["result"] == ["a", "b"]


def test_poison_replay_is_retried_then_parked(cluster):
    cluster.env["RELAY_ADVANCE_BACKOFF_S"] = "0.05"
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    run = c.wait(c.start("poison", None), timeout=30, statuses={"parked"})
    assert run["error"]["type"] == "TypeError" and run["error"]["attempt"] == 5


def test_cancel_runs_the_refund(cluster):
    server = cluster.start_server()
    cluster.start_effects()
    cluster.env["ORDER_DAY_SECONDS"] = "60"  # nothing times out by itself during the test
    cluster.start_worker("examples.order")
    c = Client(server)
    run_id = c.start("order", {"order_id": 7, "amount": 5000})
    c.wait(run_id, timeout=10, statuses={"suspended"})  # charged, waiting for "shipped"
    c.cancel(run_id)
    run = c.wait(run_id, timeout=15)
    assert run["status"] == "cancelled"
    assert cluster.stats()["objects"] == {"charge": 1, "refund": 1}
    r = httpx.post(f"{server}/v1/runs/{run_id}/cancel")
    assert r.status_code == 409  # already closed


def test_100k_iteration_loop_via_continue_as_new(cluster):
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    rid = c.start("looper", {"total": 0, "round": 0, "rounds": 100, "per_round": 1000})
    chain = 1
    while True:
        run = c.wait(rid, timeout=60)
        assert run["events"] <= 1003
        if run["status"] != "continued":
            break
        rid, chain = run["result"]["continued_as"], chain + 1
    assert run["status"] == "completed" and run["result"] == 100_000 and chain == 100


def test_runaway_loop_hits_the_journal_limit(cluster):
    cluster.env["RELAY_JOURNAL_LIMIT"] = "500"
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    rid = c.start("looper", {"total": 0, "round": 0, "rounds": 1, "per_round": 1000})
    run = c.wait(rid, timeout=20)
    assert run["status"] == "failed" and run["error"]["type"] == "JournalLimit"


def test_large_step_result_is_kept_out_of_the_journal_row(cluster, tmp_path):
    cluster.env["RELAY_BLOB_DIR"] = str(tmp_path)
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    run = c.wait(c.start("big_result", {"kb": 300}), timeout=20)
    assert run["status"] == "completed" and run["result"] == 300 * 1024
    done = next(e for e in c.events(run["run_id"]) if e["type"] == "StepCompleted")
    assert "result" not in done["payload"] and done["payload"]["bytes"] > 300 * 1024
    assert list(tmp_path.iterdir())
