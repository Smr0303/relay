"""Phase 6 gate, against the obs stack (`make obs-up`): traces and the four alerts.

    uv run python -m tests.obs.drill

1. Trace: kill a worker mid-step; the run's trace in Jaeger (trace id = run id) holds the
   root span and BOTH attempts of the step.
2. Alerts: induce each failure and wait for Prometheus to fire its alert:
   - RelayTaskBacklog     a run of a workflow no worker serves (ready task ages > 60 s)
   - RelayTimerLag        timer loops disabled on every server (a due timer ages > 30 s)
   - RelayLeaseExpiries   a worker crash loop while steps are running
   - RelayNonDeterminism  deploy an ungated code change under a live run
"""

import json
import time
import uuid

import httpx

from relay.sdk.client import Client
from tests.integration.cluster import Cluster

JAEGER = "http://localhost:16686"
PROM = "http://localhost:9090"
PORTS = (8080, 8081)


def wait_for(pred, timeout, every=1.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(every)
    return None


def trace_check(cluster: Cluster, server: str) -> dict:
    cluster.start_effects()
    w = cluster.start_worker("tests.integration.apps")
    c = Client(server)
    run_id = c.start("slow_order", {"amount": 5000})
    wait_for(lambda: cluster.stats()["requests"] >= 1, 15, 0.05)
    cluster.kill_worker(w)  # mid-step: charged, not acked
    cluster.start_worker("tests.integration.apps")
    c.wait(run_id, timeout=30)
    trace_id = uuid.UUID(run_id).hex

    def spans():
        r = httpx.get(f"{JAEGER}/api/traces/{trace_id}", timeout=5)
        if r.status_code != 200 or not r.json().get("data"):
            return None
        got = r.json()["data"][0]["spans"]
        names = [s["operationName"] for s in got]
        return got if "run slow_order" in names else None

    got = wait_for(spans, 30) or []
    attempts = sorted(
        next(t["value"] for t in s["tags"] if t["key"] == "relay.attempt")
        for s in got
        if s["operationName"] == "step slow_charge"
    )
    return {
        "run_id": run_id,
        "trace_url": f"{JAEGER}/trace/{trace_id}",
        "spans": sorted({s["operationName"] for s in got}),
        "step_attempts_in_trace": attempts,
        "ok": attempts == [1, 2] and any(s["operationName"] == "run slow_order" for s in got),
    }


def firing() -> set[str]:
    r = httpx.get(f"{PROM}/api/v1/alerts", timeout=5).json()
    return {a["labels"]["alertname"] for a in r["data"]["alerts"] if a["state"] == "firing"}


def alert_drill(cluster: Cluster) -> dict:
    # every server restarts with its timer loop off (the "broken timer service")
    for url in list(cluster.server_urls):
        cluster.kill_server(url)
    cluster.env["RELAY_DISABLE_TIMERS"] = "1"
    for port in PORTS:
        cluster.start_server(port)
    for w in cluster.workers:
        if w.poll() is None:
            cluster.kill_worker(w)
    cluster.workers.clear()
    server = cluster.server_urls[0]
    c = Client(server)
    t0 = time.monotonic()

    c.start("nobody_serves_this", {})  # -> RelayTaskBacklog after 60 s
    cluster.env["FLOW_CODE"] = "old"
    flow_w = cluster.start_worker("tests.integration.apps_flow")
    timer_run = c.start("flow", {"sleep_s": 1})  # its timer will never fire -> RelayTimerLag
    nd_run = c.start("flow", {"sleep_s": 1})
    c.wait(nd_run, timeout=10, statuses={"suspended"})
    c.wait(timer_run, timeout=10, statuses={"suspended"})
    cluster.kill_worker(flow_w)
    cluster.env["FLOW_CODE"] = "unpatched"  # the bad deploy
    cluster.start_worker("tests.integration.apps_flow")
    c.signal(nd_run, "poke", None)  # wakes it: replay on the new code -> parked
    c.wait(nd_run, timeout=15, statuses={"parked"})

    # crash loop: slow steps while their worker keeps dying -> RelayLeaseExpiries
    w = cluster.start_worker("tests.integration.apps")
    time.sleep(1.5)  # worker up and polling
    for _ in range(12):
        for _ in range(20):
            c.start("slow_order", {"amount": 1})
        time.sleep(1.0)  # their 1.5 s steps are mid-flight...
        cluster.kill_worker(w)  # ...when the worker dies
        w = cluster.start_worker("tests.integration.apps")
        time.sleep(1.5)

    want = {"RelayTaskBacklog", "RelayTimerLag", "RelayLeaseExpiries", "RelayNonDeterminism"}
    seen: dict[str, float] = {}
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline and set(seen) != want:
        for name in firing() & want:
            seen.setdefault(name, round(time.monotonic() - t0, 1))
        time.sleep(2)
    return {"fired_after_s": seen, "missing": sorted(want - set(seen)), "ok": set(seen) == want}


def main() -> None:
    cluster = Cluster(
        {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318", "OTEL_BSP_SCHEDULE_DELAY": "500"}
    )
    import asyncio

    from tests.integration.cluster import ensure_database, truncate

    asyncio.run(ensure_database())
    asyncio.run(truncate())
    try:
        server = cluster.start_server(PORTS[0])
        cluster.start_server(PORTS[1])
        httpx.post(f"{PROM}/-/reload")
        result = {"trace": trace_check(cluster, server), "alerts": alert_drill(cluster)}
    finally:
        cluster.stop()
    result["verdict"] = "PASS" if result["trace"]["ok"] and result["alerts"]["ok"] else "FAIL"
    print(json.dumps(result, indent=2))
    with open("bench/runs.jsonl", "a") as f:
        f.write(json.dumps({"bench": "obs_drill", **result}) + "\n")


if __name__ == "__main__":
    main()
