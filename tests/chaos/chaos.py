"""The demo: run real workflows while killing everything, then check the books.

    uv run python -m tests.chaos.chaos --orders 1000 --lectures 200

While 1,000 orders (charge, wait for "shipped" or time out and refund, receipt, sleep, review
email) and 200 lecture-note pipelines (3 billed LLM calls each, with a misbehaving LLM) run:
  * a random worker is SIGKILLed every 2-5 s and replaced
  * a random Postgres backend is terminated every 2-5 s (server pools, LISTEN, effects service)
  * a random server is SIGKILLed every 10-20 s and restarted on the same port

Then: every run must have completed, each order charged exactly once (and refunded XOR
emailed), each lecture billed exactly 3 LLM calls, every journal gapless.
"""

import argparse
import asyncio
import concurrent.futures as cf
import json
import random
import threading
import time

import asyncpg
import httpx

from relay.server.engine import init_connection
from tests.integration.cluster import DSN, Cluster, ensure_database, truncate

TERMINAL = {"completed", "failed", "cancelled", "parked"}


class Api:
    """Client calls that ride out a dead server: retry, rotating servers."""

    def __init__(self, servers: list[str]) -> None:
        self.servers = servers
        self.http = httpx.Client(timeout=10)

    def call(self, method: str, path: str, i: int = 0, **kw) -> httpx.Response:
        for attempt in range(200):
            url = self.servers[(i + attempt) % len(self.servers)]
            try:
                r = self.http.request(method, url + path, **kw)
                if r.status_code < 500:
                    return r
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        raise RuntimeError(f"{method} {path} kept failing")


def chaos_loop(cluster: Cluster, stop: threading.Event, report: dict, apps: list[str]) -> None:
    rnd = random.Random(7)
    next_worker = time.monotonic() + rnd.uniform(2, 5)
    next_pg = time.monotonic() + rnd.uniform(2, 5)
    next_server = time.monotonic() + rnd.uniform(10, 20)
    while not stop.is_set():
        now = time.monotonic()
        if now >= next_worker:
            alive = [w for w in cluster.workers if w.poll() is None]
            if alive:
                victim = rnd.choice(alive)
                cluster.kill_worker(victim)
                report["workers_killed"] += 1
                cluster.start_worker(apps, slots=100)
            next_worker = now + rnd.uniform(2, 5)
        if now >= next_pg:
            killed = asyncio.run(kill_backend(rnd))
            report["pg_backends_terminated"] += killed
            next_pg = now + rnd.uniform(2, 5)
        if now >= next_server:
            url = rnd.choice(cluster.server_urls)
            port = cluster.kill_server(url)
            report["servers_killed"] += 1
            time.sleep(1.0)
            cluster.start_server(port)
            next_server = now + rnd.uniform(10, 20)
        time.sleep(0.1)


async def kill_backend(rnd: random.Random) -> int:
    conn = await asyncpg.connect(DSN)
    try:
        pids = [
            r["pid"]
            for r in await conn.fetch(
                "SELECT pid FROM pg_stat_activity WHERE datname = current_database() "
                "AND pid <> pg_backend_pid() AND backend_type = 'client backend'"
            )
        ]
        if not pids:
            return 0
        await conn.execute("SELECT pg_terminate_backend($1)", rnd.choice(pids))
        return 1
    finally:
        await conn.close()


async def fetch_books() -> tuple[dict, dict, dict]:
    conn = await asyncpg.connect(DSN)
    await init_connection(conn)
    try:
        runs = {
            str(r["run_id"]): dict(r)
            for r in await conn.fetch(
                "SELECT run_id, workflow, status, input, result, next_seq FROM runs"
            )
        }
        fx: dict = {}
        for r in await conn.fetch("SELECT key, kind, object_id FROM fx_objects"):
            fx[(r["key"], r["kind"])] = r["object_id"]
        dup = await conn.fetchval(
            "SELECT count(*) FROM (SELECT key FROM fx_requests GROUP BY key HAVING count(*) > 1) d"
        )
        fx[("__dup__", "__dup__")] = dup
        gaps = {
            str(r["run_id"]): (r["n"], r["max"])
            for r in await conn.fetch(
                "SELECT run_id, count(*) AS n, max(seq) AS max FROM events GROUP BY run_id"
            )
        }
        return runs, fx, gaps
    finally:
        await conn.close()


def run(
    orders: int, lectures: int, workers: int, servers: int, timeout_s: float, day_s: float = 4.0
) -> dict:
    asyncio.run(ensure_database())
    asyncio.run(truncate())
    cluster = Cluster({"ORDER_DAY_SECONDS": str(day_s)})
    apps = ["examples.order", "examples.lecture_note"]
    report = {"workers_killed": 0, "pg_backends_terminated": 0, "servers_killed": 0}
    stop = threading.Event()
    t0 = time.monotonic()
    try:
        urls = [cluster.start_server() for _ in range(servers)]
        cluster.start_effects()
        for _ in range(workers):
            cluster.start_worker(apps, slots=100)
        api = Api(urls)
        chaos = threading.Thread(target=chaos_loop, args=(cluster, stop, report, apps), daemon=True)
        chaos.start()

        def start(i: int) -> str:
            if i < orders:
                body = {
                    "workflow": "order",
                    "input": {"order_id": i, "amount": 5000},
                    "request_id": f"order-{i}",
                }
            else:
                j = i - orders
                body = {
                    "workflow": "lecture_note",
                    "input": {"textbook": f"tb-{j % 7}", "chapter": j},
                    "request_id": f"lecture-{j}",
                }
            return api.call("POST", "/v1/runs", i, json=body).json()["run_id"]

        with cf.ThreadPoolExecutor(32) as pool:
            ids = list(pool.map(start, range(orders + lectures)))
        report["started_in_s"] = round(time.monotonic() - t0, 1)

        def ship(i: int) -> None:
            """Even orders get shipped; odd ones are left to time out and refund."""
            if i % 2:
                return
            while True:
                r = api.call("GET", f"/v1/runs/{ids[i]}", i)
                status = r.json()["status"]
                if status == "suspended":
                    break
                if status in TERMINAL:
                    return
                time.sleep(0.2)
            api.call(
                "POST",
                f"/v1/runs/{ids[i]}/signals",
                i,
                json={"name": "shipped", "data": {"tracking": f"T{i}"}},
            )

        with cf.ThreadPoolExecutor(64) as pool:
            list(pool.map(ship, range(orders)))

        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            runs, _, _ = asyncio.run(fetch_books())
            open_runs = [r for r in runs.values() if r["status"] not in TERMINAL]
            if not open_runs:
                break
            time.sleep(2)
        report["all_done_in_s"] = round(time.monotonic() - t0, 1)
    finally:
        stop.set()
        cluster.stop()

    runs, fx, gaps = asyncio.run(fetch_books())
    return verify(ids, orders, runs, fx, gaps, report)


def verify(ids, orders, runs, fx, gaps, report) -> dict:
    problems: list[str] = []
    by_status: dict = {}
    for r in runs.values():
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    shipped = refunded = 0
    for i, run_id in enumerate(ids):
        r = runs.get(run_id)
        if r is None:
            problems.append(f"{run_id}: lost")
            continue
        if r["status"] != "completed":
            problems.append(f"{run_id}: {r['status']}")
            continue
        n, mx = gaps[run_id]
        if n != mx or mx != r["next_seq"] - 1:
            problems.append(f"{run_id}: journal has gaps ({n} events, max seq {mx})")
        if i < orders:
            charge = fx.get((f"{run_id}:2", "charge"))
            refund = (f"{run_id}:4", "refund") in fx
            receipt = (f"{run_id}:4", "email") in fx
            review = (f"{run_id}:6", "email") in fx
            if charge is None or r["result"]["charge_id"] != charge:
                problems.append(f"{run_id}: charge mismatch")
            if r["result"]["status"] == "done":
                shipped += 1
                if refund or not (receipt and review):
                    problems.append(f"{run_id}: shipped but effects {refund, receipt, review}")
            else:
                refunded += 1
                if not refund or receipt or review:
                    problems.append(f"{run_id}: refunded but effects {refund, receipt, review}")
        else:
            bills = [fx.get((f"{run_id}:{cs}", "llm")) for cs in (1, 2, 3)]
            if None in bills or r["result"]["bills"] != bills:
                problems.append(f"{run_id}: llm bills {bills} vs {r['result']['bills']}")
    report["retried_effects_deduplicated"] = fx.pop(("__dup__", "__dup__"))
    kinds: dict = {}
    for _, kind in fx:
        kinds[kind] = kinds.get(kind, 0) + 1
    report.update(
        {
            "runs": len(ids),
            "status": by_status,
            "orders_shipped": shipped,
            "orders_refunded": refunded,
            "effects": kinds,
            "problems": len(problems),
            "first_problems": problems[:10],
            "verdict": "PASS" if not problems else "FAIL",
        }
    )
    return report


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--orders", type=int, default=1000)
    p.add_argument("--lectures", type=int, default=200)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--servers", type=int, default=2)
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--day", type=float, default=4.0, help="seconds per order-workflow 'day'")
    p.add_argument("--out", default=None)
    args = p.parse_args()
    report = run(args.orders, args.lectures, args.workers, args.servers, args.timeout, args.day)
    print(json.dumps(report, indent=2))
    if args.out:
        with open(args.out, "a") as f:
            f.write(json.dumps({"bench": "chaos", **report}) + "\n")


if __name__ == "__main__":
    main()
