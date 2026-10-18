"""Open-loop load benchmark (E1, E2, E4, E5).

    uv run python -m bench.load --rate 120 --steps 10 --duration 600 --servers 2 --workers 6

Starts `rate` runs/s of the `bench` workflow (N steps of BENCH_STEP_MS simulated I/O) for
`duration` seconds against a fresh database, then reports steps/s sustained, dispatch latency,
backlog, bytes per run and CPU per process. Open-loop: the generator does not slow down when
the engine falls behind, so a backlog shows up as growing queue depth, not hidden.
"""

import argparse
import asyncio
import json
import os
import subprocess
import time

import asyncpg
import httpx

from bench.promtext import counter, delta, quantile, scrape
from tests.integration.cluster import ADMIN_DSN, Cluster

BENCH_DSN = ADMIN_DSN.rsplit("/", 1)[0] + "/relay_bench"


async def fresh_db() -> None:
    conn = await asyncpg.connect(ADMIN_DSN)
    try:
        await conn.execute("DROP DATABASE IF EXISTS relay_bench WITH (FORCE)")
        await conn.execute("CREATE DATABASE relay_bench")
    finally:
        await conn.close()


async def db_stats() -> dict:
    conn = await asyncpg.connect(BENCH_DSN)
    try:
        return dict(
            await conn.fetchrow(
                "SELECT (SELECT count(*) FROM runs WHERE status='completed') AS completed, "
                "(SELECT count(*) FROM runs) AS runs, "
                "(SELECT count(*) FROM events) AS events, "
                "pg_total_relation_size('events') AS events_bytes, "
                "pg_total_relation_size('runs') AS runs_bytes, "
                "(SELECT count(*) FROM tasks) AS tasks"
            )
        )
    finally:
        await conn.close()


def cpu_seconds(pid: int) -> float:
    out = subprocess.run(
        ["ps", "-o", "cputime=", "-p", str(pid)], capture_output=True, text=True
    ).stdout.strip()
    if not out:
        return 0.0
    parts = [float(p) for p in out.replace("-", ":").split(":")]
    secs = 0.0
    for p in parts:
        secs = secs * 60 + p
    return secs


async def generate(
    servers: list[str], rate: float, duration: float, steps: int, stop_at: list
) -> int:
    started = 0
    limits = httpx.Limits(max_connections=200)
    async with httpx.AsyncClient(timeout=30, limits=limits) as http:
        t0 = time.monotonic()
        pending: set = set()
        while (now := time.monotonic()) - t0 < duration:
            due = int((now - t0) * rate) - started
            for _ in range(max(due, 0)):
                url = servers[started % len(servers)]
                pending.add(
                    asyncio.create_task(
                        http.post(
                            url + "/v1/runs", json={"workflow": "bench", "input": {"steps": steps}}
                        )
                    )
                )
                started += 1
            done = {t for t in pending if t.done()}
            pending -= done
            await asyncio.sleep(0.005)
        await asyncio.gather(*pending, return_exceptions=True)
        stop_at.append(time.monotonic())
    return started


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=120, help="runs started per second")
    p.add_argument("--steps", type=int, default=10)
    p.add_argument("--duration", type=float, default=60, help="seconds of load")
    p.add_argument("--warmup", type=float, default=15, help="seconds excluded from the window")
    p.add_argument("--servers", type=int, default=2)
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--slots", type=int, default=200)
    p.add_argument("--pool", type=int, default=20, help="DB connections per server")
    p.add_argument("--step-ms", type=float, default=200)
    p.add_argument("--out", default=None, help="append the JSON result to this file")
    args = p.parse_args()

    asyncio.run(fresh_db())
    os.makedirs(".test-logs", exist_ok=True)
    cluster = Cluster(
        {
            "RELAY_DSN": BENCH_DSN,
            "BENCH_STEP_MS": str(args.step_ms),
            "RELAY_POOL": str(args.pool),
            "RELAY_ADVANCE_TIMEOUT_S": "30",
        }
    )
    try:
        servers = [cluster.start_server() for _ in range(args.servers)]
        for i in range(args.workers):
            cluster.start_worker("bench.apps", server=servers[i % len(servers)], slots=args.slots)
        time.sleep(2)
        procs = {f"server{i}": cluster.procs[i].pid for i in range(args.servers)}
        procs.update({f"worker{i}": w.pid for i, w in enumerate(cluster.workers)})
        pg_pid = None

        stop_at: list = []

        async def run():
            gen = asyncio.create_task(
                generate(servers, args.rate, args.duration, args.steps, stop_at)
            )
            await asyncio.sleep(args.warmup)
            m0, t0 = scrape(servers), time.monotonic()
            cpu0 = {k: cpu_seconds(pid) for k, pid in procs.items()}
            depth_samples = []
            while not gen.done():
                await asyncio.sleep(5)
                m = scrape(servers)
                depth_samples.append(counter(m, "relay_queue_depth"))
            m1, t1 = scrape(servers), time.monotonic()
            cpu1 = {k: cpu_seconds(pid) for k, pid in procs.items()}
            return await gen, m0, m1, t1 - t0, cpu0, cpu1, depth_samples

        started, m0, m1, window, cpu0, cpu1, depth = asyncio.run(run())
        d = delta(m0, m1)
        # drain: let in-flight runs finish so bytes/run counts whole runs
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            s = asyncio.run(db_stats())
            if s["completed"] >= started:
                break
            time.sleep(2)
        s = asyncio.run(db_stats())
        result = {
            "config": vars(args),
            "runs_started": started,
            "runs_completed": s["completed"],
            "window_s": round(window, 1),
            "steps_per_s": round(counter(d, "relay_steps_completed_total") / window, 1),
            "offered_steps_per_s": args.rate * args.steps,
            "dispatch_step_p50_ms": round(
                1000 * (quantile(d, "relay_dispatch_latency_seconds", 0.5, kind="STEP") or 0), 1
            ),
            "dispatch_step_p99_ms": round(
                1000 * (quantile(d, "relay_dispatch_latency_seconds", 0.99, kind="STEP") or 0), 1
            ),
            "dispatch_advance_p99_ms": round(
                1000 * (quantile(d, "relay_dispatch_latency_seconds", 0.99, kind="ADVANCE") or 0), 1
            ),
            "replay_p99_ms": round(1000 * (quantile(d, "relay_replay_seconds", 0.99) or 0), 2),
            "queue_depth_samples": depth,
            "events_per_run": round(s["events"] / max(s["runs"], 1), 1),
            "bytes_per_run": int((s["events_bytes"] + s["runs_bytes"]) / max(s["runs"], 1)),
            "cpu_cores": {k: round((cpu1[k] - cpu0[k]) / window, 2) for k in cpu0},
        }
        del pg_pid
        print(json.dumps(result, indent=2))
        if args.out:
            with open(args.out, "a") as f:
                f.write(json.dumps(result) + "\n")
    finally:
        cluster.stop()


if __name__ == "__main__":
    main()
