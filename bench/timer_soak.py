"""E3: 100k sleeping runs cost ~0 CPU, and their timers fire on time (p99 lag < 1 s).

    uv run python -m bench.timer_soak --runs 100000 --window 60

The sleeping runs are bulk-loaded straight into Postgres (journal + timer row, exactly what
`ctx.sleep` leaves behind) so the test measures the timer service, not run creation. Their
fire times are spread over `window` seconds; each firing wakes the run, a worker replays it,
and it completes.
"""

import argparse
import asyncio
import json
import os
import time
import uuid

import asyncpg

from bench.load import BENCH_DSN, cpu_seconds, fresh_db
from bench.promtext import delta, quantile, scrape
from relay.server.engine import SHARDS
from tests.integration.cluster import Cluster


async def load(n: int, start_in: float, window: float) -> None:
    conn = await asyncpg.connect(BENCH_DSN)
    # COPY is binary: jsonb's binary form is a version byte (1) followed by the JSON text.
    await conn.set_type_codec(
        "jsonb",
        schema="pg_catalog",
        format="binary",
        encoder=lambda v: b"\x01" + json.dumps(v).encode(),
        decoder=lambda b: json.loads(b[1:]),
    )
    try:
        now = await conn.fetchval("SELECT now()")
        runs, events, timers = [], [], []
        for i in range(n):
            rid = uuid.uuid4()
            fire_in = start_in + window * i / n
            runs.append((rid, "sleeper", 1, "suspended", {"sleep_s": fire_in}, rid.int % SHARDS, 3))
            events.append((rid, 1, "RunStarted", None, {"input": {"sleep_s": fire_in}}))
            events.append((rid, 2, "TimerCreated", 1, {"fire_in_s": fire_in, "signal": None}))
            timers.append(
                (
                    uuid.uuid4(),
                    rid,
                    rid.int % SHARDS,
                    1,
                    now + __import__("datetime").timedelta(seconds=fire_in),
                )
            )
        await conn.copy_records_to_table(
            "runs",
            records=runs,
            columns=["run_id", "workflow", "version", "status", "input", "shard", "next_seq"],
        )
        await conn.copy_records_to_table(
            "events", records=events, columns=["run_id", "seq", "type", "call_seq", "payload"]
        )
        await conn.copy_records_to_table(
            "timers", records=timers, columns=["timer_id", "run_id", "shard", "call_seq", "fire_at"]
        )
        await conn.execute("ANALYZE runs; ANALYZE events; ANALYZE timers")
    finally:
        await conn.close()


async def completed() -> int:
    conn = await asyncpg.connect(BENCH_DSN)
    try:
        return await conn.fetchval("SELECT count(*) FROM runs WHERE status='completed'")
    finally:
        await conn.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--runs", type=int, default=100_000)
    p.add_argument("--window", type=float, default=60)
    p.add_argument("--idle", type=float, default=30, help="seconds of pure sleeping measured")
    p.add_argument("--servers", type=int, default=2)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--out", default=None)
    args = p.parse_args()

    asyncio.run(fresh_db())
    os.makedirs(".test-logs", exist_ok=True)
    cluster = Cluster({"RELAY_DSN": BENCH_DSN, "RELAY_ADVANCE_TIMEOUT_S": "30"})
    try:
        servers = [cluster.start_server() for _ in range(args.servers)]
        for i in range(args.workers):
            cluster.start_worker(
                "tests.integration.apps", server=servers[i % len(servers)], slots=200
            )
        start_in = args.idle + 15
        t_load = time.monotonic()
        asyncio.run(load(args.runs, start_in, args.window))
        load_s = time.monotonic() - t_load
        procs = {f"server{i}": cluster.procs[i].pid for i in range(args.servers)}
        procs.update({f"worker{i}": w.pid for i, w in enumerate(cluster.workers)})

        time.sleep(3)  # let the load's own CPU settle
        c0 = {k: cpu_seconds(v) for k, v in procs.items()}
        time.sleep(args.idle)
        c1 = {k: cpu_seconds(v) for k, v in procs.items()}
        idle_cores = {k: round((c1[k] - c0[k]) / args.idle, 3) for k in procs}

        m0 = scrape(servers)
        t_fire = time.monotonic()
        deadline = t_fire + start_in + args.window + 120
        while time.monotonic() < deadline and asyncio.run(completed()) < args.runs:
            time.sleep(2)
        drained_s = time.monotonic() - t_fire
        d = delta(m0, scrape(servers))
        result = {
            "runs": args.runs,
            "window_s": args.window,
            "bulk_load_s": round(load_s, 1),
            "idle_cpu_cores_with_all_sleeping": idle_cores,
            "completed": asyncio.run(completed()),
            "timers_fired": int(d.get(("relay_timer_lag_seconds_count", ()), 0)),
            "timer_lag_p50_ms": round(1000 * (quantile(d, "relay_timer_lag_seconds", 0.5) or 0), 1),
            "timer_lag_p99_ms": round(
                1000 * (quantile(d, "relay_timer_lag_seconds", 0.99) or 0), 1
            ),
            "seconds_until_all_completed": round(drained_s, 1),
        }
        print(json.dumps(result, indent=2))
        if args.out:
            with open(args.out, "a") as f:
                f.write(json.dumps({"bench": "timer_soak", **result}) + "\n")
    finally:
        cluster.stop()


if __name__ == "__main__":
    main()
