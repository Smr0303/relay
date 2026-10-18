# Benchmark results

One laptop: Apple M5 Pro (15 cores), Postgres 16 in Docker Desktop (`synchronous_commit` on), servers and workers as local processes. Raw rows: `bench/runs.jsonl`.

## E1 / E2 / E4 / E5: sustained load (`bench/load.py`)

Open-loop: 120 runs/s × 10 steps of 200 ms simulated I/O = **1200 steps/s offered** for 630 s (30 s warm-up excluded), 6 server processes, 6 workers.

| metric | value |
|---|---|
| steps/s sustained (measured window 600.1 s) | **1199.2** |
| runs started / completed | 75,599 / 75,599 |
| dispatch STEP p50 / p99 | 20.9 ms / 79.1 ms |
| dispatch ADVANCE p99 | 80.3 ms |
| worker replay p99 | 0.5 ms |
| ready-queue depth, max of 5 s samples | 497 (flat = keeping up) |
| journal events per 10-step run | 22.0 |
| bytes per run (events + runs tables incl. indexes) | 5,417 |
| CPU cores, all servers / all workers | 2.93 / 0.90 |

### Ceiling (90 s runs, 8 server processes, 6 workers)

| offered steps/s | sustained steps/s | queue | Postgres |
|---|---|---|---|
| 2000 | **1296.3** | growing (21,243 → 81,335) | defaults |
| 2000 | **1315.6** | growing (21,335 → 83,843) | commit_delay=200us (group commit); no gain, reverted |

Past ~1.3k steps/s the backlog grows. At that point Postgres is at ~2.9 cores with 15 of ~20 active backends waiting on `LWLock: WALWrite`: the limit is WAL flushing (every commit is durable). Group commit (`commit_delay`) did not move it. The levers that would are fewer commits per step (e.g. acking a step and claiming its ADVANCE in one transaction) or faster storage than Docker Desktop's VM disk.

## E3: timers (`bench/timer_soak.py`)

100,000 sleeping runs bulk-loaded (journal + timer row), fire times spread over 60 s.

| metric | value |
|---|---|
| idle CPU while all sleep (cores) | {"server0": 0.013, "server1": 0.014, "worker0": 0.0, "worker1": 0.0, "worker2": 0.0, "worker3": 0.0} |
| timers fired / runs completed | 100,000 / 100,000 |
| timer lag p50 / p99 | 5.6 ms / **24.8 ms** |

## E6: replay (`bench/replay_micro.py`)

Full replay of a 40-event journal: p50 **0.029 ms**, p99 0.033 ms.

## E7: chaos (`tests/chaos/chaos.py`)

```
{
  "workers_killed": 11,
  "pg_backends_terminated": 11,
  "servers_killed": 2,
  "started_in_s": 2.3,
  "all_done_in_s": 38.7,
  "retried_effects_deduplicated": 34,
  "runs": 1200,
  "status": {
    "completed": 1200
  },
  "orders_shipped": 500,
  "orders_refunded": 500,
  "effects": {
    "charge": 1000,
    "llm": 600,
    "email": 1000,
    "refund": 500
  },
  "problems": 0,
  "first_problems": [],
  "verdict": "PASS"
}
```

## Observability drill (`tests/obs/drill.py`)

```
{
  "trace": {
    "run_id": "a6c27b56-de43-4b01-8c86-0addf9e215e6",
    "trace_url": "http://localhost:16686/trace/a6c27b56de434b018c860addf9e215e6",
    "spans": [
      "replay",
      "run slow_order",
      "step slow_charge"
    ],
    "step_attempts_in_trace": [
      1,
      2
    ],
    "ok": true
  },
  "alerts": {
    "fired_after_s": {
      "RelayNonDeterminism": 33.3,
      "RelayLeaseExpiries": 33.3,
      "RelayTimerLag": 37.3,
      "RelayTaskBacklog": 65.6
    },
    "missing": [],
    "ok": true
  },
  "verdict": "PASS"
}
```
