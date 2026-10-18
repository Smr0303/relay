# Relay

**A durable workflow engine on Postgres.** You write a long-running process as one ordinary `async` function. Relay journals every step, timer and signal, and replays the function from that journal after any crash. Side effects run in steps that carry an engine-supplied idempotency key, and every retry sends the same key.

```python
@workflow
async def order(ctx, inp):
    charge = None
    try:
        await ctx.step(reserve_stock, inp["order_id"])
        charge = await ctx.step(charge_card, inp["order_id"], inp["amount"])
        shipped = await ctx.wait_signal("shipped", timeout=timedelta(days=3))
        if shipped is None:  # no shipping news in 3 days
            await ctx.step(refund, charge["charge_id"])
            return {"status": "refunded"}
        await ctx.step(send_email, inp["order_id"], "receipt")
        await ctx.sleep(timedelta(days=7))  # a row in Postgres, not a thread
        await ctx.step(send_email, inp["order_id"], "review_request")
        return {"status": "done"}
    except Cancelled:
        if charge:
            await ctx.step(refund, charge["charge_id"])  # compensation, retried like any step
        raise


@step(timeout_s=5)
def charge_card(sctx, order_id, amount):
    return stripe.charge(amount, idempotency_key=sctx.idempotency_key)  # "run_id:2" on every retry
```

## The guarantee, and the test that proves it

`make chaos` runs 1,000 of those orders and 200 three-call LLM pipelines (built against a fake LLM that rate-limits, and that sometimes bills and then hangs past its lease). While they run:
- a random worker is `SIGKILL`ed every 2–5 s;
- a random Postgres connection is terminated every 2–5 s;
- a random server is `SIGKILL`ed and restarted every 10–20 s.

Then it checks the books:

```
run  worker kills  pg conns killed  server kills  runs   charges  LLM bills  deduplicated  verdict
  1            12               12             2   1200     1000        600            32  PASS
  2            20               19             4   1200     1000        600            29  PASS
  3            11               11             2   1200     1000        600            23  PASS
  4            11               11             2   1200     1000        600            34  PASS
```

Every run completed, every journal was gapless, and every order was charged exactly once and either refunded or emailed, never both. Each lecture was billed for exactly its 3 LLM calls. The "deduplicated" count is retries that hit an effect a second time with the same key: the case exactly-once depends on, exercised about 30 times per run.

## How it works (one diagram)

```
                 CLIENTS                                      OPERATORS
   apps / webhooks / CLI  (relay.sdk.client)              browser → /ui
          | start · signal · cancel · resume · get
          v
 +=================== RELAY SERVER  (stateless, 1..N, FastAPI) ====================+
 |  API        MATCHER (long-poll)   TIMER SVC      REAPER          ARCHIVER        |
 |             claim: SKIP LOCKED    due timers ->  expired leases  closed runs ->  |
 |             + lease token         TimerFired     -> requeue      JSONL, delete   |
 |  per-shard loops: each server owns ceil(16/N) shards via advisory locks          |
 +======|===================================================================|=======+
        |  every write = ONE tx: lock run row → append journal → enqueue next     |
        v                                                                   v
 +================================ POSTGRES 16 ===================================+
 |  runs · events (journal) · tasks (queue) · timers · signals · workflow_versions |
 +======^==========================================================================+
        |  HTTP: poll · ack (fenced by lease token) · heartbeat
 +------+--------------------------------------+
 |  WORKERS: replay engine + step executor      |──> Stripe / email / LLM
 |  stateless; kill any of them at any time     |    (idempotency key on every call)
 +---------------------------------------------+
 TELEMETRY: Prometheus /metrics + 4 alerts · Grafana dashboard · OTel trace per run (Jaeger)
```

**Docs**
- [docs/SYSTEM_DESIGN.md](docs/SYSTEM_DESIGN.md) is the design before the code: the problem, requirements, back-of-the-envelope sizing, HLD and flows, and an order traced through the database row by row.
- [docs/DESIGN.md](docs/DESIGN.md) is the design as built: the ack transaction, lease fencing, cancellation, patch gates, wake-ups, shards, and the bugs the tests caught on the way.
- [bench/results.md](bench/results.md) has every measurement.

## Estimates vs. measurements

The design was sized for **1M runs/day at 10× peak** (about 1,200 steps/s). Each estimate was a gate the build had to pass:

| # | estimate (back of the envelope) | gate | measured |
|---|---|---|---|
| E1 | 1M runs/day × 10× peak ≈ 1,200 steps/s ≈ 5.4k tx/s fits **one** Postgres | ≥ 1,200 steps/s for 10 min | **1,199.2 steps/s for 10 min**, 75,599 runs, queue flat; ceiling ≈ 1,316 (WAL-flush bound) |
| E2 | dispatch is a long-poll wake, not a poll interval | p50 < 50 ms, p99 < 500 ms | **20.9 ms / 79.1 ms** |
| E3 | a sleep is a row, not a thread | 100k sleeping, idle CPU ≈ 0, lag p99 < 1 s | **0.01 cores/server idle; lag p99 24.8 ms** |
| E4 | ~40 events × 0.5 KB ≈ 20 KB/run | ≤ 25 KB per 10-step run | **5.3 KB** (22 events) |
| E5 | 240 concurrent steps + ~4 cores of replay | ≤ 8 workers | **6 workers, 0.9 cores total** |
| E6 | replay is the hidden read cost | 40-event replay ≤ 5 ms | **0.03 ms** |
| E7 | idempotency key = run_id:call_seq | 1,000 payments under kills → 1,000 charges | **1,000 / 1,000, four runs out of four** |

**Where the estimate was wrong, and where it held.**
- **Journal size was overestimated.** A run wrote 22 events, not 40, because `StepStarted` was dropped from the design. That made a run 5 KB, not 20 KB.
- **Replay cost was overestimated.** Replay p99 was 0.5 ms at the worker, not the assumed 3 ms, so 6 workers needed 0.9 cores in total.
- **The HTTP tier was never sized.** It took 6 server processes (2.9 cores) to carry the load.
- **The transaction rate held.** Each step costs about 4 commits (claim and ack for the step and for its replay), and the measured ceiling of about 1.3k steps/s is roughly 5k durable commits/s. That is where Postgres's WAL flush tops out on this machine, matching the estimate's 5.4k tx/s.

All measurements come from one laptop: Apple M5 Pro, Postgres 16 in Docker Desktop, `synchronous_commit` on. The raw output is in [`bench/results.md`](bench/results.md).

## What's in the box

| | |
|---|---|
| `relay/sdk` | `@workflow` / `@step`, `ctx` (`step`, `sleep`, `wait_signal`, `now`/`uuid`/`random`, `patched`, `continue_as_new`), the replay driver, the worker, a client |
| `relay/server` | schema, engine (all transactional logic), HTTP API, operator UI, shard ownership, metrics, blob store |
| `examples/` | the order workflow; the lecture-note pipeline (an LLM pipeline from a real codebase, rebuilt on Relay); fake Stripe / email / LLM |
| `tests/unit` | replay semantics; **crash-at-every-point** (29 and 21 crash points of the order workflow) |
| `tests/integration` | real processes and `SIGKILL`: fencing, racing advances, signal storms, retries, versions, patch rollout, cancel, archiver, shard failover |
| `tests/chaos`, `tests/obs` | the chaos demo; the trace and alert drill |
| `bench/` | load (E1, E2, E4, E5), timer soak (E3), replay micro-benchmark (E6) |

## Run it

```bash
make setup      # uv sync
make up         # Postgres 16 on :5440
make test       # unit + integration (~2.5 min)
make chaos      # the demo above
make obs-up     # Jaeger :16686, Prometheus :9090, Grafana :3000
make demo       # live trickle + worker kills → watch it in Grafana and at :8080/ui
make bench      # 10-minute load test at 1,200 steps/s
```

## Limits

Steps within a run are sequential: there are no parallel steps or child workflows yet. The API has no authentication. Everything runs on one Postgres, with logical shards only. The blob and archive stores are filesystem-backed. The full list is at the end of [docs/DESIGN.md](docs/DESIGN.md).
