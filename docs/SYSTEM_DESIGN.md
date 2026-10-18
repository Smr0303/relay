# Relay: system design

How this system was designed before any code existed: the problem, the requirements, the back-of-the-envelope sizing (each estimate became an acceptance gate), the high-level design and its flows, and one order traced through the database row by row.

For how the code actually works, see [DESIGN.md](DESIGN.md). It also covers the places the build departed from this plan, and the bugs the gates caught. The measured results are in [../bench/results.md](../bench/results.md).

## 1. Summary

### Problem
- Business processes take many steps, call external APIs and wait hours or days. Examples: order → charge → wait for shipping → receipt; onboarding; AI agent loops.
- Teams build them from **queues + cron + status columns + hand-written retries**. That breaks in four ways:
  - **Lost work:** a crash between "commit" and "enqueue next".
  - **Duplicate effects:** a retry after a timeout charges twice.
  - **Races:** a webhook and a cron job decide on the same order.
  - **No history:** the logic is in 5 places, and the state is one overwritten column.

### Approach: durable execution (journal + replay)
- Write the process as **one normal function**. Side effects go only inside `ctx.step(...)`.
- The engine **journals** every step result, timer and signal in Postgres.
- On crash or resume, it **replays** the function from line 1. Finished steps return their recorded result, and the run continues from the first unfinished one.
- **One transaction per move:** record what happened + queue what's next. Nothing can be lost.
- **Idempotency key = `run_id:call_seq`**, identical on every retry, so the downstream API dedups.
- **Sleep and wait are rows**, not threads. Idle runs cost nothing.
- **All writes for a run lock its run row**, so the journal orders them and they can't race.

### Back-of-the-envelope = acceptance targets (each one proven by a test before "done")
| # | Estimate | Target the built system must hit | How it's proven |
|---|---|---|---|
| E1 | 1M runs/day × 10× peak ≈ 1,200 steps/s ≈ 5.4k tx/s | **one Postgres sustains ≥ 1,200 steps/s for 10 min** | load benchmark (laptop Docker PG), published curve |
| E2 | dispatch ≤ 50 ms typical | **p50 < 50 ms, p99 < 500 ms** step scheduled → started | histogram from the benchmark |
| E3 | timers are rows | **100k sleeping runs, timers fire p99 < 1 s late**, idle CPU ≈ 0 | timer soak test |
| E4 | ~40 journal rows ≈ 20 KB per run | **≤ 25 KB per 10-step run** | measure table size after the benchmark |
| E5 | 240 concurrent steps → 6–8 workers | **≤ 8 worker processes** reach E1 | benchmark config |
| E6 | replay reads = biggest read load | **replay ≤ 5 ms** for a 40-event journal | micro-benchmark |
| E7 | effects once | **1,000 payment runs with random `kill -9` → exactly 1,000 charges, 0 lost runs** | chaos test (the demo) |

If a measured number misses, we fix the engine or correct the estimate in this file with the real number and the reason. The README publishes measured numbers only.

### HLD: all components

```
                 CLIENTS                                      OPERATORS
   shop API / webhooks / CLI  (relay-client SDK)          browser
          | start · signal · cancel · get                     |
          v                                                   v
 +=================== RELAY SERVER  (stateless, 2..N, FastAPI) =================+
 |                                                                             |
 |  [API]            [MATCHER]           [TIMER SVC]       [REAPER]   [UI]     |
 |  POST /runs       GET /tasks/poll     every 1s:         every 1s:  dashboard|
 |  POST /signals    (long-poll, LISTEN  due timers ->     expired    (reads   |
 |  POST /cancel      wakes on NOTIFY)   TimerFired +      leases ->  PG only) |
 |  GET  /runs/:id   POST /tasks/ack     ADVANCE           requeue             |
 |                   POST /heartbeat                                           |
 |  shard ownership for Timer/Reaper = Postgres advisory locks (one owner each)|
 +=====|==================|===================|================|===============+
       |   every write = ONE tx: lock runs[row] -> append events -> insert next task -> NOTIFY
       v                  v                   v                v
 +============================== POSTGRES 16 ===================================+
 |  runs      status, version, input, result, request_id (unique)             |
 |  events    THE JOURNAL  PK(run_id, seq), append-only                       |
 |  tasks     THE QUEUE    ADVANCE | STEP, lease_owner/lease_until, attempt   |
 |            partial idx: unclaimed; unique: one pending ADVANCE per run     |
 |  timers    fire_at, fired         signals   name, payload                  |
 |  hash(run_id) -> 16 logical shards (physical split later)                  |
 +==============|============================================|================+
                ^ HTTP: poll / ack / heartbeat               | nightly
                |                                            v
 +--------------+---------------------------+      +-------------------------+
 |        WORKERS  (2..8 processes)          |      | ARCHIVER                |
 |  poller ─> REPLAY ENGINE (workflow code)  |      | closed runs > 7 days -> |
 |        └─> STEP EXECUTOR (side effects) ──┼──>   | S3 JSONL, drop partition|
 |            + heartbeat for long steps     |   |  +-------------------------+
 |  registry: {workflow, version} -> code    |   |
 +-------------------------------------------+   +--> Stripe / email / LLM / DBs
                |                                       (with idempotency key)
                v
 +-------------------------------------------------------------------------+
 | TELEMETRY  OTel traces (trace = run, span = step attempt) -> Jaeger/     |
 |            Grafana · Prometheus /metrics -> Grafana · 4 alerts:          |
 |            oldest unclaimed task > 60s · timer lag > 30s ·               |
 |            lease expiries > 1% · NonDeterminismError > 0                 |
 +-------------------------------------------------------------------------+
```

### Flow 1: happy path (one step + one sleep)

```
 client      SERVER/API      POSTGRES                    MATCHER        WORKER            Stripe
   |--start-->|--tx: run + RunStarted + ADVANCE + NOTIFY-->|               |                 |
   |<-run_id--|                                            |--wake poll--->|                 |
   |          |                                            |     claim ADVANCE (lease 30s)   |
   |          |                                            |               | replay: empty   |
   |          |                                            |               | hits step 1     |
   |          |      <--tx: StepScheduled(1) + STEP task + delete ADVANCE--|                 |
   |          |                                            |--wake-------->| claim STEP      |
   |          |                                            |               |--charge key r:1>|
   |          |      <--tx: StepCompleted(1) + ADVANCE----------------------|<--ch_1---------|
   |          |                                            |               | replay: 1 cached|
   |          |                                            |               | hits sleep(24h) |
   |          |      <--tx: TimerCreated(2) + timers row, run=suspended----|                 |
   |   ~~~~~~~~~~~~~~~~~~~~~~~~~ 24h: zero tasks, zero CPU ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  |
   |      TIMER SVC --tx: TimerFired(2) + ADVANCE + NOTIFY-->  ... replay 1,2 cached -> next  |
```

### Flow 2: worker crash mid-step

```
 WORKER B  claim STEP(2) ──> Stripe charge key r:2 ──> ch_1 ──> 💥 killed (no ack)
 POSTGRES  tasks: lease_owner=B, lease_until=10:00:31        journal ends at StepScheduled(2)
 REAPER    10:00:32  lease expired -> lease_owner=NULL, attempt=2
 WORKER C  claim STEP(2) ──> Stripe charge key r:2 ──> "seen key" -> ch_1 (no new charge)
           tx: StepCompleted(2){ch_1} + ADVANCE         => exactly one charge, run continues
```

### Flow 3: signal vs timeout race

```
 webhook "shipped" ─┐                       ┌─ timer svc: 3-day timeout due
                    v                       v
              tx: SELECT runs[r] FOR UPDATE  (only one gets the lock first)
                    |
     first:  append SignalReceived + ADVANCE -> replay resolves call 3, emits CancelTimer
     second: a TimerFired that slipped in later is ignored by replay (call 3 already resolved)
     => shipped XOR refunded, never both
```

### Flow 4: deploy new code mid-flight

```
 run started on v3 ── runs.version=3 ── only workers registered for {order,v3} get its ADVANCEs
 new runs start on v4.  Changing v3 code in place?  wrap it:  if ctx.patched("x"): ...
 replay mismatch anyway -> NonDeterminismError -> run parked (not lost) + alert
```


## 2. Use case and requirements

### The problem in one line
A business process with several steps, long waits and external calls has to finish exactly once, even when servers crash, deploys happen or APIs time out.

### Anchor scenario (we size everything against this)
**Payment + order fulfilment** for a mid-size e-commerce company:

```
order_placed
  -> reserve_stock          (inventory API)
  -> charge_card            (Stripe; MUST NOT double-charge)
  -> wait_signal("shipped", timeout=3d)    (warehouse webhook)
  -> send_receipt           (email API)
  -> sleep(7d)
  -> request_review         (email API)
  on failure after charge -> refund + release_stock   (compensation)
```

This one scenario covers every feature: steps, retries, a dangerous side effect, a signal, a timeout, a long sleep and compensation.

Secondary scenarios, which must fit with no new primitives:
- user onboarding/KYC (wait days for a human)
- an AI agent loop (an LLM call per step; a crash must not re-pay for finished calls)
- a deploy pipeline (deploy → wait for health → roll back)

### Functional requirements
| # | Requirement |
|---|---|
| F1 | Start a run of a named workflow with JSON input; get `run_id`. Optional client `request_id` makes start idempotent. |
| F2 | Steps: run side effects with retry policy, timeout and heartbeat. Each step gets an engine-supplied idempotency key. |
| F3 | Durable sleep (seconds to months) costing nothing while idle. |
| F4 | Signals: deliver a named external event into a run; wait with timeout. |
| F5 | Query status/result/history of a run. |
| F6 | Cancel a run; user code runs compensation. |
| F7 | Version workflow code while runs are in flight (pinned version + patch gates). |
| F8 | Dashboard: list, filter, open history timeline, retry, signal, cancel. |

### Non-functional requirements
| # | Requirement | Target |
|---|---|---|
| N1 | Durability | zero lost runs, zero lost timers across any single process/node crash |
| N2 | Effect correctness | each step's effect observed once (with downstream idempotency) |
| N3 | Dispatch latency | step scheduled → step started: p50 < 50 ms, p99 < 500 ms |
| N4 | Timer accuracy | fires within 1 s of `fire_at` (p99) |
| N5 | Throughput | anchor load below on **one** Postgres, with headroom to shard |
| N6 | Availability | servers/workers stateless; Postgres is the only SPOF (managed failover) |
| N7 | Observability | every run has history + trace; the four engine-health alerts |

### Out of scope (v1)
Multi-region, child workflows, non-Python SDKs, sub-10 ms latency, streaming workloads (Kafka/Flink territory).


## 3. Back-of-the-envelope estimation

### Assumptions
| Quantity | Value | Why |
|---|---|---|
| Runs started / day | **1,000,000** | mid-size company; ~12/s average |
| Peak : average | **10×** | sales, batch kickoffs |
| Steps per run | **10** | anchor has 6 + compensation/retries |
| Timers + signals per run | **2** | one sleep, one signal wait |
| Average run lifetime | **1 day** | most runs sleep or wait |
| Step duration | **200 ms** | external API calls |
| Event row size | **~0.5 KB** | ~150 B row overhead + JSON payload |

### Derived per run
- **Events (journal rows):** start 1 + steps 10×3 (Scheduled, Completed, a few Failed) + waits 2×2 + end 1 ≈ **~40 events ≈ 20 KB**
- **Tasks:** each step = 1 STEP + 1 ADVANCE after it = 20, + 1 per wake-up ×2, + 1 start ≈ **~23 tasks**
- **DB transactions per task:** claim + ack = **2**

### Throughput
| Metric | Average | Peak (10×) |
|---|---|---|
| Runs started / s | 12 | 120 |
| Steps / s | 120 | **1,200** |
| Tasks / s | 270 | 2,700 |
| Postgres write tx / s | 540 | **~5,400** |
| Journal rows appended / s | 460 | 4,600 |

**Conclusion:** peak ~5.4k simple tx/s fits on **one** well-sized Postgres (a 16-vCPU instance does ~10–20k short tx/s). HLD stage 1/2 is enough for the anchor load. Sharding (stage 3) is the story for 10× beyond it, not a v1 need.

### Replay read cost (the hidden one)
Each ADVANCE replays the whole journal so far. A run has ~12 ADVANCEs and the average journal at replay is ~20 events, so that is ~240 event reads per run.
- 1M runs/day × 240 = 240M reads/day ≈ **2.8k rows/s avg, 28k/s peak**. These are index range scans on `(run_id, seq)`, so they are cheap but form the largest read load.
- Mitigation (later): a **sticky worker cache**. The worker that last advanced a run keeps its journal in memory and fetches only new events. This is Temporal's "sticky queue".

### Storage
| Table | Size | Note |
|---|---|---|
| events | 20 KB × 1M = **20 GB/day** | the big one |
| runs | ~1 KB × 1M = 1 GB/day | |
| tasks | in-flight only (deleted on ack) | ≈ thousands of rows, MBs |
| timers | live sleepers only | see concurrency below |

- Retention of **7 days hot** for closed runs is ~150 GB in Postgres. Older runs are exported to S3 (JSONL/Parquet) and partitions are dropped. Partition `events` by time as well as by shard, so dropping is cheap.
- Steps return small JSON. Results over 256 KB go to S3, and the journal stores the key.

### Concurrency (Little's law: L = λ × W)
- **Live runs** = 12/s × 86,400 s ≈ **~1M runs in flight**, almost all sleeping/waiting. That is ~1M timer/signal rows (~100 MB) and costs zero CPU.
- **Concurrent steps at peak** = 1,200/s × 0.2 s = **240**. One async worker process runs ~100 concurrent I/O steps, so **3 processes**.
- **Replay CPU at peak** = ~1,300 ADVANCE/s × ~3 ms (Python) ≈ **4 cores**.
- **Workers at peak ≈ 6–8 processes.** Servers: 2–3 replicas, for availability rather than load.

### Network
- ADVANCE carries the journal (~20 KB): 1,300/s × 20 KB ≈ **26 MB/s peak** between the server and workers. That is fine. The sticky cache cuts it by ~10×.

### Laptop / demo scale (what we will actually run)
- One local Postgres in Docker, 4–8 worker processes.
- Expected ceiling: ~1–3k steps/s. The benchmark in the execution plan measures the real number, which goes in the README.


## 4. Worked example: order #123, today vs Relay

Flow: **reserve stock → charge card → wait for "shipped" (give up after 3 days → refund) → send receipt.**

### Today: Celery + status column + cron (how most teams build it)

```
 Customer   API server        Postgres: orders          Celery/Redis      Worker          Stripe
    |           |                    |                        |               |               |
 1  |--order--->|                    |                        |               |               |
    |           |--INSERT orders---->|  {id:123,              |               |               |
    |           |  status='placed'   |   status:'placed'}     |               |               |
    |           |--COMMIT----------->|                        |               |               |
    |           |   ❌ P1 crash HERE: committed, never queued -> order stuck 'placed' forever
    |           |--reserve_stock.delay(123)------------------>|               |               |
    |<--200-----|                    |                        |--task-------->|               |
 2  |           |                    |<--UPDATE status='reserved'-------------|               |
    |           |                    |                        |<-charge_card.delay(123)-------|
    |           |                    |                        |--task-------->|               |
 3  |           |                    |                        |               |--POST charge->|  ch_1 created
    |           |                    |                        |               |  💥 worker killed (deploy)
    |           |                    |                        |               |   before UPDATE
    |           |                    |  row still says        |               |               |
    |           |                    |  status:'reserved'     |--redeliver--->|               |
    |           |                    |                        |  (acks_late)  |--POST charge->|  ch_2 created
    |           |   ❌ P2 no idempotency key -> customer charged TWICE; or with early ack the task is
    |           |      simply lost -> money taken, order stuck 'reserved'
    |           |                    |<--UPDATE status='charged', charged_at=now--|           |
 4  |           |                    |                        |               |               |
    |  Warehouse webhook            CRON every 5 min:                                         |
    |  POST /webhooks/shipped       SELECT * FROM orders                                      |
    |    UPDATE status='shipped'    WHERE status='charged' AND charged_at < now()-'3 days'     |
    |    send_receipt.delay(123)      -> refund.delay(id)                                      |
    |           ❌ P3 webhook lands while the cron is mid-run -> order is SHIPPED and REFUNDED
 5  |   send_receipt: hand-written self.retry(countdown=30*2**n)
    |           ❌ P4 logic is spread across API + 3 tasks + webhook + cron; no history of what happened
```

What the database says after step 3. The row **lies**:

```
 orders:  { id: 123, status: "reserved", charged_at: null }
 Stripe:  ch_1 ($50), ch_2 ($50)                    <- reality
 logs:    scattered across 3 pods, 1 already gone
```

| # | Problem | Root cause |
|---|---|---|
| P1 | Lost work | commit and enqueue are two separate writes (dual write) |
| P2 | Double charge or lost task | retries carry no stable identity; the queue can't know the effect happened |
| P3 | Shipped AND refunded | two writers (webhook, cron) decide on the same order without ordering |
| P4 | No history, logic smeared | the process lives in 5 places, and its state is one overwritten column |

### With Relay: the same process is one function

```python
@workflow
async def order(ctx, order_id):
    await ctx.step("reserve_stock", reserve, order_id)                  # call_seq 1
    charge = await ctx.step("charge_card", charge_card, order_id)       # call_seq 2
    shipped = await ctx.wait_signal("shipped", timeout=days(3))         # call_seq 3
    if shipped is None:
        await ctx.step("refund", refund, charge["charge_id"])           # call_seq 4
        return "refunded"
    await ctx.step("send_receipt", send_email, order_id)                # call_seq 4
    return "done"

async def charge_card(ctx, order_id):          # a step: side effects live here
    return stripe.charge(amount=5000, idempotency_key=ctx.idempotency_key)  # "r-7f3a:2"
```

### HLD for this flow

```
  Shop API            Warehouse webhook
     | start run            | signal "shipped"
     v                      v
 +---------------------- RELAY SERVER (x2, stateless) ----------------------+
 |  API            Matcher             Timer svc            Reaper          |
 |  start/signal   hands tasks to      every 1s: fire       every 1s: free  |
 |  /cancel/get    long-polling        due timers           expired leases  |
 |                 workers (NOTIFY)                                         |
 +-----|----------------|--------------------|-------------------|----------+
       |   every arrow = ONE transaction that first locks runs[r-7f3a]       |
       v                v                    v                   v
 +--------------------------------- POSTGRES -------------------------------+
 |  runs     events (journal)     tasks (queue)     timers     signals      |
 +--------------------------------------------------------------------------+
       ^                                   ^
       | claim task (SKIP LOCKED, 30s lease) | ack: append events + next task
 +-----+-----------------------------------+------+
 | WORKER A / B / C (your code: order + steps)    |----> Stripe, inventory, email
 +------------------------------------------------+       (idempotency key on every call)
```

### Step-by-step with the rows each transaction writes

**T1 · Shop API → `POST /runs {workflow:"order", input:{order_id:123}, request_id:"ord-123"}`**
```
 runs    + { run_id:"r-7f3a", workflow:"order", version:3, status:"running",
             input:{order_id:123}, request_id:"ord-123", shard:5 }
 events  + { run_id:"r-7f3a", seq:1, type:"RunStarted", payload:{input:{order_id:123}} }
 tasks   + { task_id:"t1", run_id:"r-7f3a", kind:"ADVANCE", attempt:1,
             not_before:"10:00:00", lease_owner:null }
 NOTIFY tasks   -> Matcher wakes a parked worker
```
✅ fixes P1: the run, its history and its first task are one commit, so they all exist or none do.
A retried POST with the same `request_id` returns `r-7f3a`; it does not create a second run.

**T2 · Worker A claims t1, replays (journal = [RunStarted]), reaches call 1 → not in journal**
```
 tasks   ~ t1 { lease_owner:"worker-A", lease_until:"10:00:30" }       (claim)
 --- ack tx ---
 events  + { seq:2, type:"StepScheduled", call_seq:1, payload:{name:"reserve_stock"} }
 tasks   + { task_id:"t2", kind:"STEP", step_name:"reserve_stock",
             idempotency_key:"r-7f3a:1", attempt:1 }
 tasks   - t1                                                          (done tasks are deleted)
```

**T3 · Worker B runs reserve_stock → ack**
```
 events  + { seq:3, type:"StepCompleted", call_seq:1, payload:{result:{reservation:"res-88"}} }
 tasks   + { task_id:"t3", kind:"ADVANCE" }      tasks - t2
```

**T4 · Worker A replays: call 1 → cached `res-88`; call 2 → not in journal**
```
 events  + { seq:4, type:"StepScheduled", call_seq:2, payload:{name:"charge_card"} }
 tasks   + { task_id:"t4", kind:"STEP", step_name:"charge_card",
             idempotency_key:"r-7f3a:2", attempt:1 }      tasks - t3
```

**💥 Crash · Worker B claims t4, calls Stripe with key `r-7f3a:2`, Stripe creates `ch_1`, B is killed before ack**
```
 tasks   t4 { lease_owner:"worker-B", lease_until:"10:00:31" }    <- nobody will ack it
 events  last seq = 4                                              <- journal says "scheduled, not done"

 10:00:32  REAPER:  UPDATE tasks SET lease_owner=null, attempt=attempt+1
                    WHERE lease_until < now()                 -> t4 { attempt:2, lease_owner:null }
 Worker C claims t4, calls Stripe with the SAME key r-7f3a:2
          -> Stripe: "seen this key" -> returns ch_1, creates nothing
```
**T5 · Worker C acks**
```
 events  + { seq:5, type:"StepCompleted", call_seq:2,
             payload:{result:{charge_id:"ch_1"}, attempt:2} }
 tasks   + { task_id:"t5", kind:"ADVANCE" }      tasks - t4
```
✅ fixes P2: retry is safe because the key comes from (run, call position), not from the attempt. There is one charge.

**T6 · Worker A replays: calls 1, 2 cached; call 3 `wait_signal("shipped", 3d)` → not in journal**
```
 events  + { seq:6, type:"TimerCreated", call_seq:3,
             payload:{timer_id:"tm1", purpose:"signal_timeout", signal:"shipped"} }
 timers  + { timer_id:"tm1", run_id:"r-7f3a", call_seq:3, fire_at:"+3 days", fired:false }
 runs    ~ { status:"suspended" }                  tasks - t5
```
```
 ~~~~~~~~~~~~~~ 1 day passes: no task, no thread, no CPU. Only 2 rows are waiting. ~~~~~~~~~~~~~~
 Deploys, restarts and worker crashes in this window change nothing.
```

**T7 · Warehouse → `POST /runs/r-7f3a/signals {name:"shipped", payload:{tracking:"1Z999"}}`**
```
 (tx locks runs[r-7f3a] first)
 signals + { run_id:"r-7f3a", name:"shipped", payload:{tracking:"1Z999"}, delivered:true }
 events  + { seq:7, type:"SignalReceived", call_seq:3, payload:{name:"shipped", data:{tracking:"1Z999"}} }
 (the next replay resolves call 3 with the signal and emits CancelTimer -> timers ~ tm1 { state:'cancelled' };
  see "Signals and timeouts" in DESIGN.md)
 runs    ~ { status:"running" }
 tasks   + { task_id:"t6", kind:"ADVANCE" }
```
✅ fixes P3: the timer service and the signal endpoint both lock `runs[r-7f3a]` before writing, so they cannot interleave. The first commit gets seq 7. The second sees call 3 already resolved and does nothing. A shipped order can't also be refunded.

**T8–T9 · Replay: calls 1–3 cached (call 3 returns `{tracking:"1Z999"}`) → send_receipt scheduled, run, completed**
```
 events  + { seq:8, type:"StepScheduled", call_seq:4, payload:{name:"send_receipt"} }
 events  + { seq:9, type:"StepCompleted", call_seq:4, payload:{result:{message_id:"m-55"}} }
```
**T10 · Last replay: function returns "done"**
```
 events  + { seq:10, type:"RunCompleted", payload:{result:"done"} }
 runs    ~ { status:"completed", result:"done", closed_at:"+1d 00:00:04" }
```

### The journal for r-7f3a at the end, which is the whole story (✅ fixes P4)

```
 seq  type            call  payload                                   at
 ---  --------------  ----  ----------------------------------------  ----------------
  1   RunStarted       -    {input:{order_id:123}}                    day0 10:00:00.010
  2   StepScheduled    1    {name:"reserve_stock"}                    day0 10:00:00.040
  3   StepCompleted    1    {result:{reservation:"res-88"}}           day0 10:00:00.250
  4   StepScheduled    2    {name:"charge_card"}                      day0 10:00:00.290
  5   StepCompleted    2    {result:{charge_id:"ch_1"}, attempt:2}    day0 10:00:32.700
  6   TimerCreated     3    {timer_id:"tm1", signal:"shipped"}        day0 10:00:32.760
  7   SignalReceived   3    {name:"shipped", data:{tracking:"1Z999"}} day1 09:12:05.000
  8   StepScheduled    4    {name:"send_receipt"}                     day1 09:12:05.040
  9   StepCompleted    4    {result:{message_id:"m-55"}}              day1 09:12:05.300
 10   RunCompleted     -    {result:"done"}                           day1 09:12:05.340
```
The 32-second gap between seq 4 and 5 is the crash, visible in the history. Stripe shows exactly one charge, `ch_1`.

**The timeout path:** if no webhook arrives, the timer service fires tm1 at day 3 (`TimerFired` seq 7). Call 3 then returns `None`, and the function runs the `refund` step with its own key `r-7f3a:4`.

### Problem → fix

| Today | Relay mechanism |
|---|---|
| P1 commit-then-enqueue loses work | state change + next task in **one transaction** |
| P2 retry double-charges | **idempotency key = run_id:call_seq**, same on every attempt |
| P3 webhook vs cron race | all writes for a run **lock the run row**; the journal orders them |
| P4 logic in 5 places, no history | **one function**; the **journal is the history** |
| cron for timeouts | **timer row**, fired by the timer service |
| hand-written retry per task | **retry policy declared once** per step |
