# Relay design (as built)

This describes the code in this repo. The original HLD and plan are in the author's notes; where the build departed from them it says so below.

## The model

A **workflow** is an `async def` that may only await `ctx.*` calls. A **step** is an ordinary function decorated with `@step`, and steps are where side effects live. The engine journals every `ctx` call's outcome. Resuming a run means calling the function again from line 1 with that journal (**replay**):
- A recorded call returns its recorded result.
- The first unrecorded call becomes a command, and the run suspends there.

```
   ctx call                 journal events (call_seq = position of the call in the function)
   ---------------------    ---------------------------------------------------------------
   await ctx.step(f, x)     StepScheduled · StepFailed(attempt, final?)* · StepCompleted
   await ctx.sleep(d)       TimerCreated · TimerFired
   await ctx.wait_signal()  TimerCreated(signal) · [SignalReceived] · TimerFired | TimerCancelled
   await ctx.now/uuid/...   ValueRecorded                       (recorded, never re-generated)
   await ctx.patched(id)    MarkerRecorded                      (only on the new code path)
   ctx.continue_as_new(x)   RunContinued                        (new run starts with x)
   run level                RunStarted · CancelRequested · RunCompleted | RunFailed
```

If replay finds a recorded call that doesn't match what the code asks for, it raises `NonDeterminismError`, and the server **parks** the run. A parked run is never guessed at and never lost: roll back or add a patch gate, then `POST /resume`.

## The one rule

> Every state change and the work it causes are one Postgres transaction:
> lock the run row → append journal rows → enqueue tasks / timers.

The rule eliminates the dual-write class of bug (commit, then enqueue). Waking workers happens *after* commit, outside the transaction (see Wake-ups).

## Tables

| table | role | notes |
|---|---|---|
| `runs` | one row per run | status, pinned `version`, `next_seq` (journal length + 1, assigned under the row lock), `request_id` UNIQUE (idempotent start) |
| `events` | the journal | PK `(run_id, seq)`, append-only; unique `(run_id, call_seq)` for StepCompleted |
| `tasks` | the queue | `ADVANCE` (replay the run) or `STEP`; lease columns; `queue = workflow:version`; at most one *unclaimed* ADVANCE per run (partial unique index) |
| `timers` | durable sleeps / wait timeouts | `fire_at`, `state`, `shard` |
| `signals` | audit of received signals | the journal is authoritative |
| `workflow_versions` | which versions live workers serve | used to pick the version for new runs |
| `servers` | live server heartbeats | used to share out shard ownership |

## Task lifecycle and fencing

```
 ready ──claim (SKIP LOCKED, lease_token = new uuid, lease_until = now + timeout)──> leased
 leased ──ack with matching lease_token──> deleted (+ events + next tasks, one tx)
 leased ──heartbeat with matching token──> lease_until extended
 leased ──lease_until passes──> reaper: STEP → StepFailed(LeaseExpired), attempt+1, ready
                                        ADVANCE → ready again (parked after 5)
```

The **lease token** is the fence. A worker that was presumed dead and comes back gets **409** on ack or heartbeat, because its token is no longer on the row. Its late result is dropped, and the attempt that replaced it is the only one that records anything.

**Idempotency key** = `run_id:call_seq`. It is the same on every attempt, so a step that charged a card and then died before acking sends the same key on retry. The downstream API returns the original charge. This is crash case C in the design, and the chaos test exercises it about 30 times per run.

## The ADVANCE ack (where correctness lives)

```
lock runs[run]          ── serialises every writer of this run
check lease_token       ── fencing
if journal length != what the worker replayed:
    stale → release this task (unless another ADVANCE is waiting), 409
append events, insert STEP tasks / timers, cancel timers, close the run if finished
```

**Bug found by tests (lost wake-up).** The original stale path *deleted* the task, assuming whoever appended the new event had queued a fresh ADVANCE. But that appender's `INSERT … ON CONFLICT DO NOTHING` can be skipped because an ADVANCE is pending at that instant, while a worker is mid-claim on it and reads the journal before the event commits. The result was a run with 20 signals in its journal and nothing left to wake it. A signal storm test caught it about 1 run in 6. The fix is to release instead of delete.

## Signals and timeouts

`wait_signal(name, timeout)` resolves to whichever comes first *in journal order*: the earliest unconsumed matching signal, or its `TimerFired`. Because journal order is the only clock, every replay agrees, and "shipped and also refunded" cannot happen. That claim was checked over 1,000 runs where the signal and timeout land within ±250 ms of each other. A signal that arrives before the wait starts is buffered.

## Cancellation

`POST /cancel` appends `CancelRequested`. Replay raises `Cancelled` at the **first call that was not already resolved before that event**. That point is a function of the journal, so it doesn't move between replays. The workflow can catch `Cancelled` and run compensation steps (the order example refunds), and open timers at that call are cancelled.

## Versioning

- **Pinning.** `runs.version` is fixed at start, and tasks route on `queue = workflow:version`, so a worker only receives runs it has the code for. A start without a version uses the newest version a worker served in the last 5 minutes.
- **Patch gates.** `ctx.patched(id)` looks at the next call position:
  - A marker for this patch means True.
  - Any other recorded call means False, and **no call number is consumed**, so old runs keep lining up.
  - Nothing recorded yet means True, and the marker is recorded.

## Wake-ups

**Bug found by the benchmark.** Workers long-poll, and originally every transaction that queued work called `pg_notify`. Under load, 79 backends sat in `Lock: object`: Postgres serialises committing transactions that NOTIFY on one cluster-wide queue lock. The fix:
- After commit, the server wakes its own long-polls in memory.
- One publisher per server sends a single coalesced NOTIFY to the others, with at most one in flight.
- Long-polls also re-check every second, and a ~100 ms loop NOTIFYs when a backed-off task's `not_before` passes.

**Bug found by the kill test (orphaned long-polls).** A SIGKILLed worker leaves its open long-poll requests behind on the server, and those kept claiming tasks for a process that no longer existed. Each such task then sat until its lease expired. The poll now checks for a client disconnect before each claim.

## Throughput decisions

These were measured in `bench/load.py`, laptop, Docker Postgres:

| change | steps/s |
|---|---|
| first working version | 290 |
| claim filtered on `(workflow, version) IN (unnest …)`: the planner sorted *every* ready task per claim → single `queue` column so the claim walks the `not_before` index in order | 280 (not the bottleneck yet) |
| worker HTTP client httpx → aiohttp: the same server served ~1k req/s to httpx's async pool and ~24k req/s to aiohttp | 790 |
| NOTIFY out of the transactions (above) | **1,200 (the full offered load)** |

## Shards

Runs hash into 16 shards (`run_id % 16`). The timer service, reaper and archiver each run per shard:
- A server owns a shard by holding a session-level advisory lock on it, and takes `ceil(16 / live servers)` shards.
- A dead server's locks vanish with its session, and the survivors take its shards within about 2 s.

Ownership is an efficiency measure, not a correctness one: every loop re-checks rows under row locks. Claims are not sharded. At the measured load, one ordered index walk with `SKIP LOCKED` was not a hotspot.

## Large results, journal growth, retention

- Step results over 256 KB go to a blob store, and the journal row keeps `result_ref` (filesystem store; S3 is the same two methods). The server inlines them when it hands the journal to a worker.
- Journals: a warning metric fires at 10k events, and the hard limit is 50k (`RELAY_JOURNAL_LIMIT`). Long loops use `continue_as_new`.
- The archiver moves closed runs older than `RELAY_RETENTION_S` (7 days) to JSONL and deletes them. Archiving is at-least-once.

## Not done / limits (honest list)

- **Steps within one run are sequential.** Parallel steps (`gather`) and child workflows are not implemented.
- **No authentication** on the HTTP API.
- **One Postgres.** Shards are logical; moving shards to separate databases is designed for (nothing crosses a shard) but not built.
- **The blob and archive stores are filesystem-backed.** S3 is not wired.
- **A workflow stuck in an infinite loop with no `ctx` call blocks its worker's event loop.** The lease expires and the run is parked after 5 attempts, but that worker stays stuck. Replays are not run in a subprocess.
- **All numbers are from one laptop** (Apple M5 Pro, Postgres 16 in Docker Desktop with `synchronous_commit` on). They are not from a production cluster.
