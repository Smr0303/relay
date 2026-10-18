"""Every durable decision the server makes, one Postgres transaction each.

The rule for every write: lock the run row, append to the journal, enqueue what happens next.
All in one transaction, so the journal and the queue can never disagree.

Waking workers happens AFTER commit and outside the transaction (`on_tasks`): the server wakes
its own long-polls in memory and sends one coalesced NOTIFY for the others. A pg_notify inside
every transaction serialised all commits on Postgres's notification-queue lock (bench/README).
"""

import contextlib
import json
import os
import random
import uuid
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any

import asyncpg

from relay.common.protocol import (
    AckAdvance,
    AckStep,
    CancelTimer,
    CompleteRun,
    ContinueAsNew,
    CreateTimer,
    Event,
    EventType,
    FailRun,
    Poll,
    RetryPolicy,
    ScheduleStep,
    StartRun,
    Task,
    events_for,
)
from relay.server import metrics
from relay.server.blobs import BlobStore, blob_store_from_env

SHARDS = 16
SCHEMA_VERSION = 2  # bump when schema.sql changes
JOURNAL_WARN = 10_000
JOURNAL_LIMIT = int(os.environ.get("RELAY_JOURNAL_LIMIT", 50_000))
ADVANCE_BACKOFF_S = float(os.environ.get("RELAY_ADVANCE_BACKOFF_S", 1.0))
TERMINAL = ("completed", "failed", "cancelled", "continued")
ADVANCE_MAX_ATTEMPTS = 5
ADVANCE_TIMEOUT_S = float(os.environ.get("RELAY_ADVANCE_TIMEOUT_S", 30))
_WAKE: ContextVar[list | None] = ContextVar("relay_wake", default=None)


class LeaseLost(Exception):
    """409: the caller's lease expired and the task went to someone else (or is gone)."""


class StaleReplay(Exception):
    """409: new events arrived after the worker read the journal; its decision is discarded."""


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


async def init_connection(conn: asyncpg.Connection) -> None:
    for typ in ("json", "jsonb"):
        await conn.set_type_codec(typ, encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(dsn: str, **kw) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, init=init_connection, **kw)


async def apply_schema(pool: asyncpg.Pool) -> None:
    from importlib.resources import files

    sql = files("relay.server").joinpath("schema.sql").read_text()
    async with pool.acquire() as conn:
        # The script takes exclusive table locks (ALTER TABLE), so a server joining a busy
        # cluster must not run it: apply only when the recorded version is behind.
        await conn.execute("CREATE TABLE IF NOT EXISTS relay_schema (version int NOT NULL)")
        if await conn.fetchval("SELECT max(version) FROM relay_schema") == SCHEMA_VERSION:
            return
        # Serialise concurrent server starts: CREATE ... IF NOT EXISTS is not race-safe.
        await conn.execute("SELECT pg_advisory_lock(424242)")
        try:
            if await conn.fetchval("SELECT max(version) FROM relay_schema") != SCHEMA_VERSION:
                async with conn.transaction():
                    await conn.execute(sql)
                    await conn.execute("DELETE FROM relay_schema")
                    await conn.execute("INSERT INTO relay_schema VALUES ($1)", SCHEMA_VERSION)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(424242)")


# ---- small helpers used inside transactions --------------------------------------------------


async def _lock_run(conn, run_id) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT run_id, workflow, version, status, shard, next_seq FROM runs "
        "WHERE run_id = $1 FOR UPDATE",
        run_id,
    )


async def _append(conn, run_id, next_seq: int, rows: list[tuple[str, int | None, dict]]) -> int:
    """Append journal rows starting at next_seq; returns the new next_seq."""
    if rows:
        await conn.executemany(
            "INSERT INTO events (run_id, seq, type, call_seq, payload) VALUES ($1,$2,$3,$4,$5)",
            [(run_id, next_seq + i, str(t), cs, p) for i, (t, cs, p) in enumerate(rows)],
        )
    return next_seq + len(rows)


async def _enqueue_advance(conn, run) -> None:
    await conn.execute(
        "INSERT INTO tasks (task_id, run_id, shard, kind, workflow, version, timeout_s) "
        "VALUES ($1, $2, $3, 'ADVANCE', $4, $5, $6) "
        "ON CONFLICT (run_id) WHERE kind = 'ADVANCE' AND lease_owner IS NULL DO NOTHING",
        uuid.uuid4(),
        run["run_id"],
        run["shard"],
        run["workflow"],
        run["version"],
        ADVANCE_TIMEOUT_S,
    )


async def _close_run(conn, run_id, status: str, result: Any = None, error: Any = None) -> None:
    await conn.execute(
        "UPDATE runs SET status=$2, result=$3, error=$4, updated_at=now(), closed_at=now() "
        "WHERE run_id=$1",
        run_id,
        status,
        result,
        error,
    )
    await conn.execute("DELETE FROM tasks WHERE run_id=$1", run_id)
    await conn.execute(
        "UPDATE timers SET state='cancelled' WHERE run_id=$1 AND state='pending'", run_id
    )
    metrics.RUNS_CLOSED.labels(status).inc()


async def _park(conn, run_id, error: dict) -> None:
    """Stop a run without losing it: an operator fixes the code and resumes it."""
    await conn.execute(
        "UPDATE runs SET status='parked', error=$2, updated_at=now() WHERE run_id=$1",
        run_id,
        error,
    )
    await conn.execute("DELETE FROM tasks WHERE run_id=$1 AND kind='ADVANCE'", run_id)
    metrics.RUNS_PARKED.inc()


def _uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except (ValueError, TypeError):
        raise NotFound(value) from None


# ---- the engine ----------------------------------------------------------------------------


class Engine:
    def __init__(
        self,
        pool: asyncpg.Pool,
        on_tasks: Callable[[], None] | None = None,
        blobs: BlobStore | None = None,
    ) -> None:
        self.pool = pool
        self.on_tasks = on_tasks or (lambda: None)
        self.blobs = blobs or blob_store_from_env()
        self._seen: dict[str, float] = {}  # queue -> last time we recorded a worker serving it

    @contextlib.asynccontextmanager
    async def _tx(self):
        """A transaction; if it queued work (`_wake`), wake workers once it has committed."""
        flag: list = []
        token = _WAKE.set(flag)
        try:
            async with self.pool.acquire() as conn, conn.transaction():
                yield conn
        finally:
            _WAKE.reset(token)
        if flag:  # not reached when the transaction raised and rolled back
            self.on_tasks()

    def _wake(self) -> None:
        flag = _WAKE.get()
        if flag is None:
            self.on_tasks()
        else:
            flag.append(1)

    # client side

    async def start_run(self, req: StartRun) -> tuple[str, bool]:
        async with self._tx() as conn:
            if req.request_id is not None:
                existing = await conn.fetchval(
                    "SELECT run_id FROM runs WHERE request_id=$1", req.request_id
                )
                if existing:
                    return str(existing), False
            version = req.version or await self._live_version(conn, req.workflow)
            run_id = await self._create_run(conn, req.workflow, version, req.input, req.request_id)
            if run_id is None:  # lost a race on the same request_id
                existing = await conn.fetchval(
                    "SELECT run_id FROM runs WHERE request_id=$1", req.request_id
                )
                return str(existing), False
        return str(run_id), True

    async def _live_version(self, conn, workflow: str) -> int:
        """The newest version a worker served in the last 5 minutes (new runs start there;
        old runs stay pinned to the version they started on)."""
        v = await conn.fetchval(
            "SELECT max(version) FROM workflow_versions "
            "WHERE workflow=$1 AND seen_at > now() - interval '5 minutes'",
            workflow,
        )
        return v or 1

    async def _create_run(
        self, conn, workflow, version, input, request_id=None, continued_from=None
    ):
        run_id = uuid.uuid4()
        run = await conn.fetchrow(
            "INSERT INTO runs (run_id, workflow, version, status, input, request_id, shard, "
            "next_seq) VALUES ($1,$2,$3,'running',$4,$5,$6,2) "
            "ON CONFLICT (request_id) DO NOTHING "
            "RETURNING run_id, workflow, version, shard",
            run_id,
            workflow,
            version,
            input,
            request_id,
            run_id.int % SHARDS,
        )
        if run is None:
            return None
        payload = {"input": input}
        if continued_from is not None:
            payload["continued_from"] = str(continued_from)
        await _append(conn, run_id, 1, [(EventType.RUN_STARTED, None, payload)])
        await _enqueue_advance(conn, run)
        self._wake()
        metrics.RUNS_STARTED.inc()
        return run_id

    async def cancel(self, run_id: str) -> None:
        """Record the request; the workflow sees Cancelled at its next ctx call and may run
        compensation steps before it ends."""
        rid = _uuid(run_id)
        async with self._tx() as conn:
            run = await _lock_run(conn, rid)
            if run is None:
                raise NotFound(run_id)
            if run["status"] in TERMINAL:
                raise Conflict(f"run is {run['status']}")
            seq = await _append(
                conn, rid, run["next_seq"], [(EventType.CANCEL_REQUESTED, None, {})]
            )
            await conn.execute(
                "UPDATE runs SET next_seq=$2, updated_at=now() WHERE run_id=$1", rid, seq
            )
            if run["status"] != "parked":
                await _enqueue_advance(conn, run)
                self._wake()

    async def resume(self, run_id: str) -> None:
        """Un-park a run (after deploying fixed code): replay it again from its journal."""
        rid = _uuid(run_id)
        async with self._tx() as conn:
            run = await _lock_run(conn, rid)
            if run is None:
                raise NotFound(run_id)
            if run["status"] != "parked":
                raise Conflict(f"run is {run['status']}, not parked")
            await conn.execute(
                "UPDATE runs SET status='running', error=NULL, updated_at=now() WHERE run_id=$1",
                rid,
            )
            await _enqueue_advance(conn, run)
            self._wake()

    async def get_run(self, run_id: str) -> dict | None:
        row = await self.pool.fetchrow(
            "SELECT run_id, workflow, version, status, input, result, error, request_id, "
            "next_seq - 1 AS events, created_at, updated_at, closed_at FROM runs WHERE run_id=$1",
            _uuid(run_id),
        )
        if row is None:
            return None
        out = dict(row)
        out["run_id"] = str(out["run_id"])
        for k in ("created_at", "updated_at", "closed_at"):
            out[k] = out[k].isoformat() if out[k] else None
        return out

    async def get_events(self, run_id: str) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT seq, type, call_seq, payload, at FROM events WHERE run_id=$1 ORDER BY seq",
            _uuid(run_id),
        )
        return [{**dict(r), "at": r["at"].isoformat()} for r in rows]

    async def signal(self, run_id: str, name: str, data: Any) -> None:
        rid = _uuid(run_id)
        async with self._tx() as conn:
            run = await _lock_run(conn, rid)
            if run is None:
                raise NotFound(run_id)
            if run["status"] in TERMINAL:
                raise Conflict(f"run is {run['status']}")
            await conn.execute(
                "INSERT INTO signals (run_id, name, payload) VALUES ($1,$2,$3)", rid, name, data
            )
            seq = await _append(
                conn,
                rid,
                run["next_seq"],
                [(EventType.SIGNAL_RECEIVED, None, {"name": name, "data": data})],
            )
            await conn.execute(
                "UPDATE runs SET next_seq=$2, updated_at=now() WHERE run_id=$1", rid, seq
            )
            if run["status"] != "parked":
                await _enqueue_advance(conn, run)
                self._wake()

    # worker side

    async def claim(self, poll: Poll) -> list[Task]:
        queues = [f"{name}:{version}" for name, version in poll.handles]
        await self._record_handles(poll.handles)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH c AS (
                  SELECT task_id FROM tasks
                  WHERE lease_owner IS NULL AND not_before <= now() AND queue = ANY($2::text[])
                  ORDER BY not_before
                  LIMIT $3
                  FOR UPDATE SKIP LOCKED)
                UPDATE tasks t SET lease_owner = $1, lease_token = gen_random_uuid(),
                       lease_until = now() + make_interval(secs => t.timeout_s)
                FROM c WHERE t.task_id = c.task_id
                RETURNING t.*, extract(epoch FROM now() - t.not_before) AS waited_s
                """,
                poll.worker_id,
                queues,
                poll.max_tasks,
            )
            if not rows:
                return []
            advance_runs = [r["run_id"] for r in rows if r["kind"] == "ADVANCE"]
            inputs: dict = {}
            journals: dict = {rid: [] for rid in advance_runs}
            if advance_runs:
                for r in await conn.fetch(
                    "SELECT run_id, input FROM runs WHERE run_id = ANY($1::uuid[])", advance_runs
                ):
                    inputs[r["run_id"]] = r["input"]
                for e in await conn.fetch(
                    "SELECT run_id, seq, type, call_seq, payload FROM events "
                    "WHERE run_id = ANY($1::uuid[]) ORDER BY run_id, seq",
                    advance_runs,
                ):
                    payload = e["payload"]
                    if "result_ref" in payload:  # large step result kept out of Postgres
                        payload = {**payload, "result": self.blobs.get(payload["result_ref"])}
                    journals[e["run_id"]].append(
                        Event(
                            seq=e["seq"],
                            type=e["type"],
                            call_seq=e["call_seq"],
                            payload=payload,
                        )
                    )
        tasks = []
        for r in rows:
            metrics.DISPATCH_LATENCY.labels(r["kind"]).observe(max(float(r["waited_s"]), 0.0))
            tasks.append(
                Task(
                    task_id=str(r["task_id"]),
                    lease_token=str(r["lease_token"]),
                    kind=r["kind"],
                    run_id=str(r["run_id"]),
                    workflow=r["workflow"],
                    version=r["version"],
                    attempt=r["attempt"],
                    timeout_s=r["timeout_s"],
                    input=inputs.get(r["run_id"]),
                    journal=journals.get(r["run_id"], []),
                    call_seq=r["call_seq"],
                    step_name=r["step_name"],
                    args=r["args"] or [],
                )
            )
        return tasks

    async def _record_handles(self, handles) -> None:
        import time

        now = time.monotonic()
        due = [(n, v) for n, v in handles if now - self._seen.get(f"{n}:{v}", -1e9) > 10]
        if not due:
            return
        for n, v in due:
            self._seen[f"{n}:{v}"] = now
        await self.pool.executemany(
            "INSERT INTO workflow_versions (workflow, version, seen_at) VALUES ($1,$2,now()) "
            "ON CONFLICT (workflow, version) DO UPDATE SET seen_at = now()",
            due,
        )

    async def _locked_task(self, conn, task_id: str, token: str, kind: str):
        """Lock run then task, and check the caller still holds the lease (fencing)."""
        tid = _uuid(task_id)
        rid = await conn.fetchval("SELECT run_id FROM tasks WHERE task_id=$1", tid)
        if rid is None:
            raise LeaseLost(task_id)
        run = await _lock_run(conn, rid)
        task = await conn.fetchrow(
            "SELECT * FROM tasks WHERE task_id=$1 AND lease_token=$2 AND kind=$3 FOR UPDATE",
            tid,
            _uuid(token),
            kind,
        )
        if task is None or run is None:
            raise LeaseLost(task_id)
        return run, task

    async def ack_advance(self, task_id: str, ack: AckAdvance) -> None:
        if ack.replay_ms is not None:
            metrics.REPLAY_SECONDS.observe(ack.replay_ms / 1000)
        stale: str | None = None
        async with self._tx() as conn:
            stale = await self._apply_advance(conn, task_id, ack)
        if stale:
            raise StaleReplay(stale)

    async def _apply_advance(self, conn, task_id: str, ack: AckAdvance) -> str | None:
        notify = False
        run, task = await self._locked_task(conn, task_id, ack.lease_token, "ADVANCE")
        rid = run["run_id"]
        if run["status"] in TERMINAL or run["status"] == "parked":
            await conn.execute("DELETE FROM tasks WHERE task_id=$1", task["task_id"])
            return None
        if ack.replay_error is not None:
            await self._replay_failed(conn, run, task, ack)
            return None
        if run["next_seq"] - 1 != ack.journal_len:
            # New events landed after this worker read the journal, so its decision is dropped.
            # The appender usually queued a fresh ADVANCE, but not always: if one was being
            # claimed at that instant the appender skipped its insert (ON CONFLICT DO NOTHING)
            # while the claimer read the journal before the event committed. Release this task
            # instead of deleting it, unless another wake-up is already waiting.
            await self._release_advance(conn, task)
            return f"journal has {run['next_seq'] - 1} events, worker saw {ack.journal_len}"
        await conn.execute("DELETE FROM tasks WHERE task_id=$1", task["task_id"])
        if not ack.commands:
            return None  # still waiting on a step, timer or signal

        rows, steps, timers, cancels = [], [], [], []
        close: tuple | None = None
        for cmd in ack.commands:
            rows.extend(events_for(cmd))
            match cmd:
                case ScheduleStep():
                    steps.append(cmd)
                case CreateTimer(fire_in_s=fire_in) if fire_in is not None:
                    timers.append(cmd)
                case CancelTimer():
                    cancels.append(cmd.call_seq)
                case CompleteRun():
                    close = ("completed", cmd.result, None)
                case FailRun():
                    status = "cancelled" if cmd.error.get("type") == "Cancelled" else "failed"
                    close = (status, None, cmd.error)
                case ContinueAsNew():
                    new_id = await self._create_run(
                        conn, run["workflow"], run["version"], cmd.input, continued_from=rid
                    )
                    close = ("continued", {"continued_as": str(new_id)}, None)
        length = run["next_seq"] - 1 + len(rows)
        if length > JOURNAL_LIMIT:
            # A run that loops without continue_as_new would replay ever slower; stop it.
            await _close_run(
                conn,
                rid,
                "failed",
                None,
                {
                    "type": "JournalLimit",
                    "message": f"journal would reach {length} events (limit {JOURNAL_LIMIT}); "
                    "use ctx.continue_as_new in long loops",
                },
            )
            return None
        if run["next_seq"] - 1 < JOURNAL_WARN <= length:
            metrics.JOURNAL_WARNINGS.inc()
        seq = await _append(conn, rid, run["next_seq"], rows)
        status = "running" if steps else "suspended"
        await conn.execute(
            "UPDATE runs SET next_seq=$2, status=$3, updated_at=now() WHERE run_id=$1",
            rid,
            seq,
            status,
        )
        if steps:
            await conn.executemany(
                "INSERT INTO tasks (task_id, run_id, shard, kind, workflow, version, call_seq,"
                " step_name, args, retry, timeout_s) "
                "VALUES ($1,$2,$3,'STEP',$4,$5,$6,$7,$8,$9,$10)",
                [
                    (
                        uuid.uuid4(),
                        rid,
                        run["shard"],
                        run["workflow"],
                        run["version"],
                        s.call_seq,
                        s.name,
                        s.args,
                        s.retry.model_dump(),
                        s.timeout_s,
                    )
                    for s in steps
                ],
            )
            notify = True
        if timers:
            await conn.executemany(
                "INSERT INTO timers (timer_id, run_id, call_seq, fire_at) "
                "VALUES ($1, $2, $3, now() + make_interval(secs => $4))",
                [(uuid.uuid4(), rid, t.call_seq, t.fire_in_s) for t in timers],
            )
        if cancels:
            await conn.execute(
                "UPDATE timers SET state='cancelled' "
                "WHERE run_id=$1 AND call_seq = ANY($2::int[]) AND state='pending'",
                rid,
                cancels,
            )
        if close is not None:
            await _close_run(conn, rid, *close)
        if notify:
            self._wake()

    async def _replay_failed(self, conn, run, task, ack: AckAdvance) -> None:
        error = {**ack.replay_error, "attempt": task["attempt"]}
        if ack.nondeterminism:
            metrics.NONDETERMINISM.inc()
            await _park(conn, run["run_id"], error)
            return
        if task["attempt"] >= ADVANCE_MAX_ATTEMPTS:
            await _park(conn, run["run_id"], error)
            return
        delay = ADVANCE_BACKOFF_S * 2.0 ** (task["attempt"] - 1)
        await self._requeue_advance(conn, task, delay_s=delay)

    async def _release_advance(self, conn, task) -> None:
        pending = await conn.fetchval(
            "SELECT 1 FROM tasks WHERE run_id=$1 AND kind='ADVANCE' AND lease_owner IS NULL",
            task["run_id"],
        )
        if pending:
            await conn.execute("DELETE FROM tasks WHERE task_id=$1", task["task_id"])
            return
        await conn.execute(
            "UPDATE tasks SET not_before=now(), lease_owner=NULL, lease_token=NULL, "
            "lease_until=NULL WHERE task_id=$1",
            task["task_id"],
        )
        self._wake()

    async def _requeue_advance(self, conn, task, delay_s: float) -> None:
        pending = await conn.fetchval(
            "SELECT 1 FROM tasks WHERE run_id=$1 AND kind='ADVANCE' AND lease_owner IS NULL",
            task["run_id"],
        )
        if pending:  # a newer wake-up already exists; it carries the work
            await conn.execute("DELETE FROM tasks WHERE task_id=$1", task["task_id"])
            return
        await conn.execute(
            "UPDATE tasks SET attempt=attempt+1, not_before=now() + make_interval(secs => $2), "
            "lease_owner=NULL, lease_token=NULL, lease_until=NULL WHERE task_id=$1",
            task["task_id"],
            delay_s,
        )

    async def ack_step(self, task_id: str, ack: AckStep) -> None:
        if ack.duration_ms is not None:
            metrics.STEP_SECONDS.observe(ack.duration_ms / 1000)
        async with self._tx() as conn:
            run, task = await self._locked_task(conn, task_id, ack.lease_token, "STEP")
            rid, cs, attempt = run["run_id"], task["call_seq"], task["attempt"]
            if run["status"] in TERMINAL:
                await conn.execute("DELETE FROM tasks WHERE task_id=$1", task["task_id"])
                return  # e.g. cancelled while this step ran: the result is discarded
            if ack.error is None:
                payload = {"result": ack.result, "attempt": attempt}
                raw = json.dumps(ack.result).encode()
                if len(raw) > self.blobs.max_inline:  # keep the journal row small
                    ref = f"{rid}/{cs}"
                    self.blobs.put(ref, ack.result)
                    payload = {"result_ref": ref, "bytes": len(raw), "attempt": attempt}
                rows = [(EventType.STEP_COMPLETED, cs, payload)]
                await self._finish_step(conn, run, task, rows)
                metrics.STEPS_COMPLETED.inc()
                return
            policy = RetryPolicy(**(task["retry"] or {}))
            final = not ack.retryable or attempt >= policy.max_attempts
            rows = [
                (
                    EventType.STEP_FAILED,
                    cs,
                    {"error": ack.error, "attempt": attempt, "final": final},
                )
            ]
            metrics.STEP_FAILURES.labels(str(final).lower()).inc()
            if final:
                await self._finish_step(conn, run, task, rows)
                return
            seq = await _append(conn, rid, run["next_seq"], rows)
            await conn.execute(
                "UPDATE runs SET next_seq=$2, updated_at=now() WHERE run_id=$1", rid, seq
            )
            await conn.execute(
                "UPDATE tasks SET attempt=attempt+1, not_before=now() + make_interval(secs => $2),"
                " lease_owner=NULL, lease_token=NULL, lease_until=NULL WHERE task_id=$1",
                task["task_id"],
                policy.backoff_s(attempt, random.random()),
            )

    async def _finish_step(self, conn, run, task, rows) -> None:
        seq = await _append(conn, run["run_id"], run["next_seq"], rows)
        await conn.execute(
            "UPDATE runs SET next_seq=$2, updated_at=now() WHERE run_id=$1", run["run_id"], seq
        )
        await conn.execute("DELETE FROM tasks WHERE task_id=$1", task["task_id"])
        if run["status"] != "parked":
            await _enqueue_advance(conn, run)
            self._wake()

    async def heartbeat(self, task_id: str, token: str) -> None:
        ok = await self.pool.fetchval(
            "UPDATE tasks SET lease_until = now() + make_interval(secs => timeout_s) "
            "WHERE task_id=$1 AND lease_token=$2 AND lease_owner IS NOT NULL RETURNING 1",
            _uuid(task_id),
            _uuid(token),
        )
        if not ok:
            raise LeaseLost(task_id)

    # background loops

    async def fire_timers(self, limit: int = 500) -> int:
        """Fire due timers in one batch. Never waits for a run lock (SKIP LOCKED), because it
        holds timer rows that an ack (which holds the run lock) may want to cancel."""
        async with self._tx() as conn:
            due = await conn.fetch(
                "SELECT timer_id, run_id, call_seq, extract(epoch FROM now() - fire_at) AS lag "
                "FROM timers WHERE state='pending' AND fire_at <= now() "
                "ORDER BY fire_at LIMIT $1 FOR UPDATE SKIP LOCKED",
                limit,
            )
            if not due:
                return 0
            runs = {
                r["run_id"]: dict(r)
                for r in await conn.fetch(
                    "SELECT run_id, workflow, version, status, shard, next_seq FROM runs "
                    "WHERE run_id = ANY($1::uuid[]) ORDER BY run_id FOR UPDATE SKIP LOCKED",
                    list({t["run_id"] for t in due}),
                )
            }
            events, fired, cancelled, woken = [], [], [], {}
            for t in due:
                run = runs.get(t["run_id"])
                if run is None:
                    continue  # run busy right now; this timer fires on the next pass
                if run["status"] in TERMINAL:
                    cancelled.append(t["timer_id"])
                    continue
                events.append(
                    (run["run_id"], run["next_seq"], str(EventType.TIMER_FIRED), t["call_seq"], {})
                )
                run["next_seq"] += 1
                fired.append(t["timer_id"])
                woken[run["run_id"]] = run
                metrics.TIMER_LAG.observe(max(float(t["lag"]), 0.0))
            if events:
                await conn.executemany(
                    "INSERT INTO events (run_id, seq, type, call_seq, payload) "
                    "VALUES ($1,$2,$3,$4,$5)",
                    events,
                )
                await conn.executemany(
                    "UPDATE runs SET next_seq=$2, updated_at=now() WHERE run_id=$1",
                    [(r["run_id"], r["next_seq"]) for r in woken.values()],
                )
                for run in woken.values():
                    if run["status"] != "parked":
                        await _enqueue_advance(conn, run)
                self._wake()
            if fired:
                await conn.execute(
                    "UPDATE timers SET state='fired' WHERE timer_id = ANY($1::uuid[])", fired
                )
            if cancelled:
                await conn.execute(
                    "UPDATE timers SET state='cancelled' WHERE timer_id = ANY($1::uuid[])",
                    cancelled,
                )
            return len(fired)

    async def wake_delayed(self, since):
        """NOTIFY when a backed-off task's not_before passed since the last check, so a retry
        starts within ~0.1 s of its deadline instead of at the next 1 s fallback poll."""
        row = await self.pool.fetchrow(
            "SELECT now() AS now, EXISTS (SELECT 1 FROM tasks WHERE lease_owner IS NULL "
            "AND not_before > $1 AND not_before <= now()) AS due",
            since,
        )
        if row["due"]:
            self.on_tasks()
        return row["now"]

    async def reap(self, limit: int = 500) -> int:
        """Take back tasks whose lease expired: the worker holding them is presumed dead."""
        expired = await self.pool.fetch(
            "SELECT task_id FROM tasks WHERE lease_owner IS NOT NULL AND lease_until < now() "
            "LIMIT $1",
            limit,
        )
        n = 0
        for row in expired:
            async with self._tx() as conn:
                rid = await conn.fetchval(
                    "SELECT run_id FROM tasks WHERE task_id=$1", row["task_id"]
                )
                if rid is None:
                    continue
                run = await _lock_run(conn, rid)
                task = await conn.fetchrow(
                    "SELECT * FROM tasks WHERE task_id=$1 AND lease_owner IS NOT NULL "
                    "AND lease_until < now() FOR UPDATE",
                    row["task_id"],
                )
                if task is None or run is None:
                    continue
                n += 1
                metrics.LEASE_EXPIRIES.labels(task["kind"]).inc()
                if run["status"] in TERMINAL:
                    await conn.execute("DELETE FROM tasks WHERE task_id=$1", task["task_id"])
                elif task["kind"] == "ADVANCE":
                    if task["attempt"] >= ADVANCE_MAX_ATTEMPTS:
                        await _park(
                            conn,
                            rid,
                            {
                                "type": "LeaseExpired",
                                "message": "replay timed out repeatedly",
                                "attempt": task["attempt"],
                            },
                        )
                    else:
                        await self._requeue_advance(conn, task, delay_s=0)
                else:
                    await self._step_lease_expired(conn, run, task)
        return n

    async def _step_lease_expired(self, conn, run, task) -> None:
        policy = RetryPolicy(**(task["retry"] or {}))
        attempt = task["attempt"]
        final = attempt >= policy.max_attempts
        error = {"type": "LeaseExpired", "message": f"no ack within {task['timeout_s']}s"}
        rows = [
            (
                EventType.STEP_FAILED,
                task["call_seq"],
                {"error": error, "attempt": attempt, "final": final},
            )
        ]
        if final:
            await self._finish_step(conn, run, task, rows)
            return
        seq = await _append(conn, run["run_id"], run["next_seq"], rows)
        await conn.execute(
            "UPDATE runs SET next_seq=$2, updated_at=now() WHERE run_id=$1", run["run_id"], seq
        )
        await conn.execute(
            "UPDATE tasks SET attempt=attempt+1, not_before=now(), lease_owner=NULL, "
            "lease_token=NULL, lease_until=NULL WHERE task_id=$1",
            task["task_id"],
        )
        self._wake()
