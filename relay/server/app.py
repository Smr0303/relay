"""HTTP API + the background loops (matcher wake-ups, timer service, reaper, stats)."""

import asyncio
import contextlib
import logging
import os
import time
import uuid

import asyncpg
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from relay.common import tracing
from relay.common.protocol import AckAdvance, AckStep, Heartbeat, Poll, SignalIn, StartRun
from relay.server import metrics
from relay.server.engine import (
    Conflict,
    Engine,
    LeaseLost,
    NotFound,
    StaleReplay,
    apply_schema,
    create_pool,
)
from relay.server.shards import ShardOwner

log = logging.getLogger("relay.server")
DEFAULT_DSN = "postgresql://relay:relay@localhost:5440/relay"


class Notifier:
    """Wakes parked long-polls when tasks may be ready.

    Local work (`publish`, called after a commit) wakes this server's polls in memory at once,
    and marks a cross-server NOTIFY as due. One publisher coroutine sends at most one NOTIFY at
    a time on its own connection, so a burst of commits costs one notification, not one each.

    Callers take `current()` BEFORE trying to claim and wait on it after an empty claim, so a
    wake-up that lands between the two is never missed.
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._dirty = asyncio.Event()
        self._id = uuid.uuid4().hex
        self._dsn = ""
        self._tasks: list[asyncio.Task] = []

    async def start(self, dsn: str) -> None:
        self._dsn = dsn
        self._tasks = [asyncio.create_task(self._listen()), asyncio.create_task(self._publisher())]

    def _on_notify(self, _conn, _pid, _channel, payload) -> None:
        if payload != self._id:  # our own NOTIFY: local polls were already woken
            self.wake()

    def publish(self) -> None:
        self.wake()
        self._dirty.set()

    async def _listen(self) -> None:
        """Keep a LISTEN connection up. If Postgres drops it, reconnect and wake everyone,
        since notifications sent meanwhile are gone (polls also re-check every second)."""
        while True:
            conn = None
            try:
                conn = await asyncpg.connect(self._dsn)
                lost = asyncio.Event()
                conn.add_termination_listener(lambda _c, ev=lost: ev.set())
                await conn.add_listener("relay_tasks", self._on_notify)
                self.wake()
                await lost.wait()
                log.warning("LISTEN connection lost; reconnecting")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("LISTEN connection failed")
            finally:
                if conn is not None and not conn.is_closed():
                    conn.terminate()
            await asyncio.sleep(0.5)

    async def _publisher(self) -> None:
        conn = None
        while True:
            await self._dirty.wait()
            self._dirty.clear()
            try:
                if conn is None or conn.is_closed():
                    conn = await asyncpg.connect(self._dsn)
                await conn.execute("SELECT pg_notify('relay_tasks', $1)", self._id)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("NOTIFY failed; reconnecting")
                conn = None
                self._dirty.set()
                await asyncio.sleep(0.5)

    def wake(self) -> None:
        ev, self._event = self._event, asyncio.Event()
        ev.set()

    def current(self) -> asyncio.Event:
        return self._event

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)


async def _loop(name: str, interval_s: float, fn) -> None:
    while True:
        try:
            busy = await fn()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("%s loop failed", name)
            busy = False
        if not busy:
            await asyncio.sleep(interval_s)


def create_app(
    dsn: str | None = None,
    *,
    timer_interval_s: float = 0.1,
    reaper_interval_s: float = 1.0,
    background: bool = True,
) -> FastAPI:
    dsn = dsn or os.environ.get("RELAY_DSN", DEFAULT_DSN)
    state: dict = {}

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        tracing.init("relay-server")
        pool = await create_pool(dsn, min_size=2, max_size=int(os.environ.get("RELAY_POOL", 20)))
        await apply_schema(pool)
        notifier = Notifier()
        await notifier.start(dsn)
        engine = Engine(pool, on_tasks=notifier.publish)
        state.update(pool=pool, engine=engine, notifier=notifier)
        loops = []
        if background:
            owner = ShardOwner(dsn, notifier._id)
            state["shards"] = owner
            # RELAY_DISABLE_TIMERS simulates a broken timer service (alert drills only).
            timers_on = not os.environ.get("RELAY_DISABLE_TIMERS")
            timers = _timers_once(engine, owner)
            loops = [asyncio.create_task(owner.run())]
            if timers_on:
                loops.append(asyncio.create_task(_loop("timers", timer_interval_s, timers)))
            loops += [
                asyncio.create_task(_loop("reaper", reaper_interval_s, _reap_once(engine, owner))),
                asyncio.create_task(_loop("archiver", 30.0, _archive_once(engine, owner))),
                asyncio.create_task(_loop("stats", 2.0, _stats_once(pool))),
            ]
        try:
            yield
        finally:
            for t in loops:
                t.cancel()
            await asyncio.gather(*loops, return_exceptions=True)
            await notifier.stop()
            await pool.close()

    app = FastAPI(title="Relay", lifespan=lifespan)

    def eng() -> Engine:
        return state["engine"]

    @app.post("/v1/runs")
    async def start_run(req: StartRun, response: Response):
        run_id, created = await eng().start_run(req)
        response.status_code = 201 if created else 200
        return {"run_id": run_id, "created": created}

    @app.get("/v1/runs/{run_id}")
    async def get_run(run_id: str):
        try:
            run = await eng().get_run(run_id)
        except NotFound:
            run = None
        if run is None:
            raise HTTPException(404, "no such run")
        return run

    @app.get("/v1/runs/{run_id}/events")
    async def get_events(run_id: str):
        try:
            return await eng().get_events(run_id)
        except NotFound:
            raise HTTPException(404, "no such run") from None

    @app.post("/v1/runs/{run_id}/signals")
    async def signal(run_id: str, body: SignalIn):
        try:
            await eng().signal(run_id, body.name, body.data)
        except NotFound:
            raise HTTPException(404, "no such run") from None
        except Conflict as err:
            raise HTTPException(409, str(err)) from None
        return {"ok": True}

    @app.post("/v1/runs/{run_id}/cancel")
    async def cancel(run_id: str):
        try:
            await eng().cancel(run_id)
        except NotFound:
            raise HTTPException(404, "no such run") from None
        except Conflict as err:
            raise HTTPException(409, str(err)) from None
        return {"ok": True}

    @app.post("/v1/runs/{run_id}/resume")
    async def resume(run_id: str):
        try:
            await eng().resume(run_id)
        except NotFound:
            raise HTTPException(404, "no such run") from None
        except Conflict as err:
            raise HTTPException(409, str(err)) from None
        return {"ok": True}

    @app.post("/v1/tasks/poll")
    async def poll(body: Poll, request: Request, response: Response):
        notifier: Notifier = state["notifier"]
        deadline = time.monotonic() + min(body.wait_s, 60.0)
        while True:
            # A killed worker leaves its long-poll open here. Claiming for it would strand the
            # task until its lease expires, so check the caller is still there first.
            if await request.is_disconnected():
                return Response(status_code=204)
            wake = notifier.current()
            tasks = await eng().claim(body)
            if tasks:
                return [t.model_dump(mode="json") for t in tasks]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                response.status_code = 204
                return None
            # Fallback re-check every second: covers backoff deadlines and a lost NOTIFY.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wake.wait(), timeout=min(remaining, 1.0))

    @app.post("/v1/tasks/{task_id}/ack/advance")
    async def ack_advance(task_id: str, body: AckAdvance):
        try:
            await eng().ack_advance(task_id, body)
        except (LeaseLost, NotFound) as err:
            raise HTTPException(409, f"lease lost: {err}") from None
        except StaleReplay as err:
            raise HTTPException(409, f"stale replay: {err}") from None
        return {"ok": True}

    @app.post("/v1/tasks/{task_id}/ack/step")
    async def ack_step(task_id: str, body: AckStep):
        try:
            await eng().ack_step(task_id, body)
        except (LeaseLost, NotFound) as err:
            raise HTTPException(409, f"lease lost: {err}") from None
        return {"ok": True}

    @app.post("/v1/tasks/{task_id}/heartbeat")
    async def heartbeat(task_id: str, body: Heartbeat):
        try:
            await eng().heartbeat(task_id, body.lease_token)
        except (LeaseLost, NotFound) as err:
            raise HTTPException(409, f"lease lost: {err}") from None
        return {"ok": True}

    from relay.server.ui import router as ui_router

    app.include_router(ui_router(eng))

    @app.get("/metrics")
    async def prom():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/healthz")
    async def healthz():
        owner = state.get("shards")
        return {"ok": True, "shards": owner.shards() if owner else []}

    app.state.relay = state
    return app


def _timers_once(engine: Engine, owner):
    since = {"t": None}

    async def run() -> bool:
        fired = await engine.fire_timers(owner.shards())
        if since["t"] is None:
            since["t"] = await engine.pool.fetchval("SELECT now()")
        since["t"] = await engine.wake_delayed(since["t"])
        return fired > 0

    return run


def _reap_once(engine: Engine, owner):
    async def run() -> bool:
        await engine.reap(owner.shards())
        return False

    return run


def _archive_once(engine: Engine, owner):
    async def run() -> bool:
        return await engine.archive(owner.shards()) > 0

    return run


def _stats_once(pool: asyncpg.Pool):
    async def run() -> bool:
        async with pool.acquire() as conn:
            for kind in ("ADVANCE", "STEP"):
                metrics.QUEUE_DEPTH.labels(kind).set(0)
                metrics.LEASED.labels(kind).set(0)
            for r in await conn.fetch(
                "SELECT kind, count(*) FILTER (WHERE lease_owner IS NULL AND not_before <= now())"
                " AS ready, count(*) FILTER (WHERE lease_owner IS NOT NULL) AS leased "
                "FROM tasks GROUP BY kind"
            ):
                metrics.QUEUE_DEPTH.labels(r["kind"]).set(r["ready"])
                metrics.LEASED.labels(r["kind"]).set(r["leased"])
            oldest = await conn.fetchval(
                "SELECT extract(epoch FROM now() - min(not_before)) FROM tasks "
                "WHERE lease_owner IS NULL AND not_before <= now()"
            )
            metrics.OLDEST_READY.set(float(oldest or 0))
            t = await conn.fetchrow(
                "SELECT count(*) AS n, extract(epoch FROM now() - min(fire_at)) AS overdue "
                "FROM timers WHERE state='pending'"
            )
            metrics.TIMERS_PENDING.set(t["n"])
            metrics.TIMERS_OVERDUE.set(max(float(t["overdue"] or 0), 0.0))
        return False

    return run
