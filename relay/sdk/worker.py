"""The worker: long-poll the server for tasks, replay workflows, execute steps, ack.

    python -m relay.sdk.worker --app examples.order --server http://127.0.0.1:8080

Workers hold no state that matters. Kill one at any moment and the server's reaper hands its
leased tasks to another after the lease runs out; the step re-runs with the same idempotency
key. A worker whose lease was taken away gets 409 on ack and drops its result (fencing).
"""

import argparse
import asyncio
import contextlib
import importlib
import inspect
import logging
import os
import signal
import socket
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import aiohttp

from relay.common.protocol import AckAdvance, AckStep, Task, ensure_json
from relay.sdk.context import StepContext
from relay.sdk.replay import NonDeterminismError, error_dict, replay
from relay.sdk.workflow import Registry
from relay.sdk.workflow import registry as default_registry

log = logging.getLogger("relay.worker")


class Worker:
    def __init__(
        self,
        server: str,
        *,
        registry: Registry = default_registry,
        worker_id: str | None = None,
        slots: int = 100,
        pollers: int = 4,
        max_batch: int = 10,
        poll_wait_s: float = 20.0,
    ) -> None:
        self.server = server.rstrip("/")
        self.registry = registry
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.slots = slots
        self.pollers = pollers
        self.max_batch = max_batch
        self.poll_wait_s = poll_wait_s
        self._stop = asyncio.Event()
        self._inflight: set[asyncio.Task] = set()

    def handles(self) -> list[tuple[str, int]]:
        return sorted(self.registry._workflows)

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=self.slots))  # sync steps
        self._sem = asyncio.Semaphore(self.slots)
        # aiohttp, not httpx, on the hot path: httpx's async pool fell to ~1k req/s under
        # concurrency against a server that serves ~24k req/s to aiohttp (bench/README).
        connector = aiohttp.TCPConnector(limit=self.slots + self.pollers + 10)
        timeout = aiohttp.ClientTimeout(total=self.poll_wait_s + 15, connect=5)
        async with aiohttp.ClientSession(
            self.server, connector=connector, timeout=timeout
        ) as self._http:
            pollers = [asyncio.create_task(self._poll_loop()) for _ in range(self.pollers)]
            await self._stop.wait()
            for p in pollers:
                p.cancel()
            await asyncio.gather(*pollers, return_exceptions=True)
            if self._inflight:  # graceful: finish what we hold
                await asyncio.wait(self._inflight, timeout=30)

    async def _poll_loop(self) -> None:
        handles = self.handles()
        while not self._stop.is_set():
            await self._sem.acquire()
            n = 1
            while n < self.max_batch and not self._sem.locked():
                await self._sem.acquire()
                n += 1
            tasks: list[Task] = []
            try:
                async with self._http.post(
                    "/v1/tasks/poll",
                    json={
                        "worker_id": self.worker_id,
                        "handles": handles,
                        "max_tasks": n,
                        "wait_s": self.poll_wait_s,
                    },
                ) as r:
                    if r.status == 200:
                        tasks = [Task.model_validate(t) for t in await r.json()]
                    elif r.status != 204:
                        log.warning("poll returned %s", r.status)
                if not tasks and r.status != 204:
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                for _ in range(n):
                    self._sem.release()
                raise
            except (aiohttp.ClientError, TimeoutError) as err:
                log.warning("poll failed: %s", err)
                await asyncio.sleep(0.5)
            for _ in range(n - len(tasks)):
                self._sem.release()
            for task in tasks:
                t = asyncio.create_task(self._handle(task))
                self._inflight.add(t)
                t.add_done_callback(self._inflight.discard)

    async def _handle(self, task: Task) -> None:
        try:
            if task.kind == "ADVANCE":
                await self._advance(task)
            else:
                await self._step(task)
        except Exception:
            log.exception("task %s failed in the worker", task.task_id)
        finally:
            self._sem.release()

    async def _advance(self, task: Task) -> None:
        body = AckAdvance(lease_token=task.lease_token, journal_len=len(task.journal))
        t0 = time.perf_counter()
        try:
            wf = self.registry.get_workflow(task.workflow, task.version)
            body.commands = replay(wf.fn, task.input, task.journal, run_id=task.run_id).commands
        except NonDeterminismError as err:
            body.replay_error, body.nondeterminism = error_dict(err), True
        except Exception as err:  # a bug in workflow code or a missing registration
            body.replay_error = error_dict(err)
        body.replay_ms = (time.perf_counter() - t0) * 1000
        await self._ack(f"/v1/tasks/{task.task_id}/ack/advance", body, task.timeout_s)

    async def _step(self, task: Task) -> None:
        fn = self.registry.get_step(task.step_name)
        sctx = StepContext(run_id=task.run_id, call_seq=task.call_seq, attempt=task.attempt)
        beat = asyncio.create_task(self._heartbeat(task))
        t0 = time.perf_counter()
        try:
            if inspect.iscoroutinefunction(fn):
                result = await fn(sctx, *task.args)
            else:
                result = await asyncio.to_thread(fn, sctx, *task.args)
            ensure_json(result, f"result of step {task.step_name}")
            body = AckStep(lease_token=task.lease_token, result=result)
        except Exception as err:
            retryable = not isinstance(err, getattr(fn, "__relay_non_retryable__", ())) and getattr(
                err, "retryable", True
            )
            body = AckStep(
                lease_token=task.lease_token, error=error_dict(err), retryable=bool(retryable)
            )
        finally:
            beat.cancel()
        body.duration_ms = (time.perf_counter() - t0) * 1000
        await self._ack(f"/v1/tasks/{task.task_id}/ack/step", body, task.timeout_s)

    async def _heartbeat(self, task: Task) -> None:
        while True:
            await asyncio.sleep(task.timeout_s / 3)
            with contextlib.suppress(aiohttp.ClientError, TimeoutError):
                async with self._http.post(
                    f"/v1/tasks/{task.task_id}/heartbeat", json={"lease_token": task.lease_token}
                ) as r:
                    if r.status == 409:
                        return  # lease gone; the ack will be refused too

    async def _ack(self, path: str, body, timeout_s: float) -> None:
        """Retry transient failures until the lease would have run out anyway."""
        deadline = time.monotonic() + timeout_s
        delay = 0.05
        while True:
            try:
                async with self._http.post(path, json=body.model_dump(mode="json")) as r:
                    if r.status < 300:
                        return
                    if r.status == 409:
                        log.info("ack refused: %s", await r.text())
                        return
                    log.warning("ack %s returned %s", path, r.status)
            except (aiohttp.ClientError, TimeoutError) as err:
                log.warning("ack %s failed: %s", path, err)
            if time.monotonic() + delay > deadline:
                return  # the reaper will re-run it
            await asyncio.sleep(delay)
            delay = min(delay * 2, 1.0)


def main() -> None:
    p = argparse.ArgumentParser(prog="relay.sdk.worker")
    p.add_argument(
        "--app",
        action="append",
        required=True,
        help="module(s) that register @workflow / @step code",
    )
    p.add_argument("--server", default=os.environ.get("RELAY_SERVER", "http://127.0.0.1:8080"))
    p.add_argument("--slots", type=int, default=100)
    p.add_argument("--pollers", type=int, default=4)
    p.add_argument("--id", default=None)
    p.add_argument("--log-level", default="warning")
    args = p.parse_args()
    logging.basicConfig(
        level=args.log_level.upper(), format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    for mod in args.app:
        importlib.import_module(mod)
    worker = Worker(args.server, worker_id=args.id, slots=args.slots, pollers=args.pollers)

    async def runner():
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, worker.stop)
        await worker.run()

    asyncio.run(runner())


if __name__ == "__main__":
    main()
