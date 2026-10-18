"""In-process engine: the whole loop in one process, with fault injection. Phase 1 only.

`Store` stands in for Postgres. It is what survives a crash. `LocalEngine` stands in for the
server and a worker. It counts every durable write and every external effect as a "tick", and
`crash_at=k` raises `Crash` at tick k. A fresh engine built on the same store then resumes,
exactly as a new worker would after a lease expires.

Tick boundaries mirror the real crash points (HLD §7):
  advance:  replay -> [tick: crash before commit] -> commit commands -> [tick: after commit]
  step:     [tick: crash before effect] -> effect -> [tick: after effect, before ack]
            -> commit StepCompleted -> [tick: after commit]
"""

import asyncio
import inspect
import uuid as _uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from relay.common.protocol import (
    CancelTimer,
    CompleteRun,
    CreateTimer,
    Event,
    EventType,
    FailRun,
    ensure_json,
    events_for,
)
from relay.sdk.context import StepContext
from relay.sdk.replay import Decision, error_dict, replay
from relay.sdk.workflow import Registry
from relay.sdk.workflow import registry as default_registry

TERMINAL = {"completed", "failed"}
_EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


class Crash(Exception):
    """Simulated process death."""


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def advance(self, seconds: float) -> None:
        self.t += seconds

    def now(self) -> datetime:
        return _EPOCH + timedelta(seconds=self.t)


@dataclass
class RunRecord:
    run_id: str
    workflow: str
    version: int
    input: Any
    status: str = "running"
    result: Any = None
    error: dict[str, Any] | None = None


@dataclass
class Store:
    runs: dict[str, RunRecord] = field(default_factory=dict)
    journals: dict[str, list[Event]] = field(default_factory=dict)
    timers: dict[tuple[str, int], float] = field(default_factory=dict)  # (run, call_seq) -> fire_at
    request_ids: dict[str, str] = field(default_factory=dict)
    clock: FakeClock = field(default_factory=FakeClock)
    step_attempts: dict[tuple[str, int], int] = field(default_factory=dict)


class _ClockValues:
    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock

    def now(self) -> datetime:
        return self._clock.now()

    def uuid(self) -> str:
        return str(_uuid.uuid4())

    def random(self) -> float:
        import random

        return random.random()


class LocalEngine:
    def __init__(
        self,
        store: Store | None = None,
        *,
        registry: Registry = default_registry,
        crash_at: int | None = None,
    ) -> None:
        self.store = store or Store()
        self.registry = registry
        self.crash_at = crash_at
        self.ticks = 0
        self._values = _ClockValues(self.store.clock)

    @property
    def clock(self) -> FakeClock:
        return self.store.clock

    # ---- client API ------------------------------------------------------------------------

    def start(self, workflow, input: Any, *, request_id: str | None = None) -> str:
        if request_id is not None and request_id in self.store.request_ids:
            return self.store.request_ids[request_id]  # idempotent start
        name = workflow if isinstance(workflow, str) else workflow.__relay_workflow__.name
        wf = self.registry.get_workflow(name)
        run_id = str(_uuid.uuid4())
        self.store.runs[run_id] = RunRecord(
            run_id, wf.name, wf.version, ensure_json(input, "input")
        )
        self.store.journals[run_id] = []
        self._append(run_id, EventType.RUN_STARTED, None, {"input": input})
        if request_id is not None:
            self.store.request_ids[request_id] = run_id
        self._tick()
        return run_id

    def signal(self, run_id: str, name: str, data: Any = None) -> None:
        payload = {"name": name, "data": ensure_json(data, "signal data")}
        self._append(run_id, EventType.SIGNAL_RECEIVED, None, payload)
        self._tick()

    def journal(self, run_id: str) -> list[Event]:
        return self.store.journals[run_id]

    def has_signal(self, run_id: str, name: str) -> bool:
        return any(
            e.type is EventType.SIGNAL_RECEIVED and e.payload["name"] == name
            for e in self.journal(run_id)
        )

    # ---- the loop --------------------------------------------------------------------------

    def run(self, run_id: str) -> RunRecord:
        """Advance and execute steps until the run finishes or blocks on a timer or signal."""
        while True:
            rec = self.store.runs[run_id]
            if rec.status in TERMINAL:
                return rec
            pending = self._pending_steps(run_id)
            if pending:
                for call_seq, scheduled in pending:
                    self._execute_step(run_id, call_seq, scheduled)
                continue
            if not self._advance(run_id).commands:
                rec.status = "suspended"
                return rec

    def next_timer_at(self) -> float | None:
        return min(self.store.timers.values(), default=None)

    def fire_due_timers(self) -> int:
        due = sorted((at, key) for key, at in self.store.timers.items() if at <= self.store.clock.t)
        for _, (run_id, call_seq) in due:
            del self.store.timers[(run_id, call_seq)]
            self._append(run_id, EventType.TIMER_FIRED, call_seq, {})
            self._tick()
        return len(due)

    # ---- internals -------------------------------------------------------------------------

    def _tick(self) -> None:
        self.ticks += 1
        if self.crash_at is not None and self.ticks == self.crash_at:
            raise Crash(f"crash at tick {self.ticks}")

    def _append(self, run_id: str, type_: EventType, call_seq: int | None, payload: dict) -> None:
        journal = self.store.journals[run_id]
        journal.append(Event(seq=len(journal) + 1, type=type_, call_seq=call_seq, payload=payload))

    def _advance(self, run_id: str) -> Decision:
        rec = self.store.runs[run_id]
        wf = self.registry.get_workflow(rec.workflow, rec.version)
        decision = replay(
            wf.fn, rec.input, self.journal(run_id), run_id=run_id, values=self._values
        )
        if not decision.commands:
            return decision
        self._tick()  # crash here = the decision is lost; the next replay makes it again
        for cmd in decision.commands:  # one "transaction"
            for type_, call_seq, payload in events_for(cmd):
                self._append(run_id, type_, call_seq, payload)
            match cmd:
                case CreateTimer(fire_in_s=float() as fire_in):
                    self.store.timers[(run_id, cmd.call_seq)] = self.store.clock.t + fire_in
                case CancelTimer():
                    self.store.timers.pop((run_id, cmd.call_seq), None)
                case CompleteRun():
                    rec.status, rec.result = "completed", cmd.result
                case FailRun():
                    rec.status, rec.error = "failed", cmd.error
        if rec.status not in TERMINAL:
            rec.status = "running"
        self._tick()
        return decision

    def _pending_steps(self, run_id: str) -> list[tuple[int, Event]]:
        scheduled: dict[int, Event] = {}
        for ev in self.journal(run_id):
            if ev.type is EventType.STEP_SCHEDULED:
                scheduled[ev.call_seq] = ev
            elif ev.type is EventType.STEP_COMPLETED or (
                ev.type is EventType.STEP_FAILED and ev.payload.get("final")
            ):
                scheduled.pop(ev.call_seq, None)
        return sorted(scheduled.items())

    def _execute_step(self, run_id: str, call_seq: int, scheduled: Event) -> None:
        key = (run_id, call_seq)
        attempt = self.store.step_attempts.get(key, 0) + 1
        self.store.step_attempts[key] = attempt
        fn = self.registry.get_step(scheduled.payload["name"])
        sctx = StepContext(run_id=run_id, call_seq=call_seq, attempt=attempt)

        self._tick()  # crash before the effect
        try:
            result = fn(sctx, *scheduled.payload["args"])
            if inspect.isawaitable(result):
                result = asyncio.run(_await(result))
            ensure_json(result, f"result of step {scheduled.payload['name']}")
        except Crash:
            raise
        except Exception as err:  # Phase 1: no retries yet, a failure is final
            self._tick()
            self._append(
                run_id,
                EventType.STEP_FAILED,
                call_seq,
                {"error": error_dict(err), "attempt": attempt, "final": True},
            )
            self._tick()
            return
        self._tick()  # crash after the effect, before the ack: the dangerous one
        self._append(
            run_id, EventType.STEP_COMPLETED, call_seq, {"result": result, "attempt": attempt}
        )
        self._tick()


async def _await(awaitable):
    return await awaitable
