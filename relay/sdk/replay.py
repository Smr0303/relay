"""The replay driver: run a workflow function against its journal and decide what happens next.

    replay(fn, input, journal) -> Decision(commands)

The workflow coroutine is stepped by hand. Each `await ctx.*` yields an op; the driver numbers
it (call_seq 1, 2, 3...) and looks for that call in the journal:

  * recorded and resolved  -> send the recorded result back in and keep going
  * recorded, not resolved -> suspend (a step is running, a timer or signal is pending)
  * not recorded           -> emit a command and suspend (values are recorded and continue)
  * recorded as something else -> NonDeterminismError: the code changed under a live run

Nothing here performs I/O. The caller (in-process engine now, server ack later) journals the
commands, so the same driver runs unchanged in every phase.
"""

import random as _random
import uuid as _uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from relay.common.protocol import (
    CancelTimer,
    Command,
    CompleteRun,
    CreateTimer,
    Event,
    EventType,
    FailRun,
    RecordValue,
    RetryPolicy,
    ScheduleStep,
    ensure_json,
)
from relay.sdk.context import Op, SleepOp, StepOp, ValueOp, WaitSignalOp, WorkflowContext


class NonDeterminismError(Exception):
    """The journal does not match what the code asks for. The run must be parked, not guessed."""


class StepError(Exception):
    """Raised inside the workflow when a step failed for good; the workflow may catch it."""

    def __init__(self, step: str, error: dict[str, Any]) -> None:
        super().__init__(f"step {step} failed: {error.get('type')}: {error.get('message')}")
        self.step = step
        self.error = error


class ValueSource(Protocol):
    def now(self) -> datetime: ...
    def uuid(self) -> str: ...
    def random(self) -> float: ...


class SystemValues:
    def now(self) -> datetime:
        return datetime.now(UTC)

    def uuid(self) -> str:
        return str(_uuid.uuid4())

    def random(self) -> float:
        return _random.random()


@dataclass
class Decision:
    commands: list[Command] = field(default_factory=list)

    @property
    def finished(self) -> bool:
        return any(isinstance(c, CompleteRun | FailRun) for c in self.commands)


_SUSPEND = object()


@dataclass(frozen=True)
class _Throw:
    exc: BaseException


def error_dict(err: BaseException) -> dict[str, Any]:
    return {"type": type(err).__name__, "message": str(err)}


def replay(
    fn: Callable[..., Any],
    input: Any,
    journal: list[Event],
    *,
    run_id: str,
    values: ValueSource | None = None,
) -> Decision:
    values = values or SystemValues()
    by_call: dict[int, list[Event]] = defaultdict(list)
    signals: list[Event] = []
    for ev in journal:
        if ev.type is EventType.SIGNAL_RECEIVED:
            signals.append(ev)
        elif ev.call_seq is not None:
            by_call[ev.call_seq].append(ev)
    consumed: set[int] = set()

    commands: list[Command] = []
    coro = fn(WorkflowContext(run_id), input)
    value: Any = None
    exc: BaseException | None = None
    call_seq = 0

    while True:
        try:
            op = coro.throw(exc) if exc is not None else coro.send(value)
        except StopIteration as stop:
            if by_call and max(by_call) > call_seq:
                raise NonDeterminismError(
                    f"workflow returned after call {call_seq}, "
                    f"but the journal records calls up to {max(by_call)}"
                ) from None
            commands.append(CompleteRun(result=ensure_json(stop.value, "workflow result")))
            return Decision(commands)
        except Exception as err:  # the workflow itself raised: the run fails with that error
            commands.append(FailRun(error=error_dict(err)))
            return Decision(commands)

        if not isinstance(op, Op):
            coro.close()
            raise TypeError(
                f"workflow awaited {op!r}; workflows may only await ctx.* calls "
                "(put I/O inside a @step)"
            )

        call_seq += 1
        evs = by_call.get(call_seq, [])
        match op:
            case StepOp():
                outcome = _resolve_step(op, call_seq, evs, commands)
            case SleepOp():
                outcome = _resolve_sleep(op, call_seq, evs, commands)
            case WaitSignalOp():
                outcome = _resolve_wait(op, call_seq, evs, commands, signals, consumed)
            case ValueOp():
                outcome = _resolve_value(op, call_seq, evs, commands, values)

        value, exc = None, None
        if outcome is _SUSPEND:
            coro.close()
            return Decision(commands)
        if isinstance(outcome, _Throw):
            exc = outcome.exc
        else:
            value = outcome


def _expect(ev: Event, type_: EventType, call_seq: int, **fields: Any) -> None:
    if ev.type is not type_ or any(ev.payload.get(k) != v for k, v in fields.items()):
        raise NonDeterminismError(
            f"call {call_seq}: journal has {ev.type} {ev.payload}, "
            f"but the code asks for {type_} {fields}"
        )


def _resolve_step(op: StepOp, call_seq: int, evs: list[Event], commands: list[Command]):
    if not evs:
        commands.append(
            ScheduleStep(
                call_seq=call_seq,
                name=op.name,
                args=list(op.args),
                retry=op.retry or RetryPolicy(),
                timeout_s=op.timeout_s,
            )
        )
        return _SUSPEND
    _expect(evs[0], EventType.STEP_SCHEDULED, call_seq, name=op.name)
    for ev in evs[1:]:
        if ev.type is EventType.STEP_COMPLETED:
            return ev.payload.get("result")
        if ev.type is EventType.STEP_FAILED and ev.payload.get("final"):
            return _Throw(StepError(op.name, ev.payload["error"]))
    return _SUSPEND  # scheduled, still running


def _resolve_sleep(op: SleepOp, call_seq: int, evs: list[Event], commands: list[Command]):
    if not evs:
        commands.append(CreateTimer(call_seq=call_seq, fire_in_s=op.seconds))
        return _SUSPEND
    _expect(evs[0], EventType.TIMER_CREATED, call_seq, signal=None)
    return None if any(e.type is EventType.TIMER_FIRED for e in evs) else _SUSPEND


def _resolve_wait(
    op: WaitSignalOp,
    call_seq: int,
    evs: list[Event],
    commands: list[Command],
    signals: list[Event],
    consumed: set[int],
):
    """Resolves to the earliest unconsumed matching signal, or None if the timeout fired first.

    "First" is journal order, so every replay reaches the same answer. A signal that arrived
    before the wait started is buffered and counts. A TimerFired appended after the signal won
    is simply ignored.
    """
    if not evs:
        commands.append(CreateTimer(call_seq=call_seq, fire_in_s=op.timeout_s, signal=op.name))
    else:
        _expect(evs[0], EventType.TIMER_CREATED, call_seq, signal=op.name)

    fired = next((e for e in evs if e.type is EventType.TIMER_FIRED), None)
    sig = next(
        (
            s
            for s in signals
            if s.seq not in consumed
            and s.payload.get("name") == op.name
            and (fired is None or s.seq < fired.seq)
        ),
        None,
    )
    if sig is not None:
        consumed.add(sig.seq)
        timer_open = op.timeout_s is not None and fired is None
        if timer_open and not any(e.type is EventType.TIMER_CANCELLED for e in evs):
            commands.append(CancelTimer(call_seq=call_seq))
        return sig.payload.get("data")
    if fired is not None:
        return None
    return _SUSPEND


def _resolve_value(
    op: ValueOp, call_seq: int, evs: list[Event], commands: list[Command], values: ValueSource
):
    if evs:
        _expect(evs[0], EventType.VALUE_RECORDED, call_seq, kind=op.kind)
        return _decode(op.kind, evs[0].payload["value"])
    match op.kind:
        case "now":
            raw: Any = values.now().isoformat()
        case "uuid":
            raw = values.uuid()
        case "random":
            raw = values.random()
        case _:
            raise ValueError(f"unknown value kind {op.kind}")
    commands.append(RecordValue(call_seq=call_seq, value_kind=op.kind, value=raw))
    return _decode(op.kind, raw)  # recorded values don't suspend: the workflow keeps going


def _decode(kind: str, raw: Any) -> Any:
    return datetime.fromisoformat(raw) if kind == "now" else raw
