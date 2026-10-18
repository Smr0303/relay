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
    ContinueAsNew,
    CreateTimer,
    Event,
    EventType,
    FailRun,
    RecordMarker,
    RecordValue,
    RetryPolicy,
    ScheduleStep,
    ensure_json,
)
from relay.sdk.context import (
    ContinueAsNewOp,
    Op,
    PatchOp,
    SleepOp,
    StepOp,
    ValueOp,
    WaitSignalOp,
    WorkflowContext,
)


class NonDeterminismError(Exception):
    """The journal does not match what the code asks for. The run must be parked, not guessed."""


class Cancelled(Exception):
    """Raised inside the workflow at its next ctx call after the run was cancelled. Catch it
    to run compensation steps, then re-raise (or let it propagate) to end the run cancelled."""


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
    cancel_seq: int | None = None
    for ev in journal:
        if ev.type is EventType.SIGNAL_RECEIVED:
            signals.append(ev)
        elif ev.type is EventType.CANCEL_REQUESTED:
            cancel_seq = cancel_seq or ev.seq
        elif ev.call_seq is not None:
            by_call[ev.call_seq].append(ev)
    consumed: set[int] = set()
    cancel_delivered = False

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
        value, exc = None, None

        if not isinstance(op, Op):
            coro.close()
            raise TypeError(
                f"workflow awaited {op!r}; workflows may only await ctx.* calls "
                "(put I/O inside a @step)"
            )

        if isinstance(op, PatchOp):  # may or may not take a call number: see _resolve_patch
            value, call_seq = _resolve_patch(op, call_seq, by_call, commands)
            continue
        if isinstance(op, ContinueAsNewOp):
            commands.append(ContinueAsNew(input=op.input))
            coro.close()
            return Decision(commands)

        call_seq += 1
        evs = by_call.get(call_seq, [])
        mark = len(commands)
        consume = None
        match op:
            case StepOp():
                outcome, resolved_at = _resolve_step(op, call_seq, evs, commands)
            case SleepOp():
                outcome, resolved_at = _resolve_sleep(op, call_seq, evs, commands)
            case WaitSignalOp():
                outcome, resolved_at, consume = _resolve_wait(
                    op, call_seq, evs, commands, signals, consumed
                )
            case ValueOp():
                outcome, resolved_at = _resolve_value(op, call_seq, evs, commands, values)

        # Cancellation lands at the first call that was not already resolved before the
        # CancelRequested event. That point is fixed by journal order, so every replay agrees.
        if (
            cancel_seq is not None
            and not cancel_delivered
            and (resolved_at is None or resolved_at > cancel_seq)
        ):
            del commands[mark:]  # whatever this call was about to start, it won't
            if _timer_open(evs):
                commands.append(CancelTimer(call_seq=call_seq))
            cancel_delivered = True
            exc = Cancelled("run cancelled")
            continue

        if consume is not None:
            consumed.add(consume)
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


def _timer_open(evs: list[Event]) -> bool:
    created = next((e for e in evs if e.type is EventType.TIMER_CREATED), None)
    if created is None or created.payload.get("fire_in_s") is None:
        return False
    closed = (EventType.TIMER_FIRED, EventType.TIMER_CANCELLED)
    return not any(e.type in closed for e in evs)


def _resolve_patch(op: PatchOp, call_seq: int, by_call, commands: list[Command]):
    """Returns (taken, new call_seq).

    The next call position tells which code this run executed there. A marker for this patch
    means the new path. Any other recorded call means the run passed this point on the old
    code: answer False and do NOT take a call number, so the old calls still line up. Nothing
    recorded yet means the run is reaching this point now: take the new path and record it.
    """
    nxt = by_call.get(call_seq + 1)
    if nxt:
        first = nxt[0]
        if first.type is EventType.MARKER_RECORDED and first.payload.get("patch_id") == op.patch_id:
            return True, call_seq + 1
        return False, call_seq
    commands.append(RecordMarker(call_seq=call_seq + 1, patch_id=op.patch_id))
    return True, call_seq + 1


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
        return _SUSPEND, None
    _expect(evs[0], EventType.STEP_SCHEDULED, call_seq, name=op.name)
    for ev in evs[1:]:
        if ev.type is EventType.STEP_COMPLETED:
            return ev.payload.get("result"), ev.seq
        if ev.type is EventType.STEP_FAILED and ev.payload.get("final"):
            return _Throw(StepError(op.name, ev.payload["error"])), ev.seq
    return _SUSPEND, None  # scheduled, still running


def _resolve_sleep(op: SleepOp, call_seq: int, evs: list[Event], commands: list[Command]):
    if not evs:
        commands.append(CreateTimer(call_seq=call_seq, fire_in_s=op.seconds))
        return _SUSPEND, None
    _expect(evs[0], EventType.TIMER_CREATED, call_seq, signal=None)
    fired = next((e for e in evs if e.type is EventType.TIMER_FIRED), None)
    return (None, fired.seq) if fired else (_SUSPEND, None)


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
    is simply ignored. Returns (outcome, resolving seq, signal seq to mark consumed).
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
        timer_open = op.timeout_s is not None and fired is None
        if timer_open and not any(e.type is EventType.TIMER_CANCELLED for e in evs):
            commands.append(CancelTimer(call_seq=call_seq))
        # A signal that arrived before the wait started resolves it "now": the wait's own
        # TimerCreated is the later event.
        resolved_at = max(sig.seq, evs[0].seq) if evs else None
        return sig.payload.get("data"), resolved_at, sig.seq
    if fired is not None:
        return None, fired.seq, None
    return _SUSPEND, None, None


def _resolve_value(
    op: ValueOp, call_seq: int, evs: list[Event], commands: list[Command], values: ValueSource
):
    if evs:
        _expect(evs[0], EventType.VALUE_RECORDED, call_seq, kind=op.kind)
        return _decode(op.kind, evs[0].payload["value"]), evs[0].seq
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
    return _decode(op.kind, raw), None  # recorded values don't suspend: the workflow continues


def _decode(kind: str, raw: Any) -> Any:
    return datetime.fromisoformat(raw) if kind == "now" else raw
