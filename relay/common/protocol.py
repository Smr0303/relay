"""The wire contract: journal events, and the commands a replay emits.

Everything here is JSON-only, so the same shapes travel over HTTP once the server exists
(Phase 2). `events_for` is the single mapping from a command to the journal rows it writes;
the in-process engine and the server both use it.
"""

import json
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field


class EventType(StrEnum):
    RUN_STARTED = "RunStarted"
    STEP_SCHEDULED = "StepScheduled"
    STEP_COMPLETED = "StepCompleted"
    STEP_FAILED = "StepFailed"
    TIMER_CREATED = "TimerCreated"
    TIMER_FIRED = "TimerFired"
    TIMER_CANCELLED = "TimerCancelled"
    SIGNAL_RECEIVED = "SignalReceived"
    VALUE_RECORDED = "ValueRecorded"
    RUN_COMPLETED = "RunCompleted"
    RUN_FAILED = "RunFailed"


class Event(BaseModel):
    """One journal row. `seq` is 1, 2, 3... per run; `call_seq` ties it to the Nth ctx call."""

    seq: int
    type: EventType
    call_seq: int | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class ScheduleStep(BaseModel):
    kind: Literal["schedule_step"] = "schedule_step"
    call_seq: int
    name: str
    args: list[Any]


class CreateTimer(BaseModel):
    """A sleep (signal=None) or the timeout of a wait_signal. fire_in_s=None means no timeout."""

    kind: Literal["create_timer"] = "create_timer"
    call_seq: int
    fire_in_s: float | None
    signal: str | None = None


class CancelTimer(BaseModel):
    kind: Literal["cancel_timer"] = "cancel_timer"
    call_seq: int


class RecordValue(BaseModel):
    """A journaled non-deterministic value: ctx.now(), ctx.uuid(), ctx.random()."""

    kind: Literal["record_value"] = "record_value"
    call_seq: int
    value_kind: str
    value: Any


class CompleteRun(BaseModel):
    kind: Literal["complete_run"] = "complete_run"
    result: Any


class FailRun(BaseModel):
    kind: Literal["fail_run"] = "fail_run"
    error: dict[str, Any]


Command = Annotated[
    ScheduleStep | CreateTimer | CancelTimer | RecordValue | CompleteRun | FailRun,
    Field(discriminator="kind"),
]


def events_for(cmd: Command) -> list[tuple[EventType, int | None, dict[str, Any]]]:
    """The journal rows a command writes: (type, call_seq, payload)."""
    match cmd:
        case ScheduleStep():
            return [(EventType.STEP_SCHEDULED, cmd.call_seq, {"name": cmd.name, "args": cmd.args})]
        case CreateTimer():
            payload = {"fire_in_s": cmd.fire_in_s, "signal": cmd.signal}
            return [(EventType.TIMER_CREATED, cmd.call_seq, payload)]
        case CancelTimer():
            return [(EventType.TIMER_CANCELLED, cmd.call_seq, {})]
        case RecordValue():
            payload = {"kind": cmd.value_kind, "value": cmd.value}
            return [(EventType.VALUE_RECORDED, cmd.call_seq, payload)]
        case CompleteRun():
            return [(EventType.RUN_COMPLETED, None, {"result": cmd.result})]
        case FailRun():
            return [(EventType.RUN_FAILED, None, {"error": cmd.error})]
    raise TypeError(f"unknown command {cmd!r}")


def ensure_json(value: Any, what: str) -> Any:
    """Journal payloads must survive a JSON round trip; fail loudly at the call site if not."""
    try:
        json.dumps(value)
    except (TypeError, ValueError) as err:
        raise TypeError(f"{what} must be JSON-serializable: {err}") from None
    return value
