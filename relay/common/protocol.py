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


class RetryPolicy(BaseModel):
    max_attempts: int = 5
    initial_s: float = 1.0
    multiplier: float = 2.0
    max_s: float = 300.0
    jitter: float = 0.2

    def backoff_s(self, failed_attempt: int, rand: float = 0.5) -> float:
        """Delay before the attempt after `failed_attempt` (1-based). rand in [0, 1)."""
        base = min(self.initial_s * self.multiplier ** (failed_attempt - 1), self.max_s)
        return base * (1 + self.jitter * (2 * rand - 1))


class ScheduleStep(BaseModel):
    kind: Literal["schedule_step"] = "schedule_step"
    call_seq: int
    name: str
    args: list[Any]
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    timeout_s: float = 30.0  # lease length; heartbeats extend it


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
            payload = {"name": cmd.name, "args": cmd.args}
            return [(EventType.STEP_SCHEDULED, cmd.call_seq, payload)]
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


# ---- HTTP wire models (server <-> worker, server <-> client) --------------------------------


class StartRun(BaseModel):
    workflow: str
    version: int = 1
    input: Any = None
    request_id: str | None = None


class SignalIn(BaseModel):
    name: str
    data: Any = None


class Poll(BaseModel):
    worker_id: str
    handles: list[tuple[str, int]]  # (workflow, version) pairs this worker can run
    max_tasks: int = 1
    wait_s: float = 20.0


class Task(BaseModel):
    task_id: str
    lease_token: str
    kind: Literal["ADVANCE", "STEP"]
    run_id: str
    workflow: str
    version: int
    attempt: int
    timeout_s: float
    # ADVANCE
    input: Any = None
    journal: list[Event] = Field(default_factory=list)
    # STEP
    call_seq: int | None = None
    step_name: str | None = None
    args: list[Any] = Field(default_factory=list)


class AckAdvance(BaseModel):
    lease_token: str
    journal_len: int
    commands: list[Command] = Field(default_factory=list)
    replay_error: dict[str, Any] | None = None  # set when replay itself blew up
    nondeterminism: bool = False
    replay_ms: float | None = None


class AckStep(BaseModel):
    lease_token: str
    result: Any = None
    error: dict[str, Any] | None = None
    retryable: bool = True
    duration_ms: float | None = None


class Heartbeat(BaseModel):
    lease_token: str
