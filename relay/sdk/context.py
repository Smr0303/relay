"""What workflow and step code sees: `ctx` for workflows, `StepContext` for steps.

Every `ctx.*` method returns an awaitable that yields one op to the replay driver and receives
the result back. Workflow code therefore never touches the outside world itself: the driver
decides whether an op is answered from the journal or becomes a new command.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from relay.common.protocol import RetryPolicy, ensure_json


@dataclass(frozen=True)
class StepOp:
    name: str
    args: tuple[Any, ...]
    retry: RetryPolicy | None = None
    timeout_s: float = 30.0


@dataclass(frozen=True)
class SleepOp:
    seconds: float


@dataclass(frozen=True)
class WaitSignalOp:
    name: str
    timeout_s: float | None


@dataclass(frozen=True)
class ValueOp:
    kind: str  # "now" | "uuid" | "random"


Op = StepOp | SleepOp | WaitSignalOp | ValueOp


class _Call:
    __slots__ = ("op",)

    def __init__(self, op: Op) -> None:
        self.op = op

    def __await__(self):
        result = yield self.op
        return result


def _seconds(d: float | timedelta) -> float:
    return d.total_seconds() if isinstance(d, timedelta) else float(d)


class WorkflowContext:
    """Passed as the first argument to every @workflow function."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id

    def step(self, fn, *args: Any) -> _Call:
        name = getattr(fn, "__relay_step__", None)
        if name is None:
            raise TypeError(f"{fn!r} is not a @step")
        ensure_json(list(args), f"arguments to step {name}")
        return _Call(StepOp(name, args, fn.__relay_retry__, fn.__relay_timeout_s__))

    def sleep(self, duration: float | timedelta) -> _Call:
        return _Call(SleepOp(_seconds(duration)))

    def wait_signal(self, name: str, timeout: float | timedelta | None = None) -> _Call:
        """Returns the signal's data, or None if the timeout fired first."""
        return _Call(WaitSignalOp(name, None if timeout is None else _seconds(timeout)))

    def now(self) -> _Call:
        return _Call(ValueOp("now"))

    def uuid(self) -> _Call:
        return _Call(ValueOp("uuid"))

    def random(self) -> _Call:
        return _Call(ValueOp("random"))


@dataclass(frozen=True)
class StepContext:
    """Passed as the first argument to every @step function."""

    run_id: str
    call_seq: int
    attempt: int

    @property
    def idempotency_key(self) -> str:
        # Derived from the call's position, never the attempt: every retry of this step sends
        # the same key, so the downstream system can return the original result.
        return f"{self.run_id}:{self.call_seq}"
