"""OpenTelemetry: one trace per run, a span per step attempt and per replay.

The trace id IS the run id (both 128-bit), so a run's trace is found from its id with no
lookup. The run's root span id is derived from it too: workers parent their spans on it
before the root exists, and the server emits the root (start = run created, end = run closed)
when the run finishes. Off unless OTEL_EXPORTER_OTLP_ENDPOINT is set.
"""

import os
import threading
import uuid
from contextlib import contextmanager

_tracer = None
_ids = None


def _root_span_id(run_id: str) -> int:
    return (uuid.UUID(run_id).int & 0xFFFFFFFFFFFFFFFF) or 1


def init(service: str):
    global _tracer, _ids
    if _tracer is not None or not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return _tracer
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.id_generator import RandomIdGenerator

    class Ids(RandomIdGenerator):
        """Random ids, except when the root span of a run is being emitted."""

        def __init__(self) -> None:
            self.forced = threading.local()

        def generate_span_id(self) -> int:
            v = getattr(self.forced, "span", None)
            if v is not None:
                self.forced.span = None
                return v
            return super().generate_span_id()

        def generate_trace_id(self) -> int:
            v = getattr(self.forced, "trace", None)
            if v is not None:
                self.forced.trace = None
                return v
            return super().generate_trace_id()

    _ids = Ids()
    provider = TracerProvider(
        resource=Resource.create({"service.name": service}), id_generator=_ids
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("relay")
    return _tracer


def _run_context(run_id: str):
    from opentelemetry import trace
    from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

    sc = SpanContext(
        trace_id=uuid.UUID(run_id).int,
        span_id=_root_span_id(run_id),
        is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return trace.set_span_in_context(NonRecordingSpan(sc))


@contextmanager
def span(name: str, run_id: str, **attrs):
    """A child span of the run's trace (no-op when tracing is off)."""
    if _tracer is None:
        yield None
        return
    from opentelemetry.trace import Status, StatusCode

    with _tracer.start_as_current_span(
        name, context=_run_context(run_id), record_exception=False
    ) as s:
        for k, v in attrs.items():
            if v is not None:
                s.set_attribute(f"relay.{k}", v)
        try:
            yield s
        except Exception as err:
            s.set_status(Status(StatusCode.ERROR, f"{type(err).__name__}: {err}"))
            raise


def run_root(run_id: str, workflow: str, status: str, start_ns: int, end_ns: int) -> None:
    """Emit the run's root span, with the ids the attempt spans already point at."""
    if _tracer is None:
        return
    from opentelemetry.context import Context
    from opentelemetry.trace import Status, StatusCode

    _ids.forced.trace = uuid.UUID(run_id).int
    _ids.forced.span = _root_span_id(run_id)
    s = _tracer.start_span(f"run {workflow}", context=Context(), start_time=start_ns)
    s.set_attribute("relay.run_id", run_id)
    s.set_attribute("relay.status", status)
    if status in ("failed", "parked"):
        s.set_status(Status(StatusCode.ERROR, status))
    s.end(end_time=end_ns)


def lost_attempt(
    run_id: str, step: str, attempt: int, call_seq: int, start_ns: int, end_ns: int
) -> None:
    """A step attempt whose worker died: its own span died with it, so the server records
    the attempt (claim .. lease expiry) when the reaper takes the lease back."""
    if _tracer is None:
        return
    from opentelemetry.trace import Status, StatusCode

    s = _tracer.start_span(f"step {step}", context=_run_context(run_id), start_time=start_ns)
    s.set_attribute("relay.attempt", attempt)
    s.set_attribute("relay.call_seq", call_seq)
    s.set_attribute("relay.lease_expired", True)
    s.set_status(Status(StatusCode.ERROR, "lease expired: worker presumed dead"))
    s.end(end_time=end_ns)
