"""Phase 1 gate: crash the engine at EVERY durable write and effect of the order workflow,
resume on a fresh engine, and require the same result with every effect happening once."""

import pytest

from examples.fake_stripe import OUTBOX, STRIPE
from examples.order import order
from relay.sdk.local import TERMINAL, Crash, LocalEngine, Store

INPUT = {"order_id": 123, "amount": 5000}


def drive(engine: LocalEngine, *, ship: bool):
    """The outside world: start the order, send "shipped" (or not), let time pass."""
    run_id = engine.start(order, INPUT, request_id="ord-123")
    while True:
        rec = engine.run(run_id)
        if rec.status in TERMINAL:
            return rec
        if ship and not engine.has_signal(run_id, "shipped"):
            engine.clock.advance(3600)
            engine.signal(run_id, "shipped", {"tracking": "1Z999"})
            continue
        due = engine.next_timer_at()
        assert due is not None, "run is blocked with nothing that could wake it"
        engine.clock.t = max(engine.clock.t, due)
        engine.fire_due_timers()


def run_with_crash(crash_at: int | None, *, ship: bool):
    STRIPE.reset()
    OUTBOX.reset()
    store = Store()
    try:
        rec = drive(LocalEngine(store, crash_at=crash_at), ship=ship)
    except Crash:
        rec = drive(LocalEngine(store), ship=ship)  # a new process picks up from the journal
    return rec, store


@pytest.mark.parametrize("ship", [True, False], ids=["shipped", "timed_out"])
def test_crash_at_every_point_resumes_to_the_same_result(ship):
    baseline_engine = LocalEngine(Store())
    STRIPE.reset()
    OUTBOX.reset()
    baseline = drive(baseline_engine, ship=ship)
    total_ticks = baseline_engine.ticks
    assert total_ticks > 20  # sanity: the scenario actually has many crash points

    duplicate_requests_seen = 0
    for k in range(1, total_ticks + 1):
        rec, store = run_with_crash(k, ship=ship)
        assert rec.status == "completed", f"crash at tick {k}"
        assert rec.result == baseline.result, f"crash at tick {k}"
        assert len(store.runs) == 1, f"crash at tick {k}: start was not idempotent"
        assert len(STRIPE.charges) == 1, f"crash at tick {k}: charged {len(STRIPE.charges)}x"
        assert len(STRIPE.refunds) == (0 if ship else 1), f"crash at tick {k}"
        assert len(OUTBOX.sent) == (2 if ship else 0), f"crash at tick {k}"
        duplicate_requests_seen += STRIPE.calls - len(STRIPE.charges) - len(STRIPE.refunds)

    # Some crash points land after the charge but before its ack: the step re-ran with the
    # same idempotency key and Stripe returned the original charge. That is the case the
    # design exists for, so make sure the test actually exercised it.
    assert duplicate_requests_seen > 0


def test_shipped_path_result():
    rec, _ = run_with_crash(None, ship=True)
    assert rec.result == {"status": "done", "charge_id": "ch_1", "tracking": "1Z999"}


def test_timed_out_path_refunds():
    rec, _ = run_with_crash(None, ship=False)
    assert rec.result == {"status": "refunded", "charge_id": "ch_1"}
