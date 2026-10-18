"""Replay-driver behaviour, independent of any engine."""

import pytest

from relay import NonDeterminismError, StepError
from relay.common.protocol import CancelTimer, CompleteRun, CreateTimer, Event, ScheduleStep
from relay.sdk.local import LocalEngine
from relay.sdk.replay import replay
from relay.sdk.workflow import Registry

reg = Registry()


@reg.step
def add(sctx, a, b):
    return a + b


@reg.step
def boom(sctx):
    raise ValueError("nope")


@reg.workflow
async def two_steps(ctx, inp):
    x = await ctx.step(add, inp, 1)
    y = await ctx.step(add, x, 10)
    return y


def test_empty_journal_schedules_the_first_step():
    d = replay(two_steps, 1, [], run_id="r")
    assert d.commands == [ScheduleStep(call_seq=1, name="add", args=[1, 1])]


def test_a_full_journal_completes_without_scheduling_anything():
    eng = LocalEngine(registry=reg)
    rid = eng.start(two_steps, 1)
    assert eng.run(rid).result == 12
    d = replay(two_steps, 1, eng.journal(rid), run_id=rid)
    assert d.commands == [CompleteRun(result=12)]


def test_reordered_code_raises_nondeterminism():
    eng = LocalEngine(registry=reg)
    rid = eng.start(two_steps, 1)
    eng.run(rid)

    async def edited(ctx, inp):  # a new step inserted before the old ones
        await ctx.step(boom)
        return await ctx.step(add, inp, 1)

    with pytest.raises(NonDeterminismError, match="call 1"):
        replay(edited, 1, eng.journal(rid), run_id=rid)


def test_code_that_stops_early_raises_nondeterminism():
    eng = LocalEngine(registry=reg)
    rid = eng.start(two_steps, 1)
    eng.run(rid)

    async def shorter(ctx, inp):
        return await ctx.step(add, inp, 1)

    with pytest.raises(NonDeterminismError, match="returned after call 1"):
        replay(shorter, 1, eng.journal(rid), run_id=rid)


def test_failed_step_is_raised_inside_the_workflow_and_can_be_caught():
    @reg.workflow
    async def catches(ctx, inp):
        try:
            await ctx.step(boom)
        except StepError as err:
            return {"caught": err.error["type"]}

    eng = LocalEngine(registry=reg)
    rec = eng.run(eng.start(catches, None))
    assert rec.status == "completed"
    assert rec.result == {"caught": "ValueError"}


def test_uncaught_step_failure_fails_the_run():
    @reg.workflow
    async def lets_it_fail(ctx, inp):
        await ctx.step(boom)

    eng = LocalEngine(registry=reg)
    rec = eng.run(eng.start(lets_it_fail, None))
    assert rec.status == "failed"
    assert rec.error["type"] == "StepError"


def test_values_are_recorded_once_and_replayed():
    @reg.workflow
    async def uses_values(ctx, inp):
        a = await ctx.uuid()
        when = await ctx.now()
        await ctx.sleep(10)
        return {"id": a, "when": when.isoformat(), "again": await ctx.uuid()}

    eng = LocalEngine(registry=reg)
    rid = eng.start(uses_values, None)
    eng.run(rid)
    first_id = [e for e in eng.journal(rid) if e.type == "ValueRecorded"][0].payload["value"]
    eng.clock.advance(10)
    eng.fire_due_timers()
    rec = eng.run(rid)
    assert rec.result["id"] == first_id  # not a fresh uuid on replay
    assert rec.result["again"] != first_id


def test_awaiting_non_ctx_things_is_rejected():
    import asyncio

    async def sneaky(ctx, inp):
        await asyncio.sleep(0)

    with pytest.raises(TypeError, match="may only await ctx"):
        replay(sneaky, None, [], run_id="r")


def test_step_args_must_be_json():
    @reg.workflow
    async def bad_args(ctx, inp):
        await ctx.step(add, object(), 1)

    eng = LocalEngine(registry=reg)
    rec = eng.run(eng.start(bad_args, None))
    assert rec.status == "failed"
    assert "JSON-serializable" in rec.error["message"]


# ---- signals ------------------------------------------------------------------------------


@reg.workflow
async def waits(ctx, inp):
    got = await ctx.wait_signal("go", timeout=60)
    return {"got": got}


def _sig(seq, name, data):
    return Event(seq=seq, type="SignalReceived", payload={"name": name, "data": data})


def test_signal_resolves_the_wait_and_cancels_its_timeout():
    journal = [
        Event(seq=1, type="RunStarted"),
        Event(seq=2, type="TimerCreated", call_seq=1, payload={"fire_in_s": 60, "signal": "go"}),
        _sig(3, "go", {"n": 1}),
    ]
    d = replay(waits, None, journal, run_id="r")
    assert d.commands == [CancelTimer(call_seq=1), CompleteRun(result={"got": {"n": 1}})]


def test_signal_sent_before_the_wait_is_buffered():
    journal = [Event(seq=1, type="RunStarted"), _sig(2, "go", "early")]
    d = replay(waits, None, journal, run_id="r")
    assert d.commands == [
        CreateTimer(call_seq=1, fire_in_s=60, signal="go"),
        CancelTimer(call_seq=1),
        CompleteRun(result={"got": "early"}),
    ]


def test_timeout_that_fired_first_wins_over_a_later_signal():
    journal = [
        Event(seq=1, type="RunStarted"),
        Event(seq=2, type="TimerCreated", call_seq=1, payload={"fire_in_s": 60, "signal": "go"}),
        Event(seq=3, type="TimerFired", call_seq=1),
        _sig(4, "go", "late"),
    ]
    assert replay(waits, None, journal, run_id="r").commands == [CompleteRun(result={"got": None})]


def test_timer_fired_after_the_signal_is_ignored():
    journal = [
        Event(seq=1, type="RunStarted"),
        Event(seq=2, type="TimerCreated", call_seq=1, payload={"fire_in_s": 60, "signal": "go"}),
        _sig(3, "go", "first"),
        Event(seq=4, type="TimerFired", call_seq=1),
    ]
    assert replay(waits, None, journal, run_id="r").commands == [
        CompleteRun(result={"got": "first"})
    ]
