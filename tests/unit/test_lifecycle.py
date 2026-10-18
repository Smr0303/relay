"""Phase 5 replay semantics: patch gates, cancellation, continue_as_new."""

from examples.fake_stripe import STRIPE
from examples.order import order
from relay import Cancelled, NonDeterminismError
from relay.sdk.local import LocalEngine
from relay.sdk.replay import replay
from relay.sdk.workflow import Registry

reg = Registry()


@reg.step
def a(sctx):
    return "a"


@reg.step
def a2(sctx):
    return "a2"


@reg.step
def b(sctx):
    return "b"


async def old_code(ctx, inp):
    out = [await ctx.step(a)]
    await ctx.sleep(10)
    out.append(await ctx.step(b))
    return out


async def new_code(ctx, inp):
    out = [await ctx.step(a)]
    if await ctx.patched("add-a2"):
        out.append(await ctx.step(a2))
    await ctx.sleep(10)
    out.append(await ctx.step(b))
    return out


async def new_code_unpatched(ctx, inp):  # the same change made without a patch gate
    out = [await ctx.step(a)]
    out.append(await ctx.step(a2))
    await ctx.sleep(10)
    out.append(await ctx.step(b))
    return out


def _sleeping_old_run():
    reg_old = Registry()
    reg_old._steps = reg._steps
    reg_old.workflow(name="flow")(old_code)
    eng = LocalEngine(registry=reg_old)
    rid = eng.start("flow", None)
    eng.run(rid)  # step a done, sleeping
    return eng, rid


def test_patch_gate_keeps_an_old_run_on_the_old_path():
    eng, rid = _sleeping_old_run()
    eng.registry._workflows[("flow", 1)] = eng.registry._workflows[("flow", 1)].__class__(
        "flow", 1, new_code
    )  # deploy the new code under the same version
    eng.clock.advance(10)
    eng.fire_due_timers()
    rec = eng.run(rid)
    assert rec.status == "completed" and rec.result == ["a", "b"]  # a2 skipped: passed already


def test_patch_gate_takes_the_new_path_for_a_new_run():
    r = Registry()
    r._steps = reg._steps
    r.workflow(name="flow")(new_code)
    eng = LocalEngine(registry=r)
    rid = eng.start("flow", None)
    eng.run(rid)
    eng.clock.advance(10)
    eng.fire_due_timers()
    assert eng.run(rid).result == ["a", "a2", "b"]
    assert any(e.type == "MarkerRecorded" for e in eng.journal(rid))


def test_the_same_change_without_a_patch_gate_is_caught():
    eng, rid = _sleeping_old_run()
    try:
        replay(new_code_unpatched, None, eng.journal(rid), run_id=rid)
    except NonDeterminismError:
        return
    raise AssertionError("expected NonDeterminismError")


def test_cancel_while_waiting_refunds_and_ends_cancelled():
    STRIPE.reset()
    eng = LocalEngine()
    rid = eng.start(order, {"order_id": 1, "amount": 5000})
    assert eng.run(rid).status == "suspended"  # charged, waiting for "shipped"
    eng.cancel(rid)
    rec = eng.run(rid)
    assert rec.status == "cancelled"
    assert len(STRIPE.charges) == 1 and len(STRIPE.refunds) == 1  # compensation ran
    assert not eng.store.timers  # the 3-day timeout was cancelled with it


def test_cancel_before_anything_ran_needs_no_refund():
    STRIPE.reset()
    eng = LocalEngine()
    rid = eng.start(order, {"order_id": 2, "amount": 5000})
    eng.cancel(rid)
    assert eng.run(rid).status == "cancelled"
    assert not STRIPE.charges and not STRIPE.refunds


def test_cancel_delivery_point_is_stable_across_replays():
    STRIPE.reset()
    eng = LocalEngine()
    rid = eng.start(order, {"order_id": 3, "amount": 5000})
    eng.run(rid)
    eng.cancel(rid)
    eng.run(rid)
    journal = eng.journal(rid)
    # replaying the finished journal again decides nothing new
    d = replay(order, {"order_id": 3, "amount": 5000}, journal, run_id=rid)
    assert [c.kind for c in d.commands] == ["fail_run"]
    assert d.commands[0].error["type"] == "Cancelled"


def test_continue_as_new_chains_runs_with_short_journals():
    r = Registry()

    @r.workflow(name="looper")
    async def looper(ctx, inp):
        total = inp["total"]
        for _ in range(100):
            total += int((await ctx.random()) >= 0)  # +1, journaled
        if inp["round"] + 1 < inp["rounds"]:
            return await ctx.continue_as_new(
                {"total": total, "round": inp["round"] + 1, "rounds": inp["rounds"]}
            )
        return total

    eng = LocalEngine(registry=r)
    rid = eng.start("looper", {"total": 0, "round": 0, "rounds": 20})
    chain = [rid]
    while True:
        rec = eng.run(chain[-1])
        assert len(eng.journal(chain[-1])) <= 103
        if rec.status != "continued":
            break
        chain.append(rec.result["continued_as"])
    assert len(chain) == 20 and rec.result == 2000


def test_cancelled_is_exported():
    assert issubclass(Cancelled, Exception)
