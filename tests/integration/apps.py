"""Workflows used by the integration tests (imported by worker processes via --app)."""

import time

from examples.fake_stripe import STRIPE
from relay import StepContext, step, workflow
from relay.common.protocol import RetryPolicy


@step(timeout_s=2)
def slow_charge(sctx: StepContext, amount: int) -> dict:
    charge = STRIPE.charge(amount, idempotency_key=sctx.idempotency_key)
    time.sleep(1.5)  # the effect happened; a kill now is crash case C (after effect, before ack)
    return charge


@workflow
async def slow_order(ctx, inp):
    charge = await ctx.step(slow_charge, inp["amount"])
    return {"charge_id": charge["charge_id"]}


@workflow
async def counter(ctx, inp):
    total = 0
    for _ in range(inp["n"]):
        total += (await ctx.wait_signal("tick"))["v"]
    return total


@step(retry=RetryPolicy(max_attempts=5, initial_s=1.0, multiplier=2.0, jitter=0.0))
def flaky(sctx: StepContext, succeed_on: int) -> dict:
    if sctx.attempt < succeed_on:
        raise ConnectionError(f"attempt {sctx.attempt} failed")
    return {"attempt": sctx.attempt}


class InvalidCard(Exception):
    pass


@step(non_retryable=(InvalidCard,))
def strict(sctx: StepContext) -> None:
    raise InvalidCard("card declined")


@workflow
async def retrying(ctx, inp):
    return await ctx.step(flaky, inp["succeed_on"])


@workflow
async def non_retryable(ctx, inp):
    await ctx.step(strict)


@workflow
async def sleeper(ctx, inp):
    await ctx.sleep(inp["sleep_s"])
    return "woke"


@step(retry=RetryPolicy(max_attempts=5, initial_s=0.2, multiplier=2.0, jitter=0.0))
def always_fails(sctx: StepContext) -> None:
    raise ConnectionError(f"attempt {sctx.attempt} failed")


@workflow
async def gives_up(ctx, inp):
    await ctx.step(always_fails)


@step(timeout_s=2)
def long_step(sctx: StepContext, seconds: float) -> dict:
    time.sleep(seconds)  # far longer than the 2 s lease: only heartbeats keep it alive
    return {"slept": seconds, "attempt": sctx.attempt}


@workflow
async def long_running(ctx, inp):
    return await ctx.step(long_step, inp["seconds"])


@workflow
async def poison(ctx, inp):
    import asyncio

    await asyncio.sleep(0)  # a bug: awaits something that is not a ctx call


@workflow
async def looper(ctx, inp):
    total = inp["total"]
    for _ in range(inp["per_round"]):
        total += int((await ctx.random()) >= 0)
    if inp["round"] + 1 < inp["rounds"]:
        return await ctx.continue_as_new({**inp, "total": total, "round": inp["round"] + 1})
    return total


@step
def big(sctx, kb: int) -> str:
    return "x" * (kb * 1024)


@workflow
async def big_result(ctx, inp):
    return len(await ctx.step(big, inp["kb"]))
