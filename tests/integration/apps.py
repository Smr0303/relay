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
