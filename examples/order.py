"""The anchor scenario from the design doc (§3a): reserve -> charge -> wait for "shipped"
(refund after 3 days) -> receipt -> sleep 7 days -> ask for a review."""

from examples.fake_stripe import DAY, OUTBOX, STRIPE
from relay import StepContext, step, workflow


@step
def reserve_stock(sctx: StepContext, order_id: int) -> dict:
    return {"reservation": f"res-{order_id}"}


@step
def charge_card(sctx: StepContext, order_id: int, amount: int) -> dict:
    return STRIPE.charge(amount, idempotency_key=sctx.idempotency_key)


@step
def refund(sctx: StepContext, charge_id: str) -> dict:
    return STRIPE.refund(charge_id, idempotency_key=sctx.idempotency_key)


@step
def send_email(sctx: StepContext, order_id: int, template: str) -> dict:
    return OUTBOX.send(f"customer-{order_id}", template, idempotency_key=sctx.idempotency_key)


@workflow
async def order(ctx, inp: dict) -> dict:
    order_id = inp["order_id"]
    await ctx.step(reserve_stock, order_id)
    charge = await ctx.step(charge_card, order_id, inp["amount"])
    shipped = await ctx.wait_signal("shipped", timeout=3 * DAY)
    if shipped is None:
        await ctx.step(refund, charge["charge_id"])
        return {"status": "refunded", "charge_id": charge["charge_id"]}
    await ctx.step(send_email, order_id, "receipt")
    await ctx.sleep(7 * DAY)
    await ctx.step(send_email, order_id, "review_request")
    return {"status": "done", "charge_id": charge["charge_id"], "tracking": shipped["tracking"]}
