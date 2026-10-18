"""Two deployed versions of the same workflow; each worker process loads one module."""

import os

from relay import step, workflow

VERSION = int(os.environ.get("APP_VERSION", "1"))


@step(name=f"quote_v{VERSION}")
def quote(sctx, base: int) -> dict:
    return {"price": base * VERSION}


@workflow(name="pricing", version=VERSION)
async def pricing(ctx, inp):
    q = await ctx.step(quote, inp["base"])
    await ctx.sleep(inp.get("sleep_s", 0))
    return {"code_version": VERSION, "price": q["price"]}
