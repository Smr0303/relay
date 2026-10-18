"""One workflow, "flow" v1, in three deployable variants (FLOW_CODE=old|patched|unpatched):
how a code change to live runs is made safely (patched) or unsafely (unpatched)."""

import os

from relay import step, workflow

CODE = os.environ.get("FLOW_CODE", "old")


@step
def a(sctx):
    return "a"


@step
def a2(sctx):
    return "a2"


@step
def b(sctx):
    return "b"


@workflow(name="flow")
async def flow(ctx, inp):
    out = [await ctx.step(a)]
    if CODE == "patched":
        if await ctx.patched("add-a2"):
            out.append(await ctx.step(a2))
    elif CODE == "unpatched":
        out.append(await ctx.step(a2))
    await ctx.sleep(inp["sleep_s"])
    out.append(await ctx.step(b))
    return out
