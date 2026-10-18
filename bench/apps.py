"""The benchmark workflow: N steps of simulated I/O (asyncio.sleep), the anchor's shape."""

import asyncio
import os

from relay import step, workflow

STEP_S = float(os.environ.get("BENCH_STEP_MS", 200)) / 1000


@step
async def io_step(sctx, i: int) -> dict:
    await asyncio.sleep(STEP_S)
    return {"i": i, "ok": True}


@workflow
async def bench(ctx, inp):
    for i in range(inp["steps"]):
        await ctx.step(io_step, i)
    return inp["steps"]
