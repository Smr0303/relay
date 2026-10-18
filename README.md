# Relay

A durable workflow engine on Postgres. Workflow code is replayed from an append-only
journal, so a crash at any point resumes at the last recorded step. Side effects run in
steps that carry an engine-supplied idempotency key (`run_id:call_seq`), identical on every
retry.

```python
@workflow
async def order(ctx, inp):
    await ctx.step(reserve_stock, inp["order_id"])
    charge = await ctx.step(charge_card, inp["order_id"], inp["amount"])
    shipped = await ctx.wait_signal("shipped", timeout=3 * DAY)
    if shipped is None:
        await ctx.step(refund, charge["charge_id"])
        return {"status": "refunded"}
    ...
```

Design and plan: `~/Documents/personal/RELAY_SYSTEM_DESIGN_PLAN_2026-10-11.md`.

## Status

| Phase | State |
|---|---|
| P0 skeleton | done |
| P1 replay engine, in-process | done: crash at every point resumes identically; replay of 40 events p50 0.025 ms (target ≤ 5 ms) |
| P2 Postgres + server + worker | next |

## Run

```bash
make setup          # uv sync
make test           # unit tests, including the crash-at-every-point gate
make bench-replay   # E6 micro-benchmark
make up             # Postgres 16 on :5440 (Phase 2+)
```
