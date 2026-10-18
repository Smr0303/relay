"""E6: how long does one replay of a 40-event journal take?

A 19-step workflow gives 1 RunStarted + 19 x (StepScheduled + StepCompleted) + 1 RunCompleted
= 40 events, the per-run journal size the estimate assumes. We time `replay()` on the full
journal (the worst case: every call is answered from the journal).

    uv run python -m bench.replay_micro
"""

import statistics
import time

from relay.sdk.local import LocalEngine
from relay.sdk.replay import replay
from relay.sdk.workflow import Registry

STEPS = 19
TARGET_MS = 5.0

reg = Registry()


@reg.step
def work(sctx, i):
    return {"i": i, "payload": "x" * 200}


@reg.workflow
async def nineteen_steps(ctx, inp):
    out = []
    for i in range(STEPS):
        out.append((await ctx.step(work, i))["i"])
    return out


def measure(iterations: int = 2000) -> dict:
    eng = LocalEngine(registry=reg)
    run_id = eng.start(nineteen_steps, None)
    eng.run(run_id)
    journal = eng.journal(run_id)
    assert len(journal) == 40, len(journal)

    for _ in range(100):  # warm up
        replay(nineteen_steps, None, journal, run_id=run_id)
    samples = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        replay(nineteen_steps, None, journal, run_id=run_id)
        samples.append((time.perf_counter() - t0) * 1000)
    samples.sort()
    return {
        "events": len(journal),
        "p50_ms": statistics.median(samples),
        "p99_ms": samples[int(len(samples) * 0.99)],
    }


if __name__ == "__main__":
    r = measure()
    verdict = "PASS" if r["p50_ms"] <= TARGET_MS else "FAIL"
    print(
        f"replay of {r['events']} events: p50 {r['p50_ms']:.3f} ms, p99 {r['p99_ms']:.3f} ms "
        f"(target p50 <= {TARGET_MS} ms) {verdict}"
    )
