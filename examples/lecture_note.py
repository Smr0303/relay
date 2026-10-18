"""An LLM content pipeline rebuilt on Relay.

Before (Celery): status column "generating" -> LLM body -> "generated" -> COMMIT, then
`.delay()` the cabinet task. A hung call stranded notes in "generating"; a crash between the
commit and the enqueue lost stage 2; retries were hand-written per task.

Here it is one function. A hung LLM call just loses its lease and the step retries; the
cabinet stage cannot be lost because "body done" and "run the judge" are one transaction;
the retry policy is declared once.
"""

from examples.fake_llm import LLM
from relay import StepContext, step, workflow
from relay.common.protocol import RetryPolicy

LLM_RETRY = RetryPolicy(max_attempts=10, initial_s=0.2, multiplier=2.0, max_s=2.0)


@step(timeout_s=3, retry=LLM_RETRY)
def llm(sctx: StepContext, purpose: str, prompt: str) -> dict:
    return LLM.complete(purpose, prompt, idempotency_key=sctx.idempotency_key)


@workflow
async def lecture_note(ctx, inp: dict) -> dict:
    chapter = inp["chapter"]
    body = await ctx.step(llm, "body", f"textbook {inp['textbook']} chapter {chapter}")
    verdict = await ctx.step(llm, "judge", body["text"])
    cabinet = await ctx.step(llm, "cabinet", body["text"])
    return {
        "chapter": chapter,
        "words": body["words"],
        "bills": [body["bill"], verdict["bill"], cabinet["bill"]],
    }
