"""An LLM content pipeline (generate a note, judge it, build quiz + flashcards) on Relay.

The usual way to build this, on a task queue: set status="generating", call the LLM, set
status="generated", COMMIT, then enqueue the next stage as a separate call. Three classic
failures follow: a hung LLM call strands the row in "generating"; a crash between the commit
and the enqueue silently loses the next stage; and retry/backoff is hand-written per task.

Here it is one function. A hung LLM call just loses its lease and the step retries; the next
stage cannot be lost because "body done" and "run the judge" are one transaction; the retry
policy is declared once.
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
