"""A fake LLM that misbehaves like a real one: it rate-limits (429), and sometimes it bills
for a call and then hangs far past the caller's timeout. Billing goes through the effects
service keyed by the step's idempotency key, so a retried call is billed once."""

import random
import time

from examples.fake_stripe import STRIPE


class RateLimited(Exception):
    """HTTP 429; retryable."""


class FakeLLM:
    def __init__(self, rate_limit_p: float = 0.10, hang_p: float = 0.05, hang_s: float = 8.0):
        self.rate_limit_p = rate_limit_p
        self.hang_p = hang_p
        self.hang_s = hang_s

    def complete(self, purpose: str, prompt: str, idempotency_key: str) -> dict:
        roll = random.random()
        if roll < self.rate_limit_p:
            raise RateLimited(f"429 on {purpose}")
        bill = (
            STRIPE.bill_llm(purpose, idempotency_key)
            if hasattr(STRIPE, "bill_llm")
            else {"id": idempotency_key}
        )
        if roll < self.rate_limit_p + self.hang_p:
            time.sleep(self.hang_s)  # billed, then hung: the lease expires and the step retries
        else:
            time.sleep(random.uniform(0.05, 0.3))
        words = 400 + hash(prompt) % 400
        return {
            "text": f"[{purpose}] {words} words for {prompt[:40]}",
            "words": words,
            "bill": bill["id"],
        }


LLM = FakeLLM()
