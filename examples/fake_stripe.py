"""Stand-ins for Stripe and an email provider. Both honour idempotency keys, which is what
makes a retried step safe. `calls` counts requests; `charges`/`sent` count real effects.

In-memory by default (unit tests). With FAKE_EFFECTS_URL set, the same interface talks to
examples/effects_service.py, so effects are counted across worker processes.
"""

import os
from datetime import timedelta

import httpx


class FakeStripe:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.calls = 0
        self.charges: dict[str, dict] = {}
        self.refunds: dict[str, dict] = {}

    def charge(self, amount: int, idempotency_key: str) -> dict:
        self.calls += 1
        if idempotency_key not in self.charges:  # a repeat key returns the original charge
            self.charges[idempotency_key] = {
                "charge_id": f"ch_{len(self.charges) + 1}",
                "amount": amount,
            }
        return self.charges[idempotency_key]

    def refund(self, charge_id: str, idempotency_key: str) -> dict:
        self.calls += 1
        if idempotency_key not in self.refunds:
            self.refunds[idempotency_key] = {
                "refund_id": f"re_{len(self.refunds) + 1}",
                "charge_id": charge_id,
            }
        return self.refunds[idempotency_key]


class FakeOutbox:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.calls = 0
        self.sent: dict[str, dict] = {}

    def send(self, to: str, template: str, idempotency_key: str) -> dict:
        self.calls += 1
        if idempotency_key not in self.sent:
            self.sent[idempotency_key] = {
                "message_id": f"m-{len(self.sent) + 1}",
                "to": to,
                "template": template,
            }
        return self.sent[idempotency_key]


class HttpEffects:
    """FakeStripe + FakeOutbox over HTTP (thread-safe; steps run in worker threads)."""

    def __init__(self, url: str) -> None:
        self._http = httpx.Client(base_url=url, timeout=10)

    def _post(self, kind: str, key: str, data: dict) -> dict:
        r = self._http.post("/effects", json={"kind": kind, "key": key, "data": data})
        r.raise_for_status()
        return r.json()

    def charge(self, amount: int, idempotency_key: str) -> dict:
        obj = self._post("charge", idempotency_key, {"amount": amount})
        return {"charge_id": obj["id"], "amount": amount}

    def refund(self, charge_id: str, idempotency_key: str) -> dict:
        obj = self._post("refund", idempotency_key, {"charge_id": charge_id})
        return {"refund_id": obj["id"], "charge_id": charge_id}

    def bill_llm(self, purpose: str, idempotency_key: str) -> dict:
        return self._post("llm", idempotency_key, {"purpose": purpose})

    def send(self, to: str, template: str, idempotency_key: str) -> dict:
        obj = self._post("email", idempotency_key, {"to": to, "template": template})
        return {"message_id": obj["id"], "to": to, "template": template}


if os.environ.get("FAKE_EFFECTS_URL"):
    STRIPE = OUTBOX = HttpEffects(os.environ["FAKE_EFFECTS_URL"])
else:
    STRIPE = FakeStripe()
    OUTBOX = FakeOutbox()
# Demos shrink a "day" (e.g. ORDER_DAY_SECONDS=1) so the 3-day timeout fits in a test run.
DAY = timedelta(seconds=float(os.environ.get("ORDER_DAY_SECONDS", 86400)))
