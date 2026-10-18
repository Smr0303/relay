"""In-memory stand-ins for Stripe and an email provider. Both honour idempotency keys, which is
what makes a retried step safe. `calls` counts requests; `charges`/`sent` count real effects."""

from datetime import timedelta


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


STRIPE = FakeStripe()
OUTBOX = FakeOutbox()
DAY = timedelta(days=1)
