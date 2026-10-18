"""A small synchronous client for apps, tests and the CLI."""

import time
from typing import Any

import httpx

TERMINAL = {"completed", "failed", "cancelled", "continued", "parked"}


class Client:
    def __init__(self, server: str = "http://127.0.0.1:8080", timeout: float = 10.0) -> None:
        self._http = httpx.Client(base_url=server.rstrip("/"), timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _ok(self, r: httpx.Response) -> Any:
        r.raise_for_status()
        return r.json()

    def start(
        self,
        workflow: str,
        input: Any = None,
        *,
        version: int | None = None,
        request_id: str | None = None,
    ) -> str:
        body = {"workflow": workflow, "version": version, "input": input, "request_id": request_id}
        return self._ok(self._http.post("/v1/runs", json=body))["run_id"]

    def get(self, run_id: str) -> dict:
        return self._ok(self._http.get(f"/v1/runs/{run_id}"))

    def events(self, run_id: str) -> list[dict]:
        return self._ok(self._http.get(f"/v1/runs/{run_id}/events"))

    def signal(self, run_id: str, name: str, data: Any = None) -> None:
        self._ok(self._http.post(f"/v1/runs/{run_id}/signals", json={"name": name, "data": data}))

    def cancel(self, run_id: str) -> None:
        self._ok(self._http.post(f"/v1/runs/{run_id}/cancel"))

    def resume(self, run_id: str) -> None:
        self._ok(self._http.post(f"/v1/runs/{run_id}/resume"))

    def wait(self, run_id: str, timeout: float = 30.0, statuses=TERMINAL) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            run = self.get(run_id)
            if run["status"] in statuses:
                return run
            if time.monotonic() > deadline:
                raise TimeoutError(f"run {run_id} still {run['status']} after {timeout}s")
            time.sleep(0.05)
