"""Real processes for integration and chaos tests: Postgres (docker), N servers, the effects
service, and workers that can be SIGKILLed."""

import os
import signal
import socket
import subprocess
import sys
import time

import asyncpg
import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DSN = os.environ.get("RELAY_TEST_DSN", "postgresql://relay:relay@localhost:5440/relay_test")
ADMIN_DSN = DSN.rsplit("/", 1)[0] + "/relay"
TABLES = "runs, events, tasks, timers, signals"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def ensure_database() -> None:
    conn = await asyncpg.connect(ADMIN_DSN)
    try:
        if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname='relay_test'"):
            await conn.execute("CREATE DATABASE relay_test")
    finally:
        await conn.close()


async def truncate() -> None:
    conn = await asyncpg.connect(DSN)
    try:
        for t in [*TABLES.split(", "), "fx_requests", "fx_objects"]:
            exists = await conn.fetchval("SELECT to_regclass($1)", t)
            if exists:
                await conn.execute(f"TRUNCATE {t}")
    finally:
        await conn.close()


def wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise TimeoutError(url)


class Cluster:
    def __init__(self, env_extra: dict | None = None) -> None:
        self.procs: list[subprocess.Popen] = []
        self.workers: list[subprocess.Popen] = []
        self.env = {
            **os.environ,
            "RELAY_DSN": DSN,
            "PYTHONPATH": ROOT,
            "RELAY_ADVANCE_TIMEOUT_S": "3",
            "ORDER_DAY_SECONDS": "1",
            **(env_extra or {}),
        }
        self.server_urls: list[str] = []
        self.servers: dict[int, subprocess.Popen] = {}
        self.effects_url = ""

    def _spawn(self, args: list[str], log_name: str) -> subprocess.Popen:
        log = open(os.path.join(ROOT, ".test-logs", log_name), "ab")
        return subprocess.Popen(
            [sys.executable, *args], env=self.env, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT
        )

    def start_server(self, port: int | None = None) -> str:
        port = port or free_port()
        proc = self._spawn(["-m", "relay.server", "--port", str(port)], f"server-{port}.log")
        self.procs.append(proc)
        self.servers[port] = proc
        url = f"http://127.0.0.1:{port}"
        wait_http(url + "/healthz")
        if url not in self.server_urls:
            self.server_urls.append(url)
        return url

    def kill_server(self, url: str) -> int:
        port = int(url.rsplit(":", 1)[1])
        proc = self.servers.pop(port)
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        return port

    def start_effects(self) -> str:
        port = free_port()
        self.procs.append(
            self._spawn(
                ["-m", "examples.effects_service", "--port", str(port)], f"effects-{port}.log"
            )
        )
        self.effects_url = f"http://127.0.0.1:{port}"
        wait_http(self.effects_url + "/healthz")
        self.env["FAKE_EFFECTS_URL"] = self.effects_url
        return self.effects_url

    def start_worker(
        self, app: str | list[str] = "examples.order", server: str | None = None, slots: int = 50
    ) -> subprocess.Popen:
        server = server or self.server_urls[len(self.workers) % len(self.server_urls)]
        apps = [app] if isinstance(app, str) else app
        app_args = [a for name in apps for a in ("--app", name)]
        args = ["-m", "relay.sdk.worker", *app_args, "--server", server]
        args += ["--slots", str(slots), "--pollers", "2"]
        p = self._spawn(args, f"worker-{len(self.workers)}.log")
        self.workers.append(p)
        return p

    def kill_worker(self, p: subprocess.Popen) -> None:
        p.send_signal(signal.SIGKILL)
        p.wait()

    def stats(self) -> dict:
        return httpx.get(self.effects_url + "/stats", timeout=5).json()

    def stop(self) -> None:
        for p in self.workers + self.procs:
            if p.poll() is None:
                p.terminate()
        for p in self.workers + self.procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
