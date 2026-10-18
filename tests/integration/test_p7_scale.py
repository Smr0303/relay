"""Phase 7: shard ownership across servers, and the archiver."""

import asyncio
import json
import time

import asyncpg
import httpx

from relay.sdk.client import Client
from tests.integration.cluster import DSN

APPS = "tests.integration.apps"


def _owned(url):
    return set(httpx.get(url + "/healthz", timeout=5).json()["shards"])


def _wait(pred, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.3)
    return False


def test_shards_split_across_servers_and_fail_over(cluster):
    a, b, c = cluster.start_server(), cluster.start_server(), cluster.start_server()

    def split():
        sets = [_owned(u) for u in (a, b, c)]
        union = set().union(*sets)
        # every shard owned exactly once, nobody above ceil(16 / 3) = 6
        return (
            union == set(range(16)) and sum(map(len, sets)) == 16 and all(len(x) <= 6 for x in sets)
        )

    assert _wait(split), [sorted(_owned(u)) for u in (a, b, c)]
    cluster.kill_server(c)  # its locks die with its session; a and b take its shards
    assert _wait(
        lambda: _owned(a) | _owned(b) == set(range(16)) and len(_owned(a)) + len(_owned(b)) == 16,
        timeout=30,
    )

    cluster.start_worker(APPS, server=a)  # and timers in every shard still fire
    cl = Client(a)
    runs = [cl.start("sleeper", {"sleep_s": 1}) for _ in range(60)]
    shards = set()
    for rid in runs:
        assert cl.wait(rid, timeout=30)["result"] == "woke"
        shards.add(__import__("uuid").UUID(rid).int % 16)
    assert len(shards) >= 12  # the runs really spread over the shards


def test_archiver_moves_old_closed_runs_out(cluster, tmp_path):
    cluster.env.update({"RELAY_RETENTION_S": "2", "RELAY_ARCHIVE_DIR": str(tmp_path)})
    server = cluster.start_server()
    cluster.start_worker(APPS)
    c = Client(server)
    done = [c.start("sleeper", {"sleep_s": 0}) for _ in range(20)]
    for rid in done:
        c.wait(rid, timeout=20)
    open_run = c.start("counter", {"n": 1})  # still open: must stay

    async def count():
        conn = await asyncpg.connect(DSN)
        try:
            return (
                await conn.fetchval("SELECT count(*) FROM runs"),
                await conn.fetchval("SELECT count(*) FROM events"),
            )
        finally:
            await conn.close()

    assert _wait(lambda: asyncio.run(count())[0] == 1, timeout=60)  # archiver ticks every 30 s
    lines = [json.loads(line) for f in tmp_path.glob("*.jsonl") for line in f.open()]
    archived = {row["run"]["run_id"] for row in lines}
    assert archived == set(done)
    assert all(row["events"][-1]["type"] == "RunCompleted" for row in lines)
    assert c.get(open_run)["status"] != "completed"
