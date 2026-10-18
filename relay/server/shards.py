"""Shard ownership for the per-shard background loops (timer service, reaper, archiver).

Runs are hashed into 16 shards. Each live server takes ceil(16 / live servers) of them by
holding a session-level advisory lock per shard on its own connection. When a server dies
its session ends, Postgres drops its locks, and the survivors take the shards on their next
tick (~2 s). Membership is a heartbeat row per server. Correctness never depends on this:
every loop re-checks rows under row locks, so a brief double-ownership only costs work.
"""

import asyncio
import logging
import math

import asyncpg

from relay.server.engine import SHARDS

log = logging.getLogger("relay.shards")
LOCK_BASE = 7_340_000
TICK_S = 2.0
LIVE_S = 10


class ShardOwner:
    def __init__(self, dsn: str, server_id: str) -> None:
        self.dsn = dsn
        self.server_id = server_id
        self.owned: set[int] = set()

    def shards(self) -> list[int]:
        return sorted(self.owned)

    async def run(self) -> None:
        while True:
            conn = None
            try:
                conn = await asyncpg.connect(self.dsn)
                conn.add_termination_listener(lambda _c: self.owned.clear())
                while True:
                    await self._tick(conn)
                    await asyncio.sleep(TICK_S)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("shard ownership connection failed")
            finally:
                self.owned.clear()  # locks die with the session
                if conn is not None and not conn.is_closed():
                    conn.terminate()
            await asyncio.sleep(1.0)

    async def _tick(self, conn) -> None:
        await conn.execute(
            "INSERT INTO servers (server_id, seen_at) VALUES ($1, now()) "
            "ON CONFLICT (server_id) DO UPDATE SET seen_at = now()",
            self.server_id,
        )
        await conn.execute("DELETE FROM servers WHERE seen_at < now() - interval '60 seconds'")
        live = await conn.fetchval(
            f"SELECT count(*) FROM servers WHERE seen_at > now() - interval '{LIVE_S} seconds'"
        )
        target = math.ceil(SHARDS / max(live, 1))
        for shard in sorted(self.owned, reverse=True):
            if len(self.owned) <= target:
                break
            await conn.execute("SELECT pg_advisory_unlock($1)", LOCK_BASE + shard)
            self.owned.discard(shard)
        for shard in range(SHARDS):
            if len(self.owned) >= target:
                break
            if shard in self.owned:
                continue
            if await conn.fetchval("SELECT pg_try_advisory_lock($1)", LOCK_BASE + shard):
                self.owned.add(shard)

    async def stop(self) -> None:
        self.owned.clear()
