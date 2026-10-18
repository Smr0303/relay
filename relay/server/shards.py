"""Shard ownership for the per-shard background loops (timer service, reaper, archiver).

Runs are hashed into 16 shards. Each live server takes ceil(16 / live servers) of them by
holding a session-level advisory lock per shard on its own connection. When a server dies
its session ends and Postgres drops its locks. A clean shutdown also deletes its heartbeat
row, so the survivors' targets grow on their next tick (~2 s). After a crash the row lingers
until it ages out, so shards that NO session holds for two consecutive ticks are adopted
regardless of target (~4-6 s); without that they would sit unowned (no reaper, no timers)
until the stale row expired. Membership is a heartbeat row per server.

Correctness never depends on this: every loop re-checks rows under row locks, so a brief
double-ownership only costs work.
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
        self._orphan_ticks = 0

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
        held = {
            r["objid"] - LOCK_BASE
            for r in await conn.fetch(
                "SELECT objid FROM pg_locks WHERE locktype = 'advisory' AND granted "
                "AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) "
                "AND classid = 0 AND objsubid = 1 AND objid BETWEEN $1 AND $2",
                LOCK_BASE,
                LOCK_BASE + SHARDS - 1,
            )
        }
        orphans = set(range(SHARDS)) - held - self.owned
        self._orphan_ticks = self._orphan_ticks + 1 if orphans else 0
        if self._orphan_ticks >= 2:  # nobody picked them up last tick either: adopt
            for shard in sorted(orphans):
                if await conn.fetchval("SELECT pg_try_advisory_lock($1)", LOCK_BASE + shard):
                    self.owned.add(shard)
            self._orphan_ticks = 0

    async def stop(self) -> None:
        """Leave cleanly: drop the heartbeat row so survivors rebalance at once."""
        self.owned.clear()
        try:
            conn = await asyncpg.connect(self.dsn)
            try:
                await conn.execute("DELETE FROM servers WHERE server_id = $1", self.server_id)
            finally:
                await conn.close()
        except Exception:
            log.warning("could not remove server row on shutdown", exc_info=True)
