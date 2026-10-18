"""A fake Stripe + email provider as an HTTP service, so effects can be counted across worker
processes that get killed. Every request is logged; each idempotency key creates one object.

    python -m examples.effects_service --port 8090

POST /effects {kind, key, data}  -> the object for that key (created on first sight)
GET  /stats                      -> {requests, objects: {kind: n}, keys_seen_twice}
POST /reset
"""

import argparse
import contextlib
import hashlib
import os

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from relay.server.engine import create_pool

DSN = os.environ.get("RELAY_DSN", "postgresql://relay:relay@localhost:5440/relay")

SCHEMA = """
CREATE TABLE IF NOT EXISTS fx_requests (id bigserial PRIMARY KEY, key text NOT NULL,
  kind text NOT NULL, at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS fx_objects (key text PRIMARY KEY, kind text NOT NULL,
  object_id text NOT NULL, data jsonb, created_at timestamptz NOT NULL DEFAULT now());
"""
PREFIX = {"charge": "ch", "refund": "re", "email": "m"}


class EffectIn(BaseModel):
    kind: str
    key: str
    data: dict = {}


def create_app(dsn: str = DSN) -> FastAPI:
    state = {}

    @contextlib.asynccontextmanager
    async def lifespan(app):
        state["pool"] = await create_pool(dsn, min_size=2, max_size=20)
        async with state["pool"].acquire() as conn:
            await conn.execute("SELECT pg_advisory_lock(434343)")
            await conn.execute(SCHEMA)
            await conn.execute("SELECT pg_advisory_unlock(434343)")
        yield
        await state["pool"].close()

    app = FastAPI(lifespan=lifespan)

    @app.post("/effects")
    async def effect(body: EffectIn):
        object_id = (
            f"{PREFIX.get(body.kind, 'obj')}_{hashlib.md5(body.key.encode()).hexdigest()[:10]}"
        )
        async with state["pool"].acquire() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO fx_requests (key, kind) VALUES ($1,$2)", body.key, body.kind
            )
            await conn.execute(
                "INSERT INTO fx_objects (key, kind, object_id, data) VALUES ($1,$2,$3,$4) "
                "ON CONFLICT (key) DO NOTHING",
                body.key,
                body.kind,
                object_id,
                body.data,
            )
            row = await conn.fetchrow(
                "SELECT object_id, data FROM fx_objects WHERE key=$1", body.key
            )
        return {"id": row["object_id"], **(row["data"] or {})}

    @app.get("/stats")
    async def stats():
        pool = state["pool"]
        objects = {
            r["kind"]: r["n"]
            for r in await pool.fetch("SELECT kind, count(*) AS n FROM fx_objects GROUP BY kind")
        }
        return {
            "requests": await pool.fetchval("SELECT count(*) FROM fx_requests"),
            "objects": objects,
            "keys_seen_twice": await pool.fetchval(
                "SELECT count(*) FROM (SELECT key FROM fx_requests GROUP BY key "
                "HAVING count(*) > 1) d"
            ),
        }

    @app.post("/reset")
    async def reset():
        await state["pool"].execute("TRUNCATE fx_requests, fx_objects")
        return {"ok": True}

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=8090)
    args = p.parse_args()
    uvicorn.run(
        create_app(), host="127.0.0.1", port=args.port, log_level="warning", access_log=False
    )
