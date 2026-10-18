"""A live demo for the dashboards: `make obs-up`, then `make demo`, then open
Grafana http://localhost:3000 (Relay dashboard), Jaeger http://localhost:16686 and the Relay UI
http://localhost:8080/ui. Runs a steady trickle of orders and lecture pipelines and kills a
random worker every 15 s. Ctrl-C to stop."""

import asyncio
import random
import time

from relay.sdk.client import Client
from tests.integration.cluster import Cluster, ensure_database, truncate


def main() -> None:
    asyncio.run(ensure_database())
    asyncio.run(truncate())
    cluster = Cluster(
        {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318", "ORDER_DAY_SECONDS": "5"}
    )
    apps = ["examples.order", "examples.lecture_note"]
    try:
        servers = [cluster.start_server(8080), cluster.start_server(8081)]
        cluster.start_effects()
        for _ in range(3):
            cluster.start_worker(apps, slots=100)
        c = Client(servers[0])
        print(
            "Relay UI http://localhost:8080/ui · Grafana http://localhost:3000 · "
            "Jaeger http://localhost:16686"
        )
        i, last_kill, shipped = 0, time.monotonic(), []
        while True:
            rid = c.start("order", {"order_id": i, "amount": 1000 + i})
            if i % 2 == 0:
                shipped.append((time.monotonic() + random.uniform(1, 20), rid))
            if i % 5 == 0:
                c.start("lecture_note", {"textbook": "physics", "chapter": i})
            for item in [s for s in shipped if s[0] <= time.monotonic()]:
                shipped.remove(item)
                try:
                    c.signal(item[1], "shipped", {"tracking": f"T{i}"})
                except Exception:
                    pass  # already timed out and refunded
            if time.monotonic() - last_kill > 15:
                alive = [w for w in cluster.workers if w.poll() is None]
                cluster.kill_worker(random.choice(alive))
                cluster.start_worker(apps, slots=100)
                last_kill = time.monotonic()
            i += 1
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        cluster.stop()


if __name__ == "__main__":
    main()
