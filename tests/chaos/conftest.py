import asyncio
import os

import pytest

from tests.integration.cluster import ROOT, Cluster, ensure_database, truncate


def _pg_available() -> bool:
    try:
        asyncio.run(ensure_database())
        return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def pg():
    if not _pg_available():
        pytest.skip("Postgres not reachable on :5440 (make up)")
    os.makedirs(os.path.join(ROOT, ".test-logs"), exist_ok=True)


@pytest.fixture
def cluster(pg):
    asyncio.run(truncate())
    c = Cluster()
    yield c
    c.stop()
