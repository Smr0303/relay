"""Scrape and merge /metrics from several server processes."""

from collections import defaultdict

import httpx
from prometheus_client.parser import text_string_to_metric_families


def scrape(urls: list[str]) -> dict:
    """{(name, labels_tuple): value} summed across servers (counters, buckets, gauges)."""
    out: dict = defaultdict(float)
    for url in urls:
        text = httpx.get(url + "/metrics", timeout=10).text
        for fam in text_string_to_metric_families(text):
            for s in fam.samples:
                out[(s.name, tuple(sorted(s.labels.items())))] += s.value
    return out


def delta(a: dict, b: dict) -> dict:
    return {k: b.get(k, 0) - a.get(k, 0) for k in set(a) | set(b)}


def counter(m: dict, name: str, **labels) -> float:
    want = set(labels.items())
    return sum(v for (n, ls), v in m.items() if n == name and want <= set(ls))


def quantile(m: dict, name: str, q: float, **labels) -> float | None:
    """Histogram quantile (upper bucket bound, like Prometheus without interpolation)."""
    want = set(labels.items())
    buckets = defaultdict(float)
    for (n, ls), v in m.items():
        if n == f"{name}_bucket" and want <= set(ls):
            le = dict(ls)["le"]
            buckets[float("inf") if le == "+Inf" else float(le)] += v
    if not buckets:
        return None
    total = buckets[float("inf")]
    if total <= 0:
        return None
    prev_le, prev_c = 0.0, 0.0
    for le in sorted(buckets):
        c = buckets[le]
        if c >= q * total:
            if le == float("inf"):
                return prev_le
            # linear interpolation inside the bucket, as Prometheus histogram_quantile does
            frac = (q * total - prev_c) / (c - prev_c) if c > prev_c else 1.0
            return prev_le + (le - prev_le) * frac
        prev_le, prev_c = le, c
    return None
