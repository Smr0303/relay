"""Render bench/results.md from the latest entry of each benchmark in bench/runs.jsonl.

uv run python -m bench.report
"""

import json

from bench.replay_micro import measure


def latest(rows, pred):
    found = [r for r in rows if pred(r)]
    return found[-1] if found else None


def main() -> None:
    rows = [json.loads(line) for line in open("bench/runs.jsonl") if line.strip()]
    e1 = latest(rows, lambda r: r.get("config", {}).get("duration", 0) >= 600)
    e3 = latest(rows, lambda r: r.get("bench") == "timer_soak")
    e7 = latest(rows, lambda r: r.get("bench") == "chaos")
    obs = latest(rows, lambda r: r.get("bench") == "obs_drill")
    e6 = measure(2000)
    ceiling = [r for r in rows if r.get("config", {}).get("duration") == 90]

    out = [
        "# Benchmark results",
        "",
        "One laptop: Apple M5 Pro (15 cores), Postgres 16 in Docker Desktop "
        "(`synchronous_commit` on), servers and workers as local processes. "
        "Raw rows: `bench/runs.jsonl`.",
        "",
    ]
    if e1:
        c = e1["config"]
        out += [
            "## E1 / E2 / E4 / E5: sustained load (`bench/load.py`)",
            "",
            f"Open-loop: {c['rate']:.0f} runs/s × {c['steps']} steps of {c['step_ms']:.0f} ms "
            f"simulated I/O = **{e1['offered_steps_per_s']:.0f} steps/s offered** for "
            f"{c['duration']:.0f} s ({c['warmup']:.0f} s warm-up excluded), "
            f"{c['servers']} server processes, {c['workers']} workers.",
            "",
            "| metric | value |",
            "|---|---|",
            f"| steps/s sustained (measured window {e1['window_s']} s) | **{e1['steps_per_s']}** |",
            f"| runs started / completed | {e1['runs_started']:,} / {e1['runs_completed']:,} |",
            f"| dispatch STEP p50 / p99 | {e1['dispatch_step_p50_ms']} ms / "
            f"{e1['dispatch_step_p99_ms']} ms |",
            f"| dispatch ADVANCE p99 | {e1['dispatch_advance_p99_ms']} ms |",
            f"| worker replay p99 | {e1['replay_p99_ms']} ms |",
            f"| ready-queue depth, max of 5 s samples | {max(e1['queue_depth_samples']):.0f} "
            "(flat = keeping up) |",
            f"| journal events per 10-step run | {e1['events_per_run']} |",
            f"| bytes per run (events + runs tables incl. indexes) | {e1['bytes_per_run']:,} |",
            f"| CPU cores, all servers / all workers | "
            f"{sum(v for k, v in e1['cpu_cores'].items() if k.startswith('server')):.2f} / "
            f"{sum(v for k, v in e1['cpu_cores'].items() if k.startswith('worker')):.2f} |",
            "",
        ]
    if ceiling:
        out += [
            "### Ceiling (90 s runs, 8 server processes, 6 workers)",
            "",
            "| offered steps/s | sustained steps/s | queue | Postgres |",
            "|---|---|---|---|",
        ]
        for r in ceiling:
            q = r["queue_depth_samples"]
            trend = (
                f"growing ({q[0]:,.0f} → {q[-1]:,.0f})"
                if q[-1] > 1.5 * q[0] and q[-1] > 1000
                else "flat"
            )
            out.append(
                f"| {r['offered_steps_per_s']:.0f} | **{r['steps_per_s']}** | {trend} | "
                f"{r.get('note', '')} |"
            )
        out += [
            "",
            "Past ~1.3k steps/s the backlog grows. At that point Postgres is at ~2.9 cores "
            "with 15 of ~20 active backends waiting on `LWLock: WALWrite`: the limit is WAL "
            "flushing (every commit is durable). Group commit (`commit_delay`) did not move "
            "it. The levers that would are fewer commits per step (e.g. acking a step and "
            "claiming its ADVANCE in one transaction) or faster storage than Docker Desktop's "
            "VM disk.",
            "",
        ]
    if e3:
        out += [
            "## E3: timers (`bench/timer_soak.py`)",
            "",
            f"{e3['runs']:,} sleeping runs bulk-loaded (journal + timer row), fire times spread "
            f"over {e3['window_s']:.0f} s.",
            "",
            "| metric | value |",
            "|---|---|",
            f"| idle CPU while all sleep (cores) | "
            f"{json.dumps(e3['idle_cpu_cores_with_all_sleeping'])} |",
            f"| timers fired / runs completed | {e3['timers_fired']:,} / {e3['completed']:,} |",
            f"| timer lag p50 / p99 | {e3['timer_lag_p50_ms']} ms / "
            f"**{e3['timer_lag_p99_ms']} ms** |",
            "",
        ]
    out += [
        "## E6: replay (`bench/replay_micro.py`)",
        "",
        f"Full replay of a {e6['events']}-event journal: p50 **{e6['p50_ms']:.3f} ms**, "
        f"p99 {e6['p99_ms']:.3f} ms.",
        "",
    ]
    if e7:
        out += [
            "## E7: chaos (`tests/chaos/chaos.py`)",
            "",
            "```",
            json.dumps({k: v for k, v in e7.items() if k != "bench"}, indent=2),
            "```",
            "",
        ]
    if obs:
        out += [
            "## Observability drill (`tests/obs/drill.py`)",
            "",
            "```",
            json.dumps({k: v for k, v in obs.items() if k != "bench"}, indent=2),
            "```",
            "",
        ]
    open("bench/results.md", "w").write("\n".join(out))
    print("\n".join(out))


if __name__ == "__main__":
    main()
