"""Prometheus metrics. Names match the alerts in deploy/prometheus/alerts.yml."""

from prometheus_client import Counter, Gauge, Histogram

_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)

RUNS_STARTED = Counter("relay_runs_started_total", "Runs started")
RUNS_CLOSED = Counter("relay_runs_closed_total", "Runs closed, by final status", ["status"])
RUNS_PARKED = Counter("relay_runs_parked_total", "Runs parked for an operator")
STEPS_COMPLETED = Counter("relay_steps_completed_total", "Step attempts that succeeded")
STEP_FAILURES = Counter("relay_step_failures_total", "Step attempts that failed", ["final"])
LEASE_EXPIRIES = Counter("relay_lease_expiries_total", "Leases that expired", ["kind"])
NONDETERMINISM = Counter("relay_nondeterminism_total", "Replays that hit NonDeterminismError")
DISPATCH_LATENCY = Histogram(
    "relay_dispatch_latency_seconds",
    "Task ready -> claimed by a worker",
    ["kind"],
    buckets=_LATENCY_BUCKETS,
)
TIMER_LAG = Histogram("relay_timer_lag_seconds", "How late a timer fired", buckets=_LATENCY_BUCKETS)
REPLAY_SECONDS = Histogram(
    "relay_replay_seconds",
    "Worker-side replay time per ADVANCE",
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5),
)
STEP_SECONDS = Histogram(
    "relay_step_duration_seconds", "Worker-side step execution time", buckets=_LATENCY_BUCKETS
)
QUEUE_DEPTH = Gauge("relay_queue_depth", "Unclaimed tasks that are ready", ["kind"])
OLDEST_READY = Gauge("relay_oldest_ready_task_seconds", "Age of the oldest ready, unclaimed task")
TIMERS_PENDING = Gauge("relay_timers_pending", "Timers waiting to fire")
TIMERS_OVERDUE = Gauge("relay_timers_overdue_seconds", "How overdue the oldest pending timer is")
LEASED = Gauge("relay_tasks_leased", "Tasks currently leased by workers", ["kind"])
JOURNAL_WARNINGS = Counter(
    "relay_journal_warnings_total", "Runs whose journal crossed 10k events (use continue_as_new)"
)
RUNS_ARCHIVED = Counter("relay_runs_archived_total", "Closed runs moved out to the archive")
