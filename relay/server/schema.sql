-- Relay schema. Idempotent: applied on every server start.
-- Lock order everywhere: runs row first, then tasks / timers. The timer service is the one
-- exception (it takes timer rows first) and therefore never WAITS for a run lock (SKIP LOCKED).

CREATE TABLE IF NOT EXISTS runs (
  run_id      uuid PRIMARY KEY,
  workflow    text NOT NULL,
  version     int  NOT NULL,
  status      text NOT NULL,      -- running|suspended|completed|failed|cancelled|parked|continued
  input       jsonb NOT NULL,
  result      jsonb,
  error       jsonb,
  request_id  text UNIQUE,        -- idempotent start
  shard       smallint NOT NULL,  -- hash(run_id) % 16
  next_seq    bigint NOT NULL DEFAULT 1,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  closed_at   timestamptz
);
CREATE INDEX IF NOT EXISTS runs_status ON runs (status, updated_at);
CREATE INDEX IF NOT EXISTS runs_closed ON runs (closed_at) WHERE closed_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS events (
  run_id    uuid   NOT NULL,
  seq       bigint NOT NULL,
  type      text   NOT NULL,
  call_seq  int,
  payload   jsonb  NOT NULL DEFAULT '{}',
  at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_completion
  ON events (run_id, call_seq) WHERE type = 'StepCompleted';

CREATE TABLE IF NOT EXISTS tasks (
  task_id     uuid PRIMARY KEY,
  run_id      uuid NOT NULL,
  shard       smallint NOT NULL,
  kind        text NOT NULL,          -- ADVANCE | STEP
  workflow    text NOT NULL,
  version     int  NOT NULL,
  call_seq    int,
  step_name   text,
  args        jsonb,
  retry       jsonb,
  timeout_s   real NOT NULL DEFAULT 30,
  attempt     int  NOT NULL DEFAULT 1,
  not_before  timestamptz NOT NULL DEFAULT now(),
  created_at  timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_token uuid,
  lease_until timestamptz
);
-- Routing key: one text column, so the claim can walk tasks_ready in not_before order with a
-- plain filter. Matching on (workflow, version) pairs via a join made Postgres sort every
-- ready task on every claim, which collapses under a backlog.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS queue text
  GENERATED ALWAYS AS (workflow || ':' || version::text) STORED;
-- The queue table churns (insert, claim, delete): vacuum and analyze it early and often.
ALTER TABLE tasks SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_analyze_scale_factor = 0.01,
  autovacuum_vacuum_cost_delay = 0);
CREATE INDEX IF NOT EXISTS tasks_ready ON tasks (not_before) WHERE lease_owner IS NULL;
CREATE INDEX IF NOT EXISTS tasks_leased ON tasks (lease_until) WHERE lease_owner IS NOT NULL;
CREATE INDEX IF NOT EXISTS tasks_run ON tasks (run_id);
-- At most one ADVANCE waiting per run: a burst of events wakes the run once.
CREATE UNIQUE INDEX IF NOT EXISTS one_pending_advance
  ON tasks (run_id) WHERE kind = 'ADVANCE' AND lease_owner IS NULL;

CREATE TABLE IF NOT EXISTS timers (
  timer_id  uuid PRIMARY KEY,
  run_id    uuid NOT NULL,
  call_seq  int  NOT NULL,
  fire_at   timestamptz NOT NULL,
  state     text NOT NULL DEFAULT 'pending'   -- pending|fired|cancelled
);
CREATE INDEX IF NOT EXISTS timers_due ON timers (fire_at) WHERE state = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS timers_call ON timers (run_id, call_seq);

CREATE TABLE IF NOT EXISTS signals (
  run_id      uuid NOT NULL,
  name        text NOT NULL,
  payload     jsonb,
  received_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS signals_run ON signals (run_id);
