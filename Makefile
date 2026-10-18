.PHONY: setup up down test lint bench-replay chaos bench timer-soak

setup:
	uv sync

up:
	docker compose up -d --wait

down:
	docker compose down

test:
	uv run pytest -q

lint:
	uv run ruff check .
	uv run ruff format --check .

bench-replay:
	uv run python -m bench.replay_micro

chaos:
	uv run python -m tests.chaos.chaos --orders 1000 --lectures 200

bench:
	uv run python -m bench.load --rate 120 --steps 10 --duration 600 --servers 6 --workers 6

timer-soak:
	uv run python -m bench.timer_soak --runs 100000 --window 60
