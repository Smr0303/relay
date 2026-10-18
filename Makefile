.PHONY: setup up down test lint bench-replay

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
