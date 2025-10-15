# Project Root Makefile
# Robust: same DB for seed/run/dag/test, CLI fallback, path-safe

SHELL := /bin/bash

.PHONY: install dev-venv seed run dag test demo demo-open clean ci test-pg-batch unittest utest fmt lint fix type

# Defaults (per CLI überschreibbar): make PROJECT=examples/postgres ENV=stg
PROJECT ?= examples/simple_duckdb
DB ?= $(PROJECT)/.local/demo.duckdb
ENV ?= dev

# Detect OS opener (macOS: open, Linux: xdg-open)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	OPENER := open
else
	OPENER := xdg-open
endif

install:
	python -m pip install --upgrade pip
	pip install -e .

# Optional: lokale venv anlegen
dev-venv:
	python -m venv .venv && . .venv/bin/activate && python -m pip install --upgrade pip && pip install -e .

seed:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge seed "$(PROJECT)" --env dev

# Run/DAG/Test: immer denselben DB-Pfad setzen (ENV kann Engine überschreiben)
run:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge run "$(PROJECT)" --env "$(ENV)"

dag:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge dag "$(PROJECT)" --env "$(ENV)" --html

# Öffnet die generierte HTML-Datei (macOS/Linux). Unter Windows bitte manuell öffnen.
demo-open:
	@if [ -f "$(PROJECT)/docs/index.html" ]; then \
		$(OPENER) "$(PROJECT)/docs/index.html" 2>/dev/null || echo "Öffne manuell: $(PROJECT)/docs/index.html"; \
	else \
		echo "Keine HTML-Datei gefunden: $(PROJECT)/docs/index.html"; \
	fi

test:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge test "$(PROJECT)" --env "$(ENV)" --select batch

# Komplettablauf: Seed → Run → DAG → Open → Tests
demo: seed run dag demo-open test
	@echo "\n✓ Demo fertig: Tabellen gebaut, DAG generiert, Tests gelaufen."

clean:
	rm -rf .local "$(PROJECT)/docs" dist build *.egg-info

# Schnelles CI-Lokal (für mehr siehe GitHub Actions)
ci:
	pytest -q

test-pg-batch:
	FLOWFORGE_SQL_DEBUG=1 uv run pytest -q tests/test_smoke_postgres.py::test_pg_batch_tests_green

unittest:
	FLOWFORGE_SQL_DEBUG=1 uv run pytest -q tests
	
utest-duckdb:
	flowforge utest examples/simple_duckdb --env dev --model users_enriched

# Lint & Format
fmt:
	ruff format .

lint:
	ruff check .

fix:
	ruff check . --fix
	ruff format .

type:
	mypy