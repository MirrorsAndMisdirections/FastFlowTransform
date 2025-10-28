.PHONY: demo seed run dag test artifacts incr state-mod state-mod-plus res-error res-warn clean

DB ?= .local/demo.duckdb
PROJECT ?= .
UV ?= uv

# Detect OS opener (macOS: open, Linux: xdg-open)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	OPENER := open
else
	OPENER := xdg-open
endif

seed:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" $(UV) run fft seed "$(PROJECT)" --env dev

run:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" $(UV) run fft run "$(PROJECT)" --env dev

test:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" $(UV) run fft test "$(PROJECT)" --env dev

dag:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" $(UV) run fft dag  "$(PROJECT)" --env dev --html

artifacts:
	@echo
	@echo "== 📦 Artifacts =="
	@echo "  $(PROJECT)/.fastflowtransform/target/{manifest.json,run_results.json,catalog.json}"
	@echo "  DAG HTML: $(PROJECT)/site/dag/index.html"

incr:
	$(UV) run fft run "$(PROJECT)" --env dev --select fct_events_inc.ff --cache rw || true

state-mod:
	@if [ -f "$(PROJECT)/models/users.ff.sql" ]; then touch "$(PROJECT)/models/users.ff.sql"; fi
	FF_ENGINE=duckdb $(UV) run fft run "$(PROJECT)" --env dev --cache rw --select state:modified

state-mod-plus:
	FF_ENGINE=duckdb $(UV) run fft run "$(PROJECT)" --env dev --cache rw --select state:modified+

res-error:
	FF_ENGINE=duckdb $(UV) run fft run "$(PROJECT)" --env dev --select result:error || true

res-warn:
	FF_ENGINE=duckdb $(UV) run fft run "$(PROJECT)" --env dev --select result:warn  || true

pg-seed:
	FF_ENGINE=postgres FF_PG_DSN="$(FF_PG_DSN)" FF_PG_SCHEMA="$(FF_PG_SCHEMA)" $(UV) run fft seed "$(PROJECT)" --env stg

pg-run:
	FF_ENGINE=postgres FF_PG_DSN="$(FF_PG_DSN)" FF_PG_SCHEMA="$(FF_PG_SCHEMA)" $(UV) run fft run  "$(PROJECT)" --env stg

clean:
	rm -rf .local "$(PROJECT)/docs" dist build *.egg-info .fastflowtransform

demo-open:
	@if [ -f "$(PROJECT)/site/dag/index.html" ]; then \
		$(OPENER) "$(PROJECT)/site/dag/index.html" 2>/dev/null || echo "Open manually at: $(PROJECT)/site/dag/index.html"; \
	else \
		echo "No HTML found: $(PROJECT)/site/dag/index.html"; \
	fi

demo: clean
	@echo "== 🚀 R1 Demo (DuckDB) =="
	@echo "DB=$(DB)  PROJECT=$(PROJECT)"
	+$(MAKE) seed
	+$(MAKE) run
	+$(MAKE) dag
	+$(MAKE) test
	+$(MAKE) artifacts
	@echo
	@echo "== 🔁 Incremental Model =="
	+$(MAKE) incr
	@echo
	@echo "== 🧠 State Selection (changed only) =="
	+$(MAKE) state-mod
	+$(MAKE) state-mod-plus
	@echo
	@echo "== 🧪 Result Selection (from last run_results.json) =="
	+$(MAKE) res-error
	+$(MAKE) res-warn
	@echo
	@echo "✅ Demo done. Open DAG here: $(PROJECT)/site/dag/index.html"
	+$(MAKE) demo-open
