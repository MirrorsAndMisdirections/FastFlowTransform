.PHONY: demo seed run dag test artifacts incr state-mod state-mod-plus res-error res-warn \
        clean demo-open \
        api-run api-offline api-warm api-cache-clear api-show-http api-demo

# --- Config -------------------------------------------------------------------

# DuckDB database file and project path (for the API demo typically examples/api_demo)
DB ?= .local/demo.duckdb
PROJECT ?= .
UV ?= uv

# HTTP wrapper defaults (override per call if needed)
# Allowed domains are comma-separated (no https://)
FF_HTTP_ALLOWED_DOMAINS ?= jsonplaceholder.typicode.com,api.github.com
FF_HTTP_CACHE_DIR ?= .local/http-cache
FF_HTTP_MAX_RPS ?= 5
FF_HTTP_MAX_RETRIES ?= 3
FF_HTTP_TIMEOUT ?= 20

# Detect OS opener (macOS: open, Linux: xdg-open)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	OPENER := open
else
	OPENER := xdg-open
endif

# --- Shared env for all runs (DuckDB + HTTP) ---------------------------------
RUN_ENV = FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" \
          FF_HTTP_ALLOWED_DOMAINS="$(FF_HTTP_ALLOWED_DOMAINS)" \
          FF_HTTP_CACHE_DIR="$(FF_HTTP_CACHE_DIR)" \
          FF_HTTP_MAX_RPS="$(FF_HTTP_MAX_RPS)" \
          FF_HTTP_MAX_RETRIES="$(FF_HTTP_MAX_RETRIES)" \
          FF_HTTP_TIMEOUT="$(FF_HTTP_TIMEOUT)"

# Engine env
ifeq ($(ENGINE),duckdb)
  ENGINE_ENV = FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)"
  ENGINE_TAG = engine:duckdb
endif
ifeq ($(ENGINE),postgres)
  ENGINE_ENV = FF_ENGINE=postgres FF_PG_DSN="$(FF_PG_DSN)" FF_PG_SCHEMA="$(FF_PG_SCHEMA)"
  ENGINE_TAG = engine:postgres
endif
ifeq ($(ENGINE),databricks_spark)
  ENGINE_ENV = FF_ENGINE=databricks_spark FF_SPARK_MASTER="$(FF_SPARK_MASTER)"
  ENGINE_TAG = engine:databricks_spark
endif

# Select only common + this engine; keeps DAG clean and avoids executing foreign variants
SELECT = tag:example:api_demo,tag:scope:common,tag:$(ENGINE_TAG)

# --- Standard R1 targets ------------------------------------------------------

seed:
	$(ENGINE_ENV) $(UV) run fft seed "$(PROJECT)" --env dev

run:
	$(ENGINE_ENV) $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env dev

test:
	$(ENGINE_ENV) $(UV) run fft test "$(PROJECT)" --env dev

dag:
	$(ENGINE_ENV) $(UV) run fft dag  "$(PROJECT)" --env dev --html

artifacts:
	@echo
	@echo "== 📦 Artifacts =="
	@echo "  $(PROJECT)/.fastflowtransform/target/{manifest.json,run_results.json,catalog.json}"
	@echo "  DAG HTML: $(PROJECT)/site/dag/index.html"

incr:
	$(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env dev --select fct_events_inc.ff --cache rw || true

state-mod:
	@if [ -f "$(PROJECT)/models/users.ff.sql" ]; then touch "$(PROJECT)/models/users.ff.sql"; fi
	$(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env dev --cache rw --select state:modified

state-mod-plus:
	$(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env dev --cache rw --select state:modified+

res-error:
	$(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env dev --select result:error || true

res-warn:
	$(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env dev --select result:warn  || true

pg-seed:
	FF_ENGINE=postgres FF_PG_DSN="$(FF_PG_DSN)" FF_PG_SCHEMA="$(FF_PG_SCHEMA)" $(UV) run fft seed "$(PROJECT)" --env stg

pg-run:
	FF_ENGINE=postgres FF_PG_DSN="$(FF_PG_DSN)" FF_PG_SCHEMA="$(FF_PG_SCHEMA)" \
	FF_HTTP_ALLOWED_DOMAINS="$(FF_HTTP_ALLOWED_DOMAINS)" \
	FF_HTTP_CACHE_DIR="$(FF_HTTP_CACHE_DIR)" \
	FF_HTTP_MAX_RPS="$(FF_HTTP_MAX_RPS)" \
	FF_HTTP_MAX_RETRIES="$(FF_HTTP_MAX_RETRIES)" \
	FF_HTTP_TIMEOUT="$(FF_HTTP_TIMEOUT)" \
	$(UV) run fft run  "$(PROJECT)" --env stg

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

# --- API-specific convenience targets ----------------------------------------

api-run:
	$(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env dev --select "kind:python" --cache rw

api-warm:
	+$(MAKE) api-run

api-offline:
	$(RUN_ENV) FF_HTTP_OFFLINE=1 $(UV) run fft run "$(PROJECT)" --env dev --select "kind:python" --cache rw

api-cache-clear:
	rm -rf "$(FF_HTTP_CACHE_DIR)"

api-show-http:
	@if command -v jq >/dev/null 2>&1; then \
	  echo "== HTTP snapshots from run_results.json =="; \
	  jq -r '.results[] | select(.http!=null) | "\(.name): requests=\(.http.requests) cache_hits=\(.http.cache_hits) bytes=\(.http.bytes) offline=\(.http.used_offline)"' \
	    "$(PROJECT)/.fastflowtransform/target/run_results.json" || true; \
	else \
	  echo "Install 'jq' to pretty-print HTTP snapshots from run_results.json."; \
	fi

api-demo: clean
	@echo "== 🌐 API Demo (DuckDB) =="
	@echo "DB=$(DB)  PROJECT=$(PROJECT)"
	+$(MAKE) run
	+$(MAKE) dag
	+$(MAKE) api-show-http || true
	@echo "✅ API Demo done. Open DAG here: $(PROJECT)/site/dag/index.html"
	+$(MAKE) demo-open
