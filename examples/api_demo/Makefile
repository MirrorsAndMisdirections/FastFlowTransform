.PHONY: demo seed run dag test artifacts incr state-mod state-mod-plus res-error res-warn \
        clean demo-open \
        api-run api-offline api-warm api-cache-clear api-show-http api-demo

# --- Config -------------------------------------------------------------------

# DuckDB database file and project path (for the API demo typically examples/api_demo)
DB ?= .local/api_demo.duckdb
PROJECT ?= .
UV ?= uv

# Engine selector (duckdb|postgres|databricks_spark)
ENGINE ?= duckdb

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


ifeq ($(ENGINE),duckdb)
  PROFILE_ENV = dev_duckdb
  ENGINE_TAG = engine:duckdb
endif
ifeq ($(ENGINE),postgres)
  PROFILE_ENV = dev_postgres
  ENGINE_TAG = engine:postgres
endif
ifeq ($(ENGINE),databricks_spark)
  PROFILE_ENV = dev_databricks
  ENGINE_TAG = engine:databricks_spark
endif


BASE_ENV = FFT_ACTIVE_ENV=$(PROFILE_ENV)
RUN_ENV = $(BASE_ENV)

# Select only API demo models for the active engine (common models carry all engine tags)
SELECT_FLAGS = --select tag:example:api_demo --select tag:$(ENGINE_TAG)

CLEAN_SCRIPT = ../_scripts/cleanup_env.py

ifeq ($(ENGINE),duckdb)
  CLEAN_CMD = env $(BASE_ENV) $(UV) run python $(CLEAN_SCRIPT) --engine duckdb --env "$(PROFILE_ENV)" --project "$(PROJECT)" --duckdb-path "$(DB)"
else ifeq ($(ENGINE),postgres)
  CLEAN_CMD = env $(BASE_ENV) $(UV) run python $(CLEAN_SCRIPT) --engine postgres --env "$(PROFILE_ENV)" --project "$(PROJECT)"
else ifeq ($(ENGINE),databricks_spark)
  CLEAN_CMD = env $(BASE_ENV) $(UV) run python $(CLEAN_SCRIPT) --engine databricks_spark --env "$(PROFILE_ENV)" --project "$(PROJECT)"
else
  CLEAN_CMD = $(error Unsupported ENGINE=$(ENGINE) for cleanup)
endif

# --- Standard R1 targets ------------------------------------------------------

seed:
	env $(BASE_ENV) $(UV) run fft seed "$(PROJECT)" --env $(PROFILE_ENV)

run:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

test:
	env $(BASE_ENV) $(UV) run fft test "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

dag:
	env $(RUN_ENV) $(UV) run fft dag  "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS) --html

artifacts:
	@echo
	@echo "== 📦 Artifacts =="
	@echo "  $(PROJECT)/.fastflowtransform/target/{manifest.json,run_results.json,catalog.json}"
	@echo "  DAG HTML: $(PROJECT)/site/dag/index.html"

incr:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) --select fct_events_inc.ff --cache rw || true

state-mod:
	@if [ -f "$(PROJECT)/models/users.ff.sql" ]; then touch "$(PROJECT)/models/users.ff.sql"; fi
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) --cache rw --select state:modified

state-mod-plus:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) --cache rw --select state:modified+

res-error:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) --select result:error || true

res-warn:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) --select result:warn  || true

clean:
	$(CLEAN_CMD)

demo-open:
	@if [ -f "$(PROJECT)/site/dag/index.html" ]; then \
		$(OPENER) "$(PROJECT)/site/dag/index.html" 2>/dev/null || echo "Open manually at: $(PROJECT)/site/dag/index.html"; \
	else \
		echo "No HTML found: $(PROJECT)/site/dag/index.html"; \
	fi

demo: clean
	@echo "== 🚀 R1 Demo (DuckDB) =="
	@echo "Profile=$(PROFILE_ENV)  DB=$(DB)  PROJECT=$(PROJECT)"
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
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) --select "kind:python" --cache rw

api-warm:
	+$(MAKE) api-run

api-offline:
	env $(RUN_ENV) FF_HTTP_OFFLINE=1 $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) --select "kind:python" --cache rw

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
	@echo "Profile=$(PROFILE_ENV)  DB=$(DB)  PROJECT=$(PROJECT)"
	+$(MAKE) run
	+$(MAKE) dag
	+$(MAKE) api-show-http || true
	@echo "✅ API Demo done. Open DAG here: $(PROJECT)/site/dag/index.html"
	+$(MAKE) demo-open
