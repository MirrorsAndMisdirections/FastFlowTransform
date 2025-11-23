.PHONY: demo ci-check run-all run-changed run-changed-demo sarif artifacts clean

# --- Config -------------------------------------------------------------------

DB ?= .local/ci_demo.duckdb
PROJECT ?= .
UV ?= uv

# For the CI demo we keep it simple and default to DuckDB.
ENGINE ?= duckdb

# BigQuery frame type selector (pandas | bigframes)
BQ_FRAME ?= bigframes

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
  ENGINE_TAG = engine:databricks_spark
  PROFILE_ENV = dev_databricks
endif
ifeq ($(ENGINE),bigquery)
  ENGINE_TAG = engine:bigquery
  ifeq ($(BQ_FRAME),pandas)
    PROFILE_ENV = dev_bigquery_pandas
  else
    PROFILE_ENV = dev_bigquery_bigframes
  endif
endif
ifeq ($(ENGINE),snowflake_snowpark)
  PROFILE_ENV = dev_snowflake
  ENGINE_TAG = engine:snowflake_snowpark
endif

BASE_ENV = FFT_ACTIVE_ENV=$(PROFILE_ENV) FF_ENGINE=$(ENGINE)
ifeq ($(ENGINE),bigquery)
  BASE_ENV := $(BASE_ENV) FF_ENGINE_VARIANT=$(BQ_FRAME)
endif

RUN_ENV  = $(BASE_ENV)

SELECT_FLAGS = --select tag:example:ci_demo --select tag:$(ENGINE_TAG)

CLEAN_SCRIPT = ../_scripts/cleanup_env.py

ifeq ($(ENGINE),duckdb)
  CLEAN_CMD = env $(BASE_ENV) $(UV) run python $(CLEAN_SCRIPT) --engine duckdb --env "$(PROFILE_ENV)" --project "$(PROJECT)" --duckdb-path "$(DB)"
else ifeq ($(ENGINE),postgres)
  CLEAN_CMD = env $(BASE_ENV) $(UV) run python $(CLEAN_SCRIPT) --engine postgres --env "$(PROFILE_ENV)" --project "$(PROJECT)"
else ifeq ($(ENGINE),databricks_spark)
  CLEAN_CMD = env $(BASE_ENV) $(UV) run python $(CLEAN_SCRIPT) --engine databricks_spark --env "$(PROFILE_ENV)" --project "$(PROJECT)"
else ifeq ($(ENGINE),bigquery)
  CLEAN_CMD = env $(BASE_ENV) $(UV) run python $(CLEAN_SCRIPT) --engine bigquery --env "$(PROFILE_ENV)" --project "$(PROJECT)"
else ifeq ($(ENGINE),snowflake_snowpark)
  CLEAN_CMD = env $(BASE_ENV) $(UV) run python $(CLEAN_SCRIPT) --engine snowflake_snowpark --env "$(PROFILE_ENV)" --project "$(PROJECT)"
else
  CLEAN_CMD = $(error Unsupported ENGINE=$(ENGINE) for cleanup)
endif

# Git ref for change-aware runs (can override on the command line)
#   make ENGINE=duckdb CHANGED_SINCE=origin/main run-changed
CHANGED_SINCE ?= HEAD~1

# --- Core targets -------------------------------------------------------------

# Static CI check: parses project, builds DAG, validates deps/cycles,
# and shows which nodes would be selected (no DB required).
ci-check:
	env $(BASE_ENV) $(UV) run fft ci-check "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

# Full run of all CI demo models (normal fft run).
run-all:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

# Change-aware run: only models affected by Git changes since CHANGED_SINCE.
run-changed:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" \
	  --env $(PROFILE_ENV) \
	  $(SELECT_FLAGS) \
	  --changed-since "$(CHANGED_SINCE)"

run-changed-demo:
	@echo "== Running only 'changed' models (demo override) =="
	env $(RUN_ENV) \
	  FF_CI_CHANGED_MODELS="fct_events.ff,py_enrich.ff" \
	  $(UV) run fft run "$(PROJECT)" \
	  --env $(PROFILE_ENV) \
	  $(SELECT_FLAGS) \
	  --changed-since "$(CHANGED_SINCE)"

# Emit a SARIF file from the CI check summary, suitable for GitHub Code Scanning.
# Assumes you have examples/ci_demo/scripts/ci_to_sarif.py wired up to run_ci_check + write_sarif.
sarif:
	env $(RUN_ENV) $(UV) run python scripts/ci_to_sarif.py \
	  "$(PROJECT)" \
	  --engine "$(ENGINE)" \
	  --env "$(PROFILE_ENV)" \
	  $(SELECT_FLAGS) \
	  --out ".fastflowtransform/ci/ci_demo.sarif"

artifacts:
	@echo
	@echo "== 📦 CI Demo Artifacts =="
	@echo "  CI summary:   .fastflowtransform/ci/ci_demo.sarif (SARIF)"
	@echo "  Run manifest: .fastflowtransform/target/manifest.json"
	@echo "  Run results:  .fastflowtransform/target/run_results.json"

clean:
	$(CLEAN_CMD)

# One-shot demo: static check, then a change-aware run, then SARIF output.
demo: clean
	@echo "== 🧪 ci_demo (CI & change-aware runs) =="
	@echo "Profile=$(PROFILE_ENV)  ENGINE=$(ENGINE)  PROJECT=$(PROJECT)"
	+$(MAKE) ci-check
	+$(MAKE) run-changed
	+$(MAKE) run-changed-demo
	+$(MAKE) sarif
	+$(MAKE) artifacts
	@echo "✅ CI demo done."
