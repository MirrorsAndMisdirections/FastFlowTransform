.PHONY: seed run snapshot test dag show artifacts clean demo help

# --- Configuration -----------------------------------------------------------

DB ?= .local/snapshot_demo.duckdb
PROJECT ?= .
UV ?= uv

# Engine selector (duckdb|postgres|databricks_spark|bigquery)
ENGINE ?= duckdb

# BigQuery frame type selector (pandas | bigframes)
BQ_FRAME ?= bigframes

# Databricks table format (parquet | delta | iceberg)
DBR_TABLE_FORMAT ?= parquet

# Resolve profile and tags per engine
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
  ifeq ($(DBR_TABLE_FORMAT),delta)
    PROFILE_ENV = dev_databricks_delta
  else ifeq ($(DBR_TABLE_FORMAT),iceberg)
    PROFILE_ENV = dev_databricks_iceberg
  else
    PROFILE_ENV = dev_databricks_parquet
  endif
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
ifeq ($(ENGINE),databricks_spark)
  BASE_ENV := $(BASE_ENV) FF_DBR_TABLE_FORMAT=$(DBR_TABLE_FORMAT)
endif

RUN_ENV = $(BASE_ENV)

# Select only models belonging to this example + the active engine
SELECT_FLAGS = --select tag:example:snapshot_demo --select tag:$(ENGINE_TAG)

SHOW_MODEL ?= mart_users_by_domain

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
  $(error Unsupported ENGINE=$(ENGINE) - pick duckdb|postgres|databricks_spark|bigquery|snowflake_snowpark)
endif

# --- Targets ----------------------------------------------------------------

help:
	@echo "FastFlowTransform Snapshot Demo"
	@echo "Targets:"
	@echo "  make seed       ENGINE=$(ENGINE)"
	@echo "  make run        ENGINE=$(ENGINE)    # staging + marts"
	@echo "  make snapshot   ENGINE=$(ENGINE)    # snapshot models (materialized='snapshot')"
	@echo "  make dag        ENGINE=$(ENGINE)"
	@echo "  make test       ENGINE=$(ENGINE)"
	@echo "  make show       ENGINE=$(ENGINE) SHOW_MODEL=$(SHOW_MODEL)"
	@echo "  make demo       ENGINE=$(ENGINE)"
	@echo "  make clean      ENGINE=$(ENGINE)"
	@echo
	@echo "Variables: DB=$(DB) PROJECT=$(PROJECT) UV=$(UV) ENGINE=$(ENGINE) BQ_FRAME=$(BQ_FRAME) DBR_TABLE_FORMAT=$(DBR_TABLE_FORMAT)"

seed:
	env $(BASE_ENV) $(UV) run fft seed "$(PROJECT)" --env $(PROFILE_ENV)

run:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

snapshot:
	env $(RUN_ENV) $(UV) run fft snapshot run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

test:
	env $(BASE_ENV) $(UV) run fft test "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

dag:
	env $(RUN_ENV) $(UV) run fft dag "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS) --html

show:
	@if [ -f "$(PROJECT)/site/dag/index.html" ]; then \
		$(OPENER) "$(PROJECT)/site/dag/index.html" 2>/dev/null || echo "Open manually at: $(PROJECT)/site/dag/index.html"; \
	else \
		echo "No HTML found: $(PROJECT)/site/dag/index.html"; \
	fi

artifacts:
	@echo
	@echo "== 📦 Artifacts =="
	@echo "  $(PROJECT)/.fastflowtransform/target/{manifest.json,run_results.json,catalog.json}"
	@echo "  DAG HTML: $(PROJECT)/site/dag/index.html"

clean:
	$(CLEAN_CMD)

demo: clean
	@echo "== 🚀 Snapshot Demo ($(ENGINE)) =="
	@echo "Profile=$(PROFILE_ENV)  PROJECT=$(PROJECT)  DBR_TABLE_FORMAT=$(DBR_TABLE_FORMAT)"
	+$(MAKE) seed ENGINE=$(ENGINE)
	+$(MAKE) run ENGINE=$(ENGINE)
	+$(MAKE) snapshot ENGINE=$(ENGINE)
	+$(MAKE) dag ENGINE=$(ENGINE)
	+$(MAKE) test ENGINE=$(ENGINE)
	+$(MAKE) artifacts
	@echo "✅ Demo complete."
