.PHONY: seed run_full run_incr dag test artifacts clean demo demo-open

# --- Config -------------------------------------------------------------------

DB ?= .local/incremental_demo.duckdb
PROJECT ?= .
UV ?= uv

# Engine selector (duckdb|postgres|databricks_spark|bigquery|snowflake_snowpark)
ENGINE ?= duckdb

# BigQuery frame type selector (pandas | bigframes)
BQ_FRAME ?= bigframes

# For Databricks Spark: control table format (parquet|delta|iceberg)
DBR_TABLE_FORMAT ?= parquet

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
  # Choose profile based on table format so we can have separate configs:
  # - dev_databricks_delta   (Delta Lake)
  # - dev_databricks_iceberg (Iceberg)
  # - dev_databricks         (generic / parquet)
  ifeq ($(DBR_TABLE_FORMAT),delta)
    PROFILE_ENV = dev_databricks_delta
  else ifeq ($(DBR_TABLE_FORMAT),iceberg)
    PROFILE_ENV = dev_databricks_iceberg
  else
    PROFILE_ENV = dev_databricks_delta
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
ifeq ($(ENGINE),databricks_spark)
  BASE_ENV := $(BASE_ENV) FF_DBR_TABLE_FORMAT=$(DBR_TABLE_FORMAT)
endif
ifeq ($(ENGINE),bigquery)
  BASE_ENV := $(BASE_ENV) FF_ENGINE_VARIANT=$(BQ_FRAME)
endif

RUN_ENV = $(BASE_ENV)

SELECT_FLAGS = --select tag:example:incremental_demo --select tag:$(ENGINE_TAG)

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

# --- Targets ------------------------------------------------------------------

seed:
	env $(BASE_ENV) $(UV) run fft seed "$(PROJECT)" --env $(PROFILE_ENV)

# Full refresh (first run)
run_full:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS) --cache rw

# second/subsequent run: shows incremental/delta behaviour
run_incr:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS) --cache rw

dag:
	env $(RUN_ENV) $(UV) run fft dag "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS) --html

test:
	env $(BASE_ENV) $(UV) run fft test "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

artifacts:
	@echo
	@echo "== 📦 Artifacts =="
	@echo "  $(PROJECT)/.fastflowtransform/target/{manifest.json,run_results.json,catalog.json}"
	@echo "  DAG HTML: $(PROJECT)/site/dag/index.html"

clean:
	$(CLEAN_CMD)

demo-open:
	@if [ -f "$(PROJECT)/site/dag/index.html" ]; then \
		$(OPENER) "$(PROJECT)/site/dag/index.html" 2>/dev/null || echo "Open manually at: $(PROJECT)/site/dag/index.html"; \
	else \
		echo "No HTML found: $(PROJECT)/site/dag/index.html"; \
	fi

demo: clean
	@echo "== 🚀 Incremental Demo ($(ENGINE)) =="
	@echo "Profile=$(PROFILE_ENV)  DB=$(DB)  PROJECT=$(PROJECT)  DBR_TABLE_FORMAT=$(DBR_TABLE_FORMAT)"
	+$(MAKE) seed
	+$(MAKE) run_full
	@echo
	@echo "== 🔁 Second run (Incremental/Delta) =="
	+$(MAKE) run_incr
	+$(MAKE) dag
	+$(MAKE) test
	+$(MAKE) artifacts
	@echo "✅ Demo done. Open DAG here: $(PROJECT)/site/dag/index.html"
