.PHONY: demo seed run dag test artifacts clean demo-open

# --- Config -------------------------------------------------------------------

PROJECT ?= .
UV ?= uv

# Engine selector (duckdb|postgres|databricks_spark)
ENGINE ?= duckdb

# Detect OS opener (macOS: open, Linux: xdg-open)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	OPENER := open
else
	OPENER := xdg-open
endif

# Map ENGINE -> profile + tag
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
RUN_ENV  = $(BASE_ENV)

# Select only dq_demo models for the active engine
SELECT_FLAGS = --select tag:example:dq_demo --select tag:$(ENGINE_TAG)

# --- Core targets -------------------------------------------------------------

seed:
	env $(BASE_ENV) $(UV) run fft seed "$(PROJECT)" --env $(PROFILE_ENV)

run:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

test:
	env $(BASE_ENV) $(UV) run fft test "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

dag:
	env $(RUN_ENV) $(UV) run fft dag "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS) --html

artifacts:
	@echo
	@echo "== 📦 Artifacts =="
	@echo "  $(PROJECT)/.fastflowtransform/target/{manifest.json,run_results.json,catalog.json}"
	@echo "  DAG HTML: $(PROJECT)/site/dag/index.html"

clean:
	rm -rf "$(PROJECT)/.local" "$(PROJECT)/.fastflowtransform" "$(PROJECT)/site"

demo-open:
	@if [ -f "$(PROJECT)/site/dag/index.html" ]; then \
		$(OPENER) "$(PROJECT)/site/dag/index.html" 2>/dev/null || echo "Open manually at: $(PROJECT)/site/dag/index.html"; \
	else \
		echo "No HTML found: $(PROJECT)/site/dag/index.html"; \
	fi

demo: clean
	@echo "== 🧪 dq_demo (Data Quality) =="
	@echo "Profile=$(PROFILE_ENV)  PROJECT=$(PROJECT)"
	+$(MAKE) seed
	+$(MAKE) run
	+$(MAKE) dag
	+$(MAKE) test
	+$(MAKE) artifacts
	@echo "✅ Demo done. Open DAG here: $(PROJECT)/site/dag/index.html"
