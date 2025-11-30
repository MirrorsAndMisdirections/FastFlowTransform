.PHONY: seed run test dag show artifacts clean demo help

# --- Configuration -----------------------------------------------------------

DB ?= .local/packages_demo.duckdb
PROJECT ?= .
UV ?= uv

# Only DuckDB is wired for this demo
ENGINE ?= duckdb

PROFILE_ENV = dev_duckdb
ENGINE_TAG  = engine:duckdb

BASE_ENV = FFT_ACTIVE_ENV=$(PROFILE_ENV) FF_ENGINE=$(ENGINE)
RUN_ENV  = $(BASE_ENV)

SELECT_FLAGS = --select tag:example:packages_demo --select tag:$(ENGINE_TAG)

SHOW_MODEL ?= mart_users_from_package

# Cleanup script path (relative to main_project/)
CLEAN_SCRIPT = ../../_scripts/cleanup_env.py

CLEAN_CMD = env $(BASE_ENV) $(UV) run python $(CLEAN_SCRIPT) \
	--engine duckdb --env "$(PROFILE_ENV)" --project "$(PROJECT)" \
	--duckdb-path "$(DB)"

# --- Targets ----------------------------------------------------------------

help:
	@echo "FastFlowTransform Packages Demo"
	@echo "Targets:"
	@echo "  make seed       ENGINE=$(ENGINE)"
	@echo "  make run        ENGINE=$(ENGINE)"
	@echo "  make dag        ENGINE=$(ENGINE)"
	@echo "  make test       ENGINE=$(ENGINE)"
	@echo "  make show       ENGINE=$(ENGINE) SHOW_MODEL=$(SHOW_MODEL)"
	@echo "  make demo       ENGINE=$(ENGINE)"
	@echo "  make clean      ENGINE=$(ENGINE)"
	@echo
	@echo "Variables: DB=$(DB) PROJECT=$(PROJECT) UV=$(UV) ENGINE=$(ENGINE)"

seed:
	env $(BASE_ENV) $(UV) run fft seed "$(PROJECT)" --env $(PROFILE_ENV)

run:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

test:
	env $(BASE_ENV) $(UV) run fft test "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS)

dag:
	env $(RUN_ENV) $(UV) run fft dag "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_FLAGS) --html

deps:
	env $(BASE_ENV) $(UV) run fft deps "$(PROJECT)"

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
	@echo "== 🚀 Packages Demo ($(ENGINE)) =="
	@echo "Profile=$(PROFILE_ENV)  PROJECT=$(PROJECT)"
	+$(MAKE) seed ENGINE=$(ENGINE)
	+$(MAKE) run ENGINE=$(ENGINE)
	+$(MAKE) dag ENGINE=$(ENGINE)
	+$(MAKE) test ENGINE=$(ENGINE)
	+$(MAKE) deps ENGINE=$(ENGINE)
	+$(MAKE) artifacts
	@echo "✅ Demo complete."
