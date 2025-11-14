.PHONY: seed run run_parallel cache_first cache_second \
        change_sql change_seed change_env change_py \
        http_first http_offline http_cache_clear artifacts dag clean \
		demo

ENGINE ?= duckdb
PROFILE_ENV = dev_duckdb
PROJECT ?= .
UV ?= uv

BASE_ENV = FFT_ACTIVE_ENV=$(PROFILE_ENV)
RUN_ENV  = $(BASE_ENV)

SELECT_ALL = --select tag:example:cache_demo

seed:
	env $(BASE_ENV) $(UV) run fft seed "$(PROJECT)" --env $(PROFILE_ENV)

run:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_ALL) --cache=rw

run_parallel:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_ALL) --cache=rw --jobs 4

cache_first: seed run
cache_second: run

change_sql:
	# Touch SQL to change rendered output → downstream mart rebuilds
	touch "$(PROJECT)/models/seeds_consumers/stg_users.ff.sql"
	+$(MAKE) run

change_seed:
	# Modify seed → staging and mart rebuild
	@printf "\n4,dan@example.com\n" >> "$(PROJECT)/seeds/seed_users.csv"
	+$(MAKE) seed
	+$(MAKE) run

change_env:
	# FF_* env affects fingerprint → everything rebuilds
	env $(RUN_ENV) FF_DEMO_TOGGLE=1 $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_ALL) --cache=rw

change_py:
	# Edit the constant in py_constants.ff.py to 43 manually, then:
	+$(MAKE) run

http_first:
	env $(RUN_ENV) $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) --select http_users --cache=rw

http_offline:
	# Works only if http_first warmed cache
	env $(RUN_ENV) FF_HTTP_OFFLINE=1 $(UV) run fft run "$(PROJECT)" --env $(PROFILE_ENV) --select http_users --cache=rw

http_cache_clear:
	rm -rf ".local/http-cache"

artifacts:
	@echo ".fastflowtransform/target/{manifest.json,run_results.json,catalog.json}"

dag:
	env $(RUN_ENV) $(UV) run fft dag "$(PROJECT)" --env $(PROFILE_ENV) $(SELECT_ALL) --html

clean:
	rm -rf .local cache_demo.duckdb site .fastflowtransform

demo: clean
	@echo "== 🚀 Cache Demo ($(ENGINE)) =="
	@echo "== 1) First full build (writes cache) =="
	+$(MAKE) cache_first
	@echo
	@echo "== 2) No-op run (should skip everything) =="
	+$(MAKE) cache_second
	@echo
	@echo "== 3) Parallel run (visualize level-wise concurrency) =="
	+$(MAKE) run_parallel
	@echo
	@echo "== 4) Touch SQL model (downstream mart should rebuild) =="
	+$(MAKE) change_sql
	+$(MAKE) cache_second
	@echo
	@echo "== 5) Change seed data (staging + mart should rebuild) =="
	+$(MAKE) change_seed
	+$(MAKE) cache_second
	@echo
	@echo "== 6) Change FF_* env (global cache invalidation) =="
	+$(MAKE) change_env
	+$(MAKE) cache_second
	@echo
	@echo "== 7) Edit Python model (only that one should rebuild) =="
	+$(MAKE) change_py
	+$(MAKE) cache_second
	@echo
	@echo "== 8) Warm HTTP cache & run offline =="
	+$(MAKE) http_first
	+$(MAKE) http_offline
	@echo
	@echo "== 9) DAG & artifacts =="
	+$(MAKE) dag
	+$(MAKE) artifacts
	@echo
	@echo "✅ Demo complete. Open DAG at: $(PROJECT)/site/dag/index.html"
