.PHONY: seed-dev seed-stg seed-prod run-dev run-stg run-prod dag-dev clean \
        demo-dev demo-stg demo-prod demo artifacts

PROJECT ?= .
UV ?= uv

seed-dev:
	$(UV) run fft seed "$(PROJECT)" --env dev

seed-stg:
	$(UV) run fft seed "$(PROJECT)" --env stg

seed-prod:
	$(UV) run fft seed "$(PROJECT)" --env prod

run-dev:
	$(UV) run fft run "$(PROJECT)" --env dev

run-stg:
	$(UV) run fft run "$(PROJECT)" --env stg

run-prod:
	$(UV) run fft run "$(PROJECT)" --env prod

dag-dev:
	$(UV) run fft dag "$(PROJECT)" --env dev --html

dag-stg:
	$(UV) run fft dag "$(PROJECT)" --env stg --html

dag-prod:
	$(UV) run fft dag "$(PROJECT)" --env prod --html

test-dev:
	$(UV) run fft test $(PROJECT) --env dev

test-stg:
	$(UV) run fft test $(PROJECT) --env stg

test-prod:
	$(UV) run fft test $(PROJECT) --env prod

clean:
	rm -rf .local "$(PROJECT)/docs" "$(PROJECT)/site" .fastflowtransform

# --- Convenience: show common artifacts path quickly
artifacts:
	@echo
	@echo "== 📦 Artifacts =="
	@echo "  $(PROJECT)/.fastflowtransform/target/{manifest.json,run_results.json,catalog.json}"
	@echo "  DAG HTML (if generated): $(PROJECT)/site/dag/index.html"

# --- One-shot demos per environment -----------------------------------------

demo-dev: clean seed-dev run-dev test-dev dag-dev artifacts
	@echo
	@echo "✅ Demo (dev) complete. Open DAG here (if generated): $(PROJECT)/site/dag/index.html"

demo-stg: clean seed-stg run-stg test-stg dag-stg artifacts
	@echo
	@echo "✅ Demo (stg) complete."

demo-prod: clean seed-prod run-prod test-prod dag-prod artifacts
	@echo
	@echo "✅ Demo (prod) complete."

# --- Generic demo with ENV selector (dev|stg|prod) ---------------------------

ENV ?= dev

demo:
	@echo "== 🚀 Demo ENV=$(ENV) PROJECT=$(PROJECT) =="
	@case "$(ENV)" in \
		dev)  $(MAKE) demo-dev  ;; \
		stg)  $(MAKE) demo-stg  ;; \
		prod) $(MAKE) demo-prod ;; \
		*) echo "Unknown ENV=$(ENV). Use dev|stg|prod."; exit 1 ;; \
	esac
